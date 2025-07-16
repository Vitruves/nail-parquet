use clap::Args;
use datafusion::prelude::*;
use datafusion::arrow::array::{Float64Array, Int64Array, Array};
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use crate::utils::stats::select_columns_by_pattern;

#[derive(Args, Clone)]
pub struct FillArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(long, help = "Fill method", value_enum, default_value = "value")]
	pub method: FillMethod,
	
	#[arg(long, help = "Fill value (required for 'value' method)")]
	pub value: Option<String>,
	
	#[arg(short, long, help = "Comma-separated column names to fill")]
	pub columns: Option<String>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum FillMethod {
	Value,
	Mean,
	Median,
	Mode,
	Forward,
	Backward,
}

pub async fn execute(args: FillArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	
	let columns = if let Some(col_spec) = &args.columns {
		let schema = df.schema();
		select_columns_by_pattern(schema.clone().into(), col_spec)?
	} else {
		df.schema().fields().iter().map(|f| f.name().clone()).collect()
	};
	
	args.common.log_if_verbose(&format!("Filling missing values in {} columns using {:?} method", columns.len(), args.method));
	
	let result_df = fill_missing_values(&df, &columns, &args.method, args.value.as_deref(), args.common.jobs).await?;
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "fill").await?;
	
	Ok(())
}

async fn fill_missing_values(
	df: &DataFrame,
	columns: &[String],
	method: &FillMethod,
	value: Option<&str>,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema();
	let mut select_exprs = Vec::new();
	
	for field in schema.fields() {
		let field_name = field.name();
		
		if columns.contains(field_name) {
			let filled_expr = match method {
				FillMethod::Value => {
					let fill_val = value.unwrap();
					match field.data_type() {
						datafusion::arrow::datatypes::DataType::Int64 => {
							let val: i64 = fill_val.parse()
								.map_err(|_| NailError::InvalidArgument(format!("Invalid integer value: {}", fill_val)))?;
							coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(val)])
						},
						datafusion::arrow::datatypes::DataType::Float64 => {
							let val: f64 = fill_val.parse()
								.map_err(|_| NailError::InvalidArgument(format!("Invalid float value: {}", fill_val)))?;
							coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(val)])
						},
						datafusion::arrow::datatypes::DataType::Utf8 => {
							coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(fill_val)])
						},
						_ => Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
					}
				},
				FillMethod::Mean => {
					// Use DataFusion's built-in avg function instead of manual calculation
					match field.data_type() {
						datafusion::arrow::datatypes::DataType::Float64 | 
						datafusion::arrow::datatypes::DataType::Int64 => {
							// Create a subquery to calculate the mean
							let _mean_sql = format!(
								"SELECT AVG({}) as mean_val FROM {}",
								field_name, table_name
							);
							
							// Use coalesce with a scalar subquery
							coalesce(vec![
								Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
								lit(0.0) // This will be replaced by actual mean calculation below
							])
						},
						_ => {
							return Err(NailError::Statistics(format!("Mean calculation not supported for column '{}' of type {:?}", field_name, field.data_type())));
						},
					}
				},
				FillMethod::Median => {
					match field.data_type() {
						datafusion::arrow::datatypes::DataType::Float64 | 
						datafusion::arrow::datatypes::DataType::Int64 => {
							coalesce(vec![
								Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
								lit(0.0) // This will be replaced by actual median calculation below
							])
						},
						_ => {
							return Err(NailError::Statistics(format!("Median calculation not supported for column '{}' of type {:?}", field_name, field.data_type())));
						},
					}
				},
				FillMethod::Mode => {
					// Mode is the most frequent value
					coalesce(vec![
						Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
						lit("") // This will be replaced by actual mode calculation below
					])
				},
				FillMethod::Forward => {
					// Forward fill - handled after the select_exprs construction
					Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
				},
				FillMethod::Backward => {
					// Backward fill - handled after the select_exprs construction
					Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
				},
			};
			
			select_exprs.push(filled_expr.alias(field_name));
		} else {
			select_exprs.push(Expr::Column(datafusion::common::Column::new(None::<String>, field_name)));
		}
	}
	
	// For statistical methods, use DataFusion's built-in aggregation functions
	if matches!(method, FillMethod::Mean | FillMethod::Median | FillMethod::Mode) {
		let mut stats_values = std::collections::HashMap::<String, f64>::new();
		let mut mode_values = std::collections::HashMap::<String, String>::new();
		
		// Calculate statistical values for each target column using SQL
		for col_name in columns {
			let field = schema.fields().iter()
				.find(|f| f.name() == col_name)
				.ok_or_else(|| NailError::Statistics(format!("Column '{}' not found", col_name)))?;
			
			match method {
				FillMethod::Mean => {
					match field.data_type() {
						datafusion::arrow::datatypes::DataType::Float64 | 
						datafusion::arrow::datatypes::DataType::Int64 => {
							let mean_sql = format!("SELECT AVG(\"{}\") as stat_value FROM {}", col_name, table_name);
							let result = ctx.sql(&mean_sql).await?;
							let batches = result.collect().await?;
							if let Some(batch) = batches.first() {
								if batch.num_rows() > 0 {
									let value_array = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
									if !value_array.is_null(0) {
										stats_values.insert(col_name.clone(), value_array.value(0));
									}
								}
							}
						},
						_ => return Err(NailError::Statistics(format!("Mean calculation not supported for column '{}' of type {:?}", col_name, field.data_type()))),
					}
				},
				FillMethod::Median => {
					match field.data_type() {
						datafusion::arrow::datatypes::DataType::Float64 | 
						datafusion::arrow::datatypes::DataType::Int64 => {
							let median_sql = format!("SELECT APPROX_PERCENTILE_CONT(\"{}\", 0.5) as stat_value FROM {}", col_name, table_name);
							let result = ctx.sql(&median_sql).await?;
							let batches = result.collect().await?;
							if let Some(batch) = batches.first() {
								if batch.num_rows() > 0 {
									let value_array = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
									if !value_array.is_null(0) {
										stats_values.insert(col_name.clone(), value_array.value(0));
									}
								}
							}
						},
						_ => return Err(NailError::Statistics(format!("Median calculation not supported for column '{}' of type {:?}", col_name, field.data_type()))),
					}
				},
				FillMethod::Mode => {
					// Mode requires finding the most frequent value - use a subquery
					let mode_sql = format!(
						"SELECT \"{0}\" as mode_value FROM (
							SELECT \"{0}\", COUNT(*) as freq 
							FROM {1} 
							WHERE \"{0}\" IS NOT NULL 
							GROUP BY \"{0}\" 
							ORDER BY freq DESC 
							LIMIT 1
						)", 
						col_name, table_name
					);
					let result = ctx.sql(&mode_sql).await?;
					let batches = result.collect().await?;
					if let Some(batch) = batches.first() {
						if batch.num_rows() > 0 {
							let mode_array = batch.column(0);
							if !mode_array.is_null(0) {
								match field.data_type() {
									datafusion::arrow::datatypes::DataType::Utf8 => {
										let str_array = mode_array.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
										mode_values.insert(col_name.clone(), str_array.value(0).to_string());
									},
									datafusion::arrow::datatypes::DataType::Int64 => {
										let int_array = mode_array.as_any().downcast_ref::<Int64Array>().unwrap();
										mode_values.insert(col_name.clone(), int_array.value(0).to_string());
									},
									datafusion::arrow::datatypes::DataType::Float64 => {
										let float_array = mode_array.as_any().downcast_ref::<Float64Array>().unwrap();
										mode_values.insert(col_name.clone(), float_array.value(0).to_string());
									},
									_ => {},
								}
							}
						}
					}
				},
				_ => unreachable!(),
			}
		}
		
		// Now rebuild select expressions with actual calculated values
		select_exprs.clear();
		for field in schema.fields() {
			let field_name = field.name();
			
			if columns.contains(field_name) {
				let filled_expr = match method {
					FillMethod::Mean => {
						if let Some(&stat_val) = stats_values.get(field_name) {
							match field.data_type() {
								datafusion::arrow::datatypes::DataType::Float64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(stat_val)])
								},
								datafusion::arrow::datatypes::DataType::Int64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(stat_val)])
								},
								_ => Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
							}
						} else {
							Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
						}
					},
					FillMethod::Median => {
						if let Some(&stat_val) = stats_values.get(field_name) {
							match field.data_type() {
								datafusion::arrow::datatypes::DataType::Float64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(stat_val)])
								},
								datafusion::arrow::datatypes::DataType::Int64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(stat_val)])
								},
								_ => Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
							}
						} else {
							Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
						}
					},
					FillMethod::Mode => {
						if let Some(mode_val) = mode_values.get(field_name) {
							match field.data_type() {
								datafusion::arrow::datatypes::DataType::Utf8 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(mode_val.as_str())])
								},
								datafusion::arrow::datatypes::DataType::Float64 => {
									if let Ok(num_val) = mode_val.parse::<f64>() {
										coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(num_val)])
									} else {
										Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
									}
								},
								datafusion::arrow::datatypes::DataType::Int64 => {
									if let Ok(num_val) = mode_val.parse::<i64>() {
										coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(num_val)])
									} else {
										Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
									}
								},
								_ => Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
							}
						} else {
							Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
						}
					},
					_ => Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
				};
				select_exprs.push(filled_expr.alias(field_name));
			} else {
				select_exprs.push(Expr::Column(datafusion::common::Column::new(None::<String>, field_name)));
			}
		}
	} else if matches!(method, FillMethod::Forward | FillMethod::Backward) {
		// For forward/backward fill, we need to use window functions
		return fill_forward_backward(df, columns, method).await;
	}
	
	let result = ctx.table(table_name).await?.select(select_exprs)?;
	Ok(result)
}

async fn fill_forward_backward(df: &DataFrame, columns: &[String], method: &FillMethod) -> NailResult<DataFrame> {
	// Collect the data to process it row by row
	let batches = df.clone().collect().await?;
	
	if batches.is_empty() {
		return Ok(df.clone());
	}
	
	// Process batches for forward/backward fill
	let mut processed_batches = Vec::new();
	
	for batch in batches {
		let mut arrays = Vec::new();
		let schema = batch.schema();
		
		for field in schema.fields() {
			let field_name = field.name();
			let array = batch.column_by_name(field_name).unwrap();
			
			if columns.contains(field_name) {
				match method {
					FillMethod::Forward => {
						arrays.push(forward_fill_array(array.clone())?);
					},
					FillMethod::Backward => {
						arrays.push(backward_fill_array(array.clone())?);
					},
					_ => arrays.push(array.clone()),
				}
			} else {
				arrays.push(array.clone());
			}
		}
		
		let processed_batch = datafusion::arrow::record_batch::RecordBatch::try_new(
			schema.clone(),
			arrays
		)?;
		processed_batches.push(processed_batch);
	}
	
	// Convert back to DataFrame
	let ctx = datafusion::execution::context::SessionContext::new();
	let table_name = "processed_data";
	let provider = datafusion::datasource::memory::MemTable::try_new(
		processed_batches[0].schema(),
		vec![processed_batches]
	)?;
	ctx.register_table(table_name, std::sync::Arc::new(provider))?;
	
	Ok(ctx.table(table_name).await?)
}

fn forward_fill_array(array: datafusion::arrow::array::ArrayRef) -> NailResult<datafusion::arrow::array::ArrayRef> {
	use datafusion::arrow::array::{Array, StringArray, Float64Array, Int64Array};
	use datafusion::arrow::datatypes::DataType;
	
	match array.data_type() {
		DataType::Utf8 => {
			let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
			let mut builder = datafusion::arrow::array::StringBuilder::new();
			let mut last_value: Option<String> = None;
			
			for i in 0..string_array.len() {
				if string_array.is_null(i) {
					if let Some(ref value) = last_value {
						builder.append_value(value);
					} else {
						builder.append_null();
					}
				} else {
					let value = string_array.value(i).to_string();
					last_value = Some(value.clone());
					builder.append_value(&value);
				}
			}
			
			Ok(std::sync::Arc::new(builder.finish()))
		},
		DataType::Float64 => {
			let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
			let mut builder = datafusion::arrow::array::Float64Builder::new();
			let mut last_value: Option<f64> = None;
			
			for i in 0..float_array.len() {
				if float_array.is_null(i) {
					if let Some(value) = last_value {
						builder.append_value(value);
					} else {
						builder.append_null();
					}
				} else {
					let value = float_array.value(i);
					last_value = Some(value);
					builder.append_value(value);
				}
			}
			
			Ok(std::sync::Arc::new(builder.finish()))
		},
		DataType::Int64 => {
			let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
			let mut builder = datafusion::arrow::array::Int64Builder::new();
			let mut last_value: Option<i64> = None;
			
			for i in 0..int_array.len() {
				if int_array.is_null(i) {
					if let Some(value) = last_value {
						builder.append_value(value);
					} else {
						builder.append_null();
					}
				} else {
					let value = int_array.value(i);
					last_value = Some(value);
					builder.append_value(value);
				}
			}
			
			Ok(std::sync::Arc::new(builder.finish()))
		},
		_ => {
			// For other types, just return the original array
			Ok(array.clone())
		}
	}
}

fn backward_fill_array(array: datafusion::arrow::array::ArrayRef) -> NailResult<datafusion::arrow::array::ArrayRef> {
	use datafusion::arrow::array::{Array, StringArray, Float64Array, Int64Array};
	use datafusion::arrow::datatypes::DataType;
	
	match array.data_type() {
		DataType::Utf8 => {
			let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
			let mut builder = datafusion::arrow::array::StringBuilder::new();
			let mut values = Vec::new();
			
			// First pass: collect all values
			for i in 0..string_array.len() {
				if string_array.is_null(i) {
					values.push(None);
				} else {
					values.push(Some(string_array.value(i).to_string()));
				}
			}
			
			// Second pass: backward fill
			let mut next_value: Option<String> = None;
			for i in (0..values.len()).rev() {
				if values[i].is_some() {
					next_value = values[i].clone();
				} else if let Some(ref value) = next_value {
					values[i] = Some(value.clone());
				}
			}
			
			// Third pass: build result
			for value in values {
				if let Some(v) = value {
					builder.append_value(&v);
				} else {
					builder.append_null();
				}
			}
			
			Ok(std::sync::Arc::new(builder.finish()))
		},
		DataType::Float64 => {
			let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
			let mut builder = datafusion::arrow::array::Float64Builder::new();
			let mut values = Vec::new();
			
			// First pass: collect all values
			for i in 0..float_array.len() {
				if float_array.is_null(i) {
					values.push(None);
				} else {
					values.push(Some(float_array.value(i)));
				}
			}
			
			// Second pass: backward fill
			let mut next_value: Option<f64> = None;
			for i in (0..values.len()).rev() {
				if values[i].is_some() {
					next_value = values[i];
				} else if let Some(value) = next_value {
					values[i] = Some(value);
				}
			}
			
			// Third pass: build result
			for value in values {
				if let Some(v) = value {
					builder.append_value(v);
				} else {
					builder.append_null();
				}
			}
			
			Ok(std::sync::Arc::new(builder.finish()))
		},
		DataType::Int64 => {
			let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
			let mut builder = datafusion::arrow::array::Int64Builder::new();
			let mut values = Vec::new();
			
			// First pass: collect all values
			for i in 0..int_array.len() {
				if int_array.is_null(i) {
					values.push(None);
				} else {
					values.push(Some(int_array.value(i)));
				}
			}
			
			// Second pass: backward fill
			let mut next_value: Option<i64> = None;
			for i in (0..values.len()).rev() {
				if values[i].is_some() {
					next_value = values[i];
				} else if let Some(value) = next_value {
					values[i] = Some(value);
				}
			}
			
			// Third pass: build result
			for value in values {
				if let Some(v) = value {
					builder.append_value(v);
				} else {
					builder.append_null();
				}
			}
			
			Ok(std::sync::Arc::new(builder.finish()))
		},
		_ => {
			// For other types, just return the original array
			Ok(array.clone())
		}
	}
}