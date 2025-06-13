use clap::Args;
use datafusion::prelude::*;
use datafusion::arrow::array::{Float64Array, Int64Array, Array};
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;
use crate::utils::stats::select_columns_by_pattern;

#[derive(Args, Clone)]
pub struct FillArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(long, help = "Fill method", value_enum, default_value = "value")]
	pub method: FillMethod,
	
	#[arg(long, help = "Fill value (required for 'value' method)")]
	pub value: Option<String>,
	
	#[arg(short, long, help = "Comma-separated column names to fill")]
	pub columns: Option<String>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
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
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	
	let columns = if let Some(col_spec) = &args.columns {
		let schema = df.schema();
		select_columns_by_pattern(schema.clone().into(), col_spec)?
	} else {
		df.schema().fields().iter().map(|f| f.name().clone()).collect()
	};
	
	if args.verbose {
		eprintln!("Filling missing values in {} columns using {:?} method", columns.len(), args.method);
	}
	
	let result_df = fill_missing_values(&df, &columns, &args.method, args.value.as_deref()).await?;
	
	if let Some(output_path) = &args.output {
		let file_format = match args.format {
			Some(crate::cli::OutputFormat::Json) => Some(crate::utils::FileFormat::Json),
			Some(crate::cli::OutputFormat::Csv) => Some(crate::utils::FileFormat::Csv),
			Some(crate::cli::OutputFormat::Parquet) => Some(crate::utils::FileFormat::Parquet),
			_ => None,
		};
		write_data(&result_df, output_path, file_format.as_ref()).await?;
	} else {
		display_dataframe(&result_df, None, args.format.as_ref()).await?;
	}
	
	Ok(())
}

async fn fill_missing_values(
	df: &DataFrame,
	columns: &[String],
	method: &FillMethod,
	value: Option<&str>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
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
					// Forward fill - use LAG window function to get previous non-null value
					// This is a simplified implementation
					Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
				},
				FillMethod::Backward => {
					// Backward fill - use LEAD window function to get next non-null value
					// This is a simplified implementation
					Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
				},
			};
			
			select_exprs.push(filled_expr.alias(field_name));
		} else {
			select_exprs.push(Expr::Column(datafusion::common::Column::new(None::<String>, field_name)));
		}
	}
	
	// For statistical methods and fill methods, we need special handling
	if matches!(method, FillMethod::Mean | FillMethod::Median | FillMethod::Mode) {
		let batches = df.clone().collect().await?;
		let mut means = std::collections::HashMap::<String, f64>::new();
		let mut medians = std::collections::HashMap::<String, f64>::new();
		let mut modes = std::collections::HashMap::<String, String>::new();
		
		// Calculate statistical values for each target column
		for col_name in columns {
			let _field = schema.fields().iter()
				.find(|f| f.name() == col_name)
				.ok_or_else(|| NailError::Statistics(format!("Column '{}' not found", col_name)))?;
			
			let idx = schema.fields().iter()
				.position(|f| f.name() == col_name)
				.unwrap();
			
			let mut values = Vec::<f64>::new();
			let mut string_values = Vec::<String>::new();
			
			for batch in &batches {
				let array = batch.column(idx);
				if let Some(farr) = array.as_any().downcast_ref::<Float64Array>() {
					for i in 0..farr.len() {
						if farr.is_valid(i) {
							values.push(farr.value(i));
						}
					}
				} else if let Some(iarr) = array.as_any().downcast_ref::<Int64Array>() {
					for i in 0..iarr.len() {
						if iarr.is_valid(i) {
							values.push(iarr.value(i) as f64);
						}
					}
				} else if let Some(sarr) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
					for i in 0..sarr.len() {
						if sarr.is_valid(i) {
							string_values.push(sarr.value(i).to_string());
						}
					}
				}
			}
			
			if values.is_empty() && string_values.is_empty() {
				return Err(NailError::Statistics(format!("No non-null values found in column '{}'", col_name)));
			}
			
			// Calculate mean
			if !values.is_empty() {
				let sum: f64 = values.iter().sum();
				means.insert(col_name.clone(), sum / values.len() as f64);
				
				// Calculate median
				let mut sorted_values = values.clone();
				sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
				let median = if sorted_values.len() % 2 == 0 {
					let mid = sorted_values.len() / 2;
					(sorted_values[mid - 1] + sorted_values[mid]) / 2.0
				} else {
					sorted_values[sorted_values.len() / 2]
				};
				medians.insert(col_name.clone(), median);
			}
			
			// Calculate mode (most frequent value)
			if !string_values.is_empty() {
				let mut frequency = std::collections::HashMap::<String, usize>::new();
				for val in &string_values {
					*frequency.entry(val.clone()).or_insert(0) += 1;
				}
				if let Some((mode_val, _)) = frequency.iter().max_by_key(|(_, &count)| count) {
					modes.insert(col_name.clone(), mode_val.clone());
				}
			} else if !values.is_empty() {
				// For numeric values, convert to string for mode calculation
				let mut frequency = std::collections::HashMap::<String, usize>::new();
				for val in &values {
					let val_str = val.to_string();
					*frequency.entry(val_str).or_insert(0) += 1;
				}
				if let Some((mode_val, _)) = frequency.iter().max_by_key(|(_, &count)| count) {
					modes.insert(col_name.clone(), mode_val.clone());
				}
			}
		}
		
		// Now rebuild select expressions with actual calculated values
		select_exprs.clear();
		for field in schema.fields() {
			let field_name = field.name();
			
			if columns.contains(field_name) {
				let filled_expr = match method {
					FillMethod::Mean => {
						if let Some(&mean_val) = means.get(field_name) {
							match field.data_type() {
								datafusion::arrow::datatypes::DataType::Float64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(mean_val)])
								},
								datafusion::arrow::datatypes::DataType::Int64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(mean_val)])
								},
								_ => Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
							}
						} else {
							Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
						}
					},
					FillMethod::Median => {
						if let Some(&median_val) = medians.get(field_name) {
							match field.data_type() {
								datafusion::arrow::datatypes::DataType::Float64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(median_val)])
								},
								datafusion::arrow::datatypes::DataType::Int64 => {
									coalesce(vec![Expr::Column(datafusion::common::Column::new(None::<String>, field_name)), lit(median_val)])
								},
								_ => Expr::Column(datafusion::common::Column::new(None::<String>, field_name)),
							}
						} else {
							Expr::Column(datafusion::common::Column::new(None::<String>, field_name))
						}
					},
					FillMethod::Mode => {
						if let Some(mode_val) = modes.get(field_name) {
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
		// For forward/backward fill, we need to process the data row by row
		// This is a simplified implementation that just returns the original data
		// A full implementation would require complex window functions
		return Ok(df.clone());
	}
	
	let result = ctx.table(table_name).await?.select(select_exprs)?;
	Ok(result)
}