use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct FilterArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Column filter conditions (e.g., 'age>25,salary<50000')")]
	pub columns: Option<String>,
	
	#[arg(short, long, help = "Row filter type", value_enum)]
	pub rows: Option<RowFilter>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum RowFilter {
	NoNan,
	NumericOnly,
	CharOnly,
	NoZeros,
}

pub async fn execute(args: FilterArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let mut result_df = df;
	
	if let Some(col_conditions) = &args.columns {
		if args.verbose {
			eprintln!("Applying column filters: {}", col_conditions);
		}
		result_df = apply_column_filters(&result_df, col_conditions).await?;
	}
	
	if let Some(row_filter) = &args.rows {
		if args.verbose {
			eprintln!("Applying row filter: {:?}", row_filter);
		}
		result_df = apply_row_filter(&result_df, row_filter).await?;
	}
	
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

async fn apply_column_filters(df: &DataFrame, conditions: &str) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema().clone().into();
	let mut filter_conditions = Vec::new();
	
	for condition in conditions.split(',') {
		let condition = condition.trim();
		let filter_expr = parse_condition_with_schema(condition, &schema).await?;
		filter_conditions.push(filter_expr);
	}
	
	let combined_filter = filter_conditions.into_iter()
		.reduce(|acc, expr| acc.and(expr))
		.unwrap();
	
	let result = ctx.table(table_name).await?.filter(combined_filter)?;
	Ok(result)
}

async fn parse_condition_with_schema(condition: &str, schema: &datafusion::common::DFSchemaRef) -> NailResult<Expr> {
	let operators = [">=", "<=", "!=", "=", ">", "<"];
	
	for op in &operators {
		if let Some(pos) = condition.find(op) {
			let column_name_input = condition[..pos].trim();
			let value_str = condition[pos + op.len()..].trim();
			
			// Find the actual column name (case-insensitive matching)
			let actual_column_name = schema.fields().iter()
				.find(|f| f.name().to_lowercase() == column_name_input.to_lowercase())
				.map(|f| f.name().clone())
				.ok_or_else(|| {
					let available_cols: Vec<String> = schema.fields().iter()
						.map(|f| f.name().clone())
						.collect();
					NailError::ColumnNotFound(format!(
						"Column '{}' not found. Available columns: {:?}", 
						column_name_input, available_cols
					))
				})?;
			
			let value_expr = if let Ok(int_val) = value_str.parse::<i64>() {
				lit(int_val)
			} else if let Ok(float_val) = value_str.parse::<f64>() {
				lit(float_val)
			} else {
				lit(value_str)
			};
			
			let column_expr = col(&actual_column_name);
			
			return Ok(match *op {
				"=" => column_expr.eq(value_expr),
				"!=" => column_expr.not_eq(value_expr),
				">" => column_expr.gt(value_expr),
				">=" => column_expr.gt_eq(value_expr),
				"<" => column_expr.lt(value_expr),
				"<=" => column_expr.lt_eq(value_expr),
				_ => unreachable!(),
			});
		}
	}
	
	Err(NailError::InvalidArgument(format!("Invalid condition: {}", condition)))
}

async fn apply_row_filter(df: &DataFrame, filter: &RowFilter) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema();
	let filter_expr = match filter {
		RowFilter::NoNan => {
			let conditions: Vec<Expr> = schema.fields().iter()
				.map(|f| col(f.name()).is_not_null())
				.collect();
			conditions.into_iter().reduce(|acc, expr| acc.and(expr)).unwrap()
		},
		RowFilter::NumericOnly => {
			let numeric_columns: Vec<String> = schema.fields().iter()
				.filter(|f| matches!(f.data_type(), 
					datafusion::arrow::datatypes::DataType::Int64 | 
					datafusion::arrow::datatypes::DataType::Float64 | 
					datafusion::arrow::datatypes::DataType::Int32 | 
					datafusion::arrow::datatypes::DataType::Float32
				))
				.map(|f| f.name().clone())
				.collect();
			
			if numeric_columns.is_empty() {
				return Err(NailError::InvalidArgument("No numeric columns found".to_string()));
			}
			
			return Ok(df.clone().select(numeric_columns.iter().map(|name| col(name)).collect())?);
		},
		RowFilter::CharOnly => {
			let char_columns: Vec<String> = schema.fields().iter()
				.filter(|f| matches!(f.data_type(), datafusion::arrow::datatypes::DataType::Utf8))
				.map(|f| f.name().clone())
				.collect();
			
			if char_columns.is_empty() {
				return Err(NailError::InvalidArgument("No string columns found".to_string()));
			}
			
			return Ok(df.clone().select(char_columns.iter().map(|name| col(name)).collect())?);
		},
		RowFilter::NoZeros => {
			let conditions: Vec<Expr> = schema.fields().iter()
				.filter_map(|f| {
					match f.data_type() {
						datafusion::arrow::datatypes::DataType::Int64 | 
						datafusion::arrow::datatypes::DataType::Int32 => {
							Some(col(f.name()).not_eq(lit(0)))
						},
						datafusion::arrow::datatypes::DataType::Float64 | 
						datafusion::arrow::datatypes::DataType::Float32 => {
							Some(col(f.name()).not_eq(lit(0.0)))
						},
						_ => None,
					}
				})
				.collect();
			
			if conditions.is_empty() {
				return Ok(df.clone());
			}
			
			conditions.into_iter().reduce(|acc, expr| acc.and(expr)).unwrap()
		},
	};
	
	let result = ctx.table(table_name).await?.filter(filter_expr)?;
	Ok(result)
}