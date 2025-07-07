use clap::Args;
use datafusion::prelude::*;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use crate::utils::stats::select_columns_by_pattern;
use crate::error::{NailError, NailResult};

#[derive(Args, Clone)]
pub struct SearchArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(long, help = "Value to search for")]
	pub value: String,
	
	#[arg(short, long, help = "Comma-separated column names to search in")]
	pub columns: Option<String>,
	
	#[arg(short, long, help = "Return matching row numbers only")]
	pub rows: bool,
	
	#[arg(long, help = "Case-insensitive search")]
	pub ignore_case: bool,
	
	#[arg(long, help = "Exact match only (no partial matches)")]
	pub exact: bool,
}

pub async fn execute(args: SearchArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Searching in: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let schema = df.schema();
	
	let search_columns = if let Some(col_spec) = &args.columns {
		select_columns_by_pattern(schema.clone().into(), col_spec)?
	} else {
		schema.fields().iter().map(|f| f.name().clone()).collect()
	};
	
	args.common.log_if_verbose(&format!("Searching for '{}' in {} columns: {:?}", 
		args.value, search_columns.len(), search_columns));
	
	let result_df = if args.rows {
		search_return_row_numbers(&df, &args.value, &search_columns, args.ignore_case, args.exact, args.common.jobs).await?
	} else {
		search_return_matching_rows(&df, &args.value, &search_columns, args.ignore_case, args.exact, args.common.jobs).await?
	};
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "search").await?;
	
	Ok(())
}

async fn search_return_matching_rows(
	df: &DataFrame,
	search_value: &str,
	columns: &[String],
	ignore_case: bool,
	exact: bool,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let mut conditions = Vec::new();
	
	for column in columns {
		let field = df.schema().field_with_name(None, column)
			.map_err(|_| NailError::ColumnNotFound(column.clone()))?;
		
		let condition = match field.data_type() {
			datafusion::arrow::datatypes::DataType::Utf8 => {
				let search_expr = if ignore_case {
					format!("LOWER(\"{}\")", column)
				} else {
					format!("\"{}\"", column)
				};
				
				let value_expr = if ignore_case {
					search_value.to_lowercase()
				} else {
					search_value.to_string()
				};
				
				if exact {
					format!("{} = '{}'", search_expr, value_expr)
				} else {
					format!("{} LIKE '%{}%'", search_expr, value_expr)
				}
			},
			datafusion::arrow::datatypes::DataType::Int64 | 
			datafusion::arrow::datatypes::DataType::Float64 => {
				if let Ok(num_value) = search_value.parse::<f64>() {
					if exact {
						format!("\"{}\" = {}", column, num_value)
					} else {
						format!("CAST(\"{}\" AS VARCHAR) LIKE '%{}%'", column, search_value)
					}
				} else {
					continue;
				}
			},
			_ => continue,
		};
		
		conditions.push(condition);
	}
	
	if conditions.is_empty() {
		return Err(NailError::InvalidArgument("No searchable columns found".to_string()));
	}
	
	let where_clause = conditions.join(" OR ");
	let sql = format!("SELECT * FROM {} WHERE {}", table_name, where_clause);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}

async fn search_return_row_numbers(
	df: &DataFrame,
	search_value: &str,
	columns: &[String],
	ignore_case: bool,
	exact: bool,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let mut conditions = Vec::new();
	
	for column in columns {
		let field = df.schema().field_with_name(None, column)
			.map_err(|_| NailError::ColumnNotFound(column.clone()))?;
		
		let condition = match field.data_type() {
			datafusion::arrow::datatypes::DataType::Utf8 => {
				let search_expr = if ignore_case {
					format!("LOWER(\"{}\")", column)
				} else {
					format!("\"{}\"", column)
				};
				
				let value_expr = if ignore_case {
					search_value.to_lowercase()
				} else {
					search_value.to_string()
				};
				
				if exact {
					format!("{} = '{}'", search_expr, value_expr)
				} else {
					format!("{} LIKE '%{}%'", search_expr, value_expr)
				}
			},
			datafusion::arrow::datatypes::DataType::Int64 | 
			datafusion::arrow::datatypes::DataType::Float64 => {
				if let Ok(num_value) = search_value.parse::<f64>() {
					if exact {
						format!("\"{}\" = {}", column, num_value)
					} else {
						format!("CAST(\"{}\" AS VARCHAR) LIKE '%{}%'", column, search_value)
					}
				} else {
					continue;
				}
			},
			_ => continue,
		};
		
		conditions.push(condition);
	}
	
	if conditions.is_empty() {
		return Err(NailError::InvalidArgument("No searchable columns found".to_string()));
	}
	
	let where_clause = conditions.join(" OR ");
	let sql = format!(
		"SELECT ROW_NUMBER() OVER() as row_number, '{}' as search_value, '{}' as matched_columns 
		 FROM {} WHERE {}",
		search_value, columns.join(","), table_name, where_clause
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}