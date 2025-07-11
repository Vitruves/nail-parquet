use clap::Args;
use datafusion::prelude::*;
use datafusion::common::DFSchemaRef;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use crate::utils::stats::select_columns_by_pattern;
use crate::error::{NailError, NailResult};
use datafusion::logical_expr::{ExprSchemable, expr::ScalarFunction};

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
	let schema: DFSchemaRef = df.schema().clone().into();
	
	let search_columns = if let Some(col_spec) = &args.columns {
		select_columns_by_pattern(schema.clone(), col_spec)?
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
	_jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let mut conditions = Vec::new();
	
	let schema: DFSchemaRef = df.schema().clone().into();
	for column in columns {
		// Find the field by iterating through all fields since column names are already resolved
		let field = schema.fields().iter()
			.find(|f| f.name() == column)
			.ok_or_else(|| NailError::ColumnNotFound(format!(
				"Column '{}' not found in schema", column
			)))?;
		
		let col_expr = col(column);
		
		let condition = match field.data_type() {
			datafusion::arrow::datatypes::DataType::Utf8 => {
				if ignore_case {
					// For case-insensitive search, we need to use SQL-style LOWER function
					let lower_col = Expr::ScalarFunction(ScalarFunction::new_udf(
						datafusion::functions::string::lower(),
						vec![col_expr.clone()],
					));
					
					let search_lit = lit(search_value.to_lowercase());
					
					if exact {
						lower_col.eq(search_lit)
					} else {
						let pattern = lit(format!("%{}%", search_value.to_lowercase()));
						lower_col.like(pattern)
					}
				} else {
					// Case-sensitive search
					if exact {
						col_expr.clone().eq(lit(search_value.to_string()))
					} else {
						let pattern = lit(format!("%{}%", search_value));
						col_expr.clone().like(pattern)
					}
				}
			},
			datafusion::arrow::datatypes::DataType::Int64 | 
			datafusion::arrow::datatypes::DataType::Float64 => {
				if let Ok(num_value) = search_value.parse::<f64>() {
					if exact {
						col_expr.eq(lit(num_value))
					} else {
						// For partial matching on numeric columns, cast to string and use LIKE
						let cast_expr = col_expr.cast_to(&datafusion::arrow::datatypes::DataType::Utf8, df.schema())?;
						let pattern = lit(format!("%{}%", search_value));
						cast_expr.like(pattern)
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
	
	// Combine all conditions with OR
	let combined_filter = conditions.into_iter().reduce(|acc, expr| acc.or(expr)).unwrap();
	let result = df.clone().filter(combined_filter)?;
	
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
	
	// Build search conditions the same way as in search_return_matching_rows
	let mut conditions = Vec::new();
	
	let schema: DFSchemaRef = df.schema().clone().into();
	for column in columns {
		// Find the field by iterating through all fields since column names are already resolved
		let field = schema.fields().iter()
			.find(|f| f.name() == column)
			.ok_or_else(|| NailError::ColumnNotFound(format!(
				"Column '{}' not found in schema", column
			)))?;
		
		let col_expr = col(column);
		
		let condition = match field.data_type() {
			datafusion::arrow::datatypes::DataType::Utf8 => {
				if ignore_case {
					let lower_col = Expr::ScalarFunction(ScalarFunction::new_udf(
						datafusion::functions::string::lower(),
						vec![col_expr.clone()],
					));
					
					let search_lit = lit(search_value.to_lowercase());
					
					if exact {
						lower_col.eq(search_lit)
					} else {
						let pattern = lit(format!("%{}%", search_value.to_lowercase()));
						lower_col.like(pattern)
					}
				} else {
					if exact {
						col_expr.clone().eq(lit(search_value.to_string()))
					} else {
						let pattern = lit(format!("%{}%", search_value));
						col_expr.clone().like(pattern)
					}
				}
			},
			datafusion::arrow::datatypes::DataType::Int64 | 
			datafusion::arrow::datatypes::DataType::Float64 => {
				if let Ok(num_value) = search_value.parse::<f64>() {
					if exact {
						col_expr.eq(lit(num_value))
					} else {
						let cast_expr = col_expr.cast_to(&datafusion::arrow::datatypes::DataType::Utf8, df.schema())?;
						let pattern = lit(format!("%{}%", search_value));
						cast_expr.like(pattern)
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
	
	// Combine all conditions with OR
	let combined_filter = conditions.into_iter().reduce(|acc, expr| acc.or(expr)).unwrap();
	
	// First add row numbers to the original data, THEN filter
	// This preserves the original row positions
	
	// Create the numbered dataframe first
	let numbered_sql = format!(
		"SELECT ROW_NUMBER() OVER() as row_number, * FROM {}",
		table_name
	);
	let numbered_df = ctx.sql(&numbered_sql).await?;
	ctx.register_table("numbered_data", numbered_df.into_view())?;
	
	// Apply the filter to the numbered data
	let filtered_df = ctx.table("numbered_data").await?.filter(combined_filter)?;
	
	// Select only the row number and metadata
	let result = filtered_df.select(vec![
		col("row_number"),
		lit(search_value).alias("search_value"),
		lit(columns.join(",")).alias("matched_columns"),
	])?;
	
	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_search_args_basic() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("data.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			value: "test_value".to_string(),
			columns: None,
			rows: false,
			ignore_case: false,
			exact: false,
		};

		assert_eq!(args.value, "test_value");
		assert_eq!(args.columns, None);
		assert!(!args.rows);
		assert!(!args.ignore_case);
		assert!(!args.exact);
	}

	#[test]
	fn test_search_args_with_columns() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("sales.csv"),
				output: Some(PathBuf::from("results.json")),
				format: Some(crate::cli::OutputFormat::Json),
				random: None,
				jobs: Some(4),
				verbose: true,
			},
			value: "john".to_string(),
			columns: Some("name,customer,email".to_string()),
			rows: false,
			ignore_case: true,
			exact: false,
		};

		assert_eq!(args.value, "john");
		assert_eq!(args.columns, Some("name,customer,email".to_string()));
		assert!(!args.rows);
		assert!(args.ignore_case);
		assert!(!args.exact);
		assert_eq!(args.common.jobs, Some(4));
		assert!(args.common.verbose);
	}

	#[test]
	fn test_search_args_exact_match() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("products.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			value: "Premium Widget".to_string(),
			columns: Some("product_name".to_string()),
			rows: false,
			ignore_case: false,
			exact: true,
		};

		assert_eq!(args.value, "Premium Widget");
		assert_eq!(args.columns, Some("product_name".to_string()));
		assert!(!args.rows);
		assert!(!args.ignore_case);
		assert!(args.exact);
	}

	#[test]
	fn test_search_args_row_numbers() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("logs.json"),
				output: Some(PathBuf::from("matching_rows.csv")),
				format: Some(crate::cli::OutputFormat::Csv),
				random: Some(42),
				jobs: Some(8),
				verbose: true,
			},
			value: "ERROR".to_string(),
			columns: Some("level,message".to_string()),
			rows: true,
			ignore_case: true,
			exact: false,
		};

		assert_eq!(args.value, "ERROR");
		assert_eq!(args.columns, Some("level,message".to_string()));
		assert!(args.rows);
		assert!(args.ignore_case);
		assert!(!args.exact);
		assert_eq!(args.common.random, Some(42));
		assert_eq!(args.common.jobs, Some(8));
	}

	#[test]
	fn test_search_args_numeric_value() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("numbers.xlsx"),
				output: None,
				format: Some(crate::cli::OutputFormat::Xlsx),
				random: None,
				jobs: None,
				verbose: false,
			},
			value: "123.45".to_string(),
			columns: Some("price,amount,total".to_string()),
			rows: false,
			ignore_case: false,
			exact: true,
		};

		assert_eq!(args.value, "123.45");
		assert_eq!(args.columns, Some("price,amount,total".to_string()));
		assert!(!args.rows);
		assert!(!args.ignore_case);
		assert!(args.exact);
	}

	#[test]
	fn test_search_args_case_insensitive_exact() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("users.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			value: "ADMIN".to_string(),
			columns: Some("role,status".to_string()),
			rows: false,
			ignore_case: true,
			exact: true,
		};

		assert_eq!(args.value, "ADMIN");
		assert_eq!(args.columns, Some("role,status".to_string()));
		assert!(!args.rows);
		assert!(args.ignore_case);
		assert!(args.exact);
	}

	#[test]
	fn test_search_args_clone() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			value: "search_term".to_string(),
			columns: Some("col1,col2".to_string()),
			rows: true,
			ignore_case: true,
			exact: false,
		};

		let cloned = args.clone();
		assert_eq!(args.value, cloned.value);
		assert_eq!(args.columns, cloned.columns);
		assert_eq!(args.rows, cloned.rows);
		assert_eq!(args.ignore_case, cloned.ignore_case);
		assert_eq!(args.exact, cloned.exact);
		assert_eq!(args.common.input, cloned.common.input);
	}

	#[test]
	fn test_search_args_parsing_columns() {
		let args = SearchArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			value: "test".to_string(),
			columns: Some("col_a, col_b , col_c".to_string()),
			rows: false,
			ignore_case: false,
			exact: false,
		};

		if let Some(cols) = &args.columns {
			let parsed_cols: Vec<&str> = cols.split(',').map(|s| s.trim()).collect();
			assert_eq!(parsed_cols, vec!["col_a", "col_b", "col_c"]);
		}
	}
}