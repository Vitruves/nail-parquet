use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use clap::Args;
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use regex::Regex;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum ColumnTypeFilter {
	Numeric,
	Integer,
	Float,
	String,
	Boolean,
	Temporal,
	Binary,
}

impl ColumnTypeFilter {
	pub fn matches(&self, dt: &DataType) -> bool {
		match self {
			ColumnTypeFilter::Numeric => {
				matches!(
					dt,
					DataType::Int8
						| DataType::Int16 | DataType::Int32
						| DataType::Int64 | DataType::UInt8
						| DataType::UInt16 | DataType::UInt32
						| DataType::UInt64 | DataType::Float16
						| DataType::Float32
						| DataType::Float64
						| DataType::Decimal128(_, _)
						| DataType::Decimal256(_, _)
				)
			}
			ColumnTypeFilter::Integer => {
				matches!(
					dt,
					DataType::Int8
						| DataType::Int16 | DataType::Int32
						| DataType::Int64 | DataType::UInt8
						| DataType::UInt16 | DataType::UInt32
						| DataType::UInt64
				)
			}
			ColumnTypeFilter::Float => matches!(
				dt,
				DataType::Float16
					| DataType::Float32
					| DataType::Float64
					| DataType::Decimal128(_, _)
					| DataType::Decimal256(_, _)
			),
			ColumnTypeFilter::String => matches!(dt, DataType::Utf8 | DataType::LargeUtf8),
			ColumnTypeFilter::Boolean => matches!(dt, DataType::Boolean),
			ColumnTypeFilter::Temporal => matches!(
				dt,
				DataType::Date32
					| DataType::Date64
					| DataType::Timestamp(_, _)
					| DataType::Time32(_)
					| DataType::Time64(_)
					| DataType::Duration(_)
					| DataType::Interval(_)
			),
			ColumnTypeFilter::Binary => matches!(
				dt,
				DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
			),
		}
	}
}

pub fn filter_columns_by_type(
	schema: &datafusion::common::DFSchemaRef,
	columns: &[String],
	type_filter: &ColumnTypeFilter,
) -> Vec<String> {
	columns
		.iter()
		.filter(|name| {
			schema
				.field_with_name(None, name)
				.map(|f| type_filter.matches(f.data_type()))
				.unwrap_or(false)
		})
		.cloned()
		.collect()
}

pub fn all_columns_of_type(
	schema: &datafusion::common::DFSchemaRef,
	type_filter: &ColumnTypeFilter,
) -> Vec<String> {
	schema
		.fields()
		.iter()
		.filter(|f| type_filter.matches(f.data_type()))
		.map(|f| f.name().clone())
		.collect()
}

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail select data.parquet -c id,name,price -o subset.parquet
  nail select data.csv -r 1:100 -o first100.csv")]
pub struct SelectArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Column names or regex patterns (comma-separated)")]
	pub columns: Option<String>,

	#[arg(short, long, help = "Row numbers or ranges (e.g., 1,3,5-10)")]
	pub rows: Option<String>,

	#[arg(
		short = 't',
		long = "type",
		value_enum,
		help = "Filter columns by data type. If combined with --columns, intersects (pattern AND type)."
	)]
	pub type_filter: Option<ColumnTypeFilter>,
}

pub async fn execute(args: SelectArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let mut result_df = df;

	if args.columns.is_some() || args.type_filter.is_some() {
		let schema: datafusion::common::DFSchemaRef = result_df.schema().clone().into();

		let mut selected_columns = if let Some(col_spec) = &args.columns {
			select_columns_by_pattern(schema.clone(), col_spec)?
		} else {
			schema.fields().iter().map(|f| f.name().clone()).collect()
		};

		if let Some(type_filter) = &args.type_filter {
			selected_columns = filter_columns_by_type(&schema, &selected_columns, type_filter);
			if selected_columns.is_empty() {
				return Err(NailError::InvalidArgument(format!(
					"No columns of type {:?} found matching the given criteria",
					type_filter
				)));
			}
		}

		args.common.log_if_verbose(&format!(
			"Selecting {} columns: {:?}",
			selected_columns.len(),
			selected_columns
		));

		let select_exprs: Vec<Expr> = selected_columns
			.into_iter()
			.map(|name| Expr::Column(datafusion::common::Column::new(None::<String>, &name)))
			.collect();

		result_df = result_df.select(select_exprs)?;
	}

	if let Some(row_spec) = &args.rows {
		let row_indices = parse_row_specification(row_spec)?;

		args.common
			.log_if_verbose(&format!("Selecting {} rows", row_indices.len()));

		result_df = select_rows_by_indices(&result_df, &row_indices, args.common.jobs).await?;
	}

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "select").await?;

	Ok(())
}

pub fn select_columns_by_pattern(
	schema: datafusion::common::DFSchemaRef,
	pattern: &str,
) -> NailResult<Vec<String>> {
	let patterns: Vec<&str> = pattern
		.split(',')
		.map(|s| s.trim())
		.filter(|s| !s.is_empty())
		.collect();

	// If no valid patterns after filtering, return empty vector
	if patterns.is_empty() {
		return Ok(Vec::new());
	}

	let mut selected = Vec::new();
	let mut not_found = Vec::new();

	for pattern in &patterns {
		let mut found = false;

		// First try exact match (case-sensitive)
		for field in schema.fields() {
			let field_name = field.name();

			if pattern.contains('*') || pattern.contains('^') || pattern.contains('$') {
				let regex = Regex::new(pattern)?;
				if regex.is_match(field_name) {
					selected.push(field_name.clone());
					found = true;
				}
			} else if field_name == *pattern {
				selected.push(field_name.clone());
				found = true;
				break;
			}
		}

		// If not found, try case-insensitive match
		if !found {
			for field in schema.fields() {
				let field_name = field.name();

				if pattern.contains('*') || pattern.contains('^') || pattern.contains('$') {
					// For regex patterns, create case-insensitive version
					let case_insensitive_pattern = format!("(?i){}", pattern);
					if let Ok(regex) = Regex::new(&case_insensitive_pattern) {
						if regex.is_match(field_name) {
							selected.push(field_name.clone());
							found = true;
						}
					}
				} else if field_name.to_lowercase() == pattern.to_lowercase() {
					selected.push(field_name.clone());
					found = true;
					break;
				}
			}
		}

		if !found {
			not_found.push(*pattern);
		}
	}

	if !not_found.is_empty() {
		let available_columns: Vec<String> =
			schema.fields().iter().map(|f| f.name().clone()).collect();
		return Err(NailError::ColumnNotFound(format!(
			"Columns not found: {:?}. Available columns: {:?}",
			not_found, available_columns
		)));
	}

	// Remove duplicates while preserving order
	let mut unique_selected = Vec::new();
	for col in selected {
		if !unique_selected.contains(&col) {
			unique_selected.push(col);
		}
	}

	if unique_selected.is_empty() && !patterns.is_empty() {
		return Err(NailError::ColumnNotFound(format!(
			"No columns matched pattern: {}",
			pattern
		)));
	}

	Ok(unique_selected)
}

pub fn parse_row_specification(spec: &str) -> NailResult<Vec<usize>> {
	let mut indices = Vec::new();

	for part in spec.split(',') {
		let part = part.trim();

		if part.contains('-') {
			let range_parts: Vec<&str> = part.split('-').collect();
			if range_parts.len() != 2 {
				return Err(NailError::InvalidArgument(format!(
					"Invalid range: {}",
					part
				)));
			}

			let start: usize = range_parts[0].trim().parse().map_err(|_| {
				NailError::InvalidArgument(format!(
					"Invalid start index: {}",
					range_parts[0].trim()
				))
			})?;
			let end: usize = range_parts[1].trim().parse().map_err(|_| {
				NailError::InvalidArgument(format!("Invalid end index: {}", range_parts[1].trim()))
			})?;

			if start > end {
				return Err(NailError::InvalidArgument(format!(
					"Start index {} greater than end index {}",
					start, end
				)));
			}

			for i in start..=end {
				indices.push(i.saturating_sub(1));
			}
		} else {
			let index: usize = part
				.parse()
				.map_err(|_| NailError::InvalidArgument(format!("Invalid index: {}", part)))?;
			indices.push(index.saturating_sub(1));
		}
	}

	indices.sort();
	indices.dedup();
	Ok(indices)
}

pub async fn select_rows_by_indices(
	df: &DataFrame,
	indices: &[usize],
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;

	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;

	let indices_str = indices
		.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");

	// Get the original column names and quote them to preserve case
	let original_columns: Vec<String> = df
		.schema()
		.fields()
		.iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();

	let sql = format!(
		"SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn IN ({})",
		original_columns.join(", "),
		original_columns.join(", "),
		table_name,
		indices_str
	);

	let result = ctx.sql(&sql).await?;

	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_select_args_basic() {
		let args = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("data.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			columns: None,
			rows: None,
			type_filter: None,
		};

		assert_eq!(args.columns, None);
		assert_eq!(args.rows, None);
		assert_eq!(args.common.input, PathBuf::from("data.parquet"));
	}

	#[test]
	fn test_select_args_with_columns() {
		let args = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("sales.csv"),
				output: Some(PathBuf::from("filtered.json")),
				format: Some(crate::cli::OutputFormat::Json),
				random: None,
				batch_size: None,
				jobs: Some(4),
				table: false,
				level: 1,
				verbose: true,
			},
			columns: Some("name,age,email".to_string()),
			rows: None,
			type_filter: None,
		};

		assert_eq!(args.columns, Some("name,age,email".to_string()));
		assert_eq!(args.rows, None);
		assert_eq!(args.common.jobs, Some(4));
		assert!(args.common.verbose);
	}

	#[test]
	fn test_select_args_with_rows() {
		let args = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("logs.parquet"),
				output: None,
				format: None,
				random: Some(42),
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			columns: None,
			rows: Some("1,3,5-10".to_string()),
			type_filter: None,
		};

		assert_eq!(args.columns, None);
		assert_eq!(args.rows, Some("1,3,5-10".to_string()));
		assert_eq!(args.common.random, Some(42));
		assert!(!args.common.verbose);
	}

	#[test]
	fn test_select_args_with_columns_and_rows() {
		let args = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("dataset.xlsx"),
				output: Some(PathBuf::from("subset.csv")),
				format: Some(crate::cli::OutputFormat::Csv),
				random: None,
				batch_size: None,
				jobs: Some(8),
				table: false,
				level: 1,
				verbose: true,
			},
			columns: Some("id,name,status".to_string()),
			rows: Some("1-100,200-300".to_string()),
			type_filter: None,
		};

		assert_eq!(args.columns, Some("id,name,status".to_string()));
		assert_eq!(args.rows, Some("1-100,200-300".to_string()));
		assert_eq!(args.common.jobs, Some(8));
		assert!(args.common.verbose);
	}

	#[test]
	fn test_select_args_with_regex_columns() {
		let args = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("metrics.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			columns: Some("^date.*,.*_count$,name".to_string()),
			rows: None,
			type_filter: None,
		};

		assert_eq!(args.columns, Some("^date.*,.*_count$,name".to_string()));
		assert_eq!(args.rows, None);
	}

	#[test]
	fn test_select_args_clone() {
		let args = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			columns: Some("col1,col2".to_string()),
			rows: Some("1,2,3".to_string()),
			type_filter: None,
		};

		let cloned = args.clone();
		assert_eq!(args.columns, cloned.columns);
		assert_eq!(args.rows, cloned.rows);
		assert_eq!(args.common.input, cloned.common.input);
	}

	#[test]
	fn test_parse_row_specification_single_indices() {
		let result = parse_row_specification("1,3,5");
		assert!(result.is_ok());
		let indices = result.unwrap();
		assert_eq!(indices, vec![0, 2, 4]); // 1-based to 0-based conversion
	}

	#[test]
	fn test_parse_row_specification_ranges() {
		let result = parse_row_specification("1-3,5-7");
		assert!(result.is_ok());
		let indices = result.unwrap();
		assert_eq!(indices, vec![0, 1, 2, 4, 5, 6]); // 1-based to 0-based conversion
	}

	#[test]
	fn test_parse_row_specification_mixed() {
		let result = parse_row_specification("1,3-5,10");
		assert!(result.is_ok());
		let indices = result.unwrap();
		assert_eq!(indices, vec![0, 2, 3, 4, 9]); // 1-based to 0-based conversion
	}

	#[test]
	fn test_parse_row_specification_with_spaces() {
		let result = parse_row_specification("1, 3 - 5 , 10");
		assert!(result.is_ok());
		let indices = result.unwrap();
		assert_eq!(indices, vec![0, 2, 3, 4, 9]);
	}

	#[test]
	fn test_parse_row_specification_invalid_range() {
		let result = parse_row_specification("5-3");
		assert!(result.is_err());
		assert!(result
			.unwrap_err()
			.to_string()
			.contains("Start index 5 greater than end index 3"));
	}

	#[test]
	fn test_parse_row_specification_invalid_number() {
		let result = parse_row_specification("1,abc,3");
		assert!(result.is_err());
		assert!(result
			.unwrap_err()
			.to_string()
			.contains("Invalid index: abc"));
	}

	#[test]
	fn test_parse_row_specification_invalid_range_format() {
		let result = parse_row_specification("1-2-3");
		assert!(result.is_err());
		assert!(result
			.unwrap_err()
			.to_string()
			.contains("Invalid range: 1-2-3"));
	}

	#[test]
	fn test_parse_row_specification_deduplication() {
		let result = parse_row_specification("1,3,1,3");
		assert!(result.is_ok());
		let indices = result.unwrap();
		assert_eq!(indices, vec![0, 2]); // Duplicates removed
	}

	#[test]
	fn test_select_args_different_formats() {
		let args_text = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("input.csv"),
				output: Some(PathBuf::from("output.txt")),
				format: Some(crate::cli::OutputFormat::Text),
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			columns: Some("col1".to_string()),
			rows: None,
			type_filter: None,
		};

		let args_xlsx = SelectArgs {
			common: CommonArgs {
				input: PathBuf::from("input.parquet"),
				output: Some(PathBuf::from("output.xlsx")),
				format: Some(crate::cli::OutputFormat::Xlsx),
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			columns: None,
			rows: Some("1-10".to_string()),
			type_filter: None,
		};

		assert!(matches!(
			args_text.common.format,
			Some(crate::cli::OutputFormat::Text)
		));
		assert!(matches!(
			args_xlsx.common.format,
			Some(crate::cli::OutputFormat::Xlsx)
		));
	}
}
