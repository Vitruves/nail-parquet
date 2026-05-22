use crate::cli::CommonArgs;
use crate::error::NailResult;
use crate::utils::io::read_data_with_opts;
use clap::Args;
use colored::Colorize;
use datafusion::prelude::*;
use std::collections::HashMap;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail describe data.parquet
  nail describe sales.csv -o overview.json")]
pub struct DescribeArgs {
	#[command(flatten)]
	pub common: CommonArgs,
}

pub async fn execute(args: DescribeArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading file info from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let schema = df.schema();

	// Gather file information
	let file_path = args.common.input.display().to_string();
	let metadata = std::fs::metadata(&args.common.input)?;

	let file_size = metadata.len();
	let file_size_mb = file_size as f64 / 1_048_576.0;

	// Get file timestamps
	let modified = metadata.modified().ok();
	let created = metadata.created().ok();

	// Detect file format
	let file_format = match args.common.input.extension().and_then(|s| s.to_str()) {
		Some("parquet") => "Parquet",
		Some("csv") => "CSV",
		Some("json") => "JSON",
		Some("xlsx") => "Excel",
		_ => "Unknown",
	};

	// Get row and column counts
	let total_rows = df.clone().count().await?;
	let total_columns = schema.fields().len();

	// Analyze column types
	let mut type_counts: HashMap<String, usize> = HashMap::new();
	let mut numeric_count = 0;
	let mut string_count = 0;
	let mut date_count = 0;
	let mut boolean_count = 0;
	let mut other_count = 0;
	let mut numeric_cols = Vec::new();
	let mut string_cols = Vec::new();

	for field in schema.fields() {
		let type_name = format!("{:?}", field.data_type());
		*type_counts.entry(type_name.clone()).or_insert(0) += 1;

		match field.data_type() {
			datafusion::arrow::datatypes::DataType::Int64
			| datafusion::arrow::datatypes::DataType::Int32
			| datafusion::arrow::datatypes::DataType::Int16
			| datafusion::arrow::datatypes::DataType::Int8
			| datafusion::arrow::datatypes::DataType::UInt64
			| datafusion::arrow::datatypes::DataType::UInt32
			| datafusion::arrow::datatypes::DataType::UInt16
			| datafusion::arrow::datatypes::DataType::UInt8
			| datafusion::arrow::datatypes::DataType::Float64
			| datafusion::arrow::datatypes::DataType::Float32
			| datafusion::arrow::datatypes::DataType::Decimal128(_, _)
			| datafusion::arrow::datatypes::DataType::Decimal256(_, _) => {
				numeric_count += 1;
				numeric_cols.push(field.name());
			}
			datafusion::arrow::datatypes::DataType::Utf8
			| datafusion::arrow::datatypes::DataType::LargeUtf8 => {
				string_count += 1;
				string_cols.push(field.name());
			}
			datafusion::arrow::datatypes::DataType::Date32
			| datafusion::arrow::datatypes::DataType::Date64
			| datafusion::arrow::datatypes::DataType::Timestamp(_, _)
			| datafusion::arrow::datatypes::DataType::Time32(_)
			| datafusion::arrow::datatypes::DataType::Time64(_) => {
				date_count += 1;
			}
			datafusion::arrow::datatypes::DataType::Boolean => {
				boolean_count += 1;
			}
			_ => {
				other_count += 1;
			}
		}
	}

	// Calculate null statistics
	let null_info = calculate_null_info(&df).await?;

	// Estimate memory usage
	let estimated_memory_mb = (total_rows * total_columns * 8) as f64 / 1_048_576.0;

	// Calculate data density
	let total_cells = total_rows * total_columns;
	let non_null_cells = total_cells - null_info.total_nulls;
	let density = if total_cells > 0 {
		(non_null_cells as f64 / total_cells as f64) * 100.0
	} else {
		0.0
	};

	// Estimate duplicate rows
	let duplicate_info = estimate_duplicates(&df, total_rows).await?;

	// Print enhanced file description with colors
	println!();
	println!("{}", "FILE OVERVIEW".bright_cyan().bold());
	println!();
	print_field("Path", &file_path);
	print_field("Format", file_format);
	print_field(
		"Size",
		&format!("{:.2} MB ({} bytes)", file_size_mb, file_size),
	);

	if let Some(mod_time) = modified {
		if let Ok(duration) = mod_time.duration_since(std::time::UNIX_EPOCH) {
			let datetime = chrono::DateTime::from_timestamp(duration.as_secs() as i64, 0);
			if let Some(dt) = datetime {
				print_field("Modified", &dt.format("%Y-%m-%d %H:%M:%S").to_string());
			}
		}
	}

	if let Some(create_time) = created {
		if let Ok(duration) = create_time.duration_since(std::time::UNIX_EPOCH) {
			let datetime = chrono::DateTime::from_timestamp(duration.as_secs() as i64, 0);
			if let Some(dt) = datetime {
				print_field("Created", &dt.format("%Y-%m-%d %H:%M:%S").to_string());
			}
		}
	}

	println!();
	println!("{}", "DIMENSIONS".bright_cyan().bold());
	println!();
	print_metric("Rows", &total_rows.to_string(), "green");
	print_metric("Columns", &total_columns.to_string(), "green");
	print_metric("Total Cells", &total_cells.to_string(), "white");
	print_metric(
		"Estimated Memory",
		&format!("{:.2} MB", estimated_memory_mb),
		"yellow",
	);

	// Memory efficiency
	let efficiency = if file_size > 0 {
		(file_size as f64 / (estimated_memory_mb * 1_048_576.0)) * 100.0
	} else {
		0.0
	};
	print_metric("Storage Efficiency", &format!("{:.1}%", efficiency), "cyan");

	println!();
	println!("{}", "COLUMN TYPES".bright_cyan().bold());
	println!();
	if numeric_count > 0 {
		print_metric("Numeric", &numeric_count.to_string(), "green");
	}
	if string_count > 0 {
		print_metric("String", &string_count.to_string(), "blue");
	}
	if date_count > 0 {
		print_metric("Date/Time", &date_count.to_string(), "magenta");
	}
	if boolean_count > 0 {
		print_metric("Boolean", &boolean_count.to_string(), "yellow");
	}
	if other_count > 0 {
		print_metric("Other", &other_count.to_string(), "white");
	}

	println!();
	println!("{}", "DATA QUALITY".bright_cyan().bold());
	println!();
	print_metric(
		"Data Density",
		&format!("{:.2}%", density),
		if density > 95.0 {
			"green"
		} else if density > 80.0 {
			"yellow"
		} else {
			"red"
		},
	);
	print_metric("Non-Null Values", &non_null_cells.to_string(), "white");
	print_metric(
		"Null Values",
		&null_info.total_nulls.to_string(),
		if null_info.total_nulls == 0 {
			"green"
		} else {
			"yellow"
		},
	);
	print_metric(
		"Null Percentage",
		&format!("{:.2}%", null_info.null_percentage),
		if null_info.null_percentage < 1.0 {
			"green"
		} else if null_info.null_percentage < 10.0 {
			"yellow"
		} else {
			"red"
		},
	);

	if null_info.columns_with_nulls > 0 {
		print_metric(
			"Columns w/ Nulls",
			&format!("{} of {}", null_info.columns_with_nulls, total_columns),
			"yellow",
		);
	}

	if duplicate_info.estimated_duplicates > 0 {
		print_metric(
			"Est. Duplicate Rows",
			&duplicate_info.estimated_duplicates.to_string(),
			"yellow",
		);
		print_metric(
			"Duplicate %",
			&format!("{:.2}%", duplicate_info.duplicate_percentage),
			"yellow",
		);
	}

	// Show column names (first 10 of each type)
	if !numeric_cols.is_empty() {
		println!();
		println!("{}", "NUMERIC COLUMNS".bright_green().bold());
		println!();
		let display_cols: Vec<_> = numeric_cols.iter().take(10).collect();
		println!(
			"  {}",
			display_cols
				.iter()
				.map(|c| c.to_string())
				.collect::<Vec<_>>()
				.join(", ")
		);
		if numeric_cols.len() > 10 {
			println!(
				"  {} ... and {} more",
				"".dimmed(),
				(numeric_cols.len() - 10).to_string().dimmed()
			);
		}
	}

	if !string_cols.is_empty() {
		println!();
		println!("{}", "STRING COLUMNS".bright_blue().bold());
		println!();
		let display_cols: Vec<_> = string_cols.iter().take(10).collect();
		println!(
			"  {}",
			display_cols
				.iter()
				.map(|c| c.to_string())
				.collect::<Vec<_>>()
				.join(", ")
		);
		if string_cols.len() > 10 {
			println!(
				"  {} ... and {} more",
				"".dimmed(),
				(string_cols.len() - 10).to_string().dimmed()
			);
		}
	}

	// Show detailed type breakdown if many types
	if type_counts.len() > 4 {
		println!();
		println!("{}", "DETAILED TYPE BREAKDOWN".bright_cyan().bold());
		println!();
		let mut sorted_types: Vec<_> = type_counts.iter().collect();
		sorted_types.sort_by(|a, b| b.1.cmp(a.1));
		for (dtype, count) in sorted_types {
			println!(
				"  {:30} {}",
				dtype.bright_white(),
				count.to_string().yellow()
			);
		}
	}

	println!();

	Ok(())
}

fn print_field(label: &str, value: &str) {
	println!("  {:20} {}", label.bright_white().bold(), value.white());
}

fn print_metric(label: &str, value: &str, color: &str) {
	let colored_value = match color {
		"green" => value.green(),
		"yellow" => value.yellow(),
		"red" => value.red(),
		"blue" => value.blue(),
		"cyan" => value.cyan(),
		"magenta" => value.magenta(),
		_ => value.white(),
	};
	println!("  {:20} {}", label.bright_white(), colored_value.bold());
}

struct NullInfo {
	total_nulls: usize,
	null_percentage: f64,
	columns_with_nulls: usize,
}

struct DuplicateInfo {
	estimated_duplicates: usize,
	duplicate_percentage: f64,
}

async fn estimate_duplicates(df: &DataFrame, total_rows: usize) -> NailResult<DuplicateInfo> {
	// Try to count distinct rows - this is an estimation
	let ctx = crate::utils::create_context().await?;
	ctx.register_table("temp_table", df.clone().into_view())?;

	// Get all column names
	let schema = df.schema();
	let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

	// Build a query to count distinct rows
	let select_cols = columns
		.iter()
		.map(|c| format!("\"{}\"", c))
		.collect::<Vec<_>>()
		.join(", ");

	let sql = format!(
		"SELECT COUNT(*) as cnt FROM (SELECT DISTINCT {} FROM temp_table)",
		select_cols
	);

	let result = ctx.sql(&sql).await?;
	let batches = result.collect().await?;

	let distinct_count = if let Some(batch) = batches.first() {
		if let Some(int_array) = batch
			.column(0)
			.as_any()
			.downcast_ref::<datafusion::arrow::array::Int64Array>()
		{
			int_array.value(0) as usize
		} else {
			total_rows
		}
	} else {
		total_rows
	};

	let estimated_duplicates = total_rows.saturating_sub(distinct_count);

	let duplicate_percentage = if total_rows > 0 {
		(estimated_duplicates as f64 / total_rows as f64) * 100.0
	} else {
		0.0
	};

	Ok(DuplicateInfo {
		estimated_duplicates,
		duplicate_percentage,
	})
}

async fn calculate_null_info(df: &DataFrame) -> NailResult<NullInfo> {
	let schema = df.schema();
	let total_rows = df.clone().count().await?;
	let total_columns = schema.fields().len();
	let total_values = total_rows * total_columns;

	// Build a query to count nulls in all columns
	let ctx = crate::utils::create_context().await?;
	ctx.register_table("temp_table", df.clone().into_view())?;

	let null_queries: Vec<String> = schema
		.fields()
		.iter()
		.map(|f| {
			format!(
				"SUM(CASE WHEN \"{}\" IS NULL THEN 1 ELSE 0 END) as \"{}\"",
				f.name(),
				f.name()
			)
		})
		.collect();

	let sql = format!("SELECT {} FROM temp_table", null_queries.join(", "));
	let null_df = ctx.sql(&sql).await?;
	let batches = null_df.collect().await?;

	let mut total_nulls = 0;
	let mut columns_with_nulls = 0;

	if let Some(batch) = batches.first() {
		for col_idx in 0..batch.num_columns() {
			let col = batch.column(col_idx);
			if let Some(int_array) = col
				.as_any()
				.downcast_ref::<datafusion::arrow::array::Int64Array>()
			{
				if let Ok(null_count) = int_array.value(0).try_into() {
					let null_count: usize = null_count;
					total_nulls += null_count;
					if null_count > 0 {
						columns_with_nulls += 1;
					}
				}
			}
		}
	}

	let null_percentage = if total_values > 0 {
		(total_nulls as f64 / total_values as f64) * 100.0
	} else {
		0.0
	};

	Ok(NullInfo {
		total_nulls,
		null_percentage,
		columns_with_nulls,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_describe_args_parsing() {
		let args = DescribeArgs {
			common: CommonArgs {
				input: PathBuf::from("data.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				verbose: false,
			},
		};

		assert_eq!(args.common.input, PathBuf::from("data.parquet"));
	}

	#[test]
	fn test_describe_args_with_verbose() {
		let args = DescribeArgs {
			common: CommonArgs {
				input: PathBuf::from("test.csv"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: Some(4),
				table: false,
				verbose: true,
			},
		};

		assert!(args.common.verbose);
		assert_eq!(args.common.jobs, Some(4));
	}

	#[test]
	fn test_describe_args_clone() {
		let args = DescribeArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				verbose: false,
			},
		};

		let cloned = args.clone();
		assert_eq!(args.common.input, cloned.common.input);
		assert_eq!(args.common.verbose, cloned.common.verbose);
	}
}
