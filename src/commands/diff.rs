use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, read_data_with_opts};
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct DiffArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Second file to compare with")]
	pub compare: PathBuf,

	#[arg(short, long, help = "Columns to use as primary key for comparison (comma-separated)")]
	pub keys: Option<String>,

	#[arg(long, help = "Show only rows that differ")]
	pub changes_only: bool,

	#[arg(long, help = "Show only rows in left file")]
	pub left_only: bool,

	#[arg(long, help = "Show only rows in right file")]
	pub right_only: bool,
}

pub async fn execute(args: DiffArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading left file from: {}", args.common.input.display()));
	args.common.log_if_verbose(&format!("Reading right file from: {}", args.compare.display()));

	let left_df = read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let right_df = read_data(&args.compare).await?;

	// Validate schemas are compatible
	let left_schema = left_df.schema();
	let right_schema = right_df.schema();

	if left_schema.fields().len() != right_schema.fields().len() {
		return Err(NailError::InvalidArgument(
			format!("Schema mismatch: left has {} columns, right has {} columns",
				left_schema.fields().len(),
				right_schema.fields().len()
			)
		));
	}

	let diff_df = if let Some(key_cols) = &args.keys {
		// Key-based comparison
		let keys: Vec<String> = key_cols.split(',').map(|s| s.trim().to_string()).collect();
		args.common.log_if_verbose(&format!("Comparing using key columns: {:?}", keys));

		perform_keyed_diff(&left_df, &right_df, &keys, &args).await?
	} else {
		// Row-based comparison (by position)
		args.common.log_if_verbose("Performing row-by-row comparison");
		perform_row_diff(&left_df, &right_df, &args).await?
	};

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&diff_df, "diff").await?;

	Ok(())
}

async fn perform_keyed_diff(
	left_df: &DataFrame,
	right_df: &DataFrame,
	keys: &[String],
	args: &DiffArgs,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(args.common.jobs).await?;

	// Register tables
	ctx.register_table("left_table", left_df.clone().into_view())?;
	ctx.register_table("right_table", right_df.clone().into_view())?;

	// Build key join condition
	let join_conditions: Vec<String> = keys.iter()
		.map(|k| format!("l.\"{}\" = r.\"{}\"", k, k))
		.collect();
	let join_clause = join_conditions.join(" AND ");

	// Get all columns
	let left_schema = left_df.schema();
	let mut select_cols = Vec::new();

	// Add key columns first
	for key in keys {
		select_cols.push(format!("COALESCE(l.\"{0}\", r.\"{0}\") as \"{0}\"", key));
	}

	// Add a status column to indicate the type of difference
	select_cols.push(
		"CASE \
			WHEN l.\"{}\" IS NULL THEN 'ADDED' \
			WHEN r.\"{}\" IS NULL THEN 'REMOVED' \
			ELSE 'MODIFIED' \
		END as diff_status".replace("{}", keys.first().unwrap())
	);

	// Add all non-key columns with left/right prefixes
	for field in left_schema.fields() {
		if !keys.contains(field.name()) {
			select_cols.push(format!("l.\"{}\" as \"left_{}\"", field.name(), field.name()));
			select_cols.push(format!("r.\"{}\" as \"right_{}\"", field.name(), field.name()));
		}
	}

	// Build the full outer join query
	let sql = format!(
		"SELECT {} FROM left_table l FULL OUTER JOIN right_table r ON {}",
		select_cols.join(", "),
		join_clause
	);

	let mut result_df = ctx.sql(&sql).await?;

	// Apply filters based on flags
	if args.left_only {
		result_df = result_df.filter(col("diff_status").eq(lit("REMOVED")))?;
	} else if args.right_only {
		result_df = result_df.filter(col("diff_status").eq(lit("ADDED")))?;
	} else if args.changes_only {
		result_df = result_df.filter(col("diff_status").not_eq(lit("UNCHANGED")))?;
	}

	Ok(result_df)
}

async fn perform_row_diff(
	left_df: &DataFrame,
	right_df: &DataFrame,
	args: &DiffArgs,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(args.common.jobs).await?;

	// Add row numbers to both dataframes
	ctx.register_table("left_table", left_df.clone().into_view())?;
	ctx.register_table("right_table", right_df.clone().into_view())?;

	let left_with_row = ctx.sql(
		"SELECT ROW_NUMBER() OVER () as row_num, * FROM left_table"
	).await?;

	let right_with_row = ctx.sql(
		"SELECT ROW_NUMBER() OVER () as row_num, * FROM right_table"
	).await?;

	ctx.deregister_table("left_table")?;
	ctx.deregister_table("right_table")?;
	ctx.register_table("left_numbered", left_with_row.into_view())?;
	ctx.register_table("right_numbered", right_with_row.into_view())?;

	let left_schema = left_df.schema();
	let mut select_cols = vec!["COALESCE(l.row_num, r.row_num) as row_num".to_string()];

	// Add status column
	select_cols.push(
		"CASE \
			WHEN l.row_num IS NULL THEN 'ADDED' \
			WHEN r.row_num IS NULL THEN 'REMOVED' \
			ELSE 'EXISTS' \
		END as diff_status".to_string()
	);

	// Add columns with left/right prefixes
	for field in left_schema.fields() {
		select_cols.push(format!("l.\"{}\" as \"left_{}\"", field.name(), field.name()));
		select_cols.push(format!("r.\"{}\" as \"right_{}\"", field.name(), field.name()));
	}

	let sql = format!(
		"SELECT {} FROM left_numbered l FULL OUTER JOIN right_numbered r ON l.row_num = r.row_num",
		select_cols.join(", ")
	);

	let mut result_df = ctx.sql(&sql).await?;

	// Apply filters
	if args.left_only {
		result_df = result_df.filter(col("diff_status").eq(lit("REMOVED")))?;
	} else if args.right_only {
		result_df = result_df.filter(col("diff_status").eq(lit("ADDED")))?;
	}

	Ok(result_df)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_diff_args_parsing() {
		let args = DiffArgs {
			common: CommonArgs {
				input: PathBuf::from("left.parquet"),
				output: None,
				format: None,
				random: None,				batch_size: None,
				jobs: None,
                table: false,				verbose: false,
			},
			compare: PathBuf::from("right.parquet"),
			keys: Some("id".to_string()),
			changes_only: false,
			left_only: false,
			right_only: false,
		};

		assert_eq!(args.compare, PathBuf::from("right.parquet"));
		assert_eq!(args.keys, Some("id".to_string()));
		assert!(!args.changes_only);
		assert!(!args.left_only);
		assert!(!args.right_only);
	}

	#[test]
	fn test_diff_args_with_multiple_keys() {
		let args = DiffArgs {
			common: CommonArgs {
				input: PathBuf::from("data1.csv"),
				output: Some(PathBuf::from("diff_result.parquet")),
				format: Some(crate::cli::OutputFormat::Parquet),
				random: None,				batch_size: None,
				jobs: Some(4),
                table: false,				verbose: true,
			},
			compare: PathBuf::from("data2.csv"),
			keys: Some("user_id,timestamp".to_string()),
			changes_only: true,
			left_only: false,
			right_only: false,
		};

		assert_eq!(args.keys, Some("user_id,timestamp".to_string()));
		assert!(args.changes_only);
		assert!(args.common.verbose);
	}

	#[test]
	fn test_diff_args_left_only() {
		let args = DiffArgs {
			common: CommonArgs {
				input: PathBuf::from("old.parquet"),
				output: None,
				format: None,
				random: None,				batch_size: None,
				jobs: None,
                table: false,				verbose: false,
			},
			compare: PathBuf::from("new.parquet"),
			keys: None,
			changes_only: false,
			left_only: true,
			right_only: false,
		};

		assert!(args.left_only);
		assert!(!args.right_only);
		assert!(!args.changes_only);
		assert_eq!(args.keys, None);
	}

	#[test]
	fn test_diff_args_right_only() {
		let args = DiffArgs {
			common: CommonArgs {
				input: PathBuf::from("old.json"),
				output: None,
				format: None,
				random: None,				batch_size: None,
				jobs: None,
                table: false,				verbose: false,
			},
			compare: PathBuf::from("new.json"),
			keys: Some("id".to_string()),
			changes_only: false,
			left_only: false,
			right_only: true,
		};

		assert!(!args.left_only);
		assert!(args.right_only);
		assert!(!args.changes_only);
	}

	#[test]
	fn test_diff_args_clone() {
		let args = DiffArgs {
			common: CommonArgs {
				input: PathBuf::from("test1.parquet"),
				output: None,
				format: None,
				random: None,				batch_size: None,
				jobs: None,
                table: false,				verbose: false,
			},
			compare: PathBuf::from("test2.parquet"),
			keys: Some("key_col".to_string()),
			changes_only: true,
			left_only: false,
			right_only: false,
		};

		let cloned = args.clone();
		assert_eq!(args.compare, cloned.compare);
		assert_eq!(args.keys, cloned.keys);
		assert_eq!(args.changes_only, cloned.changes_only);
		assert_eq!(args.left_only, cloned.left_only);
		assert_eq!(args.right_only, cloned.right_only);
	}
}
