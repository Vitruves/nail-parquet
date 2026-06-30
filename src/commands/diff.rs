use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, read_data_with_opts};
use crate::utils::output::OutputHandler;
use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail diff before.parquet -c after.parquet
  nail diff a.csv -c b.csv -k id")]
pub struct DiffArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Second file to compare with")]
	pub compare: PathBuf,

	#[arg(
		short,
		long,
		help = "Columns to use as primary key for comparison (comma-separated)"
	)]
	pub keys: Option<String>,

	#[arg(long, help = "Show only rows that differ")]
	pub changes_only: bool,

	#[arg(long, help = "Show only rows in left file")]
	pub left_only: bool,

	#[arg(long, help = "Show only rows in right file")]
	pub right_only: bool,
}

pub async fn execute(args: DiffArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading left file from: {}",
		args.common.input.display()
	));
	args.common.log_if_verbose(&format!(
		"Reading right file from: {}",
		args.compare.display()
	));

	let left_df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let right_df = read_data(&args.compare).await?;

	// Validate schemas are compatible
	let left_schema = left_df.schema();
	let right_schema = right_df.schema();

	if left_schema.fields().len() != right_schema.fields().len() {
		return Err(NailError::InvalidArgument(format!(
			"Schema mismatch: left has {} columns, right has {} columns",
			left_schema.fields().len(),
			right_schema.fields().len()
		)));
	}

	let diff_df = if let Some(key_cols) = &args.keys {
		// Key-based comparison
		let keys: Vec<String> = key_cols.split(',').map(|s| s.trim().to_string()).collect();
		args.common
			.log_if_verbose(&format!("Comparing using key columns: {:?}", keys));

		perform_keyed_diff(&left_df, &right_df, &keys, &args).await?
	} else {
		// Row-based comparison (by position)
		args.common
			.log_if_verbose("Performing row-by-row comparison");
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
	let join_conditions: Vec<String> = keys
		.iter()
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

	// Build a per-column inequality test across the non-key columns so we can
	// distinguish a genuinely MODIFIED row from one that matched on its key but
	// is otherwise identical. `IS DISTINCT FROM` treats NULLs as comparable
	// values (NULL vs non-NULL counts as different; NULL vs NULL does not).
	let non_key_cols: Vec<&String> = left_schema
		.fields()
		.iter()
		.map(|f| f.name())
		.filter(|n| !keys.contains(*n))
		.collect();
	let modified_predicate = if non_key_cols.is_empty() {
		// No non-key columns to compare: a key match is always UNCHANGED.
		"FALSE".to_string()
	} else {
		non_key_cols
			.iter()
			.map(|n| format!("(l.\"{0}\" IS DISTINCT FROM r.\"{0}\")", n))
			.collect::<Vec<_>>()
			.join(" OR ")
	};

	// Add a status column to indicate the type of difference
	select_cols.push(format!(
		"CASE \
			WHEN l.\"{key}\" IS NULL THEN 'ADDED' \
			WHEN r.\"{key}\" IS NULL THEN 'REMOVED' \
			WHEN {pred} THEN 'MODIFIED' \
			ELSE 'UNCHANGED' \
		END as diff_status",
		key = keys.first().unwrap(),
		pred = modified_predicate,
	));

	// Add all non-key columns with left/right prefixes
	for field in left_schema.fields() {
		if !keys.contains(field.name()) {
			select_cols.push(format!(
				"l.\"{}\" as \"left_{}\"",
				field.name(),
				field.name()
			));
			select_cols.push(format!(
				"r.\"{}\" as \"right_{}\"",
				field.name(),
				field.name()
			));
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

	let left_with_row = ctx
		.sql("SELECT ROW_NUMBER() OVER () as row_num, * FROM left_table")
		.await?;

	let right_with_row = ctx
		.sql("SELECT ROW_NUMBER() OVER () as row_num, * FROM right_table")
		.await?;

	ctx.deregister_table("left_table")?;
	ctx.deregister_table("right_table")?;
	ctx.register_table("left_numbered", left_with_row.into_view())?;
	ctx.register_table("right_numbered", right_with_row.into_view())?;

	let left_schema = left_df.schema();
	let mut select_cols = vec!["COALESCE(l.row_num, r.row_num) as row_num".to_string()];

	// Compare every data column for rows that line up by position, so a row
	// present in both sides is reported as MODIFIED only when a value actually
	// differs (NULL-aware via `IS DISTINCT FROM`).
	let data_cols: Vec<&String> = left_schema.fields().iter().map(|f| f.name()).collect();
	let modified_predicate = if data_cols.is_empty() {
		"FALSE".to_string()
	} else {
		data_cols
			.iter()
			.map(|n| format!("(l.\"{0}\" IS DISTINCT FROM r.\"{0}\")", n))
			.collect::<Vec<_>>()
			.join(" OR ")
	};

	// Add status column
	select_cols.push(format!(
		"CASE \
			WHEN l.row_num IS NULL THEN 'ADDED' \
			WHEN r.row_num IS NULL THEN 'REMOVED' \
			WHEN {pred} THEN 'MODIFIED' \
			ELSE 'UNCHANGED' \
		END as diff_status",
		pred = modified_predicate,
	));

	// Add columns with left/right prefixes
	for field in left_schema.fields() {
		select_cols.push(format!(
			"l.\"{}\" as \"left_{}\"",
			field.name(),
			field.name()
		));
		select_cols.push(format!(
			"r.\"{}\" as \"right_{}\"",
			field.name(),
			field.name()
		));
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
	} else if args.changes_only {
		result_df = result_df.filter(col("diff_status").not_eq(lit("UNCHANGED")))?;
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
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
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
				random: None,
				batch_size: None,
				jobs: Some(4),
				table: false,
				level: 1,
				verbose: true,
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
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
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
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
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
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
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
