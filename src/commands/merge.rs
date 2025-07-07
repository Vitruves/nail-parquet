use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct MergeArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Right table file to merge with")]
	pub right: PathBuf,
	
	#[arg(long, help = "Perform left join")]
	pub left_join: bool,
	
	#[arg(long, help = "Perform right join")]
	pub right_join: bool,
	
	#[arg(long, help = "Join key column name")]
	pub key: Option<String>,
	
	#[arg(long, help = "Key mapping for different column names (format: left_col=right_col)")]
	pub key_mapping: Option<String>,
}

pub async fn execute(args: MergeArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading left table from: {}", args.common.input.display()));
	args.common.log_if_verbose(&format!("Reading right table from: {}", args.right.display()));
	
	let left_df = read_data(&args.common.input).await?;
	let right_df = read_data(&args.right).await?;
	
	let join_type = if args.left_join {
		JoinType::Left
	} else if args.right_join {
		JoinType::Right
	} else {
		JoinType::Inner
	};
	
	let (left_key, right_key) = if let Some(key_mapping) = &args.key_mapping {
		parse_key_mapping(key_mapping)?
	} else if let Some(key) = &args.key {
		// Handle case-insensitive key matching
		let left_schema = left_df.schema();
		let right_schema = right_df.schema();
		
		let actual_left_key = left_schema.fields().iter()
			.find(|f| f.name().to_lowercase() == key.to_lowercase())
			.map(|f| f.name().clone())
			.ok_or_else(|| {
				let available_cols: Vec<String> = left_schema.fields().iter()
					.map(|f| f.name().clone())
					.collect();
				NailError::ColumnNotFound(format!(
					"Join key '{}' not found in left table. Available columns: {:?}", 
					key, available_cols
				))
			})?;
			
		let actual_right_key = right_schema.fields().iter()
			.find(|f| f.name().to_lowercase() == key.to_lowercase())
			.map(|f| f.name().clone())
			.ok_or_else(|| {
				let available_cols: Vec<String> = right_schema.fields().iter()
					.map(|f| f.name().clone())
					.collect();
				NailError::ColumnNotFound(format!(
					"Join key '{}' not found in right table. Available columns: {:?}", 
					key, available_cols
				))
			})?;
			
		(actual_left_key, actual_right_key)
	} else {
		return Err(NailError::InvalidArgument("Either --key or --key-mapping must be specified".to_string()));
	};
	
	args.common.log_if_verbose(&format!("Performing {:?} join on left.{} = right.{}", join_type, left_key, right_key));
	
	let result_df = perform_join(&left_df, &right_df, &left_key, &right_key, join_type, args.common.jobs).await?;
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "merge").await?;
	
	Ok(())
}

fn parse_key_mapping(mapping: &str) -> NailResult<(String, String)> {
	let parts: Vec<&str> = mapping.split('=').collect();
	if parts.len() != 2 {
		return Err(NailError::InvalidArgument("Key mapping must be in format 'left_col=right_col'".to_string()));
	}
	Ok((parts[0].trim().to_string(), parts[1].trim().to_string()))
}

async fn perform_join(
	left_df: &DataFrame,
	right_df: &DataFrame,
	left_key: &str,
	right_key: &str,
	join_type: JoinType,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	
	ctx.register_table("left_table", left_df.clone().into_view())?;
	ctx.register_table("right_table", right_df.clone().into_view())?;
	
	let left_schema = left_df.schema();
	let right_schema = right_df.schema();
	
	let mut left_cols = Vec::new();
	let mut right_cols = Vec::new();
	
	for field in left_schema.fields() {
		left_cols.push(format!("l.\"{}\"", field.name()));
	}
	
	for field in right_schema.fields() {
		if field.name() != right_key {
			right_cols.push(format!("r.\"{}\" as \"r_{}\"", field.name(), field.name()));
		}
	}
	
	let join_clause = match join_type {
		JoinType::Inner => "INNER JOIN",
		JoinType::Left => "LEFT JOIN",
		JoinType::Right => "RIGHT JOIN",
		_ => "INNER JOIN",
	};
	
	let sql = format!(
		"SELECT {} FROM left_table l {} right_table r ON l.\"{}\" = r.\"{}\"",
		[left_cols, right_cols].concat().join(", "),
		join_clause,
		left_key,
		right_key
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_merge_args_parsing() {
		let args = MergeArgs {
			common: CommonArgs {
				input: PathBuf::from("left.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			right: PathBuf::from("right.parquet"),
			left_join: false,
			right_join: false,
			key: Some("id".to_string()),
			key_mapping: None,
		};

		assert_eq!(args.right, PathBuf::from("right.parquet"));
		assert_eq!(args.key, Some("id".to_string()));
		assert!(!args.left_join);
		assert!(!args.right_join);
		assert_eq!(args.key_mapping, None);
	}

	#[test]
	fn test_merge_args_with_left_join() {
		let args = MergeArgs {
			common: CommonArgs {
				input: PathBuf::from("table1.csv"),
				output: Some(PathBuf::from("merged.parquet")),
				format: Some(crate::cli::OutputFormat::Parquet),
				random: Some(123),
				jobs: Some(8),
				verbose: true,
			},
			right: PathBuf::from("table2.csv"),
			left_join: true,
			right_join: false,
			key: None,
			key_mapping: Some("user_id=id".to_string()),
		};

		assert_eq!(args.right, PathBuf::from("table2.csv"));
		assert!(args.left_join);
		assert!(!args.right_join);
		assert_eq!(args.key, None);
		assert_eq!(args.key_mapping, Some("user_id=id".to_string()));
		assert_eq!(args.common.jobs, Some(8));
		assert!(args.common.verbose);
	}

	#[test]
	fn test_merge_args_with_right_join() {
		let args = MergeArgs {
			common: CommonArgs {
				input: PathBuf::from("data.json"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			right: PathBuf::from("lookup.json"),
			left_join: false,
			right_join: true,
			key: Some("product_id".to_string()),
			key_mapping: None,
		};

		assert_eq!(args.right, PathBuf::from("lookup.json"));
		assert!(!args.left_join);
		assert!(args.right_join);
		assert_eq!(args.key, Some("product_id".to_string()));
		assert_eq!(args.key_mapping, None);
	}

	#[test]
	fn test_parse_key_mapping_valid() {
		let result = parse_key_mapping("left_col=right_col");
		assert!(result.is_ok());
		let (left, right) = result.unwrap();
		assert_eq!(left, "left_col");
		assert_eq!(right, "right_col");
	}

	#[test]
	fn test_parse_key_mapping_with_spaces() {
		let result = parse_key_mapping("  left_col  =  right_col  ");
		assert!(result.is_ok());
		let (left, right) = result.unwrap();
		assert_eq!(left, "left_col");
		assert_eq!(right, "right_col");
	}

	#[test]
	fn test_parse_key_mapping_invalid() {
		let result = parse_key_mapping("invalid_format");
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("Key mapping must be in format"));
	}

	#[test]
	fn test_parse_key_mapping_empty() {
		let result = parse_key_mapping("");
		assert!(result.is_err());
	}

	#[test]
	fn test_merge_args_clone() {
		let args = MergeArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
			right: PathBuf::from("right.parquet"),
			left_join: true,
			right_join: false,
			key: Some("id".to_string()),
			key_mapping: None,
		};

		let cloned = args.clone();
		assert_eq!(args.right, cloned.right);
		assert_eq!(args.left_join, cloned.left_join);
		assert_eq!(args.right_join, cloned.right_join);
		assert_eq!(args.key, cloned.key);
		assert_eq!(args.key_mapping, cloned.key_mapping);
	}
}