use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct MergeArgs {
	#[arg(short, long, help = "Input file (left table)")]
	pub input: PathBuf,
	
	#[arg(long, help = "Right table file to merge with")]
	pub right: PathBuf,
	
	#[arg(long, help = "Perform left join")]
	pub left_join: bool,
	
	#[arg(long, help = "Perform right join")]
	pub right_join: bool,
	
	#[arg(long, help = "Join key column name")]
	pub key: Option<String>,
	
	#[arg(long, help = "Key mapping for different column names (format: left_col=right_col)")]
	pub key_mapping: Option<String>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: MergeArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading left table from: {}", args.input.display());
		eprintln!("Reading right table from: {}", args.right.display());
	}
	
	let left_df = read_data(&args.input).await?;
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
	
	if args.verbose {
		eprintln!("Performing {:?} join on left.{} = right.{}", join_type, left_key, right_key);
	}
	
	let result_df = perform_join(&left_df, &right_df, &left_key, &right_key, join_type).await?;
	
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
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	
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