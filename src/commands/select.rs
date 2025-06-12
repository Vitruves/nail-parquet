use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use regex::Regex;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct SelectArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Column names or regex patterns (comma-separated)")]
	pub columns: Option<String>,
	
	#[arg(short, long, help = "Row numbers or ranges (e.g., 1,3,5-10)")]
	pub rows: Option<String>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: SelectArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let mut result_df = df;
	
	if let Some(col_spec) = &args.columns {
		let schema = result_df.schema();
		let selected_columns = select_columns_by_pattern(schema.clone().into(), col_spec)?;
		
		if args.verbose {
			eprintln!("Selecting {} columns: {:?}", selected_columns.len(), selected_columns);
		}
		
		let select_exprs: Vec<Expr> = selected_columns.into_iter()
			.map(|name| col(name))
			.collect();
		
		result_df = result_df.select(select_exprs)?;
	}
	
	if let Some(row_spec) = &args.rows {
		let row_indices = parse_row_specification(row_spec)?;
		
		if args.verbose {
			eprintln!("Selecting {} rows", row_indices.len());
		}
		
		result_df = select_rows_by_indices(&result_df, &row_indices).await?;
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

pub fn select_columns_by_pattern(schema: datafusion::common::DFSchemaRef, pattern: &str) -> NailResult<Vec<String>> {
	let patterns: Vec<&str> = pattern.split(',').map(|s| s.trim()).collect();
	let mut selected = Vec::new();
	
	for field in schema.fields() {
		let field_name = field.name();
		
		for pattern in &patterns {
			if pattern.contains('*') || pattern.contains('^') || pattern.contains('$') {
				let regex = Regex::new(pattern)?;
				if regex.is_match(field_name) {
					selected.push(field_name.clone());
					break;
				}
			} else if field_name == *pattern {
				selected.push(field_name.clone());
				break;
			}
		}
	}
	
	if selected.is_empty() {
		return Err(NailError::ColumnNotFound(format!("No columns matched pattern: {}", pattern)));
	}
	
	Ok(selected)
}

pub fn parse_row_specification(spec: &str) -> NailResult<Vec<usize>> {
	let mut indices = Vec::new();
	
	for part in spec.split(',') {
		let part = part.trim();
		
		if part.contains('-') {
			let range_parts: Vec<&str> = part.split('-').collect();
			if range_parts.len() != 2 {
				return Err(NailError::InvalidArgument(format!("Invalid range: {}", part)));
			}
			
			let start: usize = range_parts[0].parse()
				.map_err(|_| NailError::InvalidArgument(format!("Invalid start index: {}", range_parts[0])))?;
			let end: usize = range_parts[1].parse()
				.map_err(|_| NailError::InvalidArgument(format!("Invalid end index: {}", range_parts[1])))?;
			
			if start > end {
				return Err(NailError::InvalidArgument(format!("Start index {} greater than end index {}", start, end)));
			}
			
			for i in start..=end {
				indices.push(i.saturating_sub(1));
			}
		} else {
			let index: usize = part.parse()
				.map_err(|_| NailError::InvalidArgument(format!("Invalid index: {}", part)))?;
			indices.push(index.saturating_sub(1));
		}
	}
	
	indices.sort();
	indices.dedup();
	Ok(indices)
}

async fn select_rows_by_indices(df: &DataFrame, indices: &[usize]) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let indices_str = indices.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");
	
	let sql = format!(
		"SELECT * FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn IN ({})",
		table_name, indices_str
	);
	
	let result = ctx.sql(&sql).await?;
	let final_result = result.select(
		df.schema().fields().iter()
			.map(|f| col(f.name()))
			.collect()
	)?;
	
	Ok(final_result)
}