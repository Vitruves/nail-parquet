use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;
use crate::commands::select::{select_columns_by_pattern, parse_row_specification};

#[derive(Args, Clone)]
pub struct DropArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Column names or regex patterns to drop (comma-separated)")]
	pub columns: Option<String>,
	
	#[arg(short, long, help = "Row numbers or ranges to drop (e.g., 1,3,5-10)")]
	pub rows: Option<String>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: DropArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let mut result_df = df;
	
	if let Some(col_spec) = &args.columns {
		let schema = result_df.schema();
		let columns_to_drop = select_columns_by_pattern(schema.clone().into(), col_spec)?;
		
		if args.verbose {
			eprintln!("Dropping {} columns: {:?}", columns_to_drop.len(), columns_to_drop);
		}
		
		let remaining_columns: Vec<Expr> = result_df.schema().fields().iter()
			.filter(|f| !columns_to_drop.contains(f.name()))
			.map(|f| col(f.name()))
			.collect();
		
		result_df = result_df.select(remaining_columns)?;
	}
	
	if let Some(row_spec) = &args.rows {
		let row_indices = parse_row_specification(row_spec)?;
		
		if args.verbose {
			eprintln!("Dropping {} rows", row_indices.len());
		}
		
		result_df = drop_rows_by_indices(&result_df, &row_indices).await?;
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

async fn drop_rows_by_indices(df: &DataFrame, indices: &[usize]) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let indices_str = indices.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");
	
	let sql = format!(
		"SELECT {} FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn NOT IN ({})",
		df.schema().fields().iter()
			.map(|f| f.name())
			.cloned()
			.collect::<Vec<_>>()
			.join(", "),
		table_name, 
		indices_str
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}