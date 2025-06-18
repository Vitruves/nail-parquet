use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::format::display_dataframe;
use crate::utils::parquet_utils::{get_parquet_row_count_fast, can_use_fast_metadata};

#[derive(Args, Clone)]
pub struct TailArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Number of rows to display", default_value = "5")]
	pub number: usize,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: TailArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}

	// Optimize for Parquet files by using metadata for row counting
	if can_use_fast_metadata(&args.input) {
		execute_parquet_optimized(args).await
	} else {
		execute_fallback(args).await
	}
}

async fn execute_parquet_optimized(args: TailArgs) -> NailResult<()> {
	use datafusion::prelude::*;
	use crate::utils::create_context;
	
	// Fast row count using Parquet metadata
	let total_rows = get_parquet_row_count_fast(&args.input).await?;
	
	if args.verbose {
		eprintln!("Total rows (from metadata): {}", total_rows);
	}
	
	if total_rows <= args.number {
		// Read all data if we need all rows anyway
		let df = read_data(&args.input).await?;
		display_dataframe(&df, args.output.as_deref(), args.format.as_ref()).await?;
	} else {
		// Use optimized limit/offset for tail operation
		let ctx = create_context().await?;
		let df = ctx.read_parquet(args.input.to_str().unwrap(), ParquetReadOptions::default()).await
			.map_err(crate::error::NailError::DataFusion)?;
		
		let skip_rows = total_rows - args.number;
		let tail_df = df.limit(skip_rows, Some(args.number)).map_err(crate::error::NailError::DataFusion)?;
		
		if args.verbose {
			eprintln!("Displaying last {} rows (total: {})", args.number, total_rows);
		}
		
		display_dataframe(&tail_df, args.output.as_deref(), args.format.as_ref()).await?;
	}
	
	Ok(())
}

async fn execute_fallback(args: TailArgs) -> NailResult<()> {
	let df = read_data(&args.input).await?;
	let total_rows = df.clone().count().await.map_err(crate::error::NailError::DataFusion)?;
	
	if total_rows <= args.number {
		display_dataframe(&df, args.output.as_deref(), args.format.as_ref()).await?;
	} else {
		let skip_rows = total_rows - args.number;
		let tail_df = df.limit(skip_rows, Some(args.number)).map_err(crate::error::NailError::DataFusion)?;
		
		if args.verbose {
			eprintln!("Displaying last {} rows (total: {})", args.number, total_rows);
		}
		
		display_dataframe(&tail_df, args.output.as_deref(), args.format.as_ref()).await?;
	}
	
	Ok(())
}