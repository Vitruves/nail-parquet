use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::utils::parquet_utils::{get_parquet_row_count_fast, can_use_fast_metadata};
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct TailArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Number of rows to display", default_value = "5")]
	pub number: usize,
}

pub async fn execute(args: TailArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

	// Optimize for Parquet files by using metadata for row counting
	if can_use_fast_metadata(&args.common.input) {
		execute_parquet_optimized(args).await
	} else {
		execute_fallback(args).await
	}
}

async fn execute_parquet_optimized(args: TailArgs) -> NailResult<()> {
	use datafusion::prelude::*;
	use crate::utils::create_context;
	
	// Fast row count using Parquet metadata
	let total_rows = get_parquet_row_count_fast(&args.common.input).await?;
	
	args.common.log_if_verbose(&format!("Total rows (from metadata): {}", total_rows));
	
	if total_rows <= args.number {
		// Read all data if we need all rows anyway
		let df = read_data(&args.common.input).await?;
		let output_handler = OutputHandler::new(&args.common);
		output_handler.handle_output(&df, "tail").await?;
	} else {
		// Use optimized limit/offset for tail operation
		let ctx = create_context().await?;
		let df = ctx.read_parquet(args.common.input.to_str().unwrap(), ParquetReadOptions::default()).await
			.map_err(crate::error::NailError::DataFusion)?;
		
		let skip_rows = total_rows - args.number;
		let tail_df = df.limit(skip_rows, Some(args.number)).map_err(crate::error::NailError::DataFusion)?;
		
		args.common.log_if_verbose(&format!("Displaying last {} rows (total: {})", args.number, total_rows));
		
		let output_handler = OutputHandler::new(&args.common);
		output_handler.handle_output(&tail_df, "tail").await?;
	}
	
	Ok(())
}

async fn execute_fallback(args: TailArgs) -> NailResult<()> {
	let df = read_data(&args.common.input).await?;
	let total_rows = df.clone().count().await.map_err(crate::error::NailError::DataFusion)?;
	
	let output_handler = OutputHandler::new(&args.common);
	
	if total_rows <= args.number {
		output_handler.handle_output(&df, "tail").await?;
	} else {
		let skip_rows = total_rows - args.number;
		let tail_df = df.limit(skip_rows, Some(args.number)).map_err(crate::error::NailError::DataFusion)?;
		
		args.common.log_if_verbose(&format!("Displaying last {} rows (total: {})", args.number, total_rows));
		
		output_handler.handle_output(&tail_df, "tail").await?;
	}
	
	Ok(())
}