use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::parquet_utils::{get_parquet_row_count_fast, can_use_fast_metadata};
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use datafusion::prelude::*;

#[derive(Args, Clone)]
pub struct CountArgs {
	#[command(flatten)]
	pub common: CommonArgs,
}

pub async fn execute(args: CountArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	// Use fast metadata reading for Parquet files
	let row_count = if can_use_fast_metadata(&args.common.input) {
		args.common.log_if_verbose("Using fast Parquet metadata for counting");
		get_parquet_row_count_fast(&args.common.input).await?
	} else {
		args.common.log_if_verbose("Using DataFusion for counting");
		let df = read_data(&args.common.input).await?;
		df.clone().count().await.map_err(crate::error::NailError::DataFusion)?
	};
	
	args.common.log_if_verbose(&format!("Counted {} rows", row_count));
	
	// For count command, output simple number to console or structured data to file
	match &args.common.output {
		Some(_output_path) => {
			// Create a DataFrame with the count result for file output
			let ctx = SessionContext::new();
			let result_df = ctx.sql(&format!("SELECT {} as row_count", row_count)).await
				.map_err(crate::error::NailError::DataFusion)?;
			
			let output_handler = OutputHandler::new(&args.common);
			output_handler.handle_output(&result_df, "count").await?;
		}
		None => {
			// Simple console output
			println!("{}", row_count);
		}
	}
	
	Ok(())
}