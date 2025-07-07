use clap::Args;
use datafusion::prelude::*;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct ShuffleArgs {
	#[command(flatten)]
	pub common: CommonArgs,
}

pub async fn execute(args: ShuffleArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	
	if args.common.verbose {
		let total_rows = df.clone().count().await?;
		args.common.log_if_verbose(&format!("Shuffling {} rows", total_rows));
	}
	
	let shuffled_df = shuffle_dataframe(&df, args.common.random, args.common.jobs).await?;
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&shuffled_df, "shuffle").await?;
	
	Ok(())
}

async fn shuffle_dataframe(df: &DataFrame, _seed: Option<u64>, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	ctx.register_table("temp_table", df.clone().into_view())?;

	// Simple shuffling using ORDER BY RANDOM() 
	// For now, ignore the seed parameter since DataFusion's RANDOM() doesn't support seeding reliably
	let sql = "SELECT * FROM temp_table ORDER BY RANDOM()";
	
	let result = ctx.sql(sql).await?;
	Ok(result)
}