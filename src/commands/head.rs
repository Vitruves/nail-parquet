use clap::Args;

use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct HeadArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Number of rows to display", default_value = "5")]
	pub number: usize,
}

pub async fn execute(args: HeadArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let limited_df = df.limit(0, Some(args.number))?;
	
	args.common.log_if_verbose(&format!("Displaying first {} rows", args.number));
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&limited_df, "head").await?;
	
	Ok(())
}