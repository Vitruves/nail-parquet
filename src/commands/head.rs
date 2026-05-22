use clap::Args;

use crate::cli::CommonArgs;
use crate::error::NailResult;
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail head data.parquet -n 10
  cat data.csv | nail head -")]
pub struct HeadArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Number of rows to display", default_value = "5")]
	pub number: usize,
}

pub async fn execute(args: HeadArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let limited_df = df.limit(0, Some(args.number))?;

	args.common
		.log_if_verbose(&format!("Displaying first {} rows", args.number));

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&limited_df, "head").await?;

	Ok(())
}
