use clap::Args;

use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct HeadArgs {
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

pub async fn execute(args: HeadArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let limited_df = df.limit(0, Some(args.number))?;
	
	if args.verbose {
		eprintln!("Displaying first {} rows", args.number);
	}
	
	display_dataframe(&limited_df, args.output.as_deref(), args.format.as_ref()).await?;
	
	Ok(())
}