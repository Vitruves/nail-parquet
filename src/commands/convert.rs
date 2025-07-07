use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::{read_data, write_data};
use crate::utils::{detect_file_format};

#[derive(Args, Clone)]
pub struct ConvertArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Output file.\n\
	                           Supported output formats:\n\
	                           • Parquet (.parquet)\n\
	                           • CSV (.csv)\n\
	                           • JSON (.json)\n\
	                           • Excel (.xlsx) - write support")]
	pub output: PathBuf,
	
	#[arg(long, help = "Random seed for reproducible results")]
	pub random: Option<u64>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
}

pub async fn execute(args: ConvertArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Converting {} to {}", args.input.display(), args.output.display());
	}
	
	let input_format = detect_file_format(&args.input)?;
	let output_format = detect_file_format(&args.output)?;
	
	if args.verbose {
		eprintln!("Input format: {:?}, Output format: {:?}", input_format, output_format);
	}
	
	let df = read_data(&args.input).await?;
	
	let rows = df.clone().count().await?;
	let cols = df.schema().fields().len();
	if args.verbose {
		eprintln!("Processing {} rows, {} columns", rows, cols);
	}
	
	write_data(&df, &args.output, Some(&output_format)).await?;
	
	if args.verbose {
		eprintln!("Conversion completed successfully");
	}
	
	Ok(())
}