use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::{read_data, write_data};
use crate::utils::{detect_file_format};

#[derive(Args, Clone)]
pub struct ConvertArgs {
	#[arg(help = "Input file.\n\
	              Supported input formats:\n\
	              • Parquet (.parquet)\n\
	              • CSV (.csv)\n\
	              • JSON (.json)\n\
	              • Excel (.xlsx)")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Output file.\n\
	                           Supported output formats:\n\
	                           • Parquet (.parquet)\n\
	                           • CSV (.csv)\n\
	                           • JSON (.json)\n\
	                           • Excel (.xlsx) - write support")]
	pub output: PathBuf,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
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
	
	if args.verbose {
		let rows = df.clone().count().await?;
		let cols = df.schema().fields().len();
		eprintln!("Processing {} rows, {} columns", rows, cols);
	}
	
	write_data(&df, &args.output, Some(&output_format)).await?;
	
	// Conversion completed successfully
	
	if args.verbose {
		eprintln!("Conversion completed successfully");
	}
	
	Ok(())
}