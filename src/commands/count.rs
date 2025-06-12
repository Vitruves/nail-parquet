use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;

#[derive(Args, Clone)]
pub struct CountArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: CountArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let row_count = df.clone().count().await?;
	
	if args.verbose {
		eprintln!("Counted {} rows", row_count);
	}
	
	// Output the count
	if let Some(output_path) = &args.output {
		match args.format.as_ref().unwrap_or(&crate::cli::OutputFormat::Text) {
			crate::cli::OutputFormat::Json => {
				let json_output = format!(r#"{{"row_count": {}}}"#, row_count);
				std::fs::write(output_path, json_output)?;
			},
			crate::cli::OutputFormat::Csv => {
				let csv_output = format!("row_count\n{}", row_count);
				std::fs::write(output_path, csv_output)?;
			},
			_ => {
				std::fs::write(output_path, row_count.to_string())?;
			}
		}
		if args.verbose {
			eprintln!("Count written to: {}", output_path.display());
		}
	} else {
		println!("{}", row_count);
	}
	
	Ok(())
} 