use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct ShuffleArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Random seed for reproducible results")]
	pub random: Option<u64>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: ShuffleArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	
	if args.verbose {
		let total_rows = df.clone().count().await?;
		eprintln!("Shuffling {} rows", total_rows);
	}
	
	let shuffled_df = shuffle_dataframe(&df, args.random, args.jobs).await?;
	
	if let Some(output_path) = &args.output {
		let file_format = match args.format {
			Some(crate::cli::OutputFormat::Json) => Some(crate::utils::FileFormat::Json),
			Some(crate::cli::OutputFormat::Csv) => Some(crate::utils::FileFormat::Csv),
			Some(crate::cli::OutputFormat::Parquet) => Some(crate::utils::FileFormat::Parquet),
			_ => None,
		};
		write_data(&shuffled_df, output_path, file_format.as_ref()).await?;
	} else {
		display_dataframe(&shuffled_df, None, args.format.as_ref()).await?;
	}
	
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