use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::format::display_dataframe;
use crate::utils::stats::{calculate_basic_stats, calculate_exhaustive_stats, calculate_hypothesis_tests, select_columns_by_pattern};

#[derive(Args, Clone)]
pub struct StatsArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Comma-separated column names or regex patterns")]
	pub columns: Option<String>,
	
	#[arg(short = 't', long, help = "Statistics type", value_enum, default_value = "basic")]
	pub stats_type: StatsType,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum StatsType {
	Basic,
	Exhaustive,
	Hypothesis,
}

pub async fn execute(args: StatsArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let schema = df.schema();
	
	let target_columns = if let Some(col_spec) = &args.columns {
		select_columns_by_pattern(schema.clone().into(), col_spec)?
	} else {
		schema.fields().iter().map(|f| f.name().clone()).collect()
	};
	
	if args.verbose {
		eprintln!("Computing {:?} statistics for {} columns", args.stats_type, target_columns.len());
	}
	
	let stats_df = match args.stats_type {
		StatsType::Basic => calculate_basic_stats(&df, &target_columns).await?,
		StatsType::Exhaustive => calculate_exhaustive_stats(&df, &target_columns).await?,
		StatsType::Hypothesis => calculate_hypothesis_tests(&df, &target_columns).await?,
	};
	
	display_dataframe(&stats_df, args.output.as_deref(), args.format.as_ref()).await?;
	
	// Print overall row count for basic stats when outputting to console
	if args.output.is_none() && args.format.is_none() {
		let total_rows = read_data(&args.input).await?.clone().count().await?;
		println!("count | {}", total_rows);
	}
	
	Ok(())
}