use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::utils::stats::{calculate_basic_stats, calculate_exhaustive_stats, calculate_hypothesis_tests, select_columns_by_pattern};
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct StatsArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Comma-separated column names or regex patterns")]
	pub columns: Option<String>,
	
	#[arg(short = 't', long, help = "Statistics type", value_enum, default_value = "basic")]
	pub stats_type: StatsType,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum StatsType {
	Basic,
	Exhaustive,
	Hypothesis,
}

pub async fn execute(args: StatsArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let schema = df.schema();
	
	let target_columns = if let Some(col_spec) = &args.columns {
		select_columns_by_pattern(schema.clone().into(), col_spec)?
	} else {
		schema.fields().iter().map(|f| f.name().clone()).collect()
	};
	
	args.common.log_if_verbose(&format!("Computing {:?} statistics for {} columns", args.stats_type, target_columns.len()));
	
	let stats_df = match args.stats_type {
		StatsType::Basic => calculate_basic_stats(&df, &target_columns).await?,
		StatsType::Exhaustive => calculate_exhaustive_stats(&df, &target_columns).await?,
		StatsType::Hypothesis => calculate_hypothesis_tests(&df, &target_columns).await?,
	};
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&stats_df, "stats").await?;
	
	// Print overall row count for basic stats when outputting to console
	if args.common.output.is_none() && args.common.format.is_none() {
		let total_rows = read_data(&args.common.input).await?.clone().count().await?;
		println!("count | {}", total_rows);
	}
	
	Ok(())
}