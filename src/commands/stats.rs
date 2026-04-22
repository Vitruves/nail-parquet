use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use crate::utils::stats::{calculate_basic_stats, calculate_exhaustive_stats, calculate_custom_stats, calculate_hypothesis_tests, select_columns_by_pattern};
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct StatsArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Comma-separated column names or regex patterns")]
	pub columns: Option<String>,

	#[arg(short = 't', long, help = "Statistics type", value_enum, default_value = "basic")]
	pub stats_type: StatsType,

	#[arg(long, help = "Include only numeric columns")]
	pub numeric_only: bool,

	#[arg(long, help = "Include only categorical (string) columns")]
	pub categorical_only: bool,

	#[arg(short, long, help = "Custom percentiles (comma-separated, e.g., '0.1,0.5,0.9')")]
	pub percentiles: Option<String>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum StatsType {
	Basic,
	Exhaustive,
	Hypothesis,
}

pub async fn execute(args: StatsArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

	let df = read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let schema = df.schema();

	// Get target columns based on pattern
	let mut target_columns = if let Some(col_spec) = &args.columns {
		select_columns_by_pattern(schema.clone().into(), col_spec)?
	} else {
		schema.fields().iter().map(|f| f.name().clone()).collect()
	};

	// Filter by column type if requested
	if args.numeric_only || args.categorical_only {
		target_columns = filter_columns_by_type(&df, &target_columns, args.numeric_only, args.categorical_only)?;
	}

	args.common.log_if_verbose(&format!("Computing {:?} statistics for {} columns", args.stats_type, target_columns.len()));

	// Use custom stats if percentiles specified
	let stats_df = if let Some(ref percentile_str) = args.percentiles {
		let percentiles = parse_percentiles(percentile_str)?;
		calculate_custom_stats(&df, &target_columns, &percentiles, args.categorical_only).await?
	} else {
		match args.stats_type {
			StatsType::Basic => calculate_basic_stats(&df, &target_columns).await?,
			StatsType::Exhaustive => calculate_exhaustive_stats(&df, &target_columns).await?,
			StatsType::Hypothesis => calculate_hypothesis_tests(&df, &target_columns).await?,
		}
	};

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&stats_df, "stats").await?;

	// Print overall row count for basic stats when outputting to console
	if args.common.output.is_none() && args.common.format.is_none() {
		let total_rows = df.clone().count().await?;
		println!("count | {}", total_rows);
	}

	Ok(())
}

fn parse_percentiles(percentile_str: &str) -> NailResult<Vec<f64>> {
	percentile_str
		.split(',')
		.map(|s| {
			s.trim()
				.parse::<f64>()
				.map_err(|_| crate::error::NailError::InvalidArgument(
					format!("Invalid percentile value: {}", s)
				))
		})
		.collect()
}

fn filter_columns_by_type(
	df: &datafusion::prelude::DataFrame,
	columns: &[String],
	numeric_only: bool,
	categorical_only: bool,
) -> NailResult<Vec<String>> {
	use datafusion::arrow::datatypes::DataType;

	let mut filtered = Vec::new();

	for col in columns {
		let field = df.schema().field_with_name(None, col);
		if field.is_err() {
			continue;
		}

		let field = field.unwrap();
		let is_numeric = matches!(
			field.data_type(),
			DataType::Int64 | DataType::Float64 | DataType::Int32 | DataType::Float32
				| DataType::Int16 | DataType::Int8 | DataType::UInt64 | DataType::UInt32
				| DataType::UInt16 | DataType::UInt8
		);

		let is_string = matches!(
			field.data_type(),
			DataType::Utf8 | DataType::LargeUtf8
		);

		let include = (numeric_only && is_numeric)
			|| (categorical_only && is_string)
			|| (!numeric_only && !categorical_only);
		if include {
			filtered.push(col.clone());
		}
	}

	Ok(filtered)
}