use clap::Args;
use datafusion::prelude::*;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct ShuffleArgs {
	#[command(flatten)]
	pub common: CommonArgs,
}

pub async fn execute(args: ShuffleArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	
	if args.common.verbose {
		let total_rows = df.clone().count().await?;
		args.common.log_if_verbose(&format!("Shuffling {} rows", total_rows));
	}
	
	let shuffled_df = shuffle_dataframe(&df, args.common.random, args.common.jobs).await?;
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&shuffled_df, "shuffle").await?;
	
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

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_shuffle_args_basic() {
		let args = ShuffleArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				jobs: None,
				verbose: false,
			},
		};

		assert_eq!(args.common.input, PathBuf::from("test.parquet"));
		assert_eq!(args.common.output, None);
		assert_eq!(args.common.random, None);
		assert!(!args.common.verbose);
	}

	#[test]
	fn test_shuffle_args_with_seed() {
		let args = ShuffleArgs {
			common: CommonArgs {
				input: PathBuf::from("data.csv"),
				output: Some(PathBuf::from("shuffled.parquet")),
				format: Some(crate::cli::OutputFormat::Parquet),
				random: Some(42),
				jobs: Some(8),
				verbose: true,
			},
		};

		assert_eq!(args.common.input, PathBuf::from("data.csv"));
		assert_eq!(args.common.output, Some(PathBuf::from("shuffled.parquet")));
		assert_eq!(args.common.random, Some(42));
		assert_eq!(args.common.jobs, Some(8));
		assert!(args.common.verbose);
	}

	#[test]
	fn test_shuffle_args_with_jobs() {
		let args = ShuffleArgs {
			common: CommonArgs {
				input: PathBuf::from("large_dataset.json"),
				output: None,
				format: Some(crate::cli::OutputFormat::Json),
				random: Some(123456),
				jobs: Some(16),
				verbose: false,
			},
		};

		assert_eq!(args.common.input, PathBuf::from("large_dataset.json"));
		assert_eq!(args.common.jobs, Some(16));
		assert_eq!(args.common.random, Some(123456));
		assert!(!args.common.verbose);
	}

	#[test]
	fn test_shuffle_args_clone() {
		let args = ShuffleArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: Some(789),
				jobs: None,
				verbose: true,
			},
		};

		let cloned = args.clone();
		assert_eq!(args.common.input, cloned.common.input);
		assert_eq!(args.common.output, cloned.common.output);
		assert_eq!(args.common.random, cloned.common.random);
		assert_eq!(args.common.verbose, cloned.common.verbose);
	}

	#[test]
	fn test_shuffle_args_different_formats() {
		let args_csv = ShuffleArgs {
			common: CommonArgs {
				input: PathBuf::from("input.csv"),
				output: Some(PathBuf::from("output.csv")),
				format: Some(crate::cli::OutputFormat::Csv),
				random: None,
				jobs: None,
				verbose: false,
			},
		};

		let args_xlsx = ShuffleArgs {
			common: CommonArgs {
				input: PathBuf::from("input.xlsx"),
				output: Some(PathBuf::from("output.xlsx")),
				format: Some(crate::cli::OutputFormat::Xlsx),
				random: None,
				jobs: None,
				verbose: false,
			},
		};

		assert!(matches!(args_csv.common.format, Some(crate::cli::OutputFormat::Csv)));
		assert!(matches!(args_xlsx.common.format, Some(crate::cli::OutputFormat::Xlsx)));
	}
}