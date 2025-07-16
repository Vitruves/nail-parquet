use clap::Args;
use datafusion::prelude::*;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use arrow::array::Array;

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

async fn shuffle_dataframe(df: &DataFrame, seed: Option<u64>, jobs: Option<usize>) -> NailResult<DataFrame> {
	use rand::prelude::*;
	
	if let Some(seed_val) = seed {
		// For seeded shuffling, collect the data and shuffle it manually
		let batches = df.clone().collect().await?;
		let mut all_rows = Vec::new();
		
		// Collect all rows
		for batch in batches {
			for row_idx in 0..batch.num_rows() {
				all_rows.push((batch.clone(), row_idx));
			}
		}
		
		// Shuffle with the seed
		let mut rng = StdRng::seed_from_u64(seed_val);
		all_rows.shuffle(&mut rng);
		
		// Create new batches from shuffled rows
		if let Some((first_batch, _)) = all_rows.first() {
			let schema = first_batch.schema();
			let mut columns: Vec<std::sync::Arc<dyn Array>> = Vec::new();
			
			// Initialize column builders
			for field in schema.fields() {
				match field.data_type() {
					datafusion::arrow::datatypes::DataType::Int64 => {
						let mut builder = datafusion::arrow::array::Int64Builder::new();
						for (batch, row_idx) in &all_rows {
							let array = batch.column_by_name(field.name()).unwrap();
							let int_array = array.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
							if int_array.is_null(*row_idx) {
								builder.append_null();
							} else {
								builder.append_value(int_array.value(*row_idx));
							}
						}
						columns.push(std::sync::Arc::new(builder.finish()));
					},
					datafusion::arrow::datatypes::DataType::Float64 => {
						let mut builder = datafusion::arrow::array::Float64Builder::new();
						for (batch, row_idx) in &all_rows {
							let array = batch.column_by_name(field.name()).unwrap();
							let float_array = array.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();
							if float_array.is_null(*row_idx) {
								builder.append_null();
							} else {
								builder.append_value(float_array.value(*row_idx));
							}
						}
						columns.push(std::sync::Arc::new(builder.finish()));
					},
					datafusion::arrow::datatypes::DataType::Utf8 => {
						let mut builder = datafusion::arrow::array::StringBuilder::new();
						for (batch, row_idx) in &all_rows {
							let array = batch.column_by_name(field.name()).unwrap();
							let string_array = array.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
							if string_array.is_null(*row_idx) {
								builder.append_null();
							} else {
								builder.append_value(string_array.value(*row_idx));
							}
						}
						columns.push(std::sync::Arc::new(builder.finish()));
					},
					_ => {
						// For other types, just copy the values as-is
						let mut values = Vec::new();
						for (batch, row_idx) in &all_rows {
							let array = batch.column_by_name(field.name()).unwrap();
							values.push(array.slice(*row_idx, 1));
						}
						// Concatenate all the slices
						let arrays: Vec<&dyn Array> = values.iter().map(|a| a.as_ref()).collect();
						columns.push(std::sync::Arc::new(datafusion::arrow::compute::concat(&arrays).unwrap()));
					}
				}
			}
			
			// Create new record batch
			let new_batch = datafusion::arrow::record_batch::RecordBatch::try_new(schema, columns)?;
			
			// Convert back to DataFrame
			let ctx = crate::utils::create_context_with_jobs(jobs).await?;
			let df = ctx.read_batch(new_batch)?;
			Ok(df)
		} else {
			// Empty dataframe
			Ok(df.clone())
		}
	} else {
		// For non-seeded shuffling, use DataFusion's RANDOM()
		let ctx = crate::utils::create_context_with_jobs(jobs).await?;
		ctx.register_table("temp_table", df.clone().into_view())?;
		let result = ctx.sql("SELECT * FROM temp_table ORDER BY RANDOM()").await?;
		Ok(result)
	}
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