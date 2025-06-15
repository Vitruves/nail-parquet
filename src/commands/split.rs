use clap::Args;
use std::path::PathBuf;
use std::collections::HashMap;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};

#[derive(Args, Clone)]
pub struct SplitArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(long, help = "Split ratios (e.g., '0.7,0.2,0.1' or '70,20,10')")]
	pub ratio: String,
	
	#[arg(long, help = "Output file names (comma-separated)")]
	pub names: Option<String>,
	
	#[arg(long, help = "Prefix for auto-generated split file names", default_value = "split")]
	pub splits_prefix: String,
	
	#[arg(long, help = "Output directory for split files", default_value = ".")]
	pub output_dir: PathBuf,
	
	#[arg(long, help = "Column for stratified splitting")]
	pub stratified_by: Option<String>,
	
	#[arg(short, long, help = "Random seed for reproducible splits")]
	pub random: Option<u64>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: SplitArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	// Create output directory if it doesn't exist
	if !args.output_dir.exists() {
		std::fs::create_dir_all(&args.output_dir)?;
		if args.verbose {
			eprintln!("Created output directory: {}", args.output_dir.display());
		}
	}
	
	let df = read_data(&args.input).await?;
	let total_rows = df.clone().count().await?;
	
	let ratios = parse_ratios(&args.ratio)?;
	let file_format = determine_output_format(&args.format, &args.input);
	let extension = get_extension_for_format(&file_format);
	
	let output_names = if let Some(names) = &args.names {
		let base_names = parse_names(names)?;
		// Add extensions and output directory to the provided names
		base_names.into_iter()
			.map(|name| {
				let name_with_ext = if name.contains('.') {
					name // Keep existing extension
				} else {
					format!("{}.{}", name, extension) // Add extension
				};
				args.output_dir.join(name_with_ext)
			})
			.collect()
	} else {
		generate_names(&args.splits_prefix, ratios.len(), &args.input, &args.output_dir, &extension)
	};
	
	if ratios.len() != output_names.len() {
		return Err(NailError::InvalidArgument(
			format!("Number of ratios ({}) must match number of names ({})", 
				ratios.len(), output_names.len())
		));
	}
	
	if let Some(stratify_col) = &args.stratified_by {
		if args.verbose {
			eprintln!("Performing stratified split by column '{}' with ratios: {:?}", 
				stratify_col, ratios);
		}
		stratified_split(&df, &ratios, &output_names, stratify_col, args.random, &file_format, args.verbose, args.jobs).await?;
	} else {
		if args.verbose {
			eprintln!("Splitting {} rows into {} parts with ratios: {:?}", 
				total_rows, ratios.len(), ratios);
		}
		random_split(&df, &ratios, &output_names, args.random, &file_format, args.verbose, args.jobs).await?;
	}
	
	if args.verbose {
		eprintln!("Split complete: {} files created in {}", output_names.len(), args.output_dir.display());
		for (i, output_name) in output_names.iter().enumerate() {
			eprintln!("  Split {}: {}", i + 1, output_name.display());
		}
	}
	
	Ok(())
}

async fn stratified_split(
	df: &datafusion::prelude::DataFrame,
	ratios: &[f64],
	output_names: &[PathBuf],
	stratify_col: &str,
	seed: Option<u64>,
	file_format: &Option<crate::utils::FileFormat>,
	verbose: bool,
	jobs: Option<usize>,
) -> NailResult<()> {
	use datafusion::prelude::*;
	
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema();
	let actual_col_name = schema.fields().iter()
		.find(|f| f.name().to_lowercase() == stratify_col.to_lowercase())
		.map(|f| f.name().clone())
		.ok_or_else(|| {
			let available_cols: Vec<String> = schema.fields().iter()
				.map(|f| f.name().clone())
				.collect();
			NailError::ColumnNotFound(format!(
				"Stratification column '{}' not found. Available columns: {:?}", 
				stratify_col, available_cols
			))
		})?;
	
	let distinct_sql = format!(
		"SELECT DISTINCT {} as category, COUNT(*) as count FROM {} WHERE {} IS NOT NULL GROUP BY {}",
		actual_col_name, table_name, actual_col_name, actual_col_name
	);
	
	let distinct_df = ctx.sql(&distinct_sql).await?;
	let categories_batches = distinct_df.collect().await?;
	
	let mut category_counts = HashMap::new();
	for batch in &categories_batches {
		let category_array = batch.column(0);
		let count_array = batch.column(1);
		
		for i in 0..batch.num_rows() {
			if !category_array.is_null(i) && !count_array.is_null(i) {
				// Handle different array types for category extraction
				let category = match category_array.data_type() {
					datafusion::arrow::datatypes::DataType::Utf8 => {
						if let Some(str_arr) = category_array.as_any().downcast_ref::<datafusion::arrow::array::StringArray>() {
							str_arr.value(i).to_string()
						} else {
							continue;
						}
					},
					datafusion::arrow::datatypes::DataType::Int64 => {
						if let Some(int_arr) = category_array.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
							int_arr.value(i).to_string()
						} else {
							continue;
						}
					},
					datafusion::arrow::datatypes::DataType::Float64 => {
						if let Some(float_arr) = category_array.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>() {
							float_arr.value(i).to_string()
						} else {
							continue;
						}
					},
					_ => {
						// Fallback to string representation
						format!("{:?}", category_array.slice(i, 1))
							.trim_start_matches('[')
							.trim_end_matches(']')
							.trim_matches('"')
							.to_string()
					}
				};
				
				if let Some(count_arr) = count_array.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
					category_counts.insert(category, count_arr.value(i) as usize);
				}
			}
		}
	}
	
	if verbose {
		eprintln!("Found {} categories for stratification:", category_counts.len());
		for (cat, count) in &category_counts {
			eprintln!("  {}: {} rows", cat, count);
		}
	}
	
	let mut split_dfs: Vec<Option<DataFrame>> = vec![None; ratios.len()];
	
	for (category, _count) in &category_counts {
		// Handle different data types for the WHERE clause
		let category_sql = if category.parse::<i64>().is_ok() {
			// Numeric category - don't quote
			format!(
				"SELECT * FROM {} WHERE {} = {}",
				table_name, actual_col_name, category
			)
		} else if category.parse::<f64>().is_ok() {
			// Float category - don't quote
			format!(
				"SELECT * FROM {} WHERE {} = {}",
				table_name, actual_col_name, category
			)
		} else {
			// String category - quote it
			format!(
				"SELECT * FROM {} WHERE {} = '{}'",
				table_name, actual_col_name, category
			)
		};
		
		let category_df = ctx.sql(&category_sql).await?;
		let category_rows = category_df.clone().count().await?;
		
		let shuffled_category = if let Some(s) = seed {
			shuffle_dataframe_with_seed(&category_df, s + category.len() as u64, jobs).await?
		} else {
			category_df
		};
		
		let mut current_offset = 0;
		for (i, ratio) in ratios.iter().enumerate() {
			let split_size = if i == ratios.len() - 1 {
				category_rows - current_offset
			} else {
				(category_rows as f64 * ratio).round() as usize
			};
			
			if split_size > 0 {
				let category_split = shuffled_category.clone().limit(current_offset, Some(split_size))?;
				
				split_dfs[i] = Some(match &split_dfs[i] {
					None => category_split,
					Some(existing) => existing.clone().union(category_split)?,
				});
			}
			
			current_offset += split_size;
		}
	}
	
	for (i, (split_df, output_name)) in split_dfs.iter().zip(output_names.iter()).enumerate() {
		if let Some(df) = split_df {
			let row_count = df.clone().count().await?;
			if verbose {
				eprintln!("Writing split {}: {} rows -> {}", i + 1, row_count, output_name.display());
			}
			write_data(df, output_name, file_format.as_ref()).await?;
		} else {
			if verbose {
				eprintln!("Warning: Split {} is empty -> {}", i + 1, output_name.display());
			}
			let empty_df = df.clone().limit(0, Some(0))?;
			write_data(&empty_df, output_name, file_format.as_ref()).await?;
		}
	}
	
	Ok(())
}

async fn random_split(
	df: &datafusion::prelude::DataFrame,
	ratios: &[f64],
	output_names: &[PathBuf],
	seed: Option<u64>,
	file_format: &Option<crate::utils::FileFormat>,
	verbose: bool,
	jobs: Option<usize>,
) -> NailResult<()> {
	let total_rows = df.clone().count().await?;
	
	let shuffled_df = if let Some(s) = seed {
		shuffle_dataframe_with_seed(df, s, jobs).await?
	} else {
		df.clone()
	};
	
	let mut current_offset = 0;
	
	for (i, (ratio, output_name)) in ratios.iter().zip(output_names.iter()).enumerate() {
		let split_size = if i == ratios.len() - 1 {
			total_rows - current_offset
		} else {
			(total_rows as f64 * ratio).round() as usize
		};
		
		if verbose {
			eprintln!("Creating split {}: {} rows -> {}", i + 1, split_size, output_name.display());
		}
		
		let split_df = shuffled_df.clone().limit(current_offset, Some(split_size))?;
		write_data(&split_df, output_name, file_format.as_ref()).await?;
		
		current_offset += split_size;
	}
	
	Ok(())
}

fn parse_ratios(ratio_str: &str) -> NailResult<Vec<f64>> {
	let parts: Vec<&str> = ratio_str.split(',').map(|s| s.trim()).collect();
	let mut ratios = Vec::new();
	
	for part in parts {
		let ratio: f64 = part.parse()
			.map_err(|_| NailError::InvalidArgument(format!("Invalid ratio: {}", part)))?;
		if ratio <= 0.0 {
			return Err(NailError::InvalidArgument(format!("Ratio must be positive: {}", ratio)));
		}
		ratios.push(ratio);
	}
	
	let sum: f64 = ratios.iter().sum();
	
	if (sum - 1.0).abs() < 0.001 {
		Ok(ratios)
	} else if (sum - 100.0).abs() < 0.001 {
		Ok(ratios.into_iter().map(|r| r / 100.0).collect())
	} else {
		Err(NailError::InvalidArgument(
			format!("Ratios must sum to 1.0 or 100.0, got: {}", sum)
		))
	}
}

fn parse_names(names_str: &str) -> NailResult<Vec<String>> {
	Ok(names_str.split(',').map(|s| s.trim().to_string()).collect())
}

fn generate_names(prefix: &str, count: usize, _input_path: &PathBuf, output_dir: &PathBuf, extension: &str) -> Vec<PathBuf> {
	(0..count)
		.map(|i| output_dir.join(format!("{}_{}.{}", prefix, i + 1, extension)))
		.collect()
}

fn determine_output_format(format: &Option<crate::cli::OutputFormat>, input_path: &PathBuf) -> Option<crate::utils::FileFormat> {
	match format {
		Some(crate::cli::OutputFormat::Json) => Some(crate::utils::FileFormat::Json),
		Some(crate::cli::OutputFormat::Csv) => Some(crate::utils::FileFormat::Csv),
		Some(crate::cli::OutputFormat::Parquet) => Some(crate::utils::FileFormat::Parquet),
		_ => crate::utils::detect_file_format(input_path).ok(),
	}
}

fn get_extension_for_format(format: &Option<crate::utils::FileFormat>) -> String {
	match format {
		Some(crate::utils::FileFormat::Json) => "json".to_string(),
		Some(crate::utils::FileFormat::Csv) => "csv".to_string(),
		Some(crate::utils::FileFormat::Parquet) => "parquet".to_string(),
		Some(crate::utils::FileFormat::Excel) => "xlsx".to_string(),
		None => "parquet".to_string(), // Default
	}
}

async fn shuffle_dataframe_with_seed(df: &datafusion::prelude::DataFrame, seed: u64, jobs: Option<usize>) -> NailResult<datafusion::prelude::DataFrame> {
	use rand::{SeedableRng, seq::SliceRandom};
	use rand::rngs::StdRng;
	
	// Use the context with jobs parameter
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let total_rows = df.clone().count().await?;
	let mut rng = StdRng::seed_from_u64(seed);
	let mut indices: Vec<usize> = (0..total_rows).collect();
	indices.shuffle(&mut rng);
	
	let indices_str = indices.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");
	
	let original_columns: Vec<String> = df.schema().fields().iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();
	
	let sql = format!(
		"SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn IN ({})",
		original_columns.join(", "),
		original_columns.join(", "),
		table_name, 
		indices_str
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}