use clap::Args;
use std::path::PathBuf;
use std::collections::HashMap;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::column::resolve_column_name;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct SplitArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
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
}

pub async fn execute(args: SplitArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	// Create output directory if it doesn't exist
	if !args.output_dir.exists() {
		std::fs::create_dir_all(&args.output_dir)?;
		args.common.log_if_verbose(&format!("Created output directory: {}", args.output_dir.display()));
	}
	
	let df = read_data(&args.common.input).await?;
	let total_rows = df.clone().count().await?;
	
	let ratios = parse_ratios(&args.ratio)?;
	let file_format = determine_output_format(&args.common.format, &args.common.input);
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
		generate_names(&args.splits_prefix, ratios.len(), &args.common.input, &args.output_dir, &extension)
	};
	
	if ratios.len() != output_names.len() {
		return Err(NailError::InvalidArgument(
			format!("Number of ratios ({}) must match number of names ({})", 
				ratios.len(), output_names.len())
		));
	}
	
	if let Some(stratify_col) = &args.stratified_by {
		args.common.log_if_verbose(&format!("Performing stratified split by column '{}' with ratios: {:?}", 
			stratify_col, ratios));
		stratified_split(&df, &ratios, &output_names, stratify_col, args.common.random, &file_format, args.common.verbose, args.common.jobs).await?;
	} else {
		args.common.log_if_verbose(&format!("Splitting {} rows into {} parts with ratios: {:?}", 
			total_rows, ratios.len(), ratios));
		random_split(&df, &ratios, &output_names, args.common.random, &file_format, args.common.verbose, args.common.jobs).await?;
	}
	
	args.common.log_if_verbose(&format!("Split complete: {} files created in {}", output_names.len(), args.output_dir.display()));
	if args.common.verbose {
		for (i, output_name) in output_names.iter().enumerate() {
			args.common.log_if_verbose(&format!("  Split {}: {}", i + 1, output_name.display()));
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
	
	let schema = df.schema().clone().into();
	let actual_col_name = resolve_column_name(&schema, stratify_col)?;
	
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
		// Use parameterized queries to avoid SQL injection
		let category_df = ctx.table(table_name).await?
			.filter(col(&actual_col_name).eq(lit(category)))?;
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
			let empty_df = df.clone().limit(0, Some(1))?.filter(lit(false))?;
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
		// Use random shuffling when no seed is provided
		shuffle_dataframe(df, jobs).await?
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

async fn shuffle_dataframe(df: &datafusion::prelude::DataFrame, jobs: Option<usize>) -> NailResult<datafusion::prelude::DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	ctx.register_table("temp_table", df.clone().into_view())?;

	// Simple shuffling using ORDER BY RANDOM() 
	let sql = "SELECT * FROM temp_table ORDER BY RANDOM()";
	
	let result = ctx.sql(sql).await?;
	Ok(result)
}

async fn shuffle_dataframe_with_seed(df: &datafusion::prelude::DataFrame, seed: u64, jobs: Option<usize>) -> NailResult<datafusion::prelude::DataFrame> {
	use rand::{SeedableRng, seq::SliceRandom};
	use rand::rngs::StdRng;
	
	// Use the context with jobs parameter
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let total_rows = df.clone().count().await?;
	
	// For very large datasets, we need a more scalable approach
	// Instead of generating all indices in memory, we'll use a deterministic hash-based approach
	if total_rows > 1_000_000 {
		// Use a hash-based approach for large datasets
		let sql = format!(
			"SELECT * FROM {} ORDER BY MD5(CAST(ROW_NUMBER() OVER() AS VARCHAR) || '{}')",
			table_name, seed
		);
		let result = ctx.sql(&sql).await?;
		return Ok(result);
	}
	
	// For smaller datasets, use the shuffle approach but with batching
	let mut rng = StdRng::seed_from_u64(seed);
	let mut indices: Vec<usize> = (0..total_rows).collect();
	indices.shuffle(&mut rng);
	
	// Create a temporary table with the shuffled indices
	let indices_data: Vec<_> = indices.iter()
		.enumerate()
		.map(|(new_pos, &old_pos)| (old_pos as i64 + 1, new_pos as i64))
		.collect();
	
	// Build a values table
	let values_rows: Vec<String> = indices_data.iter()
		.map(|(old, new)| format!("({}, {})", old, new))
		.collect();
	
	// Process in chunks to avoid SQL length limits
	const CHUNK_SIZE: usize = 10000;
	let chunks: Vec<_> = values_rows.chunks(CHUNK_SIZE).collect();
	
	if chunks.len() == 1 {
		// Small dataset, use single query
		let values_sql = values_rows.join(", ");
		let sql = format!(
			"WITH shuffle_map(old_rn, new_order) AS (VALUES {}) \
			 SELECT t.* FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM {}) t \
			 JOIN shuffle_map sm ON t.rn = sm.old_rn \
			 ORDER BY sm.new_order",
			values_sql, table_name
		);
		let result = ctx.sql(&sql).await?;
		Ok(result)
	} else {
		// Large dataset, create a temporary mapping table
		ctx.sql(&format!("CREATE TEMP TABLE shuffle_map_{} (old_rn BIGINT, new_order BIGINT)", seed)).await?;
		
		for chunk in chunks {
			let values_sql = chunk.join(", ");
			let insert_sql = format!(
				"INSERT INTO shuffle_map_{} VALUES {}",
				seed, values_sql
			);
			ctx.sql(&insert_sql).await?;
		}
		
		let sql = format!(
			"SELECT t.* FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM {}) t \
			 JOIN shuffle_map_{} sm ON t.rn = sm.old_rn \
			 ORDER BY sm.new_order",
			table_name, seed
		);
		let result = ctx.sql(&sql).await?;
		
		// Clean up
		ctx.sql(&format!("DROP TABLE shuffle_map_{}", seed)).await?;
		
		Ok(result)
	}
}