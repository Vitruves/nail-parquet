use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use clap::Args;
use datafusion::arrow::array::{Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::compute::{concat_batches, take};
use datafusion::prelude::*;
use futures::StreamExt;
use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail sample data.parquet -n 100 -o sampled.parquet
  nail sample data.csv --fraction 0.1 -o -")]
pub struct SampleArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Number of samples", default_value = "10")]
	pub number: usize,

	#[arg(long, help = "Sampling method", value_enum, default_value = "random")]
	pub method: SampleMethod,

	#[arg(long, help = "Column name for stratified sampling")]
	pub stratify_by: Option<String>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum SampleMethod {
	Random,
	Stratified,
	First,
	Last,
}

pub async fn execute(args: SampleArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let total_rows =
		crate::utils::parquet_utils::row_count_fast_or_scan(&args.common.input, &df).await?;

	if args.number >= total_rows {
		args.common.log_if_verbose(&format!(
			"Requested {} samples, but only {} rows available. Returning all rows.",
			args.number, total_rows
		));
		let output_handler = OutputHandler::new(&args.common);
		output_handler.handle_output(&df, "sample").await?;
		return Ok(());
	}

	args.common.log_if_verbose(&format!(
		"Sampling {} rows from {} total using {:?} method",
		args.number, total_rows, args.method
	));

	let sampled_df = match args.method {
		SampleMethod::Random => {
			sample_random(&df, args.number, args.common.random, args.common.jobs).await?
		}
		SampleMethod::Stratified => {
			if let Some(col) = &args.stratify_by {
				sample_stratified(&df, args.number, col, args.common.random, args.common.jobs)
					.await?
			} else {
				return Err(NailError::InvalidArgument(
					"--stratify-by required for stratified sampling".to_string(),
				));
			}
		}
		SampleMethod::First => df.limit(0, Some(args.number))?,
		SampleMethod::Last => {
			let skip = total_rows.saturating_sub(args.number);
			df.limit(skip, Some(args.number))?
		}
	};

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&sampled_df, "sample").await?;

	Ok(())
}

/// Uniform random sample of `n` rows via single-pass reservoir sampling
/// (Algorithm R) over a deterministically-ordered, single-partition stream.
///
/// This streams the input once (O(rows) time, O(n) memory) and never adds a
/// helper column, so the output schema always equals the input schema. With a
/// `seed` the result is fully reproducible: the row order is pinned by reading a
/// single partition and the RNG is seeded, so the same seed always yields the
/// same rows. Output rows are returned in input order.
async fn sample_random(
	df: &DataFrame,
	n: usize,
	seed: Option<u64>,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	// A single target partition gives a deterministic row order, which is what
	// makes seeded sampling reproducible. (`jobs` still drives the output
	// context below so downstream work keeps the user's parallelism.)
	let read_ctx = crate::utils::create_context_with_opts(Some(1), None).await?;
	read_ctx.register_table("temp_table", df.clone().into_view())?;
	let mut stream = read_ctx
		.sql("SELECT * FROM temp_table")
		.await?
		.execute_stream()
		.await?;
	let schema = stream.schema();

	let mut rng = match seed {
		Some(s) => StdRng::seed_from_u64(s),
		None => StdRng::from_entropy(),
	};

	// Reservoir of (original row index, single-row batch). Each kept row is
	// copied into its own small batch so dropping a source batch frees its
	// memory — keeping the footprint at O(n) rather than O(rows).
	let mut reservoir: Vec<(usize, RecordBatch)> = Vec::with_capacity(n);
	let mut seen = 0usize;

	while let Some(batch) = stream.next().await {
		let batch = batch?;
		for row in 0..batch.num_rows() {
			if reservoir.len() < n {
				reservoir.push((seen, copy_row(&batch, row)?));
			} else {
				// Replace a random reservoir slot with probability n / seen.
				let j = rng.gen_range(0..=seen);
				if j < n {
					reservoir[j] = (seen, copy_row(&batch, row)?);
				}
			}
			seen += 1;
		}
	}

	// Emit in original input order for predictable, stable output.
	reservoir.sort_by_key(|(idx, _)| *idx);
	let rows: Vec<RecordBatch> = reservoir.into_iter().map(|(_, b)| b).collect();

	let out_ctx = crate::utils::create_context_with_jobs(jobs).await?;
	if rows.is_empty() {
		return Ok(out_ctx.read_batch(RecordBatch::new_empty(schema))?);
	}
	let combined = concat_batches(&schema, &rows)?;
	Ok(out_ctx.read_batch(combined)?)
}

/// Deep-copy a single row into a standalone one-row batch (owned buffers, so it
/// does not pin the parent batch's memory).
fn copy_row(batch: &RecordBatch, row: usize) -> NailResult<RecordBatch> {
	let indices = UInt64Array::from(vec![row as u64]);
	let columns = batch
		.columns()
		.iter()
		.map(|c| take(c, &indices, None))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

async fn sample_stratified(
	df: &DataFrame,
	n: usize,
	stratify_col: &str,
	seed: Option<u64>,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	use std::collections::HashMap;
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;

	// Preserve the original schema: the internal row-number helper column used
	// for deterministic seeding must be projected away before returning.
	let original_cols: Vec<String> = df
		.schema()
		.fields()
		.iter()
		.map(|f| f.name().clone())
		.collect();
	let original_col_refs: Vec<&str> = original_cols.iter().map(|s| s.as_str()).collect();

	// Find the actual column name (case-insensitive matching)
	let schema = df.schema();
	let actual_col_name = schema
		.fields()
		.iter()
		.find(|f| f.name().to_lowercase() == stratify_col.to_lowercase())
		.map(|f| f.name().clone())
		.ok_or_else(|| {
			let available_cols: Vec<String> =
				schema.fields().iter().map(|f| f.name().clone()).collect();
			NailError::ColumnNotFound(format!(
				"Column '{}' not found. Available columns: {:?}",
				stratify_col, available_cols
			))
		})?;

	// Get counts for each category
	let count_sql = format!(
		"SELECT \"{}\" as category, COUNT(*) as count 
         FROM {} 
         WHERE \"{}\" IS NOT NULL 
         GROUP BY \"{}\"",
		actual_col_name, table_name, actual_col_name, actual_col_name
	);

	let count_df = ctx.sql(&count_sql).await?;
	let count_batches = count_df.collect().await?;

	let mut category_counts = HashMap::new();
	let mut total_count = 0usize;

	for batch in &count_batches {
		let cat_array = batch.column(0);
		let count_array = batch
			.column(1)
			.as_any()
			.downcast_ref::<datafusion::arrow::array::Int64Array>()
			.unwrap();

		for i in 0..batch.num_rows() {
			let category = match cat_array.data_type() {
				datafusion::arrow::datatypes::DataType::Utf8 => cat_array
					.as_any()
					.downcast_ref::<StringArray>()
					.unwrap()
					.value(i)
					.to_string(),
				datafusion::arrow::datatypes::DataType::Int64 => cat_array
					.as_any()
					.downcast_ref::<datafusion::arrow::array::Int64Array>()
					.unwrap()
					.value(i)
					.to_string(),
				_ => continue,
			};
			let count = count_array.value(i) as usize;
			category_counts.insert(category, count);
			total_count += count;
		}
	}

	if category_counts.is_empty() {
		return Err(NailError::Statistics(
			"No categories found for stratified sampling".to_string(),
		));
	}

	// Calculate samples per category proportionally
	let mut samples_per_category = HashMap::new();
	let mut total_samples = 0;

	for (cat, count) in &category_counts {
		let proportion = *count as f64 / total_count as f64;
		let samples = (n as f64 * proportion).round() as usize;
		samples_per_category.insert(cat.clone(), samples.min(*count)); // Don't sample more than available
		total_samples += samples.min(*count);
	}

	// Adjust if we're short on samples due to rounding
	if total_samples < n {
		let mut remaining = n - total_samples;
		for (cat, count) in &category_counts {
			let current_samples = samples_per_category[cat];
			if current_samples < *count && remaining > 0 {
				let additional = (remaining).min(*count - current_samples);
				samples_per_category.insert(cat.clone(), current_samples + additional);
				remaining -= additional;
			}
		}
	}

	let mut combined: Option<DataFrame> = None;

	for (cat, samples) in &samples_per_category {
		if *samples == 0 {
			continue;
		}

		// Create a query to randomly sample from each category
		let category_sql = if let Some(s) = seed {
			// Deterministic sampling with seed
			format!(
				"WITH cat_data AS (
                    SELECT *, ROW_NUMBER() OVER() as __nail_row_id
                    FROM {}
                    WHERE \"{}\" = '{}'
                )
                SELECT * FROM cat_data
                WHERE ABS(HASH(CAST(__nail_row_id AS VARCHAR) || '{}')) % 1000000 < {}
                LIMIT {}",
				table_name,
				actual_col_name,
				cat,
				s,
				(*samples as f64 / category_counts[cat] as f64 * 1000000.0) as i64,
				samples
			)
		} else {
			// True random sampling
			format!(
				"SELECT * FROM {} 
                 WHERE \"{}\" = '{}' 
                 ORDER BY RANDOM() 
                 LIMIT {}",
				table_name, actual_col_name, cat, samples
			)
		};

		let category_df = ctx.sql(&category_sql).await?;

		combined = Some(match combined {
			None => category_df,
			Some(prev) => prev.union(category_df)?,
		});
	}

	let result_df = combined.ok_or_else(|| NailError::Statistics("No data sampled".to_string()))?;
	// Seeded sampling adds an internal row-number column; restore original schema.
	Ok(result_df.select_columns(&original_col_refs)?)
}
