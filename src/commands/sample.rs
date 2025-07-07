use clap::Args;
use datafusion::prelude::*;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use datafusion::arrow::array::{StringArray, Array};

#[derive(Args, Clone)]
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
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let total_rows = df.clone().count().await?;
	
	if args.number >= total_rows {
		args.common.log_if_verbose(&format!("Requested {} samples, but only {} rows available. Returning all rows.", args.number, total_rows));
		let output_handler = OutputHandler::new(&args.common);
		output_handler.handle_output(&df, "sample").await?;
		return Ok(());
	}
	
	args.common.log_if_verbose(&format!("Sampling {} rows from {} total using {:?} method", args.number, total_rows, args.method));
	
	let sampled_df = match args.method {
		SampleMethod::Random => sample_random(&df, args.number, args.common.random, args.common.jobs).await?,
		SampleMethod::Stratified => {
			if let Some(col) = &args.stratify_by {
				sample_stratified(&df, args.number, col, args.common.random, args.common.jobs).await?
			} else {
				return Err(NailError::InvalidArgument("--stratify-by required for stratified sampling".to_string()));
			}
		},
		SampleMethod::First => df.limit(0, Some(args.number))?,
		SampleMethod::Last => {
			let skip = total_rows.saturating_sub(args.number);
			df.limit(skip, Some(args.number))?
		},
	};
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&sampled_df, "sample").await?;
	
	Ok(())
}

async fn sample_random(df: &DataFrame, n: usize, seed: Option<u64>, jobs: Option<usize>) -> NailResult<DataFrame> {
	let total_rows = df.clone().count().await?;
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	// For very large datasets, use a more scalable approach
	if total_rows > 100_000 || n > 10_000 {
		// Use DataFusion's TABLESAMPLE or a hash-based approach
		let sample_ratio = n as f64 / total_rows as f64;
		
		if let Some(s) = seed {
			// Use deterministic hash-based sampling
			let sql = format!(
				"WITH numbered AS (
					SELECT *, ROW_NUMBER() OVER() as rn FROM {}
				)
				SELECT * FROM numbered 
				WHERE ABS(HASH(CAST(rn AS VARCHAR) || '{}')) % {} < {}
				ORDER BY rn
				LIMIT {}",
				table_name, s, total_rows, (total_rows as f64 * sample_ratio) as usize, n
			);
			let result = ctx.sql(&sql).await?;
			Ok(result)
		} else {
			// Use ORDER BY RANDOM() for true randomness
			let sql = format!(
				"SELECT * FROM {} ORDER BY RANDOM() LIMIT {}",
				table_name, n
			);
			let result = ctx.sql(&sql).await?;
			Ok(result)
		}
	} else {
		// For smaller datasets, use the indices approach
		let mut rng = match seed {
			Some(s) => StdRng::seed_from_u64(s),
			None => StdRng::from_entropy(),
		};
		
		let mut indices: Vec<usize> = (0..total_rows).collect();
		indices.shuffle(&mut rng);
		indices.truncate(n);
		indices.sort();
		
		// Create a temporary table with the sampled indices
		let values_rows: Vec<String> = indices.iter()
			.map(|&i| format!("({})", i + 1))
			.collect();
		
		let values_sql = values_rows.join(", ");
		let sql = format!(
			"WITH sample_indices(rn) AS (VALUES {}) 
			 SELECT t.* FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM {}) t 
			 JOIN sample_indices si ON t.rn = si.rn",
			values_sql, table_name
		);
		
		let result = ctx.sql(&sql).await?;
		Ok(result)
	}
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

    // Find the actual column name (case-insensitive matching)
    let schema = df.schema();
    let actual_col_name = schema.fields().iter()
        .find(|f| f.name().to_lowercase() == stratify_col.to_lowercase())
        .map(|f| f.name().clone())
        .ok_or_else(|| {
            let available_cols: Vec<String> = schema.fields().iter()
                .map(|f| f.name().clone())
                .collect();
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
        let count_array = batch.column(1).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
        
        for i in 0..batch.num_rows() {
            let category = match cat_array.data_type() {
                datafusion::arrow::datatypes::DataType::Utf8 => {
                    cat_array.as_any().downcast_ref::<StringArray>().unwrap().value(i).to_string()
                },
                datafusion::arrow::datatypes::DataType::Int64 => {
                    cat_array.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap().value(i).to_string()
                },
                _ => continue,
            };
            let count = count_array.value(i) as usize;
            category_counts.insert(category, count);
            total_count += count;
        }
    }
    
    if category_counts.is_empty() {
        return Err(NailError::Statistics("No categories found for stratified sampling".to_string()));
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
                    SELECT *, ROW_NUMBER() OVER() as cat_rn 
                    FROM {} 
                    WHERE \"{}\" = '{}'
                )
                SELECT * FROM cat_data 
                WHERE ABS(HASH(CAST(cat_rn AS VARCHAR) || '{}')) % 1000000 < {}
                LIMIT {}",
                table_name, actual_col_name, cat, s, 
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
    Ok(result_df)
}