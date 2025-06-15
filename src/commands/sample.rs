use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;
use datafusion::arrow::array::{StringArray, DictionaryArray, Array};
use datafusion::arrow::datatypes::UInt32Type;

#[derive(Args, Clone)]
pub struct SampleArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Number of samples", default_value = "10")]
	pub number: usize,
	
	#[arg(long, help = "Sampling method", value_enum, default_value = "random")]
	pub method: SampleMethod,
	
	#[arg(long, help = "Column name for stratified sampling")]
	pub stratify_by: Option<String>,
	
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

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum SampleMethod {
	Random,
	Stratified,
	First,
	Last,
}

pub async fn execute(args: SampleArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let total_rows = df.clone().count().await?;
	
	if args.number >= total_rows {
		if args.verbose {
			eprintln!("Requested {} samples, but only {} rows available. Returning all rows.", args.number, total_rows);
		}
		display_dataframe(&df, args.output.as_deref(), args.format.as_ref()).await?;
		return Ok(());
	}
	
	if args.verbose {
		eprintln!("Sampling {} rows from {} total using {:?} method", args.number, total_rows, args.method);
	}
	
	let sampled_df = match args.method {
		SampleMethod::Random => sample_random(&df, args.number, args.random, args.jobs).await?,
		SampleMethod::Stratified => {
			if let Some(col) = &args.stratify_by {
				sample_stratified(&df, args.number, col, args.random, args.jobs).await?
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
	
	if let Some(output_path) = &args.output {
		let file_format = match args.format {
			Some(crate::cli::OutputFormat::Json) => Some(crate::utils::FileFormat::Json),
			Some(crate::cli::OutputFormat::Csv) => Some(crate::utils::FileFormat::Csv),
			Some(crate::cli::OutputFormat::Parquet) => Some(crate::utils::FileFormat::Parquet),
			_ => None,
		};
		write_data(&sampled_df, output_path, file_format.as_ref()).await?;
	} else {
		display_dataframe(&sampled_df, None, args.format.as_ref()).await?;
	}
	
	Ok(())
}

async fn sample_random(df: &DataFrame, n: usize, seed: Option<u64>, jobs: Option<usize>) -> NailResult<DataFrame> {
	let total_rows = df.clone().count().await?;
	let mut rng = match seed {
		Some(s) => StdRng::seed_from_u64(s),
		None => StdRng::from_entropy(),
	};
	
	let mut indices: Vec<usize> = (0..total_rows).collect();
	indices.shuffle(&mut rng);
	indices.truncate(n);
	indices.sort();
	
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let indices_str = indices.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");
	
	// Get the original column names and quote them to preserve case
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

async fn sample_stratified(
    df: &DataFrame,
    n: usize,
    stratify_col: &str,
    _seed: Option<u64>,
    jobs: Option<usize>,
) -> NailResult<DataFrame> {
    use std::collections::HashSet;
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

    // First, let's try to get distinct values using SQL which is more robust
    let distinct_sql = format!(
        "SELECT DISTINCT {} FROM {} WHERE {} IS NOT NULL",
        actual_col_name, table_name, actual_col_name
    );
    
    let distinct_df = match ctx.sql(&distinct_sql).await {
        Ok(df) => df,
        Err(e) => {
            return Err(NailError::Statistics(format!("Failed to retrieve categories from column '{}': {}", actual_col_name, e)));
        }
    };
    
    let distinct_batches = distinct_df.clone().collect().await?;
    let mut categories = HashSet::new();
    
    for batch in &distinct_batches {
        if batch.num_columns() > 0 {
            let array_ref = batch.column(0);
        if let Some(arr) = array_ref.as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                        categories.insert(arr.value(i).to_string());
                }
            }
        } else if let Some(dict) = array_ref.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            let keys = dict.keys();
            if let Some(values) = dict.values().as_any().downcast_ref::<StringArray>() {
                for i in 0..keys.len() {
                    if keys.is_valid(i) {
                        let k = keys.value(i) as usize;
                            if k < values.len() {
                                categories.insert(values.value(k).to_string());
                            }
                    }
                }
            }
        } else {
                // Try to convert to string representation
                let schema = distinct_df.schema();
                if let Some(field) = schema.fields().get(0) {
                    match field.data_type() {
                        datafusion::arrow::datatypes::DataType::Utf8 => {
                            // Already handled above, but this is a fallback
                            for i in 0..array_ref.len() {
                                if !array_ref.is_null(i) {
                                    if let Some(scalar) = datafusion::arrow::compute::cast(array_ref, &datafusion::arrow::datatypes::DataType::Utf8).ok() {
                                        if let Some(str_arr) = scalar.as_any().downcast_ref::<StringArray>() {
                                            if i < str_arr.len() && str_arr.is_valid(i) {
                                                categories.insert(str_arr.value(i).to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        _ => {
                            return Err(NailError::Statistics(format!("Column '{}' must be of string type for stratified sampling", actual_col_name)));
                        }
                    }
                }
            }
        }
    }
    
    if categories.is_empty() {
        return Err(NailError::Statistics("No categories found for stratified sampling".to_string()));
    }
    
    let categories: Vec<String> = categories.into_iter().collect();
    let per_group = n / categories.len();
    let mut combined: Option<DataFrame> = None;
    
    for cat in &categories {
        // deterministic: take first per_group rows for each category
        let filtered = ctx.table(table_name).await?
            .filter(Expr::Column(datafusion::common::Column::new(None::<String>, &actual_col_name)).eq(lit(cat)))?;
        let limited = filtered.limit(0, Some(per_group))?;
        combined = Some(match combined {
            None => limited,
            Some(prev) => prev.union(limited)?,
        });
    }
    
    let mut result_df = combined.unwrap();
    // Handle remainder samples
    let remainder = n - per_group * categories.len();
    if remainder > 0 {
        // add random remainder from full dataset
        let rem = sample_random(df, remainder, None, None).await?;
        result_df = result_df.union(rem)?;
    }
    Ok(result_df)
}