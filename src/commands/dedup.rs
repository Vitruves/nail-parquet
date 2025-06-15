use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct DedupArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(long, help = "Remove duplicate rows", conflicts_with = "col_wise")]
	pub row_wise: bool,
	
	#[arg(long, help = "Remove duplicate columns", conflicts_with = "row_wise")]
	pub col_wise: bool,
	
	#[arg(short, long, help = "Columns to consider for row-wise deduplication")]
	pub columns: Option<String>,
	
	#[arg(long, help = "Keep first occurrence (default) vs last", default_value = "first")]
	pub keep: String,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: DedupArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	
	if !args.row_wise && !args.col_wise {
		return Err(NailError::InvalidArgument(
			"Must specify either --row-wise or --col-wise".to_string()
		));
	}
	
	let result_df = if args.row_wise {
		if args.verbose {
			eprintln!("Removing duplicate rows");
		}
		deduplicate_rows(&df, args.columns.as_deref(), &args.keep, args.jobs).await?
	} else {
		if args.verbose {
			eprintln!("Removing duplicate columns");
		}
		deduplicate_columns(&df).await?
	};
	
	if args.verbose {
		let original_rows = df.clone().count().await?;
		let new_rows = result_df.clone().count().await?;
		let original_cols = df.schema().fields().len();
		let new_cols = result_df.schema().fields().len();
		
		if args.row_wise {
			eprintln!("Removed {} duplicate rows ({} -> {})", 
				original_rows - new_rows, original_rows, new_rows);
		} else {
			eprintln!("Removed {} duplicate columns ({} -> {})", 
				original_cols - new_cols, original_cols, new_cols);
		}
	}
	
	if let Some(output_path) = &args.output {
		let file_format = match args.format {
			Some(crate::cli::OutputFormat::Json) => Some(crate::utils::FileFormat::Json),
			Some(crate::cli::OutputFormat::Csv) => Some(crate::utils::FileFormat::Csv),
			Some(crate::cli::OutputFormat::Parquet) => Some(crate::utils::FileFormat::Parquet),
			_ => None,
		};
		write_data(&result_df, output_path, file_format.as_ref()).await?;
	} else {
		display_dataframe(&result_df, None, args.format.as_ref()).await?;
	}
	
	Ok(())
}

async fn deduplicate_rows(df: &DataFrame, columns: Option<&str>, keep: &str, _jobs: Option<usize>) -> NailResult<DataFrame> {
	// Collect the data into RecordBatches
	let batches = df.clone().collect().await?;
	if batches.is_empty() {
		return Ok(df.clone());
	}
	
	let schema = batches[0].schema();
	
	// Determine which columns to use for deduplication
	let dedup_columns = if let Some(col_spec) = columns {
		// Convert Arrow Schema to DFSchema
		let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())
			.map_err(|e| NailError::InvalidArgument(format!("Failed to convert schema: {}", e)))?;
		crate::utils::stats::select_columns_by_pattern(Arc::new(df_schema), col_spec)?
	} else {
		schema.fields().iter().map(|f| f.name().clone()).collect()
	};
	
	// Find column indices
	let dedup_indices: Vec<usize> = dedup_columns.iter()
		.map(|col_name| {
			schema.fields().iter().position(|f| f.name() == col_name)
				.ok_or_else(|| NailError::InvalidArgument(format!("Column '{}' not found", col_name)))
		})
		.collect::<Result<Vec<_>, _>>()?;
	
	// Track seen row hashes and their first/last occurrence
	let mut seen_rows: HashMap<String, usize> = HashMap::new();
	let mut row_indices_to_keep: Vec<usize> = Vec::new();
	let mut current_row_index = 0;
	
	// Process each batch
	for batch in &batches {
		let num_rows = batch.num_rows();
		
		for row_idx in 0..num_rows {
			// Create a hash key from the deduplication columns
			let mut row_key = String::new();
			for &col_idx in &dedup_indices {
				let array = batch.column(col_idx);
				let value_str = format_array_value(array, row_idx);
				row_key.push_str(&value_str);
				row_key.push('|'); // Separator
			}
			
			match keep {
				"first" => {
					if !seen_rows.contains_key(&row_key) {
						seen_rows.insert(row_key, current_row_index);
						row_indices_to_keep.push(current_row_index);
					}
				},
				"last" => {
					if let Some(&existing_idx) = seen_rows.get(&row_key) {
						// Remove the previous occurrence
						if let Some(pos) = row_indices_to_keep.iter().position(|&x| x == existing_idx) {
							row_indices_to_keep.remove(pos);
						}
					}
					seen_rows.insert(row_key, current_row_index);
					row_indices_to_keep.push(current_row_index);
				},
				_ => return Err(NailError::InvalidArgument("keep must be 'first' or 'last'".to_string())),
			}
			
			current_row_index += 1;
		}
	}
	
	// Sort indices to maintain order
	row_indices_to_keep.sort_unstable();
	
	// Create new batches with only the selected rows
	let mut result_batches = Vec::new();
	let mut global_row_idx = 0;
	let mut keep_idx = 0;
	
	for batch in &batches {
		let num_rows = batch.num_rows();
		let mut rows_to_take = Vec::new();
		
		for local_row_idx in 0..num_rows {
			if keep_idx < row_indices_to_keep.len() && row_indices_to_keep[keep_idx] == global_row_idx {
				rows_to_take.push(local_row_idx);
				keep_idx += 1;
			}
			global_row_idx += 1;
		}
		
		if !rows_to_take.is_empty() {
			let filtered_batch = take_rows_from_batch(batch, &rows_to_take)?;
			result_batches.push(filtered_batch);
		}
	}
	
	// Convert back to DataFrame
	let ctx = crate::utils::create_context_with_jobs(_jobs).await?;
	let result_df = ctx.read_batches(result_batches)?;
	Ok(result_df)
}

async fn deduplicate_columns(df: &DataFrame) -> NailResult<DataFrame> {
	let schema = df.schema();
	let mut unique_columns = Vec::new();
	let mut seen_names = HashSet::new();
	
	for field in schema.fields() {
		let field_name = field.name();
		if !seen_names.contains(field_name) {
			seen_names.insert(field_name.clone());
			unique_columns.push(Expr::Column(datafusion::common::Column::new(None::<String>, field_name)));
		}
	}
	
	let result = df.clone().select(unique_columns)?;
	Ok(result)
}

fn format_array_value(array: &dyn Array, row_idx: usize) -> String {
	if array.is_null(row_idx) {
		return "NULL".to_string();
	}
	
	match array.data_type() {
		arrow_schema::DataType::Int64 => {
			let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
			arr.value(row_idx).to_string()
		},
		arrow_schema::DataType::Float64 => {
			let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
			arr.value(row_idx).to_string()
		},
		arrow_schema::DataType::Utf8 => {
			let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
			arr.value(row_idx).to_string()
		},
		arrow_schema::DataType::Boolean => {
			let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
			arr.value(row_idx).to_string()
		},
		_ => format!("UNSUPPORTED_TYPE_{}", row_idx),
	}
}

fn take_rows_from_batch(batch: &RecordBatch, row_indices: &[usize]) -> NailResult<RecordBatch> {
	let indices = UInt32Array::from(row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
	let indices_ref = &indices as &dyn Array;
	
	let mut new_columns = Vec::new();
	for column in batch.columns() {
		let taken = arrow::compute::take(column.as_ref(), indices_ref, None)
			.map_err(|e| NailError::Arrow(e))?;
		new_columns.push(taken);
	}
	
	let result_batch = RecordBatch::try_new(batch.schema(), new_columns)
		.map_err(|e| NailError::Arrow(e))?;
	
	Ok(result_batch)
}