pub mod io;
pub mod format;
pub mod stats;
pub mod parquet_utils;

use datafusion::prelude::*;
use std::path::Path;
use crate::error::{NailError, NailResult};

pub async fn create_context() -> NailResult<SessionContext> {
	let cpu_count = num_cpus::get();
	let target_partitions = std::cmp::max(1, cpu_count);
	
	let config = SessionConfig::new()
		.with_batch_size(32768)  // Increased for better throughput
		.with_target_partitions(target_partitions)
		.with_collect_statistics(false)  // Disable stats collection for faster reads
		.with_parquet_pruning(true)  // Enable predicate pushdown
		.with_repartition_joins(false)  // Disable for small operations
		.with_repartition_aggregations(false)  // Disable for small operations
		.with_prefer_existing_sort(true);  // Use existing sort orders
	
	let ctx = SessionContext::new_with_config(config);
	
	// Register optimizations for better performance
	Ok(ctx)
}

pub async fn create_context_with_jobs(jobs: Option<usize>) -> NailResult<SessionContext> {
	let cpu_count = num_cpus::get();
	let target_partitions = if let Some(j) = jobs {
		std::cmp::max(1, std::cmp::min(j, cpu_count))
	} else {
		std::cmp::max(1, cpu_count / 2)
	};
	
	let config = SessionConfig::new()
		.with_batch_size(8192)
		.with_target_partitions(target_partitions);
	
	Ok(SessionContext::new_with_config(config))
}

pub fn detect_file_format(path: &Path) -> NailResult<FileFormat> {
	match path.extension().and_then(|s| s.to_str()) {
		Some("parquet") => Ok(FileFormat::Parquet),
		Some("csv") => Ok(FileFormat::Csv),
		Some("json") => Ok(FileFormat::Json),
		Some("xlsx") => Ok(FileFormat::Excel),
		_ => Err(NailError::UnsupportedFormat(
			format!("Unable to detect format for file: {}", path.display())
		)),
	}
}

#[derive(Debug, Clone)]
pub enum FileFormat {
	Parquet,
	Csv,
	Json,
	Excel,
}