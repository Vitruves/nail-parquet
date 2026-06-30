pub mod column;
pub mod format;
pub mod io;
pub mod output;
pub mod parquet_utils;
pub mod predicate;
pub mod stats;
pub mod suggest;

use crate::error::{NailError, NailResult};
use datafusion::prelude::*;
use std::path::Path;

const DEFAULT_BATCH_SIZE_LARGE: usize = 32_768;
const DEFAULT_BATCH_SIZE_JOBS: usize = 8_192;

pub async fn create_context() -> NailResult<SessionContext> {
	create_context_with_opts(None, None).await
}

pub async fn create_context_with_jobs(jobs: Option<usize>) -> NailResult<SessionContext> {
	create_context_with_opts(jobs, None).await
}

pub async fn create_context_with_opts(
	jobs: Option<usize>,
	batch_size: Option<usize>,
) -> NailResult<SessionContext> {
	let cpu_count = num_cpus::get();
	let target_partitions = match jobs {
		Some(j) => std::cmp::max(1, std::cmp::min(j, cpu_count)),
		None => std::cmp::max(1, cpu_count),
	};
	let effective_batch = batch_size.unwrap_or(if jobs.is_some() {
		DEFAULT_BATCH_SIZE_JOBS
	} else {
		DEFAULT_BATCH_SIZE_LARGE
	});

	let config = SessionConfig::new()
		.with_batch_size(effective_batch)
		.with_target_partitions(target_partitions)
		.with_collect_statistics(false)
		.with_parquet_pruning(true)
		.with_prefer_existing_sort(true);

	Ok(SessionContext::new_with_config(config))
}

pub fn detect_file_format(path: &Path) -> NailResult<FileFormat> {
	match path.extension().and_then(|s| s.to_str()) {
		Some("parquet") => Ok(FileFormat::Parquet),
		Some("csv") => Ok(FileFormat::Csv),
		Some("json") => Ok(FileFormat::Json),
		Some("xlsx") => Ok(FileFormat::Excel),
		_ => Err(NailError::UnsupportedFormat(format!(
			"Unable to detect format for file: {}",
			path.display()
		))),
	}
}

#[derive(Debug, Clone)]
pub enum FileFormat {
	Parquet,
	Csv,
	Json,
	Excel,
}
