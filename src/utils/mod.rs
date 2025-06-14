pub mod io;
pub mod format;
pub mod stats;

use datafusion::prelude::*;
use std::path::Path;
use crate::error::{NailError, NailResult};

pub async fn create_context() -> NailResult<SessionContext> {
	let cpu_count = num_cpus::get();
	let target_partitions = std::cmp::max(1, cpu_count / 2);
	
	let config = SessionConfig::new()
		.with_batch_size(8192)
		.with_target_partitions(target_partitions);
	
	Ok(SessionContext::new_with_config(config))
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