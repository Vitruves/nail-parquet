pub mod io;
pub mod format;
pub mod stats;

use datafusion::prelude::*;
use std::path::Path;
use crate::error::{NailError, NailResult};

pub async fn create_context() -> NailResult<SessionContext> {
	let config = SessionConfig::new()
		.with_batch_size(8192)
		.with_target_partitions(num_cpus::get());
	
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