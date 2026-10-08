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
		Some("jsonl") | Some("ndjson") => Ok(FileFormat::Jsonl),
		// Feather v2 is the Arrow IPC file format under a different name.
		Some("arrow") | Some("ipc") | Some("feather") => Ok(FileFormat::Arrow),
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
	/// Newline-delimited JSON (`.jsonl`, `.ndjson`). Same wire format as `Json`,
	/// kept separate so generated filenames keep the extension the user asked for.
	Jsonl,
	/// Arrow IPC, either the file format (`ARROW1` magic + footer) or the stream
	/// format written by e.g. HuggingFace `datasets.save_to_disk`.
	Arrow,
	Excel,
}

impl FileFormat {
	/// Canonical file extension (without the dot) for this format.
	pub fn extension(&self) -> &'static str {
		match self {
			FileFormat::Parquet => "parquet",
			FileFormat::Csv => "csv",
			FileFormat::Json => "json",
			FileFormat::Jsonl => "jsonl",
			FileFormat::Arrow => "arrow",
			FileFormat::Excel => "xlsx",
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ext_of(name: &str) -> Option<&'static str> {
		detect_file_format(Path::new(name)).ok().map(|f| {
			let e: &'static str = f.extension();
			e
		})
	}

	#[test]
	fn detects_supported_extensions() {
		assert_eq!(ext_of("a.parquet"), Some("parquet"));
		assert_eq!(ext_of("a.csv"), Some("csv"));
		assert_eq!(ext_of("a.json"), Some("json"));
		assert_eq!(ext_of("a.jsonl"), Some("jsonl"));
		assert_eq!(ext_of("a.ndjson"), Some("jsonl"));
		assert_eq!(ext_of("data-00000-of-00001.arrow"), Some("arrow"));
		assert_eq!(ext_of("a.ipc"), Some("arrow"));
		assert_eq!(ext_of("a.feather"), Some("arrow"));
		assert_eq!(ext_of("a.xlsx"), Some("xlsx"));
		assert_eq!(ext_of("a.txt"), None);
		assert_eq!(ext_of("noext"), None);
	}
}
