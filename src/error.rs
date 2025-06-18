use thiserror::Error;
use std::fmt;

pub type NailResult<T> = Result<T, NailError>;

#[derive(Error, Debug)]
pub enum NailError {
	Io(#[from] std::io::Error),
	
	DataFusion(#[from] datafusion::error::DataFusionError),
	
	Arrow(#[from] arrow::error::ArrowError),
	
	Parquet(#[from] parquet::errors::ParquetError),
	
	Regex(#[from] regex::Error),
	
	SerdeJson(#[from] serde_json::Error),
	
	InvalidArgument(String),
	
	FileNotFound(String),
	
	UnsupportedFormat(String),
	
	ColumnNotFound(String),
	
	Statistics(String),
}

impl fmt::Display for NailError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			NailError::Io(e) => {
				if e.kind() == std::io::ErrorKind::NotFound {
					write!(f, "File not found: {}", extract_file_path(e))
				} else if e.kind() == std::io::ErrorKind::PermissionDenied {
					write!(f, "Permission denied: {}", extract_file_path(e))
				} else {
					write!(f, "I/O error: {}", e)
				}
			},
			NailError::DataFusion(e) => {
				let error_str = e.to_string();
				if error_str.contains("ObjectStore") && error_str.contains("NotFound") {
					if let Some(path) = extract_path_from_datafusion_error(&error_str) {
						write!(f, "File not found: {}", path)
					} else {
						write!(f, "File not found")
					}
				} else if error_str.contains("Schema") {
					write!(f, "Schema error: {}", simplify_schema_error(&error_str))
				} else if error_str.contains("Column") && error_str.contains("not found") {
					write!(f, "Column error: {}", simplify_column_error(&error_str))
				} else {
					write!(f, "Data processing error: {}", simplify_datafusion_error(&error_str))
				}
			},
			NailError::Arrow(e) => {
				write!(f, "Data format error: {}", e)
			},
			NailError::Parquet(e) => {
				let error_str = e.to_string();
				if error_str.contains("EOF") {
					write!(f, "Invalid or corrupted parquet file")
				} else if error_str.contains("Schema") {
					write!(f, "Parquet schema error: {}", error_str)
				} else {
					write!(f, "Parquet file error: {}", error_str)
				}
			},
			NailError::Regex(e) => {
				write!(f, "Invalid regular expression: {}", e)
			},
			NailError::SerdeJson(e) => {
				write!(f, "JSON processing error: {}", e)
			},
			NailError::InvalidArgument(msg) => {
				write!(f, "Invalid argument: {}", msg)
			},
			NailError::FileNotFound(path) => {
				write!(f, "File not found: {}", path)
			},
			NailError::UnsupportedFormat(format) => {
				write!(f, "Unsupported file format: {}", format)
			},
			NailError::ColumnNotFound(msg) => {
				write!(f, "{}", msg)
			},
			NailError::Statistics(msg) => {
				write!(f, "Statistics calculation error: {}", msg)
			},
		}
	}
}

fn extract_file_path(e: &std::io::Error) -> String {
	e.to_string()
		.split_whitespace()
		.find(|s| s.starts_with('/') || s.starts_with('.') || s.starts_with('~'))
		.unwrap_or("unknown file")
		.to_string()
}

fn extract_path_from_datafusion_error(error_str: &str) -> Option<String> {
	if let Some(start) = error_str.find("path: \"") {
		let start = start + 7; // Skip 'path: "'
		if let Some(end) = error_str[start..].find('"') {
			return Some(error_str[start..start + end].to_string());
		}
	}
	None
}

fn simplify_schema_error(error_str: &str) -> String {
	if error_str.contains("column") {
		error_str.split("Schema").nth(1).unwrap_or(error_str).trim().to_string()
	} else {
		error_str.to_string()
	}
}

fn simplify_column_error(error_str: &str) -> String {
	error_str.replace("DataFusion error: ", "").to_string()
}

fn simplify_datafusion_error(error_str: &str) -> String {
	// Remove common DataFusion prefixes and make more readable
	error_str
		.replace("DataFusion error: ", "")
		.replace("External error: ", "")
		.replace("Plan(\"", "")
		.replace("\")", "")
		.to_string()
}