use thiserror::Error;

pub type NailResult<T> = Result<T, NailError>;

#[derive(Error, Debug)]
pub enum NailError {
	#[error("IO error: {0}")]
	Io(#[from] std::io::Error),
	
	#[error("DataFusion error: {0}")]
	DataFusion(#[from] datafusion::error::DataFusionError),
	
	#[error("Arrow error: {0}")]
	Arrow(#[from] arrow::error::ArrowError),
	
	#[error("Parquet error: {0}")]
	Parquet(#[from] parquet::errors::ParquetError),
	
	#[error("Regex error: {0}")]
	Regex(#[from] regex::Error),
	
	#[error("Serde JSON error: {0}")]
	SerdeJson(#[from] serde_json::Error),
	
	#[error("Invalid argument: {0}")]
	InvalidArgument(String),
	
	#[error("File not found: {0}")]
	FileNotFound(String),
	
	#[error("Unsupported format: {0}")]
	UnsupportedFormat(String),
	
	#[error("Column not found: {0}")]
	ColumnNotFound(String),
	
	#[error("Statistics error: {0}")]
	Statistics(String),
}