use datafusion::prelude::*;
use std::path::Path;
use crate::error::NailResult;
use crate::cli::OutputFormat;
use crate::utils::io::write_data;
use crate::utils::FileFormat;

pub async fn display_dataframe(
	df: &DataFrame,
	output_path: Option<&Path>,
	format: Option<&OutputFormat>,
) -> NailResult<()> {
	match output_path {
		Some(path) => {
			let file_format = match format {
				Some(OutputFormat::Json) => Some(FileFormat::Json),
				Some(OutputFormat::Csv) => Some(FileFormat::Csv),
				Some(OutputFormat::Parquet) => Some(FileFormat::Parquet),
				Some(OutputFormat::Text) | None => {
					match path.extension().and_then(|s| s.to_str()) {
						Some("json") => Some(FileFormat::Json),
						Some("csv") => Some(FileFormat::Csv),
						Some("parquet") => Some(FileFormat::Parquet),
						_ => Some(FileFormat::Parquet),
					}
				},
			};
			
			write_data(df, path, file_format.as_ref()).await
		},
		None => {
			match format {
				Some(OutputFormat::Json) => {
					let batches = df.clone().collect().await?;
					for batch in batches {
						
						// Simple JSON output for now
						println!("{:?}", batch);
					}
				},
				Some(OutputFormat::Text) | None => {
					df.clone().show().await?;
				},
				_ => {
					return Err(crate::error::NailError::InvalidArgument(
						"CSV and Parquet formats require an output file".to_string()
					));
				},
			}
			
			Ok(())
		},
	}
}