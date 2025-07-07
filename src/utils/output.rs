use datafusion::prelude::DataFrame;
use crate::error::NailResult;
use crate::cli::{CommonArgs, OutputFormat};
use crate::utils::{format::display_dataframe, io::write_data, FileFormat};

pub struct OutputHandler<'a> {
    common_args: &'a CommonArgs,
}

impl<'a> OutputHandler<'a> {
    pub fn new(common_args: &'a CommonArgs) -> Self {
        Self { common_args }
    }

    pub async fn handle_output(&self, df: &DataFrame, operation_name: &str) -> NailResult<()> {
        self.common_args.log_if_verbose(&format!("Completing {} operation", operation_name));

        match &self.common_args.output {
            Some(output_path) => {
                let file_format = self.map_output_format(&self.common_args.format);
                write_data(df, output_path, file_format.as_ref()).await?;
                self.common_args.log_if_verbose(&format!("Output written to: {}", output_path.display()));
            }
            None => {
                display_dataframe(df, None, self.common_args.format.as_ref()).await?;
            }
        }

        Ok(())
    }


    fn map_output_format(&self, format: &Option<OutputFormat>) -> Option<FileFormat> {
        match format {
            Some(OutputFormat::Json) => Some(FileFormat::Json),
            Some(OutputFormat::Csv) => Some(FileFormat::Csv),
            Some(OutputFormat::Parquet) => Some(FileFormat::Parquet),
            Some(OutputFormat::Xlsx) => Some(FileFormat::Excel),
            Some(OutputFormat::Text) | None => None,
        }
    }
}