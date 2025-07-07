use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::parquet_utils::{get_parquet_row_count_fast, can_use_fast_metadata};
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use datafusion::prelude::*;

#[derive(Args, Clone)]
pub struct CountArgs {
	#[command(flatten)]
	pub common: CommonArgs,
}

pub async fn execute(args: CountArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	// Use fast metadata reading for Parquet files
	let row_count = if can_use_fast_metadata(&args.common.input) {
		args.common.log_if_verbose("Using fast Parquet metadata for counting");
		get_parquet_row_count_fast(&args.common.input).await?
	} else {
		args.common.log_if_verbose("Using DataFusion for counting");
		let df = read_data(&args.common.input).await?;
		df.clone().count().await.map_err(crate::error::NailError::DataFusion)?
	};
	
	args.common.log_if_verbose(&format!("Counted {} rows", row_count));
	
	// For count command, output simple number to console or structured data to file
	match &args.common.output {
		Some(_output_path) => {
			// Create a DataFrame with the count result for file output
			let ctx = SessionContext::new();
			let result_df = ctx.sql(&format!("SELECT {} as row_count", row_count)).await
				.map_err(crate::error::NailError::DataFusion)?;
			
			let output_handler = OutputHandler::new(&args.common);
			output_handler.handle_output(&result_df, "count").await?;
		}
		None => {
			// Simple console output
			println!("{}", row_count);
		}
	}
	
	Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::CommonArgs;
    use std::path::PathBuf;
    use tempfile::tempdir;
    
    use parquet::arrow::ArrowWriter;
    use arrow::array::{Int64Array, StringArray, Float64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::fs::File;
    use std::sync::Arc;

    fn create_test_data() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.parquet");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![
            Some("Alice"), Some("Bob"), Some("Charlie")
        ]);
        let value_array = Float64Array::from(vec![
            Some(100.0), Some(200.0), Some(300.0)
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        ).unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_dir, file_path)
    }

    fn create_empty_data() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("empty.parquet");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let id_array = Int64Array::from(Vec::<i64>::new());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array)],
        ).unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_dir, file_path)
    }

    #[tokio::test]
    async fn test_count_basic() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = CountArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
        };

        // Execute and capture output by redirecting it to a file
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("count_output.txt");
        
        let args_with_output = CountArgs {
            common: CommonArgs {
                input: args.common.input.clone(),
                output: Some(output_path.clone()),
                format: Some(crate::cli::OutputFormat::Json),
                random: None,
                verbose: false,
                jobs: None,
            },
        };

        execute(args_with_output).await.unwrap();

        // Read the output file and verify the count
        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("\"row_count\":3") || content.contains("\"row_count\": 3"));
    }

    #[tokio::test]
    async fn test_count_empty_file() {
        let (_temp_dir, input_path) = create_empty_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("empty_count.json");
        
        let args = CountArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: Some(crate::cli::OutputFormat::Json),
                random: None,
                verbose: false,
                jobs: None,
            },
        };

        execute(args).await.unwrap();

        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("\"row_count\":0") || content.contains("\"row_count\": 0"));
    }

    #[tokio::test]
    async fn test_count_console_output() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = CountArgs {
            common: CommonArgs {
                input: input_path,
                output: None, // No output file specified
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
        };

        // This test just ensures no error occurs with console output
        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_count_verbose_mode() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = CountArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: true, // Enable verbose mode
                jobs: None,
            },
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_count_csv_output() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("count.csv");
        
        let args = CountArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: Some(crate::cli::OutputFormat::Csv),
                random: None,
                verbose: false,
                jobs: None,
            },
        };

        execute(args).await.unwrap();

        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("row_count"));
        assert!(content.contains("3"));
    }

    #[tokio::test]
    async fn test_count_invalid_file() {
        let temp_dir = tempdir().unwrap();
        let nonexistent_path = temp_dir.path().join("nonexistent.parquet");
        
        let args = CountArgs {
            common: CommonArgs {
                input: nonexistent_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
        };

        let result = execute(args).await;
        assert!(result.is_err());
    }
}