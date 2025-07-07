use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::{read_data, write_data};
use crate::utils::{detect_file_format};

#[derive(Args, Clone)]
pub struct ConvertArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Output file.\n\
	                           Supported output formats:\n\
	                           • Parquet (.parquet)\n\
	                           • CSV (.csv)\n\
	                           • JSON (.json)\n\
	                           • Excel (.xlsx) - write support")]
	pub output: PathBuf,
	
	#[arg(long, help = "Random seed for reproducible results")]
	pub random: Option<u64>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
}

pub async fn execute(args: ConvertArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Converting {} to {}", args.input.display(), args.output.display());
	}
	
	let input_format = detect_file_format(&args.input)?;
	let output_format = detect_file_format(&args.output)?;
	
	if args.verbose {
		eprintln!("Input format: {:?}, Output format: {:?}", input_format, output_format);
	}
	
	let df = read_data(&args.input).await?;
	
	let rows = df.clone().count().await?;
	let cols = df.schema().fields().len();
	if args.verbose {
		eprintln!("Processing {} rows, {} columns", rows, cols);
	}
	
	write_data(&df, &args.output, Some(&output_format)).await?;
	
	if args.verbose {
		eprintln!("Conversion completed successfully");
	}
	
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use datafusion::prelude::*;
	use datafusion::arrow::array::{Int64Array, StringArray};
	use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
	use datafusion::arrow::record_batch::RecordBatch;
	use std::sync::Arc;
	use tempfile::tempdir;
	
	async fn create_test_dataframe() -> NailResult<DataFrame> {
		let ctx = SessionContext::new();
		
		let schema = Arc::new(ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, false),
			Field::new("value", DataType::Float64, false),
		]));
		
		let id_array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
		let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]));
		let value_array = Arc::new(datafusion::arrow::array::Float64Array::from(vec![10.5, 20.0, 30.5, 40.0, 50.5]));
		
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![id_array, name_array, value_array],
		)?;
		
		let df = ctx.read_batch(batch)?;
		Ok(df)
	}
	
	#[tokio::test]
	async fn test_execute_parquet_to_csv() {
		let temp_dir = tempdir().unwrap();
		let input_path = temp_dir.path().join("test.parquet");
		let output_path = temp_dir.path().join("output.csv");
		
		// Create test data in parquet format
		let df = create_test_dataframe().await.unwrap();
		crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
		
		// Create convert args
		let args = ConvertArgs {
			input: input_path.clone(),
			output: output_path.clone(),
			random: None,
			verbose: false,
			jobs: None,
		};
		
		// Execute conversion
		let result = execute(args).await;
		assert!(result.is_ok());
		
		// Verify output file exists and is readable
		assert!(output_path.exists());
		
		// Read back the CSV and verify content
		let result_df = crate::utils::io::read_data(&output_path).await.unwrap();
		let row_count = result_df.clone().count().await.unwrap();
		assert_eq!(row_count, 5);
		
		let schema = result_df.schema();
		assert_eq!(schema.fields().len(), 3);
		assert!(schema.field_with_name(None, "id").is_ok());
		assert!(schema.field_with_name(None, "name").is_ok());
		assert!(schema.field_with_name(None, "value").is_ok());
	}
	
	#[tokio::test]
	async fn test_execute_csv_to_parquet() {
		let temp_dir = tempdir().unwrap();
		let input_path = temp_dir.path().join("test.csv");
		let output_path = temp_dir.path().join("output.parquet");
		
		// Create test data in CSV format
		let df = create_test_dataframe().await.unwrap();
		crate::utils::io::write_data(&df, &input_path, Some(&crate::utils::FileFormat::Csv)).await.unwrap();
		
		// Create convert args
		let args = ConvertArgs {
			input: input_path.clone(),
			output: output_path.clone(),
			random: None,
			verbose: false,
			jobs: None,
		};
		
		// Execute conversion
		let result = execute(args).await;
		assert!(result.is_ok());
		
		// Verify output file exists and is readable
		assert!(output_path.exists());
		
		// Read back the parquet and verify content
		let result_df = crate::utils::io::read_data(&output_path).await.unwrap();
		let row_count = result_df.clone().count().await.unwrap();
		assert_eq!(row_count, 5);
		
		let schema = result_df.schema();
		assert_eq!(schema.fields().len(), 3);
	}
	
	#[tokio::test]
	async fn test_execute_parquet_to_json() {
		let temp_dir = tempdir().unwrap();
		let input_path = temp_dir.path().join("test.parquet");
		let output_path = temp_dir.path().join("output.json");
		
		// Create test data
		let df = create_test_dataframe().await.unwrap();
		crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
		
		// Create convert args
		let args = ConvertArgs {
			input: input_path.clone(),
			output: output_path.clone(),
			random: None,
			verbose: true, // Test verbose output
			jobs: Some(2), // Test jobs parameter
		};
		
		// Execute conversion
		let result = execute(args).await;
		assert!(result.is_ok());
		
		// Verify output file exists
		assert!(output_path.exists());
		
		// Read back the JSON and verify content
		let result_df = crate::utils::io::read_data(&output_path).await.unwrap();
		let row_count = result_df.clone().count().await.unwrap();
		assert_eq!(row_count, 5);
	}
	
	#[tokio::test]
	async fn test_execute_with_invalid_input() {
		let temp_dir = tempdir().unwrap();
		let input_path = temp_dir.path().join("nonexistent.parquet");
		let output_path = temp_dir.path().join("output.csv");
		
		// Create convert args with non-existent input
		let args = ConvertArgs {
			input: input_path.clone(),
			output: output_path.clone(),
			random: None,
			verbose: false,
			jobs: None,
		};
		
		// Execute conversion should fail
		let result = execute(args).await;
		assert!(result.is_err());
	}
	
	#[tokio::test]
	async fn test_execute_same_format_conversion() {
		let temp_dir = tempdir().unwrap();
		let input_path = temp_dir.path().join("test.parquet");
		let output_path = temp_dir.path().join("output.parquet");
		
		// Create test data
		let df = create_test_dataframe().await.unwrap();
		crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
		
		// Create convert args (parquet to parquet)
		let args = ConvertArgs {
			input: input_path.clone(),
			output: output_path.clone(),
			random: Some(42), // Test random seed
			verbose: false,
			jobs: None,
		};
		
		// Execute conversion
		let result = execute(args).await;
		assert!(result.is_ok());
		
		// Verify output file exists and has same content
		assert!(output_path.exists());
		
		let result_df = crate::utils::io::read_data(&output_path).await.unwrap();
		let row_count = result_df.clone().count().await.unwrap();
		assert_eq!(row_count, 5);
	}
	
	#[tokio::test]
	async fn test_execute_with_empty_dataframe() {
		let temp_dir = tempdir().unwrap();
		let input_path = temp_dir.path().join("empty.parquet");
		let output_path = temp_dir.path().join("output.parquet");
		
		// Create empty dataframe
		let ctx = SessionContext::new();
		let schema = Arc::new(ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
		]));
		
		let id_array = Arc::new(Int64Array::from(Vec::<i64>::new()));
		let batch = RecordBatch::try_new(schema.clone(), vec![id_array]).unwrap();
		let empty_df = ctx.read_batch(batch).unwrap();
		
		crate::utils::io::write_data(&empty_df, &input_path, None).await.unwrap();
		
		// Create convert args
		let args = ConvertArgs {
			input: input_path.clone(),
			output: output_path.clone(),
			random: None,
			verbose: false,
			jobs: None,
		};
		
		// Execute conversion should work with empty data
		let result = execute(args).await;
		assert!(result.is_ok());
		
		// Verify output file exists - parquet should be created even when empty
		assert!(output_path.exists());
		
		let result_df = crate::utils::io::read_data(&output_path).await.unwrap();
		let row_count = result_df.clone().count().await.unwrap();
		assert_eq!(row_count, 0);
	}
}