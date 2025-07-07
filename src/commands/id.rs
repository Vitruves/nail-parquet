use clap::Args;
use datafusion::prelude::*;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct IdArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(long, help = "Create new ID column")]
	pub create: bool,
	
	#[arg(long, help = "Prefix for ID values", default_value = "id")]
	pub prefix: String,
	
	#[arg(long, help = "ID column name", default_value = "id")]
	pub id_col_name: String,
}

pub async fn execute(args: IdArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	
	let result_df = if args.create {
		args.common.log_if_verbose(&format!("Creating ID column '{}' with prefix '{}'", args.id_col_name, args.prefix));
		add_id_column(&df, &args.id_col_name, &args.prefix, args.common.jobs).await?
	} else {
		df
	};
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "id").await?;
	
	Ok(())
}

async fn add_id_column(df: &DataFrame, col_name: &str, prefix: &str, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	// Check if column already exists
	let schema = df.schema();
	if schema.field_with_name(None, col_name).is_ok() {
		return Err(crate::error::NailError::InvalidArgument(
			format!("Column '{}' already exists. Use --id-col-name to specify a different name.", col_name)
		));
	}
	
	let id_col = if prefix.is_empty() {
		"ROW_NUMBER() OVER()".to_string()
	} else {
		format!("CONCAT('{}', ROW_NUMBER() OVER())", prefix)
	};
	
	let columns: Vec<String> = df.schema().fields().iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();
	
	let sql = format!(
		"SELECT {} as \"{}\", {} FROM {}",
		id_col,
		col_name,
		columns.join(", "),
		table_name
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::cli::CommonArgs;
	use std::path::PathBuf;
	use tempfile::tempdir;
	use datafusion::prelude::SessionContext;
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
			Field::new("name", DataType::Utf8, false),
			Field::new("value", DataType::Float64, false),
			Field::new("category", DataType::Utf8, false),
		]));

		let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David"]);
		let value_array = Float64Array::from(vec![100.0, 200.0, 300.0, 400.0]);
		let category_array = StringArray::from(vec!["A", "B", "A", "C"]);

		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(name_array),
				Arc::new(value_array),
				Arc::new(category_array),
			],
		).unwrap();

		let file = File::create(&file_path).unwrap();
		let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
		writer.write(&batch).unwrap();
		writer.close().unwrap();

		(temp_dir, file_path)
	}

	#[tokio::test]
	async fn test_id_create_default() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");
		
		let args = IdArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				verbose: false,
				jobs: None,
			},
			create: true,
			prefix: "id".to_string(),
			id_col_name: "id".to_string(),
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
		
		// Check that ID column was added
		assert!(df.schema().field_with_name(None, "id").is_ok());
		assert_eq!(df.schema().fields().len(), 4); // Original 3 + ID column
		
		let row_count = df.clone().count().await.unwrap();
		assert_eq!(row_count, 4);
	}

	#[tokio::test]
	async fn test_id_create_custom_name_and_prefix() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");
		
		let args = IdArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				verbose: false,
				jobs: None,
			},
			create: true,
			prefix: "row_".to_string(),
			id_col_name: "row_id".to_string(),
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
		
		// Check that custom ID column was added
		assert!(df.schema().field_with_name(None, "row_id").is_ok());
		assert!(df.schema().field_with_name(None, "id").is_err()); // Default name should not exist
	}

	#[tokio::test]
	async fn test_id_create_empty_prefix() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");
		
		let args = IdArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				verbose: false,
				jobs: None,
			},
			create: true,
			prefix: "".to_string(), // Empty prefix
			id_col_name: "row_num".to_string(),
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
		
		// Check that ID column was added with numeric values only
		assert!(df.schema().field_with_name(None, "row_num").is_ok());
	}

	#[tokio::test]
	async fn test_id_no_create_flag() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");
		
		let args = IdArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				verbose: false,
				jobs: None,
			},
			create: false, // Don't create ID column
			prefix: "id".to_string(),
			id_col_name: "id".to_string(),
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
		
		// Should be identical to input (no ID column added)
		assert!(df.schema().field_with_name(None, "id").is_err());
		assert_eq!(df.schema().fields().len(), 3); // Original columns only
	}

	#[tokio::test]
	async fn test_id_column_already_exists() {
		let temp_dir = tempdir().unwrap();
		let file_path = temp_dir.path().join("test_with_id.parquet");
		
		// Create data that already has an 'id' column
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false), // Already has ID column
			Field::new("name", DataType::Utf8, false),
		]));

		let id_array = Int64Array::from(vec![100, 200, 300]);
		let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(id_array), Arc::new(name_array)],
		).unwrap();

		let file = File::create(&file_path).unwrap();
		let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
		writer.write(&batch).unwrap();
		writer.close().unwrap();
		
		let args = IdArgs {
			common: CommonArgs {
				input: file_path,
				output: None,
				format: None,
				random: None,
				verbose: false,
				jobs: None,
			},
			create: true,
			prefix: "id".to_string(),
			id_col_name: "id".to_string(), // Same name as existing column
		};

		let result = execute(args).await;
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("already exists"));
	}

	#[tokio::test]
	async fn test_id_verbose_mode() {
		let (_temp_dir, input_path) = create_test_data();
		
		let args = IdArgs {
			common: CommonArgs {
				input: input_path,
				output: None,
				format: None,
				random: None,
				verbose: true,
				jobs: None,
			},
			create: true,
			prefix: "test_".to_string(),
			id_col_name: "test_id".to_string(),
		};

		let result = execute(args).await;
		assert!(result.is_ok());
	}

	#[tokio::test]
	async fn test_id_with_different_jobs() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");
		
		let args = IdArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				verbose: false,
				jobs: Some(2), // Test with specific job count
			},
			create: true,
			prefix: "parallel_".to_string(),
			id_col_name: "parallel_id".to_string(),
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
		
		assert!(df.schema().field_with_name(None, "parallel_id").is_ok());
		assert_eq!(df.clone().count().await.unwrap(), 4);
	}

	#[tokio::test]
	async fn test_id_empty_dataset() {
		let temp_dir = tempdir().unwrap();
		let file_path = temp_dir.path().join("empty.parquet");
		
		// Create empty dataset
		let schema = Arc::new(Schema::new(vec![
			Field::new("name", DataType::Utf8, false),
		]));

		let name_array = StringArray::from(Vec::<String>::new());
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(name_array)],
		).unwrap();

		let file = File::create(&file_path).unwrap();
		let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
		writer.write(&batch).unwrap();
		writer.close().unwrap();
		
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");
		
		let args = IdArgs {
			common: CommonArgs {
				input: file_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				verbose: false,
				jobs: None,
			},
			create: true,
			prefix: "id".to_string(),
			id_col_name: "id".to_string(),
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
		
		// Should have ID column even with empty data
		assert!(df.schema().field_with_name(None, "id").is_ok());
		assert_eq!(df.schema().fields().len(), 2); // Original + ID
		assert_eq!(df.clone().count().await.unwrap(), 0); // No rows
	}
}