use clap::Args;
use datafusion::prelude::*;
use datafusion::common::DFSchemaRef;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct AppendArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(long, help = "Files to append (comma-separated)")]
	pub files: String,
	
	#[arg(long, help = "Ignore schema mismatches")]
	pub ignore_schema: bool,
}

pub async fn execute(args: AppendArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading base table from: {}", args.common.input.display()));
	
	let mut base_df = read_data(&args.common.input).await?;
	let base_schema: DFSchemaRef = base_df.schema().clone().into();
	
	let append_files: Vec<&str> = args.files.split(',').map(|s| s.trim()).collect();
	
	args.common.log_if_verbose(&format!("Appending {} files", append_files.len()));
	
	for file_path in append_files {
		let path = PathBuf::from(file_path);
		
		args.common.log_if_verbose(&format!("Appending: {}", path.display()));
		
		let append_df = read_data(&path).await?;
		let append_schema: DFSchemaRef = append_df.schema().clone().into();
		
		if !args.ignore_schema && !schemas_compatible(&base_schema, &append_schema) {
			return Err(NailError::InvalidArgument(format!(
				"Schema mismatch in file: {}. Use --ignore-schema to force append.",
				path.display()
			)));
		}
		
		let aligned_df = if args.ignore_schema {
			align_schemas(&append_df, &base_schema, args.common.jobs).await?
		} else {
			append_df
		};
		
		base_df = base_df.union(aligned_df)?;
	}
	
	let total_rows = base_df.clone().count().await?;
	args.common.log_if_verbose(&format!("Final dataset contains {} rows", total_rows));
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&base_df, "append").await?;
	
	Ok(())
}

fn schemas_compatible(schema1: &datafusion::common::DFSchemaRef, schema2: &datafusion::common::DFSchemaRef) -> bool {
	if schema1.fields().len() != schema2.fields().len() {
		return false;
	}
	
	for (field1, field2) in schema1.fields().iter().zip(schema2.fields().iter()) {
		if field1.name() != field2.name() || field1.data_type() != field2.data_type() {
			return false;
		}
	}
	
	true
}

async fn align_schemas(df: &DataFrame, target_schema: &datafusion::common::DFSchemaRef, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let current_schema = df.schema();
	let mut select_exprs = Vec::new();
	
	for target_field in target_schema.fields() {
		let target_name = target_field.name();
		
		if let Ok(_current_field) = current_schema.field_with_name(None, target_name) {
			select_exprs.push(Expr::Column(datafusion::common::Column::new(None::<String>, target_name)));
		} else {
			let null_expr = match target_field.data_type() {
				datafusion::arrow::datatypes::DataType::Int64 => lit(0i64).alias(target_name),
				datafusion::arrow::datatypes::DataType::Float64 => lit(0.0f64).alias(target_name),
				datafusion::arrow::datatypes::DataType::Utf8 => lit("").alias(target_name),
				datafusion::arrow::datatypes::DataType::Boolean => lit(false).alias(target_name),
				_ => lit("").alias(target_name),
			};
			select_exprs.push(null_expr);
		}
	}
	
	let result = ctx.table(table_name).await?.select(select_exprs)?;
	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::*;
	use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
	use datafusion::arrow::array::{Int64Array, StringArray};
	use datafusion::arrow::record_batch::RecordBatch;
	use std::sync::Arc;
	use tempfile::tempdir;
	
	async fn create_test_dataframe(
		values: Vec<i64>,
		names: Vec<&str>
	) -> NailResult<DataFrame> {
		let ctx = SessionContext::new();
		
		let schema = Arc::new(ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, false),
		]));
		
		let id_array = Arc::new(Int64Array::from(values));
		let name_array = Arc::new(StringArray::from(names));
		
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![id_array, name_array],
		)?;
		
		let df = ctx.read_batch(batch)?;
		Ok(df)
	}
	
	#[test]
	fn test_schemas_compatible() {
		use datafusion::common::DFSchema;
		use datafusion::arrow::datatypes::{DataType, Field};
		
		// Create compatible schemas by converting from Arrow schemas
		let arrow_schema1 = ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, false),
		]);
		let schema1 = Arc::new(DFSchema::try_from(arrow_schema1).unwrap());
		
		let arrow_schema2 = ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, false),
		]);
		let schema2 = Arc::new(DFSchema::try_from(arrow_schema2).unwrap());
		
		assert!(schemas_compatible(&schema1, &schema2));
		
		// Test incompatible schemas - different number of fields
		let arrow_schema3 = ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
		]);
		let schema3 = Arc::new(DFSchema::try_from(arrow_schema3).unwrap());
		
		assert!(!schemas_compatible(&schema1, &schema3));
		
		// Test incompatible schemas - different field names
		let arrow_schema4 = ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("description", DataType::Utf8, false),
		]);
		let schema4 = Arc::new(DFSchema::try_from(arrow_schema4).unwrap());
		
		assert!(!schemas_compatible(&schema1, &schema4));
		
		// Test incompatible schemas - different data types
		let arrow_schema5 = ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Float64, false),
		]);
		let schema5 = Arc::new(DFSchema::try_from(arrow_schema5).unwrap());
		
		assert!(!schemas_compatible(&schema1, &schema5));
	}
	
	#[tokio::test]
	async fn test_align_schemas() {
		// Create a DataFrame with missing columns
		let df = create_test_dataframe(vec![1, 2, 3], vec!["a", "b", "c"]).await.unwrap();
		
		// Create target schema with additional column
		use datafusion::common::DFSchema;
		use datafusion::arrow::datatypes::{DataType, Field};
		
		let arrow_target_schema = ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, false),
			Field::new("score", DataType::Float64, false),
		]);
		let target_schema = Arc::new(DFSchema::try_from(arrow_target_schema).unwrap());
		
		let aligned_df = align_schemas(&df, &target_schema, None).await.unwrap();
		let aligned_schema = aligned_df.schema();
		
		// Check that aligned schema has all target fields
		assert_eq!(aligned_schema.fields().len(), 3);
		assert!(aligned_schema.field_with_name(None, "id").is_ok());
		assert!(aligned_schema.field_with_name(None, "name").is_ok());
		assert!(aligned_schema.field_with_name(None, "score").is_ok());
		
		// Check that default values are added for missing column
		let batches = aligned_df.collect().await.unwrap();
		assert_eq!(batches[0].num_rows(), 3);
		
		let score_array = batches[0].column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();
		assert_eq!(score_array.value(0), 0.0);
		assert_eq!(score_array.value(1), 0.0);
		assert_eq!(score_array.value(2), 0.0);
	}
	
	#[tokio::test]
	async fn test_execute_with_compatible_schemas() {
		let temp_dir = tempdir().unwrap();
		let base_path = temp_dir.path().join("base.parquet");
		let append_path = temp_dir.path().join("append.parquet");
		
		// Create base file
		let base_df = create_test_dataframe(vec![1, 2], vec!["a", "b"]).await.unwrap();
		crate::utils::io::write_data(&base_df, &base_path, None).await.unwrap();
		
		// Create append file
		let append_df = create_test_dataframe(vec![3, 4], vec!["c", "d"]).await.unwrap();
		crate::utils::io::write_data(&append_df, &append_path, None).await.unwrap();
		
		// Create args
		let args = AppendArgs {
			common: CommonArgs {
				input: base_path.clone(),
				output: None,
				format: None,
				verbose: false,
				jobs: None,
				random: None,
			},
			files: append_path.to_string_lossy().to_string(),
			ignore_schema: false,
		};
		
		// Execute should succeed
		let result = execute(args).await;
		assert!(result.is_ok());
	}
	
	#[tokio::test]
	async fn test_execute_with_incompatible_schemas() {
		let temp_dir = tempdir().unwrap();
		let base_path = temp_dir.path().join("base.parquet");
		let append_path = temp_dir.path().join("append.parquet");
		
		// Create base file
		let base_df = create_test_dataframe(vec![1, 2], vec!["a", "b"]).await.unwrap();
		crate::utils::io::write_data(&base_df, &base_path, None).await.unwrap();
		
		// Create append file with different schema
		let ctx = SessionContext::new();
		let schema = Arc::new(ArrowSchema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("value", DataType::Float64, false),
		]));
		
		let id_array = Arc::new(Int64Array::from(vec![3, 4]));
		let value_array = Arc::new(datafusion::arrow::array::Float64Array::from(vec![3.0, 4.0]));
		
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![id_array, value_array],
		).unwrap();
		
		let append_df = ctx.read_batch(batch).unwrap();
		crate::utils::io::write_data(&append_df, &append_path, None).await.unwrap();
		
		// Create args without ignore_schema
		let args = AppendArgs {
			common: CommonArgs {
				input: base_path.clone(),
				output: None,
				format: None,
				verbose: false,
				jobs: None,
				random: None,
			},
			files: append_path.to_string_lossy().to_string(),
			ignore_schema: false,
		};
		
		// Execute should fail due to schema mismatch
		let result = execute(args).await;
		assert!(result.is_err());
		
		// Now test with ignore_schema
		let args_ignore = AppendArgs {
			common: CommonArgs {
				input: base_path.clone(),
				output: None,
				format: None,
				verbose: false,
				jobs: None,
				random: None,
			},
			files: append_path.to_string_lossy().to_string(),
			ignore_schema: true,
		};
		
		// Execute should succeed with ignore_schema
		let result = execute(args_ignore).await;
		assert!(result.is_ok());
	}
}