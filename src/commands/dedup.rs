use clap::Args;
use datafusion::prelude::*;
use std::collections::HashSet;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct DedupArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(long, help = "Remove duplicate rows", conflicts_with = "col_wise")]
	pub row_wise: bool,
	
	#[arg(long, help = "Remove duplicate columns", conflicts_with = "row_wise")]
	pub col_wise: bool,
	
	#[arg(short, long, help = "Columns to consider for row-wise deduplication")]
	pub columns: Option<String>,
	
	#[arg(long, help = "Keep first occurrence (default) vs last", default_value = "first")]
	pub keep: String,
}

pub async fn execute(args: DedupArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	
	if !args.row_wise && !args.col_wise {
		return Err(NailError::InvalidArgument(
			"Must specify either --row-wise or --col-wise".to_string()
		));
	}
	
	let result_df = if args.row_wise {
		args.common.log_if_verbose("Removing duplicate rows");
		deduplicate_rows(&df, args.columns.as_deref(), &args.keep, args.common.jobs).await?
	} else {
		args.common.log_if_verbose("Removing duplicate columns");
		deduplicate_columns(&df).await?
	};
	
	if args.common.verbose {
		let original_rows = df.clone().count().await?;
		let new_rows = result_df.clone().count().await?;
		let original_cols = df.schema().fields().len();
		let new_cols = result_df.schema().fields().len();
		
		if args.row_wise {
			eprintln!("Removed {} duplicate rows ({} -> {})", 
				original_rows - new_rows, original_rows, new_rows);
		} else {
			eprintln!("Removed {} duplicate columns ({} -> {})", 
				original_cols - new_cols, original_cols, new_cols);
		}
	}
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "dedup").await?;
	
	Ok(())
}

async fn deduplicate_rows(df: &DataFrame, columns: Option<&str>, keep: &str, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema();
	let dedup_cols = if let Some(col_spec) = columns {
		// Convert DFSchemaRef to DFSchemaRef for pattern matching
		crate::utils::stats::select_columns_by_pattern(schema.clone().into(), col_spec)?
	} else {
		schema.fields().iter().map(|f| f.name().clone()).collect()
	};
	
	if dedup_cols.is_empty() {
		// Nothing to dedup on, return original
		return Ok(df.clone());
	}
	
	let keep_first = match keep {
		"first" => true,
		"last" => false,
		_ => return Err(NailError::InvalidArgument("keep must be 'first' or 'last'".to_string())),
	};
	
	// We need a column to define original order. If none exists, we add one.
	// Use ROW_NUMBER() OVER() to add row numbers
	let df_with_rn = ctx.sql(&format!(
		"SELECT *, ROW_NUMBER() OVER() as __row_num_for_dedup__ FROM {}", 
		table_name
	)).await?;
	
	// Register the dataframe with row numbers
	ctx.register_table("temp_with_rn", df_with_rn.clone().into_view())?;
	
	// Build the window function expression
	let partition_by_cols: Vec<String> = dedup_cols.iter()
		.map(|c| format!("\"{}\"", c))
		.collect();
	let partition_by_str = partition_by_cols.join(", ");
	
	// Order by determines which row is 'first' or 'last'
	let order_by = if keep_first {
		"__row_num_for_dedup__ ASC"
	} else {
		"__row_num_for_dedup__ DESC"
	};
	
	// Get original column names (excluding our temp column)
	let original_columns: Vec<String> = df.schema().fields().iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();
	let select_columns = original_columns.join(", ");
	
	// Build the deduplication query
	let dedup_sql = format!(
		"WITH ranked AS (
			SELECT {}, 
				   ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) as rn
			FROM temp_with_rn
		)
		SELECT {} FROM ranked WHERE rn = 1",
		select_columns,
		partition_by_str,
		order_by,
		select_columns
	);
	
	let result_df = ctx.sql(&dedup_sql).await?;
	Ok(result_df)
}

async fn deduplicate_columns(df: &DataFrame) -> NailResult<DataFrame> {
	let schema = df.schema();
	let mut unique_columns = Vec::new();
	let mut seen_names = HashSet::new();
	let mut duplicates = Vec::new();
	
	for field in schema.fields() {
		let field_name = field.name();
		if !seen_names.contains(field_name) {
			seen_names.insert(field_name.clone());
			unique_columns.push(Expr::Column(datafusion::common::Column::new(None::<String>, field_name)));
		} else {
			duplicates.push(field_name.clone());
		}
	}
	
	// Error out if there are duplicate column names to prevent silent data loss
	if !duplicates.is_empty() {
		return Err(NailError::InvalidArgument(format!(
			"Duplicate column names found: {:?}. This could result in data loss. \
			Please use the 'rename' command to resolve column name conflicts before deduplicating.",
			duplicates
		)));
	}
	
	let result = df.clone().select(unique_columns)?;
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

    fn create_test_data_with_duplicates() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_duplicates.parquet");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        // Data with row duplicates
        let id_array = Int64Array::from(vec![1, 2, 3, 2, 4, 3]);
        let name_array = StringArray::from(vec![
            Some("Alice"), Some("Bob"), Some("Charlie"), Some("Bob"), Some("David"), Some("Charlie")
        ]);
        let value_array = Float64Array::from(vec![
            Some(100.0), Some(200.0), Some(300.0), Some(200.0), Some(400.0), Some(300.0)
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

    fn create_test_data_with_duplicate_columns() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_dup_columns.parquet");
        
        // Create schema with duplicate column names (simulate having columns with same content)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("duplicate_col", DataType::Float64, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![
            Some("Alice"), Some("Bob"), Some("Charlie")
        ]);
        let dup_array = Float64Array::from(vec![
            Some(100.0), Some(200.0), Some(300.0)
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(dup_array),
            ],
        ).unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_dir, file_path)
    }

    #[tokio::test]
    async fn test_dedup_row_wise_all_columns() {
        let (_temp_dir, input_path) = create_test_data_with_duplicates();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("deduped.parquet");
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: true,
            col_wise: false,
            columns: None, // All columns
            keep: "first".to_string(),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 4); // Should remove exact duplicates
    }

    #[tokio::test]
    async fn test_dedup_row_wise_specific_columns() {
        let (_temp_dir, input_path) = create_test_data_with_duplicates();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("deduped_specific.parquet");
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: true,
            col_wise: false,
            columns: Some("id,name".to_string()),
            keep: "first".to_string(),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 4); // Should remove rows with duplicate id,name combinations
    }

    #[tokio::test]
    async fn test_dedup_row_wise_keep_last() {
        let (_temp_dir, input_path) = create_test_data_with_duplicates();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("deduped_last.parquet");
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: true,
            col_wise: false,
            columns: None,
            keep: "last".to_string(),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 4); // Should keep last occurrence of duplicates
    }

    #[tokio::test]
    async fn test_dedup_col_wise() {
        let (_temp_dir, input_path) = create_test_data_with_duplicate_columns();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("deduped_cols.parquet");
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: false,
            col_wise: true,
            columns: None,
            keep: "first".to_string(),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        // Column deduplication should preserve unique column names
        assert_eq!(df.schema().fields().len(), 3); // All columns are unique in this test
    }

    #[tokio::test]
    async fn test_dedup_verbose_mode() {
        let (_temp_dir, input_path) = create_test_data_with_duplicates();
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: true,
                jobs: None,
            },
            row_wise: true,
            col_wise: false,
            columns: None,
            keep: "first".to_string(),
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dedup_missing_mode_error() {
        let (_temp_dir, input_path) = create_test_data_with_duplicates();
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: false, // Neither mode enabled
            col_wise: false,
            columns: None,
            keep: "first".to_string(),
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Must specify either --row-wise or --col-wise"));
    }

    #[tokio::test]
    async fn test_dedup_invalid_keep_option() {
        let (_temp_dir, input_path) = create_test_data_with_duplicates();
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: true,
            col_wise: false,
            columns: None,
            keep: "invalid".to_string(),
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("keep must be 'first' or 'last'"));
    }

    #[tokio::test]
    async fn test_dedup_no_duplicates() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("no_duplicates.parquet");
        
        // Create data without any duplicates
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4]);
        let name_array = StringArray::from(vec![
            Some("Alice"), Some("Bob"), Some("Charlie"), Some("David")
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        ).unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("no_dups_result.parquet");
        
        let args = DedupArgs {
            common: CommonArgs {
                input: file_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: true,
            col_wise: false,
            columns: None,
            keep: "first".to_string(),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 4); // All rows should remain
    }

    #[tokio::test]
    async fn test_dedup_empty_columns_spec() {
        let (_temp_dir, input_path) = create_test_data_with_duplicates();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("deduped_empty_cols.parquet");
        
        let args = DedupArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            row_wise: true,
            col_wise: false,
            columns: Some("".to_string()), // Empty column specification
            keep: "first".to_string(),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        // With empty column spec, should return original data unchanged since no dedup columns specified
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 6); // Should return original data unchanged
    }
}