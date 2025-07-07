use clap::Args;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};

#[derive(Args, Clone)]
pub struct CreateArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(short = 'c', long = "column", 
          help = "Column creation specs (name=expression), comma-separated.\n\
                  Supported operators:\n\
                  • Arithmetic: +, -, *, / (e.g., 'total=price*quantity')\n\
                  • Comparison: >, < (e.g., 'is_expensive=price>100')\n\
                  • Parentheses: () for grouping (e.g., 'result=(a+b)*c')\n\
                  • Column references: Use existing column names\n\
                  • Constants: Numeric values (integers and floats)\n\
                  Examples:\n\
                  • Simple: 'doubled=value*2'\n\
                  • Complex: 'profit=(revenue-cost)/revenue*100'\n\
                  • Conditional: 'high_value=amount>1000'")]
    pub columns: Option<String>,

    #[arg(short = 'r', long = "row", help = "Row filter expression")]
    pub row_filter: Option<String>,
}

pub async fn execute(args: CreateArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

    let ctx = crate::utils::create_context_with_jobs(args.common.jobs).await?;
    let df = read_data(&args.common.input).await?;
    ctx.register_table("t", df.clone().into_view())?;
    
    let mut result_df = df;

    // Apply row filter if specified
    if let Some(row_expr) = &args.row_filter {
        args.common.log_if_verbose(&format!("Applying row filter: {}", row_expr));
        // Use SQL for row filtering
        let filter_sql = format!("SELECT * FROM t WHERE {}", row_expr);
        result_df = ctx.sql(&filter_sql).await
            .map_err(|e| NailError::InvalidArgument(format!("Invalid row filter expression: {}", e)))?;
        // Re-register the filtered data - need to deregister first
        ctx.deregister_table("t")?;
        ctx.register_table("t", result_df.clone().into_view())?;
    }

    // Parse and apply column creation specs
    if let Some(col_specs) = &args.columns {
        let mut column_map = Vec::new();
        for pair in col_specs.split(',') {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() != 2 {
                return Err(NailError::InvalidArgument(format!("Invalid column spec: {}", pair)));
            }
            let name = parts[0].trim();
            let expr = parts[1].trim();
            column_map.push((name.to_string(), expr.to_string()));
        }

        args.common.log_if_verbose(&format!("Creating columns: {:?}", column_map));

        // Validate column names don't already exist
        let existing_columns: Vec<String> = result_df.schema().fields().iter()
            .map(|f| f.name().clone()).collect();
        
        for (name, _) in &column_map {
            if existing_columns.contains(name) {
                return Err(NailError::InvalidArgument(format!("Column '{}' already exists", name)));
            }
        }

        // Build SQL select list
        let mut select_list = vec!["*".to_string()];
        
        for (name, expr_str) in &column_map {
            select_list.push(format!("({}) AS \"{}\"", expr_str, name));
        }
        
        let sql = format!("SELECT {} FROM t", select_list.join(", "));
        args.common.log_if_verbose(&format!("Executing SQL: {}", sql));
        
        result_df = ctx.sql(&sql).await
            .map_err(|e| NailError::InvalidArgument(format!("Invalid column expression: {}", e)))?;
    }

    // Write or display result
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&result_df, "create").await?;

    Ok(())
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
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
            Field::new("category", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec![
            Some("Alice"), Some("Bob"), Some("Charlie"), Some("David"), Some("Eve")
        ]);
        let value_array = Float64Array::from(vec![
            Some(100.0), Some(200.0), Some(300.0), Some(400.0), Some(500.0)
        ]);
        let category_array = StringArray::from(vec![
            Some("A"), Some("B"), Some("A"), Some("C"), Some("B")
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
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
    async fn test_create_arithmetic_column() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("output.parquet");
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("doubled=value*2".to_string()),
            row_filter: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.clone().count().await.unwrap(), 5);
        assert!(df.schema().field_with_name(None, "doubled").is_ok());
        assert_eq!(df.schema().fields().len(), 5);
    }

    #[tokio::test]
    async fn test_create_comparison_column() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("output.parquet");
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("high_value=value>300".to_string()),
            row_filter: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.clone().count().await.unwrap(), 5);
        assert!(df.schema().field_with_name(None, "high_value").is_ok());
    }

    #[tokio::test]
    async fn test_create_multiple_columns() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("output.parquet");
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("doubled=value*2,id_plus_one=id+1".to_string()),
            row_filter: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.clone().count().await.unwrap(), 5);
        assert!(df.schema().field_with_name(None, "doubled").is_ok());
        assert!(df.schema().field_with_name(None, "id_plus_one").is_ok());
        assert_eq!(df.schema().fields().len(), 6);
    }

    #[tokio::test]
    async fn test_create_with_row_filter() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("output.parquet");
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("doubled=value*2".to_string()),
            row_filter: Some("id>2".to_string()),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.clone().count().await.unwrap(), 3); // Only rows with id > 2
        assert!(df.schema().field_with_name(None, "doubled").is_ok());
    }

    #[tokio::test]
    async fn test_create_existing_column_error() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("id=value*2".to_string()), // 'id' already exists
            row_filter: None,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_create_invalid_column_spec() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("invalid_spec".to_string()), // Missing '='
            row_filter: None,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid column spec"));
    }

    #[tokio::test]
    async fn test_create_invalid_expression() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("test=nonexistent_column*2".to_string()),
            row_filter: None,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid column expression"));
    }

    #[tokio::test]
    async fn test_create_invalid_row_filter() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = CreateArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("doubled=value*2".to_string()),
            row_filter: Some("nonexistent_column>5".to_string()),
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid row filter expression"));
    }
}
