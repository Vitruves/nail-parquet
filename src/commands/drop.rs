use clap::Args;
use datafusion::prelude::*;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::utils::column::resolve_column_name;
use crate::commands::select::{select_columns_by_pattern, parse_row_specification};
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct DropArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Column names or regex patterns to drop (comma-separated)")]
	pub columns: Option<String>,
	
	#[arg(short, long, help = "Row numbers/ranges to drop (e.g., 1,3,5-10) OR column conditions (e.g., 'name=John', 'age>25', 'status!=active,score<=50'). Without -o/--output, acts as dry run showing remaining records")]
	pub rows: Option<String>,
}

pub async fn execute(args: DropArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let mut result_df = df;
	
	if let Some(col_spec) = &args.columns {
		let schema = result_df.schema();
		let columns_to_drop = select_columns_by_pattern(schema.clone().into(), col_spec)?;
		
		args.common.log_if_verbose(&format!("Dropping {} columns: {:?}", columns_to_drop.len(), columns_to_drop));
		
		let remaining_columns: Vec<Expr> = result_df.schema().fields().iter()
			.filter(|f| !columns_to_drop.contains(f.name()))
			.map(|f| Expr::Column(datafusion::common::Column::new(None::<String>, f.name())))
			.collect();
		
		result_df = result_df.select(remaining_columns)?;
	}
	
	if let Some(row_spec) = &args.rows {
		// Check if it's a column condition or row indices
		if is_column_condition(row_spec) {
			args.common.log_if_verbose(&format!("Dropping rows based on column conditions: {}", row_spec));
			result_df = drop_rows_by_conditions(&result_df, row_spec, args.common.jobs).await?;
		} else {
			let row_indices = parse_row_specification(row_spec)?;
			args.common.log_if_verbose(&format!("Dropping {} rows", row_indices.len()));
			result_df = drop_rows_by_indices(&result_df, &row_indices, args.common.jobs).await?;
		}
	}
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "drop").await?;
	
	Ok(())
}

async fn drop_rows_by_indices(df: &DataFrame, indices: &[usize], jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let indices_str = indices.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");
	
	// Get the original column names and quote them to preserve case
	let original_columns: Vec<String> = df.schema().fields().iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();
	
	let sql = format!(
		"SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn NOT IN ({})",
		original_columns.join(", "),
		original_columns.join(", "),
		table_name, 
		indices_str
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}

fn is_column_condition(spec: &str) -> bool {
	// Check if the spec contains any comparison operators
	spec.contains('=') || spec.contains('<') || spec.contains('>')
}

async fn drop_rows_by_conditions(df: &DataFrame, conditions: &str, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema().clone().into();
	let mut filter_conditions = Vec::new();
	
	for condition in conditions.split(',') {
		let condition = condition.trim();
		let filter_expr = parse_condition_with_schema(condition, &schema).await?;
		filter_conditions.push(filter_expr);
	}
	
	// Combine all conditions with AND (rows matching ALL conditions will be dropped)
	let combined_filter = filter_conditions.into_iter()
		.reduce(|acc, expr| acc.and(expr))
		.unwrap();
	
	// Apply NOT to the combined filter to drop matching rows
	let drop_filter = combined_filter.not();
	
	let result = ctx.table(table_name).await?.filter(drop_filter)?;
	Ok(result)
}

async fn parse_condition_with_schema(condition: &str, schema: &datafusion::common::DFSchemaRef) -> NailResult<Expr> {
	let operators = [">=", "<=", "!=", "=", ">", "<"];
	
	for op in &operators {
		if let Some(pos) = condition.find(op) {
			let column_name_input = condition[..pos].trim();
			let value_str = condition[pos + op.len()..].trim();
			
			// Use the centralized column resolution utility
			let actual_column_name = resolve_column_name(schema, column_name_input)?;
			
			let value_expr = if let Ok(int_val) = value_str.parse::<i64>() {
				lit(int_val)
			} else if let Ok(float_val) = value_str.parse::<f64>() {
				lit(float_val)
			} else if value_str.eq_ignore_ascii_case("true") {
				lit(true)
			} else if value_str.eq_ignore_ascii_case("false") {
				lit(false)
			} else {
				lit(value_str)
			};
			
			// Use quoted column name to preserve case sensitivity
			let column_expr = Expr::Column(datafusion::common::Column::new(None::<String>, &actual_column_name));
			
			return Ok(match *op {
				"=" => column_expr.eq(value_expr),
				"!=" => column_expr.not_eq(value_expr),
				">" => column_expr.gt(value_expr),
				">=" => column_expr.gt_eq(value_expr),
				"<" => column_expr.lt(value_expr),
				"<=" => column_expr.lt_eq(value_expr),
				_ => unreachable!(),
			});
		}
	}
	
	Err(NailError::InvalidArgument(format!("Invalid condition: {}", condition)))
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
    async fn test_drop_columns_by_name() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("dropped_cols.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("value,category".to_string()),
            rows: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.schema().fields().len(), 2); // Should have 2 columns left
        assert!(df.schema().field_with_name(None, "id").is_ok());
        assert!(df.schema().field_with_name(None, "name").is_ok());
        assert!(df.schema().field_with_name(None, "value").is_err());
        assert!(df.schema().field_with_name(None, "category").is_err());
    }

    #[tokio::test]
    async fn test_drop_columns_by_pattern() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("dropped_pattern.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("val.*,cat.*".to_string()),
            rows: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.schema().fields().len(), 2); // Should drop columns matching pattern
        assert!(df.schema().field_with_name(None, "id").is_ok());
        assert!(df.schema().field_with_name(None, "name").is_ok());
    }

    #[tokio::test]
    async fn test_drop_rows_by_indices() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("dropped_rows.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("1,3,5".to_string()),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 2); // Should have 2 rows left (dropped rows 1, 3, 5)
    }

    #[tokio::test]
    async fn test_drop_rows_by_range() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("dropped_range.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("2-4".to_string()),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 2); // Should have 2 rows left (dropped rows 2, 3, 4)
    }

    #[tokio::test]
    async fn test_drop_both_columns_and_rows() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("dropped_both.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("value".to_string()),
            rows: Some("1,5".to_string()),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.schema().fields().len(), 3); // Should have 3 columns left
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 3); // Should have 3 rows left
    }

    #[tokio::test]
    async fn test_drop_verbose_mode() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: true,
                jobs: None,
            },
            columns: Some("value".to_string()),
            rows: None,
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_no_arguments() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("unchanged.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        // Should output all data unchanged
        assert_eq!(df.schema().fields().len(), 4);
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 5);
    }

    #[tokio::test]
    async fn test_drop_out_of_range_rows() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("out_of_range.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("10,20".to_string()), // Indices beyond the data
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 5); // Should have all rows (no valid indices to drop)
    }

    #[tokio::test]
    async fn test_drop_single_column() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("single_col_dropped.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("name".to_string()),
            rows: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        assert_eq!(df.schema().fields().len(), 3); // Should have 3 columns left
        assert!(df.schema().field_with_name(None, "name").is_err());
    }

    #[tokio::test]
    async fn test_drop_single_row() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("single_row_dropped.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("3".to_string()),
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 4); // Should have 4 rows left
    }

    #[tokio::test]
    async fn test_drop_invalid_column() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("nonexistent_column".to_string()),
            rows: None,
        };

        let result = execute(args).await;
        // This should fail because the column doesn't exist
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_drop_empty_column_spec() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("empty_spec.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("".to_string()), // Empty column specification
            rows: None,
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        // Should not drop any columns with empty spec
        assert_eq!(df.schema().fields().len(), 4);
    }

    #[tokio::test]
    async fn test_drop_empty_row_spec() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("empty_row_spec.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("".to_string()), // Empty row specification
        };

        let result = execute(args).await;
        // Should fail with invalid row specification
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_mixed_valid_invalid_rows() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("mixed_rows.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("1,10,3".to_string()), // Mix of valid and out-of-range indices
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 3); // Should drop valid indices (1, 3) and ignore invalid ones
    }

    #[tokio::test]
    async fn test_drop_rows_by_column_condition_equals() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("condition_equals.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("name=Alice".to_string()), // Drop rows where name equals Alice
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 4); // Should have 4 rows left (dropped Alice)
    }

    #[tokio::test]
    async fn test_drop_rows_by_column_condition_not_equals() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("condition_not_equals.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("category!=A".to_string()), // Drop rows where category is not A
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 2); // Should have 2 rows left (only category=A rows)
    }

    #[tokio::test]
    async fn test_drop_rows_by_column_condition_greater_than() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("condition_greater.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("value>250".to_string()), // Drop rows where value > 250
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 2); // Should have 2 rows left (value <= 250)
    }

    #[tokio::test]
    async fn test_drop_rows_by_column_condition_less_than_equal() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("condition_less_equal.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("id<=2".to_string()), // Drop rows where id <= 2
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 3); // Should have 3 rows left (id > 2)
    }

    #[tokio::test]
    async fn test_drop_rows_by_multiple_column_conditions() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("multiple_conditions.parquet");
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("category=A,value>=300".to_string()), // Drop rows where category=A AND value>=300
        };

        execute(args).await.unwrap();

        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 4); // Should have 4 rows left (only Charlie with category=A AND value>=300 is dropped)
    }

    #[tokio::test]
    async fn test_drop_rows_dry_run() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: None, // No output file = dry run
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("name=Alice".to_string()),
        };

        // This should not fail and should act as dry run
        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_rows_invalid_column_condition() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("nonexistent_column=value".to_string()),
        };

        let result = execute(args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_rows_invalid_condition_format() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = DropArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            rows: Some("invalid_format_without_operator".to_string()),
        };

        let result = execute(args).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_is_column_condition() {
        assert!(is_column_condition("name=Alice"));
        assert!(is_column_condition("age>25"));
        assert!(is_column_condition("score<=100"));
        assert!(is_column_condition("status!=active"));
        assert!(is_column_condition("value>=50"));
        assert!(is_column_condition("count<10"));
        
        assert!(!is_column_condition("1,2,3"));
        assert!(!is_column_condition("5-10"));
        assert!(!is_column_condition("just_text"));
    }
}