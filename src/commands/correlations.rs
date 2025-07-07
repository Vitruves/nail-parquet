use clap::Args;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use crate::utils::stats::{calculate_correlations, CorrelationType, select_columns_by_pattern};
use clap::ValueEnum;

#[derive(ValueEnum, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum CorrTest {
    fisher_exact,
    chi_sqr,
    t_test,
}

#[derive(Args, Clone)]
pub struct CorrelationsArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Comma-separated column names or regex patterns")]
	pub columns: Option<String>,
	
	#[arg(short = 't', long, help = "Correlation type", value_enum, default_value = "pearson")]
	pub correlation_type: CorrelationType,
	
	#[arg(long, help = "Output correlation matrix format")]
	pub correlation_matrix: bool,
	
	#[arg(long, help = "Statistical tests to include (comma-separated)", value_enum, num_args = 1.., value_delimiter = ',')]
	pub stats_tests: Option<Vec<CorrTest>>,
	
	#[arg(long, help = "Number of decimal places for correlation values", default_value = "4")]
	pub digits: usize,
}

pub async fn execute(args: CorrelationsArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
    
    let df = read_data(&args.common.input).await?;
    let schema = df.schema();
    
    let target_columns = if let Some(col_spec) = &args.columns {
        let selected = select_columns_by_pattern(schema.clone().into(), col_spec)?;
        
        let mut numeric_columns = Vec::new();
        let mut non_numeric_columns = Vec::new();
        
        for col_name in &selected {
            if let Ok(field) = schema.field_with_name(None, col_name) {
                match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Int64 | 
                    datafusion::arrow::datatypes::DataType::Float64 | 
                    datafusion::arrow::datatypes::DataType::Int32 | 
                    datafusion::arrow::datatypes::DataType::Float32 |
                    datafusion::arrow::datatypes::DataType::Int16 |
                    datafusion::arrow::datatypes::DataType::Int8 |
                    datafusion::arrow::datatypes::DataType::UInt64 |
                    datafusion::arrow::datatypes::DataType::UInt32 |
                    datafusion::arrow::datatypes::DataType::UInt16 |
                    datafusion::arrow::datatypes::DataType::UInt8 => {
                        numeric_columns.push(col_name.clone());
                    },
                    _ => {
                        non_numeric_columns.push((col_name.clone(), field.data_type().clone()));
                    }
                }
            }
        }
        
        if !non_numeric_columns.is_empty() {
            let non_numeric_info: Vec<String> = non_numeric_columns.iter()
                .map(|(name, dtype)| format!("'{}' ({:?})", name, dtype))
                .collect();
            return Err(NailError::Statistics(
                format!("Correlation requires numeric columns only. Non-numeric columns found: {}. Available numeric columns: {:?}", 
                    non_numeric_info.join(", "), numeric_columns)
            ));
        }
        
        if numeric_columns.len() < 2 {
            return Err(NailError::Statistics(
                format!("Need at least 2 numeric columns for correlation. Found {} numeric columns: {:?}", 
                    numeric_columns.len(), numeric_columns)
            ));
        }
        
        numeric_columns
    } else {
        let numeric_columns: Vec<String> = schema.fields().iter()
            .filter(|f| matches!(f.data_type(), 
                datafusion::arrow::datatypes::DataType::Int64 | 
                datafusion::arrow::datatypes::DataType::Float64 | 
                datafusion::arrow::datatypes::DataType::Int32 | 
                datafusion::arrow::datatypes::DataType::Float32 |
                datafusion::arrow::datatypes::DataType::Int16 |
                datafusion::arrow::datatypes::DataType::Int8 |
                datafusion::arrow::datatypes::DataType::UInt64 |
                datafusion::arrow::datatypes::DataType::UInt32 |
                datafusion::arrow::datatypes::DataType::UInt16 |
                datafusion::arrow::datatypes::DataType::UInt8
            ))
            .map(|f| f.name().clone())
            .collect();
            
        if numeric_columns.len() < 2 {
            return Err(NailError::Statistics(
                format!("Need at least 2 numeric columns for correlation. Found {} numeric columns: {:?}", 
                    numeric_columns.len(), numeric_columns)
            ));
        }
        
        numeric_columns
    };
    
    args.common.log_if_verbose(&format!("Computing {:?} correlations for {} numeric columns: {:?}", 
        args.correlation_type, target_columns.len(), target_columns));
    args.common.log_if_verbose(&format!("Using {} decimal places for correlation values", args.digits));
    
    let include_tests = args.stats_tests.as_ref().map(|v| !v.is_empty()).unwrap_or(false);

let corr_df = calculate_correlations(
        &df, 
        &target_columns, 
        &args.correlation_type,
        args.correlation_matrix,
        include_tests,
        args.digits
    ).await?;
    
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&corr_df, "correlations").await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;
    
    async fn create_test_dataframe() -> NailResult<DataFrame> {
        let ctx = SessionContext::new();
        
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false), // Non-numeric column
        ]));
        
        // Create correlated data: y = 2*x + noise, z = x + 10
        let x_values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let y_values = vec![2.1, 4.2, 5.9, 8.1, 10.0, 12.1, 13.8, 16.2, 18.0, 20.1];
        let z_values = vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
        let categories = vec!["A", "B", "A", "B", "A", "B", "A", "B", "A", "B"];
        
        let x_array = Arc::new(Float64Array::from(x_values));
        let y_array = Arc::new(Float64Array::from(y_values));
        let z_array = Arc::new(Int64Array::from(z_values));
        let cat_array = Arc::new(StringArray::from(categories));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![x_array, y_array, z_array, cat_array],
        )?;
        
        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
    
    async fn create_minimal_dataframe() -> NailResult<DataFrame> {
        let ctx = SessionContext::new();
        
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        
        let a_array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let b_array = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![a_array, b_array],
        )?;
        
        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
    
    #[tokio::test]
    async fn test_execute_pearson_correlations() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_test_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: Some("x,y,z".to_string()),
            correlation_type: CorrelationType::Pearson,
            correlation_matrix: false,
            stats_tests: None,
            digits: 3,
        };
        
        // Execute correlations
        let result = execute(args).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_correlation_matrix() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_minimal_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args for matrix output
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: true, // Test verbose output
                jobs: Some(2),
                random: None,
            },
            columns: None, // Use all numeric columns
            correlation_type: CorrelationType::Pearson,
            correlation_matrix: true,
            stats_tests: None,
            digits: 4,
        };
        
        // Execute correlations
        let result = execute(args).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_with_statistical_tests() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_minimal_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args with statistical tests
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: None,
            correlation_type: CorrelationType::Pearson,
            correlation_matrix: false, // Tests only work with pairwise format
            stats_tests: Some(vec![CorrTest::fisher_exact, CorrTest::t_test]),
            digits: 3,
        };
        
        // Execute correlations
        let result = execute(args).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_spearman_correlation() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_minimal_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args for Spearman
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: None,
            correlation_type: CorrelationType::Spearman,
            correlation_matrix: false,
            stats_tests: None,
            digits: 2,
        };
        
        // Execute correlations
        let result = execute(args).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_kendall_correlation() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_minimal_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args for Kendall
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: None,
            correlation_type: CorrelationType::Kendall,
            correlation_matrix: false,
            stats_tests: None,
            digits: 3,
        };
        
        // Execute correlations
        let result = execute(args).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_with_non_numeric_columns() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data with non-numeric columns
        let df = create_test_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args including non-numeric column
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: Some("x,y,category".to_string()), // category is non-numeric
            correlation_type: CorrelationType::Pearson,
            correlation_matrix: false,
            stats_tests: None,
            digits: 3,
        };
        
        // Execute should fail because of non-numeric column
        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Non-numeric columns found"));
    }
    
    #[tokio::test]
    async fn test_execute_with_insufficient_columns() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data with only one numeric column
        let ctx = SessionContext::new();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("single", DataType::Float64, false),
        ]));
        let single_array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let batch = RecordBatch::try_new(schema.clone(), vec![single_array]).unwrap();
        let df = ctx.read_batch(batch).unwrap();
        
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: None,
            correlation_type: CorrelationType::Pearson,
            correlation_matrix: false,
            stats_tests: None,
            digits: 3,
        };
        
        // Execute should fail because we need at least 2 columns
        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Need at least 2 numeric columns"));
    }
    
    #[tokio::test]
    async fn test_execute_with_pattern_matching() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_test_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args with regex pattern
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: Some("x,y".to_string()), // Select specific columns
            correlation_type: CorrelationType::Pearson,
            correlation_matrix: false,
            stats_tests: None,
            digits: 3,
        };
        
        // Execute correlations
        let result = execute(args).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_with_all_test_types() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_minimal_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create correlation args with all statistical tests
        let args = CorrelationsArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: None,
            correlation_type: CorrelationType::Pearson,
            correlation_matrix: false,
            stats_tests: Some(vec![
                CorrTest::fisher_exact,
                CorrTest::chi_sqr,
                CorrTest::t_test,
            ]),
            digits: 5,
        };
        
        // Execute correlations
        let result = execute(args).await;
        assert!(result.is_ok());
    }
}