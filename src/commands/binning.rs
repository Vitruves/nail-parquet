use crate::error::{NailError, NailResult};
use crate::utils::{create_context_with_jobs, io::read_data};
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use clap::Args;
use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{min, max, count};
use datafusion::logical_expr::conditional_expressions::CaseBuilder;

#[derive(Args, Clone)]
pub struct BinningArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Columns to bin (comma-separated)
    #[arg(short, long, help = "Columns to bin (comma-separated)")]
    pub columns: String,

    /// Number of bins or custom edges (e.g., "5" or "0,10,50,100")
    #[arg(short, long, default_value = "10", help = "Number of bins or custom edges (e.g., \"5\" or \"0,10,50,100\")")]
    pub bins: String,

    /// Binning method
    #[arg(long, default_value = "equal-width", help = "Binning method")]
    #[arg(value_enum)]
    pub method: BinningMethod,

    /// Custom bin labels (comma-separated)
    #[arg(long, help = "Custom bin labels (comma-separated)")]
    pub labels: Option<String>,

    /// Suffix for new columns
    #[arg(long, default_value = "_binned", help = "Suffix for new columns")]
    pub suffix: String,

    /// Drop original columns after binning
    #[arg(long, help = "Drop original columns after binning")]
    pub drop_original: bool,

    /// Include lowest value in first bin
    #[arg(long, help = "Include lowest value in first bin")]
    pub include_lowest: bool,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum BinningMethod {
    #[value(name = "equal-width")]
    EqualWidth,
    #[value(name = "equal-frequency")]
    EqualFrequency,
    Custom,
}

pub async fn execute(args: BinningArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

    // Read input data
    let _ctx = create_context_with_jobs(args.common.jobs).await?;
    let df = read_data(&args.common.input).await?;
    
    // Parse columns to bin
    let columns: Vec<&str> = args.columns.split(',').map(|s| s.trim()).collect();
    
    // Validate columns exist and are numeric
    let schema = df.schema();
    for col in &columns {
        match schema.field_with_name(None, col) {
            Ok(field) => {
                match field.data_type() {
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                    DataType::Float32 | DataType::Float64 => {},
                    _ => {
                        return Err(NailError::InvalidArgument(
                            format!("Column '{}' is not numeric (type: {:?})", col, field.data_type())
                        ));
                    }
                }
            },
            Err(_) => {
                let available_cols: Vec<String> = schema.fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                return Err(NailError::ColumnNotFound(
                    format!("Column '{}' not found. Available columns: {}", 
                            col, available_cols.join(", "))
                ));
            }
        }
    }

    // Parse bins
    let (bin_edges, n_bins) = parse_bins(&args.bins, &args.method)?;
    
    // Parse labels if provided
    let labels = if let Some(labels_str) = &args.labels {
        Some(labels_str.split(',').map(|s| s.trim().to_string()).collect::<Vec<_>>())
    } else {
        None
    };

    // Validate labels count if provided
    if let Some(ref label_vec) = labels {
        let expected_labels = if bin_edges.is_some() {
            bin_edges.as_ref().unwrap().len() - 1
        } else {
            n_bins.unwrap_or(10)
        };
        if label_vec.len() != expected_labels {
            return Err(NailError::InvalidArgument(
                format!("Number of labels ({}) must match number of bins ({})", 
                        label_vec.len(), expected_labels)
            ));
        }
    }

    args.common.log_if_verbose(&format!("Binning columns: {:?}", columns));
    args.common.log_if_verbose(&format!("Method: {:?}", args.method));
    if let Some(edges) = &bin_edges {
        args.common.log_if_verbose(&format!("Bin edges: {:?}", edges));
    } else {
        args.common.log_if_verbose(&format!("Number of bins: {}", n_bins.unwrap_or(10)));
    }

    // Process each column for binning
    let mut result_df = df.clone();
    
    for column_name in &columns {
        args.common.log_if_verbose(&format!("Processing column: {}", column_name));
        
        // Calculate statistics
        let agg_df = result_df.clone()
            .aggregate(
                vec![],
                vec![
                    min(col(*column_name)).alias("min_val"),
                    max(col(*column_name)).alias("max_val"),
                    count(col(*column_name)).alias("count"),
                ]
            )?;
        
        let stats_batch = agg_df.collect().await?;
        if stats_batch.is_empty() || stats_batch[0].num_rows() == 0 {
            return Err(NailError::InvalidArgument(
                format!("No data found in column '{}'", column_name)
            ));
        }

        let min_val = extract_float_value(&stats_batch[0], 0, 0)?;
        let max_val = extract_float_value(&stats_batch[0], 1, 0)?;
        
        args.common.log_if_verbose(&format!("Column '{}' range: {} to {}", column_name, min_val, max_val));
        
        // Calculate bin edges based on method
        let edges = match &args.method {
            BinningMethod::EqualWidth => {
                if let Some(edges) = &bin_edges {
                    edges.clone()
                } else {
                    let n = n_bins.unwrap_or(10);
                    calculate_equal_width_edges(min_val, max_val, n, args.include_lowest)
                }
            },
            BinningMethod::Custom => {
                if let Some(edges) = &bin_edges {
                    edges.clone()
                } else {
                    return Err(NailError::InvalidArgument(
                        "Custom binning method requires bin edges to be specified".to_string()
                    ));
                }
            },
            BinningMethod::EqualFrequency => {
                if let Some(edges) = &bin_edges {
                    edges.clone()
                } else {
                    let n = n_bins.unwrap_or(10);
                    calculate_equal_frequency_edges(&result_df, column_name, n).await?
                }
            }
        };

        args.common.log_if_verbose(&format!("Using bin edges for {}: {:?}", column_name, edges));

        // Create binned column using a simple approach
        let mut select_exprs = vec![];
        
        // Add all current columns from result_df
        let current_schema = result_df.schema();
        for field in current_schema.fields() {
            select_exprs.push(col(field.name()));
        }
        
        // Add binned column with a simplified binning expression
        let binned_col_name = format!("{}{}", column_name, args.suffix);
        
        // Create a simple case expression for binning
        let mut case_expr: Option<CaseBuilder> = None;
        for i in 0..edges.len() - 1 {
            let lower = edges[i];
            let upper = edges[i + 1];
            
            let label = if let Some(label_vec) = &labels {
                label_vec[i].clone()
            } else {
                format!("[{:.2}, {:.2})", lower, upper)
            };
            
            let condition = if i == edges.len() - 2 {
                // Last bin includes upper bound
                col(*column_name).gt_eq(lit(lower)).and(col(*column_name).lt_eq(lit(upper)))
            } else {
                col(*column_name).gt_eq(lit(lower)).and(col(*column_name).lt(lit(upper)))
            };
            
            case_expr = Some(match case_expr {
                None => when(condition, lit(label)),
                Some(mut prev) => prev.when(condition, lit(label)),
            });
        }
        
        let final_case = case_expr.unwrap().otherwise(lit("NULL"))?;
        select_exprs.push(final_case.alias(binned_col_name));
        
        // Update result_df with the new binned column
        result_df = result_df.select(select_exprs)?;
    }
    
    // Handle dropping original columns if requested
    if args.drop_original {
        let mut final_select_exprs = vec![];
        let schema = result_df.schema();
        
        for field in schema.fields() {
            if !columns.contains(&field.name().as_str()) {
                final_select_exprs.push(col(field.name()));
            }
        }
        
        result_df = result_df.select(final_select_exprs)?;
    }

    // Display or write the results
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&result_df, "binning").await?;

    Ok(())
}

fn parse_bins(bins_str: &str, _method: &BinningMethod) -> NailResult<(Option<Vec<f64>>, Option<usize>)> {
    // Check if it's a single number (number of bins)
    if let Ok(n) = bins_str.parse::<usize>() {
        if n == 0 {
            return Err(NailError::InvalidArgument(
                "Number of bins must be greater than 0".to_string()
            ));
        }
        return Ok((None, Some(n)));
    }

    // Otherwise, try to parse as comma-separated edges
    let edges: Result<Vec<f64>, _> = bins_str
        .split(',')
        .map(|s| s.trim().parse::<f64>())
        .collect();

    match edges {
        Ok(mut edge_vec) => {
            if edge_vec.len() < 2 {
                return Err(NailError::InvalidArgument(
                    "At least 2 bin edges must be specified".to_string()
                ));
            }
            // Sort edges to ensure they're in ascending order
            edge_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
            Ok((Some(edge_vec), None))
        },
        Err(_) => Err(NailError::InvalidArgument(
            format!("Invalid bins specification: '{}'. Use a number (e.g., '5') or comma-separated edges (e.g., '0,10,50,100')", bins_str)
        ))
    }
}

fn calculate_equal_width_edges(min_val: f64, max_val: f64, n_bins: usize, include_lowest: bool) -> Vec<f64> {
    let mut edges = Vec::with_capacity(n_bins + 1);
    let width = (max_val - min_val) / n_bins as f64;
    
    for i in 0..=n_bins {
        edges.push(min_val + (i as f64) * width);
    }
    
    // Adjust for floating point precision
    if include_lowest && !edges.is_empty() {
        edges[0] = edges[0] - f64::EPSILON * edges[0].abs();
    }
    
    edges
}

async fn calculate_equal_frequency_edges(df: &DataFrame, column_name: &str, n_bins: usize) -> NailResult<Vec<f64>> {
    // Get sorted values to calculate quantiles
    let sorted_df = df.clone()
        .sort(vec![col(column_name).sort(true, true)])?;
    
    let batches = sorted_df.collect().await?;
    
    let mut all_values = Vec::new();
    for batch in batches {
        let array = batch.column_by_name(column_name)
            .ok_or_else(|| NailError::InvalidArgument(format!("Column '{}' not found", column_name)))?;
        
        if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
            for i in 0..float_array.len() {
                if !float_array.is_null(i) {
                    all_values.push(float_array.value(i));
                }
            }
        } else {
            return Err(NailError::InvalidArgument(
                format!("Column '{}' must be numeric for equal-frequency binning", column_name)
            ));
        }
    }
    
    if all_values.is_empty() {
        return Err(NailError::InvalidArgument(
            format!("No valid values found in column '{}'", column_name)
        ));
    }
    
    // Calculate quantile positions
    let mut edges = Vec::with_capacity(n_bins + 1);
    edges.push(all_values[0]); // First value (min)
    
    for i in 1..n_bins {
        let quantile_pos = (i as f64 / n_bins as f64) * (all_values.len() - 1) as f64;
        let index = quantile_pos.floor() as usize;
        let fraction = quantile_pos - quantile_pos.floor();
        
        let value = if index + 1 < all_values.len() {
            all_values[index] + fraction * (all_values[index + 1] - all_values[index])
        } else {
            all_values[index]
        };
        
        edges.push(value);
    }
    
    edges.push(all_values[all_values.len() - 1]); // Last value (max)
    
    // Remove duplicates and ensure edges are increasing
    edges.dedup_by(|a, b| (*a - *b).abs() < f64::EPSILON);
    
    Ok(edges)
}

fn extract_float_value(batch: &datafusion::arrow::record_batch::RecordBatch, col_idx: usize, row_idx: usize) -> NailResult<f64> {
    let array = batch.column(col_idx);
    
    if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
        if float_array.is_null(row_idx) {
            return Err(NailError::InvalidArgument("Null value encountered".to_string()));
        }
        Ok(float_array.value(row_idx))
    } else if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
        if int_array.is_null(row_idx) {
            return Err(NailError::InvalidArgument("Null value encountered".to_string()));
        }
        Ok(int_array.value(row_idx) as f64)
    } else {
        Err(NailError::InvalidArgument(
            format!("Unexpected data type for numeric value: {:?}", array.data_type())
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;
    
    async fn create_test_dataframe() -> NailResult<DataFrame> {
        let ctx = SessionContext::new();
        
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("value", DataType::Float64, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        
        let value_array = Arc::new(Float64Array::from(vec![1.5, 5.0, 15.0, 25.0, 35.0, 45.0, 55.0, 65.0, 75.0, 85.0]));
        let category_array = Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A", "B", "A", "B", "A", "B"]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![value_array, category_array],
        )?;
        
        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
    
    #[test]
    fn test_parse_bins() {
        // Test single number (number of bins)
        let (edges, n_bins) = parse_bins("5", &BinningMethod::EqualWidth).unwrap();
        assert!(edges.is_none());
        assert_eq!(n_bins, Some(5));
        
        // Test comma-separated edges
        let (edges, n_bins) = parse_bins("0,10,20,30", &BinningMethod::Custom).unwrap();
        assert_eq!(edges, Some(vec![0.0, 10.0, 20.0, 30.0]));
        assert!(n_bins.is_none());
        
        // Test invalid input
        assert!(parse_bins("0", &BinningMethod::EqualWidth).is_err());
        assert!(parse_bins("abc", &BinningMethod::EqualWidth).is_err());
        assert!(parse_bins("10,20", &BinningMethod::Custom).is_ok()); // At least 2 edges is valid
        assert!(parse_bins("10", &BinningMethod::Custom).is_ok()); // Single number is valid (becomes n_bins)
        
        // Test edge sorting
        let (edges, _) = parse_bins("30,10,0,20", &BinningMethod::Custom).unwrap();
        assert_eq!(edges, Some(vec![0.0, 10.0, 20.0, 30.0]));
    }
    
    #[test]
    fn test_calculate_equal_width_edges() {
        // Test basic equal width binning
        let edges = calculate_equal_width_edges(0.0, 100.0, 5, false);
        assert_eq!(edges.len(), 6); // n_bins + 1
        assert_eq!(edges[0], 0.0);
        assert_eq!(edges[5], 100.0);
        assert_eq!(edges[1], 20.0);
        assert_eq!(edges[2], 40.0);
        assert_eq!(edges[3], 60.0);
        assert_eq!(edges[4], 80.0);
        
        // Test with include_lowest
        let edges = calculate_equal_width_edges(0.0, 100.0, 5, true);
        assert!(edges[0] <= 0.0); // Should be less than or equal to 0
        
        // Test with negative range
        let edges = calculate_equal_width_edges(-50.0, 50.0, 4, false);
        assert_eq!(edges[0], -50.0);
        assert_eq!(edges[4], 50.0);
        assert_eq!(edges[2], 0.0); // Middle should be 0
    }
    
    #[test]
    fn test_extract_float_value() {
        // Create test batch with different numeric types
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("float_col", DataType::Float64, false),
            Field::new("int_col", DataType::Int64, false),
        ]));
        
        let float_array = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]));
        let int_array = Arc::new(Int64Array::from(vec![10, 20, 30]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![float_array, int_array],
        ).unwrap();
        
        // Test float extraction
        let val = extract_float_value(&batch, 0, 0).unwrap();
        assert_eq!(val, 1.5);
        
        let val = extract_float_value(&batch, 0, 2).unwrap();
        assert_eq!(val, 3.5);
        
        // Test int extraction (should convert to float)
        let val = extract_float_value(&batch, 1, 0).unwrap();
        assert_eq!(val, 10.0);
        
        let val = extract_float_value(&batch, 1, 2).unwrap();
        assert_eq!(val, 30.0);
    }
    
    #[tokio::test]
    async fn test_execute_equal_width_binning() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        let output_path = temp_dir.path().join("output.parquet");
        
        // Create test data
        let df = create_test_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create args for equal width binning
        let args = BinningArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: Some(output_path.clone()),
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: "value".to_string(),
            bins: "5".to_string(),
            method: BinningMethod::EqualWidth,
            labels: None,
            suffix: "_binned".to_string(),
            drop_original: false,
            include_lowest: false,
        };
        
        // Execute binning
        let result = execute(args).await;
        assert!(result.is_ok());
        
        // Read result and verify
        let result_df = crate::utils::io::read_data(&output_path).await.unwrap();
        let schema = result_df.schema();
        
        // Check that both original and binned columns exist
        assert!(schema.field_with_name(None, "value").is_ok());
        assert!(schema.field_with_name(None, "value_binned").is_ok());
        assert!(schema.field_with_name(None, "category").is_ok());
        
        // Check row count
        let row_count = result_df.clone().count().await.unwrap();
        assert_eq!(row_count, 10);
    }
    
    #[tokio::test]
    async fn test_execute_custom_binning_with_labels() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        let output_path = temp_dir.path().join("output.parquet");
        
        // Create test data
        let df = create_test_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create args for custom binning with labels
        let args = BinningArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: Some(output_path.clone()),
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: "value".to_string(),
            bins: "0,30,60,90".to_string(),
            method: BinningMethod::Custom,
            labels: Some("Low,Medium,High".to_string()),
            suffix: "_category".to_string(),
            drop_original: true,
            include_lowest: false,
        };
        
        // Execute binning
        let result = execute(args).await;
        assert!(result.is_ok());
        
        // Read result and verify
        let result_df = crate::utils::io::read_data(&output_path).await.unwrap();
        let schema = result_df.schema();
        
        // Check that original column is dropped and binned column exists
        assert!(schema.field_with_name(None, "value").is_err()); // Should be dropped
        assert!(schema.field_with_name(None, "value_category").is_ok());
        assert!(schema.field_with_name(None, "category").is_ok());
    }
    
    #[tokio::test]
    async fn test_execute_with_non_numeric_column() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_test_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create args attempting to bin a non-numeric column
        let args = BinningArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: "category".to_string(), // This is a string column
            bins: "5".to_string(),
            method: BinningMethod::EqualWidth,
            labels: None,
            suffix: "_binned".to_string(),
            drop_original: false,
            include_lowest: false,
        };
        
        // Execute should fail because category is not numeric
        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not numeric"));
    }
    
    #[tokio::test]
    async fn test_execute_with_mismatched_labels() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test.parquet");
        
        // Create test data
        let df = create_test_dataframe().await.unwrap();
        crate::utils::io::write_data(&df, &input_path, None).await.unwrap();
        
        // Create args with wrong number of labels
        let args = BinningArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None,
                format: None,
                verbose: false,
                jobs: None,
                random: None,
            },
            columns: "value".to_string(),
            bins: "5".to_string(), // 5 bins
            method: BinningMethod::EqualWidth,
            labels: Some("Low,High".to_string()), // Only 2 labels
            suffix: "_binned".to_string(),
            drop_original: false,
            include_lowest: false,
        };
        
        // Execute should fail due to label count mismatch
        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Number of labels"));
    }
}