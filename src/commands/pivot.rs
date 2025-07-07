use crate::error::{NailError, NailResult};
use crate::utils::{create_context_with_jobs, io::read_data};
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use clap::Args;
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{sum, avg, count, min, max};

#[derive(Args, Clone)]
pub struct PivotArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Row index columns (comma-separated)
    #[arg(short, long, help = "Row index columns (comma-separated)")]
    pub index: String,

    /// Column pivot columns (comma-separated)
    #[arg(short, long, help = "Column pivot columns (comma-separated)")]
    pub columns: String,

    /// Value columns to aggregate (comma-separated)
    #[arg(short = 'l', long = "values", help = "Value columns to aggregate (comma-separated)")]
    pub values: Option<String>,

    /// Aggregation function
    #[arg(short, long, default_value = "sum", help = "Aggregation function")]
    #[arg(value_enum)]
    pub agg: AggregationFunction,

    /// Fill missing values
    #[arg(long, default_value = "0", help = "Fill missing values")]
    pub fill: String,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum AggregationFunction {
    Sum,
    Mean,
    Count,
    Min,
    Max,
}


pub async fn execute(args: PivotArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

    // Read input data
    let _ctx = create_context_with_jobs(args.common.jobs).await?;
    let df = read_data(&args.common.input).await?;
    
    // Parse columns
    let index_cols: Vec<&str> = args.index.split(',').map(|s| s.trim()).collect();
    let pivot_cols: Vec<&str> = args.columns.split(',').map(|s| s.trim()).collect();
    
    // Validate columns exist
    let temp_df = df.clone();
    let schema = temp_df.schema();
    for col in index_cols.iter().chain(pivot_cols.iter()) {
        if schema.field_with_name(None, col).is_err() {
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

    // Determine value columns
    let value_cols: Vec<&str> = if let Some(values_str) = &args.values {
        let cols: Vec<&str> = values_str.split(',').map(|s| s.trim()).collect();
        // Validate value columns exist and are numeric
        for col in &cols {
            match schema.field_with_name(None, col) {
                Ok(field) => {
                    match field.data_type() {
                        datafusion::arrow::datatypes::DataType::Int8 | 
                        datafusion::arrow::datatypes::DataType::Int16 | 
                        datafusion::arrow::datatypes::DataType::Int32 | 
                        datafusion::arrow::datatypes::DataType::Int64 |
                        datafusion::arrow::datatypes::DataType::UInt8 | 
                        datafusion::arrow::datatypes::DataType::UInt16 | 
                        datafusion::arrow::datatypes::DataType::UInt32 | 
                        datafusion::arrow::datatypes::DataType::UInt64 |
                        datafusion::arrow::datatypes::DataType::Float32 | 
                        datafusion::arrow::datatypes::DataType::Float64 => {},
                        _ => {
                            return Err(NailError::InvalidArgument(
                                format!("Value column '{}' must be numeric (type: {:?})", col, field.data_type())
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
        cols
    } else {
        // If no value columns specified, find all numeric columns not in index or pivot columns
        schema.fields()
            .iter()
            .filter(|field| {
                let name = field.name();
                !index_cols.contains(&name.as_str()) && 
                !pivot_cols.contains(&name.as_str()) &&
                matches!(field.data_type(),
                    datafusion::arrow::datatypes::DataType::Int8 | 
                    datafusion::arrow::datatypes::DataType::Int16 | 
                    datafusion::arrow::datatypes::DataType::Int32 | 
                    datafusion::arrow::datatypes::DataType::Int64 |
                    datafusion::arrow::datatypes::DataType::UInt8 | 
                    datafusion::arrow::datatypes::DataType::UInt16 | 
                    datafusion::arrow::datatypes::DataType::UInt32 | 
                    datafusion::arrow::datatypes::DataType::UInt64 |
                    datafusion::arrow::datatypes::DataType::Float32 | 
                    datafusion::arrow::datatypes::DataType::Float64
                )
            })
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .into_iter()
            .map(|s| s)
            .collect()
    };

    if value_cols.is_empty() {
        return Err(NailError::InvalidArgument(
            "No numeric value columns found to aggregate. Please specify value columns with --values".to_string()
        ));
    }

    args.common.log_if_verbose(&format!("Index columns: {:?}", index_cols));
    args.common.log_if_verbose(&format!("Pivot columns: {:?}", pivot_cols));
    args.common.log_if_verbose(&format!("Value columns: {:?}", value_cols));
    args.common.log_if_verbose(&format!("Aggregation: {:?}", args.agg));

    // For now, provide a simplified pivot implementation
    // This is a basic version that doesn't do the full pivot table functionality
    // but provides a basic grouping and aggregation
    
    if pivot_cols.len() > 1 {
        return Err(NailError::InvalidArgument(
            "Multiple pivot columns not yet implemented. Please specify one column at a time.".to_string()
        ));
    }
    
    if value_cols.len() > 1 {
        return Err(NailError::InvalidArgument(
            "Multiple value columns not yet implemented. Please specify one column at a time.".to_string()
        ));
    }
    
    let pivot_col = pivot_cols[0];
    let value_col = value_cols[0];
    
    // Simple group by aggregation
    let group_exprs: Vec<Expr> = index_cols.iter().map(|c| col(*c)).collect();
    let agg_expr = match args.agg {
        AggregationFunction::Sum => sum(col(value_col)),
        AggregationFunction::Mean => avg(col(value_col)),
        AggregationFunction::Count => count(col(value_col)),
        AggregationFunction::Min => min(col(value_col)),
        AggregationFunction::Max => max(col(value_col)),
    };
    
    let result_df = df
        .aggregate(group_exprs, vec![agg_expr.alias(&format!("{}_{}", pivot_col, value_col))])?;

    // Display or write the results
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&result_df, "pivot").await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_pivot_args_parsing() {
        let args = PivotArgs {
            common: CommonArgs {
                input: PathBuf::from("data.parquet"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            index: "category".to_string(),
            columns: "month".to_string(),
            values: Some("sales".to_string()),
            agg: AggregationFunction::Sum,
            fill: "0".to_string(),
        };

        assert_eq!(args.index, "category");
        assert_eq!(args.columns, "month");
        assert_eq!(args.values, Some("sales".to_string()));
        assert!(matches!(args.agg, AggregationFunction::Sum));
        assert_eq!(args.fill, "0");
    }

    #[test]
    fn test_pivot_args_with_multiple_columns() {
        let args = PivotArgs {
            common: CommonArgs {
                input: PathBuf::from("sales.csv"),
                output: Some(PathBuf::from("pivot.parquet")),
                format: Some(crate::cli::OutputFormat::Parquet),
                random: Some(456),
                jobs: Some(4),
                verbose: true,
            },
            index: "region,product".to_string(),
            columns: "quarter,year".to_string(),
            values: Some("revenue,units".to_string()),
            agg: AggregationFunction::Mean,
            fill: "null".to_string(),
        };

        assert_eq!(args.index, "region,product");
        assert_eq!(args.columns, "quarter,year");
        assert_eq!(args.values, Some("revenue,units".to_string()));
        assert!(matches!(args.agg, AggregationFunction::Mean));
        assert_eq!(args.fill, "null");
        assert_eq!(args.common.jobs, Some(4));
        assert!(args.common.verbose);
    }

    #[test]
    fn test_pivot_args_with_count_aggregation() {
        let args = PivotArgs {
            common: CommonArgs {
                input: PathBuf::from("events.json"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            index: "user_id".to_string(),
            columns: "event_type".to_string(),
            values: None,
            agg: AggregationFunction::Count,
            fill: "0".to_string(),
        };

        assert_eq!(args.index, "user_id");
        assert_eq!(args.columns, "event_type");
        assert_eq!(args.values, None);
        assert!(matches!(args.agg, AggregationFunction::Count));
        assert_eq!(args.fill, "0");
    }

    #[test]
    fn test_pivot_args_with_min_max_aggregation() {
        let args_min = PivotArgs {
            common: CommonArgs {
                input: PathBuf::from("temperature.parquet"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            index: "location".to_string(),
            columns: "month".to_string(),
            values: Some("temperature".to_string()),
            agg: AggregationFunction::Min,
            fill: "-999".to_string(),
        };

        let args_max = PivotArgs {
            common: CommonArgs {
                input: PathBuf::from("temperature.parquet"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            index: "location".to_string(),
            columns: "month".to_string(),
            values: Some("temperature".to_string()),
            agg: AggregationFunction::Max,
            fill: "-999".to_string(),
        };

        assert!(matches!(args_min.agg, AggregationFunction::Min));
        assert!(matches!(args_max.agg, AggregationFunction::Max));
        assert_eq!(args_min.fill, "-999");
        assert_eq!(args_max.fill, "-999");
    }

    #[test]
    fn test_aggregation_function_debug() {
        let sum_func = AggregationFunction::Sum;
        let mean_func = AggregationFunction::Mean;
        let count_func = AggregationFunction::Count;
        let min_func = AggregationFunction::Min;
        let max_func = AggregationFunction::Max;

        assert_eq!(format!("{:?}", sum_func), "Sum");
        assert_eq!(format!("{:?}", mean_func), "Mean");
        assert_eq!(format!("{:?}", count_func), "Count");
        assert_eq!(format!("{:?}", min_func), "Min");
        assert_eq!(format!("{:?}", max_func), "Max");
    }

    #[test]
    fn test_pivot_args_clone() {
        let args = PivotArgs {
            common: CommonArgs {
                input: PathBuf::from("test.parquet"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            index: "category".to_string(),
            columns: "month".to_string(),
            values: Some("sales".to_string()),
            agg: AggregationFunction::Sum,
            fill: "0".to_string(),
        };

        let cloned = args.clone();
        assert_eq!(args.index, cloned.index);
        assert_eq!(args.columns, cloned.columns);
        assert_eq!(args.values, cloned.values);
        assert_eq!(args.fill, cloned.fill);
        assert!(matches!(cloned.agg, AggregationFunction::Sum));
    }

    #[test]
    fn test_pivot_args_parsing_columns() {
        let args = PivotArgs {
            common: CommonArgs {
                input: PathBuf::from("test.parquet"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            index: "col_a,col_b,col_c".to_string(),
            columns: "pivot_col".to_string(),
            values: Some("value1,value2".to_string()),
            agg: AggregationFunction::Sum,
            fill: "0".to_string(),
        };

        let index_cols: Vec<&str> = args.index.split(',').map(|s| s.trim()).collect();
        let value_cols: Vec<&str> = args.values.as_ref().unwrap().split(',').map(|s| s.trim()).collect();
        
        assert_eq!(index_cols, vec!["col_a", "col_b", "col_c"]);
        assert_eq!(value_cols, vec!["value1", "value2"]);
    }
}

