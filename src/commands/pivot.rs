use crate::error::{NailError, NailResult};
use crate::utils::{create_context_with_jobs, io::{read_data, write_data}, format::display_dataframe};
use crate::cli::OutputFormat;
use clap::Args;
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{sum, avg, count, min, max};
use std::path::PathBuf;

#[derive(Args, Clone)]
pub struct PivotArgs {
    /// Input file
    #[arg(help = "Input file")]
    pub input: PathBuf,

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

    /// Output file (if not specified, prints to console)
    #[arg(short, long, help = "Output file (if not specified, prints to console)")]
    pub output: Option<PathBuf>,

    /// Output format
    #[arg(short, long, help = "Output format")]
    #[arg(value_enum)]
    pub format: Option<OutputFormat>,

    /// Number of parallel jobs
    #[arg(short = 'j', long, help = "Number of parallel jobs")]
    pub jobs: Option<usize>,

    /// Enable verbose output
    #[arg(short = 'v', long, help = "Enable verbose output")]
    pub verbose: bool,
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
    if args.verbose {
        eprintln!("Reading data from: {}", args.input.display());
    }

    // Read input data
    let _ctx = create_context_with_jobs(args.jobs).await?;
    let df = read_data(&args.input).await?;
    
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

    if args.verbose {
        eprintln!("Index columns: {:?}", index_cols);
        eprintln!("Pivot columns: {:?}", pivot_cols);
        eprintln!("Value columns: {:?}", value_cols);
        eprintln!("Aggregation: {:?}", args.agg);
    }

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
    if let Some(output_path) = &args.output {
        let file_format = args.format.as_ref().map(|f| match f {
            OutputFormat::Json => crate::utils::FileFormat::Json,
            OutputFormat::Csv => crate::utils::FileFormat::Csv,
            OutputFormat::Parquet => crate::utils::FileFormat::Parquet,
            OutputFormat::Xlsx => crate::utils::FileFormat::Excel,
            OutputFormat::Text => crate::utils::FileFormat::Parquet,
        });
        write_data(&result_df, output_path, file_format.as_ref()).await?;
        if args.verbose {
            eprintln!("Results written to: {}", output_path.display());
        }
    } else {
        display_dataframe(&result_df, None, args.format.as_ref()).await?;
    }

    Ok(())
}

