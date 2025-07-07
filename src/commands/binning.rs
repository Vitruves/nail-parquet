use crate::error::{NailError, NailResult};
use crate::utils::{create_context_with_jobs, io::{read_data, write_data}, format::display_dataframe};
use crate::cli::OutputFormat;
use clap::Args;
use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{min, max, count};
use datafusion::logical_expr::conditional_expressions::CaseBuilder;
use std::path::PathBuf;

#[derive(Args, Clone)]
pub struct BinningArgs {
    /// Input file
    #[arg(help = "Input file")]
    pub input: PathBuf,

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
pub enum BinningMethod {
    #[value(name = "equal-width")]
    EqualWidth,
    #[value(name = "equal-frequency")]
    EqualFrequency,
    Custom,
}

pub async fn execute(args: BinningArgs) -> NailResult<()> {
    if args.verbose {
        eprintln!("Reading data from: {}", args.input.display());
    }

    // Read input data
    let _ctx = create_context_with_jobs(args.jobs).await?;
    let df = read_data(&args.input).await?;
    
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

    if args.verbose {
        eprintln!("Binning columns: {:?}", columns);
        eprintln!("Method: {:?}", args.method);
        if let Some(edges) = &bin_edges {
            eprintln!("Bin edges: {:?}", edges);
        } else {
            eprintln!("Number of bins: {}", n_bins.unwrap_or(10));
        }
    }

    // For simplicity, let's use a basic binning approach with DataFrame operations
    // This is a simplified version that bins the first column only
    
    if columns.len() > 1 {
        return Err(NailError::InvalidArgument(
            "Multiple column binning not yet implemented. Please specify one column at a time.".to_string()
        ));
    }
    
    let column_name = columns[0];
    
    // Calculate statistics
    let agg_df = df.clone()
        .aggregate(
            vec![],
            vec![
                min(col(column_name)).alias("min_val"),
                max(col(column_name)).alias("max_val"),
                count(col(column_name)).alias("count"),
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
    
    if args.verbose {
        eprintln!("Column '{}' range: {} to {}", column_name, min_val, max_val);
    }
    
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
            return Err(NailError::InvalidArgument(
                "Equal frequency binning not yet implemented".to_string()
            ));
        }
    };

    if args.verbose {
        eprintln!("Using bin edges: {:?}", edges);
    }

    // Create binned column using a simple approach
    let mut select_exprs = vec![];
    
    // Add original columns if not dropping them
    if !args.drop_original {
        for field in schema.fields() {
            select_exprs.push(col(field.name()));
        }
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
            col(column_name).gt_eq(lit(lower)).and(col(column_name).lt_eq(lit(upper)))
        } else {
            col(column_name).gt_eq(lit(lower)).and(col(column_name).lt(lit(upper)))
        };
        
        case_expr = Some(match case_expr {
            None => when(condition, lit(label)),
            Some(mut prev) => prev.when(condition, lit(label)),
        });
    }
    
    let final_case = case_expr.unwrap().otherwise(lit("NULL"))?;
    select_exprs.push(final_case.alias(binned_col_name));
    
    let result_df = df.select(select_exprs)?;

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