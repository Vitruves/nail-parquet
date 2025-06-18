use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::count;
use arrow::array::Array;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const BORDER_COLOR: &str = "\x1b[2;90m";
const FIELD_COLORS: [&str; 6] = [
    "\x1b[92m", // Green
    "\x1b[93m", // Yellow
    "\x1b[94m", // Blue
    "\x1b[95m", // Magenta
    "\x1b[96m", // Cyan
    "\x1b[97m", // White
];

#[derive(Args)]
pub struct FrequencyArgs {
    #[arg(help = "Input file")]
    pub input: PathBuf,
    
    #[arg(short, long, help = "Comma-separated column names to analyze")]
    pub columns: String,
    
    #[arg(short, long, help = "Output file (if not specified, prints to console)")]
    pub output: Option<PathBuf>,
    
    #[arg(short, long, help = "Output format (auto-detect by default)", value_enum)]
    pub format: Option<crate::cli::OutputFormat>,
    
    #[arg(short, long, help = "Number of parallel jobs")]
    pub jobs: Option<usize>,
    
    #[arg(short, long, help = "Enable verbose output")]
    pub verbose: bool,
}

pub async fn execute(args: FrequencyArgs) -> NailResult<()> {
    if args.verbose {
        eprintln!("Reading data from: {}", args.input.display());
        eprintln!("Analyzing frequency for columns: {}", args.columns);
    }

    let df = read_data(&args.input).await?;
    
    // Parse column names
    let column_names: Vec<&str> = args.columns
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    
    if column_names.is_empty() {
        return Err(crate::error::NailError::InvalidArgument(
            "No column names provided".to_string()
        ));
    }

    // Validate that all columns exist
    let schema = df.schema();
    for col_name in &column_names {
        if !schema.has_column_with_unqualified_name(col_name) {
            return Err(crate::error::NailError::InvalidArgument(
                format!("Column '{}' does not exist", col_name)
            ));
        }
    }

    if args.verbose {
        eprintln!("Computing frequency table for {} column(s)", column_names.len());
    }

    // Build the frequency query
    let mut group_by_cols = Vec::new();
    let mut select_cols = Vec::new();
    
    for col_name in &column_names {
        group_by_cols.push(col(*col_name));
        select_cols.push(col(*col_name));
    }
    
    // Add count column
    select_cols.push(count(lit(1)).alias("frequency"));
    
    // Execute the frequency query
    let frequency_df = df
        .aggregate(group_by_cols, vec![count(lit(1)).alias("frequency")])?
        .sort(vec![
            // Sort by frequency descending
            col("frequency").sort(false, true),
        ])?;

    // Display results
    if args.output.is_some() || args.format.is_some() {
        // Convert OutputFormat to FileFormat
        let file_format = args.format.as_ref().map(|f| match f {
            crate::cli::OutputFormat::Json => crate::utils::FileFormat::Json,
            crate::cli::OutputFormat::Csv => crate::utils::FileFormat::Csv,
            crate::cli::OutputFormat::Parquet => crate::utils::FileFormat::Parquet,
            crate::cli::OutputFormat::Text => crate::utils::FileFormat::Csv, // Default to CSV for text
        });
        
        // Write to file
        if let Some(output_path) = &args.output {
            crate::utils::io::write_data(
                &frequency_df,
                output_path,
                file_format.as_ref(),
            ).await?;
            
            if args.verbose {
                eprintln!("Frequency table written to: {}", output_path.display());
            }
        }
    } else {
        // Display to console with condensed format
        display_frequency_table(&frequency_df, &column_names).await?;
    }

    Ok(())
}

async fn display_frequency_table(df: &DataFrame, column_names: &[&str]) -> NailResult<()> {
    let batches = df.clone().collect().await?;
    
    if batches.is_empty() {
        println!("No frequency data to display");
        return Ok(());
    }

    // Get terminal width for proper formatting
    let terminal_width = if let Some((w, _)) = term_size::dimensions() {
        w.max(60).min(200)
    } else {
        120
    };

    // Calculate available width for content 
    let header_width = terminal_width.saturating_sub(4); // Account for "┌─ " and " ─"

    // Print header for the frequency analysis card
    let header_text = " Frequency Analysis ";
    let remaining_width = header_width.saturating_sub(header_text.len());
    let left_dashes = remaining_width / 2;
    let right_dashes = remaining_width - left_dashes;
    
    println!("{}┌{}{}{}{}",
        BORDER_COLOR,
        "─".repeat(left_dashes),
        header_text,
        "─".repeat(right_dashes),
        RESET
    );
    println!("{}│{}", BORDER_COLOR, RESET);

    // Print all frequency data within the single card
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            // Build the display line for this frequency entry
            let mut display_parts = Vec::new();
            
            // Add column values with colors
            for (field_idx, col_name) in column_names.iter().enumerate() {
                let field_color = FIELD_COLORS[field_idx % FIELD_COLORS.len()];
                let column = batch.column_by_name(col_name).unwrap();
                let value = format_array_value(column, row_idx);
                
                display_parts.push(format!("{}{}{}: {}{}{}", 
                    field_color, col_name, RESET, 
                    field_color, value, RESET));
            }
            
            // Add frequency with special highlighting
            let freq_column = batch.column_by_name("frequency").unwrap();
            let frequency = format_array_value(freq_column, row_idx);
            display_parts.push(format!("{}frequency{}: {}{}{}{}",
                "\x1b[93m", RESET, BOLD, "\x1b[93m", frequency, RESET));
            
            // Join all parts with comma separation
            let content = display_parts.join(", ");
            
            // Print the frequency entry within the card
            println!("{}│{} {}",
                BORDER_COLOR, RESET, content
            );
        }
    }
    
    // Print card footer
    println!("{}│{}", BORDER_COLOR, RESET);
    println!("{}└{}{}",
        BORDER_COLOR, "─".repeat(header_width), RESET
    );

    Ok(())
}

fn format_array_value(column: &dyn Array, row_idx: usize) -> String {
    if column.is_null(row_idx) {
        format!("{}\x1b[2;37mnull\x1b[0m", "\x1b[2;37m")
    } else {
        match column.data_type() {
            DataType::Utf8 => {
                if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                    array.value(row_idx).to_string()
                } else {
                    "unknown".to_string()
                }
            },
            DataType::Int64 => {
                if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                    array.value(row_idx).to_string()
                } else {
                    "0".to_string()
                }
            },
            DataType::Float64 => {
                if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                    let val = array.value(row_idx);
                    if val.is_finite() {
                        val.to_string()
                    } else {
                        format!("{}\x1b[2;37minfinite\x1b[0m", "\x1b[2;37m")
                    }
                } else {
                    "0.0".to_string()
                }
            },
            DataType::Int32 => {
                if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                    array.value(row_idx).to_string()
                } else {
                    "0".to_string()
                }
            },
            DataType::Float32 => {
                if let Some(array) = column.as_any().downcast_ref::<Float32Array>() {
                    let val = array.value(row_idx);
                    if val.is_finite() {
                        val.to_string()
                    } else {
                        format!("{}\x1b[2;37minfinite\x1b[0m", "\x1b[2;37m")
                    }
                } else {
                    "0.0".to_string()
                }
            },
            DataType::Boolean => {
                if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
                    array.value(row_idx).to_string()
                } else {
                    "false".to_string()
                }
            },
            DataType::Date32 => {
                if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
                    let days_since_epoch = array.value(row_idx);
                    let date = chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
                        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap_or_else(|| chrono::NaiveDate::default()));
                    date.format("%Y-%m-%d").to_string()
                } else {
                    "1970-01-01".to_string()
                }
            },
            DataType::Date64 => {
                if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
                    let millis_since_epoch = array.value(row_idx);
                    let datetime = chrono::DateTime::from_timestamp_millis(millis_since_epoch)
                        .unwrap_or_else(|| {
                            chrono::DateTime::from_timestamp(0, 0)
                                .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH)
                        });
                    datetime.format("%Y-%m-%d").to_string()
                } else {
                    "1970-01-01".to_string()
                }
            },
            DataType::Timestamp(_, _) => {
                "timestamp".to_string()
            },
            _ => {
                // Fallback for other types
                format!("{:?}", column.slice(row_idx, 1))
                    .lines()
                    .next()
                    .unwrap_or("unknown")
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .trim_start_matches("\"")
                    .trim_end_matches("\"")
                    .trim()
                    .to_string()
            },
        }
    }
}
