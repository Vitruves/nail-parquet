use clap::Args;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::SortExpr;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct SortArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(
        short = 'c', 
        long = "column", 
        help = "Columns to sort by (comma-separated or 'all')",
        default_value = "all"
    )]
    pub columns: String,

    #[arg(
        short = 's',
        long = "strategy", 
        help = "Sort strategy per column (comma-separated): numeric, date, alphabetic, alphabetic-numeric, numeric-alphabetic, hour",
        value_delimiter = ','
    )]
    pub strategy: Option<Vec<String>>,

    #[arg(
        long = "nulls", 
        help = "Null value handling: first, last, skip",
        default_value = "last"
    )]
    pub nulls: String,

    #[arg(
        long = "date-format", 
        help = "Date format pattern (e.g., 'mm-dd-yyyy', 'dd/mm/yyyy', 'yyyy-mm-dd')"
    )]
    pub date_format: Option<String>,

    #[arg(
        long = "hour-format", 
        help = "Hour/time format pattern (e.g., 'hh:mm:ss', 'mm:ss', 'hh:mm:ss.xxx' where x represents digits)"
    )]
    pub hour_format: Option<String>,

    #[arg(
        short = 'd',
        long = "descending",
        help = "Sort in descending order (per column, comma-separated true/false)",
        value_delimiter = ','
    )]
    pub descending: Option<Vec<bool>>,

    #[arg(
        long = "case-insensitive",
        help = "Case-insensitive alphabetic sorting"
    )]
    pub case_insensitive: bool,
}

#[derive(Debug, Clone)]
enum SortStrategy {
    Numeric,
    Date,
    Alphabetic,
    AlphabeticNumeric,
    NumericAlphabetic,
    Hour,
    Auto,
}

impl SortStrategy {
    fn from_str(s: &str) -> NailResult<Self> {
        match s.to_lowercase().as_str() {
            "numeric" | "num" => Ok(SortStrategy::Numeric),
            "date" => Ok(SortStrategy::Date),
            "alphabetic" | "alpha" => Ok(SortStrategy::Alphabetic),
            "alphabetic-numeric" | "alpha-num" => Ok(SortStrategy::AlphabeticNumeric),
            "numeric-alphabetic" | "num-alpha" => Ok(SortStrategy::NumericAlphabetic),
            "hour" | "time" => Ok(SortStrategy::Hour),
            "auto" => Ok(SortStrategy::Auto),
            _ => Err(NailError::InvalidArgument(format!("Unknown sort strategy: {}", s))),
        }
    }
}

#[derive(Debug, Clone)]
enum NullHandling {
    First,
    Last,
    Skip,
}

impl NullHandling {
    fn from_str(s: &str) -> NailResult<Self> {
        match s.to_lowercase().as_str() {
            "first" => Ok(NullHandling::First),
            "last" => Ok(NullHandling::Last),
            "skip" => Ok(NullHandling::Skip),
            _ => Err(NailError::InvalidArgument(format!("Unknown null handling: {}", s))),
        }
    }
}

pub async fn execute(args: SortArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

    let df = read_data(&args.common.input).await?;
    
    // Parse null handling
    let null_handling = NullHandling::from_str(&args.nulls)?;
    
    // If nulls should be skipped, filter them out first
    let df = if matches!(null_handling, NullHandling::Skip) {
        args.common.log_if_verbose("Filtering out rows with null values in sort columns");
        filter_nulls(df, &args.columns)?
    } else {
        df
    };
    
    let schema = df.schema();
    
    // Determine columns to sort by
    let sort_columns: Vec<String> = if args.columns == "all" {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    } else {
        args.columns.split(',').map(|s| s.trim().to_string()).collect()
    };
    
    // Validate columns exist
    for col in &sort_columns {
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
    
    args.common.log_if_verbose(&format!("Sorting by columns: {:?}", sort_columns));
    
    // Parse strategies
    let strategies = if let Some(strat_list) = &args.strategy {
        if strat_list.len() != sort_columns.len() && strat_list.len() != 1 {
            return Err(NailError::InvalidArgument(
                format!("Number of strategies ({}) must match number of columns ({}) or be 1 for all", 
                        strat_list.len(), sort_columns.len())
            ));
        }
        
        if strat_list.len() == 1 {
            vec![SortStrategy::from_str(&strat_list[0])?; sort_columns.len()]
        } else {
            strat_list.iter()
                .map(|s| SortStrategy::from_str(s))
                .collect::<NailResult<Vec<_>>>()?
        }
    } else {
        vec![SortStrategy::Auto; sort_columns.len()]
    };
    
    // Parse descending flags
    let descending_flags = if let Some(desc_list) = &args.descending {
        if desc_list.len() != sort_columns.len() && desc_list.len() != 1 {
            return Err(NailError::InvalidArgument(
                format!("Number of descending flags ({}) must match number of columns ({}) or be 1 for all", 
                        desc_list.len(), sort_columns.len())
            ));
        }
        
        if desc_list.len() == 1 {
            vec![desc_list[0]; sort_columns.len()]
        } else {
            desc_list.clone()
        }
    } else {
        vec![false; sort_columns.len()]
    };
    
    // Create sort expressions
    let mut sort_exprs = Vec::new();
    
    for (idx, col_name) in sort_columns.iter().enumerate() {
        let strategy: &SortStrategy = &strategies[idx];
        let descending = descending_flags[idx];
        let nulls_first = matches!(null_handling, NullHandling::First);
        
        let field = schema.field_with_name(None, col_name)?;
        let data_type = field.data_type();
        
        let expr = create_sort_expression(
            col_name,
            data_type,
            strategy.clone(),
            descending,
            nulls_first,
            args.case_insensitive,
            args.date_format.clone(),
            args.hour_format.clone(),
        )?;
        
        sort_exprs.push(expr);
    }
    
    args.common.log_if_verbose(&format!("Applying {} sort expressions", sort_exprs.len()));
    
    // Apply sorting
    let sorted_df = df.sort(sort_exprs)?;
    
    // Write or display result
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&sorted_df, "sort").await?;
    
    Ok(())
}

fn filter_nulls(df: DataFrame, columns_spec: &str) -> NailResult<DataFrame> {
    let schema = df.schema();
    
    let filter_columns: Vec<String> = if columns_spec == "all" {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    } else {
        columns_spec.split(',').map(|s| s.trim().to_string()).collect()
    };
    
    // Build filter expression: col1 IS NOT NULL AND col2 IS NOT NULL AND ...
    let mut filter_expr = None;
    
    for col_name in filter_columns {
        let not_null = col(&col_name).is_not_null();
        
        filter_expr = match filter_expr {
            None => Some(not_null),
            Some(existing) => Some(existing.and(not_null)),
        };
    }
    
    if let Some(expr) = filter_expr {
        Ok(df.filter(expr)?)
    } else {
        Ok(df)
    }
}

fn create_sort_expression(
    col_name: &str,
    data_type: &DataType,
    strategy: SortStrategy,
    descending: bool,
    nulls_first: bool,
    case_insensitive: bool,
    date_format: Option<String>,
    hour_format: Option<String>,
) -> NailResult<SortExpr> {
    let base_col = col(col_name);
    
    let expr = match strategy {
        SortStrategy::Auto => {
            // Auto-detect based on data type
            match data_type {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) |
                DataType::Decimal256(_, _) => {
                    base_col
                }
                DataType::Date32 | DataType::Date64 | 
                DataType::Timestamp(_, _) | DataType::Time32(_) | DataType::Time64(_) => {
                    base_col
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    if case_insensitive {
                        lower(base_col)
                    } else {
                        base_col
                    }
                }
                _ => base_col
            }
        }
        SortStrategy::Numeric => {
            // Try to cast to numeric if not already
            match data_type {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) |
                DataType::Decimal256(_, _) => base_col,
                DataType::Utf8 | DataType::LargeUtf8 => {
                    // Try to cast string to float64 for numeric sorting
                    cast(base_col, DataType::Float64)
                }
                _ => base_col
            }
        }
        SortStrategy::Date => {
            // Handle date parsing
            match data_type {
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => base_col,
                DataType::Utf8 | DataType::LargeUtf8 => {
                    if let Some(format) = date_format {
                        // Use the custom date parsing expression
                        parse_date_expr(base_col, &format)?
                    } else {
                        // Try default date parsing (ISO format)
                        to_date(vec![base_col])
                    }
                }
                _ => base_col
            }
        }
        SortStrategy::Hour => {
            // Handle time/hour parsing
            match data_type {
                DataType::Time32(_) | DataType::Time64(_) => base_col,
                DataType::Utf8 | DataType::LargeUtf8 => {
                    if let Some(format) = hour_format {
                        parse_time_expr(base_col, &format)?
                    } else {
                        // Default: try to extract hour component from timestamp
                        to_timestamp(vec![base_col])
                    }
                }
                _ => base_col
            }
        }
        SortStrategy::Alphabetic => {
            match data_type {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    if case_insensitive {
                        lower(base_col)
                    } else {
                        base_col
                    }
                }
                _ => base_col
            }
        }
        SortStrategy::AlphabeticNumeric => {
            // Sort alphabetically first, then numerically
            // This is complex and would require multiple sort keys
            // For now, we'll use a simplified approach
            match data_type {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    // Extract leading alphabetic part for primary sort
                    if case_insensitive {
                        lower(base_col)
                    } else {
                        base_col
                    }
                }
                _ => base_col
            }
        }
        SortStrategy::NumericAlphabetic => {
            // Sort numerically first, then alphabetically
            // Similar to above, simplified approach
            match data_type {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    // Try to extract leading numeric part
                    base_col
                }
                _ => base_col
            }
        }
    };
    
    Ok(expr.sort(!descending, nulls_first))
}

fn parse_date_expr(col_expr: Expr, format: &str) -> NailResult<Expr> {
    // Convert format pattern to strptime format
    let strptime_format = convert_date_format(format)?;
    
    // Use to_date with format string
    Ok(to_date(vec![col_expr, lit(strptime_format)]))
}

fn parse_time_expr(col_expr: Expr, format: &str) -> NailResult<Expr> {
    // For time parsing, we'll convert to a sortable numeric representation
    // This is a simplified approach - in practice, you might want more sophisticated parsing
    
    if format.contains("xxx") || format.contains("XXX") {
        // Handle numeric suffix
        // For now, just use the column as-is
        Ok(col_expr)
    } else {
        // Convert time format to timestamp format
        let timestamp_format = convert_time_format(format)?;
        Ok(to_timestamp(vec![col_expr, lit(timestamp_format)]))
    }
}

fn convert_date_format(format: &str) -> NailResult<String> {
    // Convert user-friendly format to strptime format
    let result = format
        .replace("yyyy", "%Y")
        .replace("yy", "%y")
        .replace("mm", "%m")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("DD", "%d")
        .replace("/", "-");
    
    Ok(result)
}

fn convert_time_format(format: &str) -> NailResult<String> {
    // Convert user-friendly time format to strptime format
    let result = format
        .replace("hh", "%H")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("MM", "%M")
        .replace("ss", "%S")
        .replace("SS", "%S")
        .replace(":", ":");
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_sort_args_basic() {
        let args = SortArgs {
            common: CommonArgs {
                input: PathBuf::from("data.parquet"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            columns: "all".to_string(),
            strategy: None,
            nulls: "last".to_string(),
            date_format: None,
            hour_format: None,
            descending: None,
            case_insensitive: false,
        };

        assert_eq!(args.columns, "all");
        assert_eq!(args.nulls, "last");
        assert!(!args.case_insensitive);
    }

    #[test]
    fn test_sort_args_with_columns() {
        let args = SortArgs {
            common: CommonArgs {
                input: PathBuf::from("sales.csv"),
                output: Some(PathBuf::from("sorted.parquet")),
                format: Some(crate::cli::OutputFormat::Parquet),
                random: None,
                jobs: Some(4),
                verbose: true,
            },
            columns: "date,amount,customer".to_string(),
            strategy: Some(vec!["date".to_string(), "numeric".to_string(), "alphabetic".to_string()]),
            nulls: "first".to_string(),
            date_format: Some("mm-dd-yyyy".to_string()),
            hour_format: None,
            descending: Some(vec![true, false, false]),
            case_insensitive: true,
        };

        assert_eq!(args.columns, "date,amount,customer");
        assert_eq!(args.nulls, "first");
        assert!(args.case_insensitive);
        assert_eq!(args.strategy.unwrap().len(), 3);
    }

    #[test]
    fn test_sort_strategy_from_str() {
        assert!(matches!(SortStrategy::from_str("numeric").unwrap(), SortStrategy::Numeric));
        assert!(matches!(SortStrategy::from_str("date").unwrap(), SortStrategy::Date));
        assert!(matches!(SortStrategy::from_str("alphabetic").unwrap(), SortStrategy::Alphabetic));
        assert!(matches!(SortStrategy::from_str("alpha-num").unwrap(), SortStrategy::AlphabeticNumeric));
        assert!(matches!(SortStrategy::from_str("num-alpha").unwrap(), SortStrategy::NumericAlphabetic));
        assert!(matches!(SortStrategy::from_str("hour").unwrap(), SortStrategy::Hour));
        assert!(matches!(SortStrategy::from_str("auto").unwrap(), SortStrategy::Auto));
        
        assert!(SortStrategy::from_str("invalid").is_err());
    }

    #[test]
    fn test_null_handling_from_str() {
        assert!(matches!(NullHandling::from_str("first").unwrap(), NullHandling::First));
        assert!(matches!(NullHandling::from_str("last").unwrap(), NullHandling::Last));
        assert!(matches!(NullHandling::from_str("skip").unwrap(), NullHandling::Skip));
        
        assert!(NullHandling::from_str("invalid").is_err());
    }

    #[test]
    fn test_convert_date_format() {
        assert_eq!(convert_date_format("yyyy-mm-dd").unwrap(), "%Y-%m-%d");
        assert_eq!(convert_date_format("dd/mm/yyyy").unwrap(), "%d-%m-%Y");
        assert_eq!(convert_date_format("mm-dd-yy").unwrap(), "%m-%d-%y");
    }

    #[test]
    fn test_convert_time_format() {
        assert_eq!(convert_time_format("hh:mm:ss").unwrap(), "%H:%M:%S");
        assert_eq!(convert_time_format("HH:MM:SS").unwrap(), "%H:%M:%S");
        assert_eq!(convert_time_format("mm:ss").unwrap(), "%M:%S");
    }

    #[test]
    fn test_sort_args_clone() {
        let args = SortArgs {
            common: CommonArgs {
                input: PathBuf::from("test.parquet"),
                output: None,
                format: None,
                random: None,
                jobs: None,
                verbose: false,
            },
            columns: "col1,col2".to_string(),
            strategy: Some(vec!["numeric".to_string()]),
            nulls: "skip".to_string(),
            date_format: None,
            hour_format: None,
            descending: Some(vec![true]),
            case_insensitive: false,
        };

        let cloned = args.clone();
        assert_eq!(args.columns, cloned.columns);
        assert_eq!(args.nulls, cloned.nulls);
        assert_eq!(args.common.input, cloned.common.input);
    }
}