use clap::Args;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::utils::stats::select_columns_by_pattern;
use crate::cli::CommonArgs;
use datafusion::prelude::*;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, DataType as ArrowDataType};
use datafusion::arrow::record_batch::RecordBatch;
use arrow::array::Array;
use std::sync::Arc;

#[derive(Args, Clone)]
pub struct OutliersArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    
    #[arg(short, long, help = "Comma-separated column names or regex patterns for outlier detection")]
    pub columns: Option<String>,
    
    #[arg(short, long, help = "Outlier detection method", value_enum, default_value = "iqr")]
    pub method: OutlierMethod,
    
    #[arg(long, help = "IQR multiplier for outlier detection (default: 1.5)", default_value = "1.5")]
    pub iqr_multiplier: f64,
    
    #[arg(long, help = "Z-score threshold for outlier detection (default: 3.0)", default_value = "3.0")]
    pub z_score_threshold: f64,
    
    #[arg(long, help = "Show outlier values instead of just flagging them")]
    pub show_values: bool,
    
    #[arg(long, help = "Include row numbers in output")]
    pub include_row_numbers: bool,
    
    #[arg(long, help = "Remove outliers from dataset and save cleaned data")]
    pub remove: bool,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutlierMethod {
    /// Interquartile Range method
    Iqr,
    /// Z-score method
    ZScore,
    /// Modified Z-score method
    ModifiedZScore,
    /// Isolation Forest (simplified implementation)
    IsolationForest,
}

pub async fn execute(args: OutliersArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!(
        "Reading data from: {} for outlier detection using {:?} method",
        args.common.input.display(),
        args.method
    ));
    
    let df = read_data(&args.common.input).await?;
    let schema = df.schema();
    
    let target_columns = if let Some(col_spec) = &args.columns {
        select_columns_by_pattern(schema.clone().into(), col_spec)?
    } else {
        // Select only numeric columns by default
        schema.fields()
            .iter()
            .filter(|field| {
                matches!(field.data_type(), 
                    ArrowDataType::Float64 | ArrowDataType::Float32 | 
                    ArrowDataType::Int64 | ArrowDataType::Int32 | 
                    ArrowDataType::Int16 | ArrowDataType::Int8
                )
            })
            .map(|f| f.name().clone())
            .collect()
    };
    
    if target_columns.is_empty() {
        return Err(NailError::InvalidArgument(
            "No numeric columns found for outlier detection".to_string()
        ));
    }
    
    if args.remove {
        args.common.log_if_verbose(&format!(
            "Removing outliers from {} columns using {:?} method",
            target_columns.len(),
            args.method
        ));
        
        let cleaned_df = match args.method {
            OutlierMethod::Iqr => remove_outliers_iqr(&df, &target_columns, args.iqr_multiplier).await?,
            OutlierMethod::ZScore => remove_outliers_zscore(&df, &target_columns, args.z_score_threshold).await?,
            OutlierMethod::ModifiedZScore => remove_outliers_modified_zscore(&df, &target_columns, args.z_score_threshold).await?,
            OutlierMethod::IsolationForest => remove_outliers_isolation_forest(&df, &target_columns).await?,
        };
        
        args.common.log_if_verbose(&format!(
            "Original rows: {}, Cleaned rows: {}",
            df.clone().count().await?,
            cleaned_df.clone().count().await?
        ));
        
        let output_handler = OutputHandler::new(&args.common);
        output_handler.handle_output(&cleaned_df, "cleaned_data").await?;
    } else {
        args.common.log_if_verbose(&format!(
            "Detecting outliers in {} columns using {:?} method",
            target_columns.len(),
            args.method
        ));
        
        let result_df = match args.method {
            OutlierMethod::Iqr => detect_outliers_iqr(&df, &target_columns, args.iqr_multiplier, args.show_values, args.include_row_numbers).await?,
            OutlierMethod::ZScore => detect_outliers_zscore(&df, &target_columns, args.z_score_threshold, args.show_values, args.include_row_numbers).await?,
            OutlierMethod::ModifiedZScore => detect_outliers_modified_zscore(&df, &target_columns, args.z_score_threshold, args.show_values, args.include_row_numbers).await?,
            OutlierMethod::IsolationForest => detect_outliers_isolation_forest(&df, &target_columns, args.show_values, args.include_row_numbers).await?,
        };
        
        let output_handler = OutputHandler::new(&args.common);
        output_handler.handle_output(&result_df, "outliers").await?;
    }
    
    Ok(())
}

async fn detect_outliers_iqr(
    df: &DataFrame,
    columns: &[String],
    multiplier: f64,
    show_values: bool,
    _include_row_numbers: bool,
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    let mut all_outliers = Vec::new();
    
    for (idx, column) in columns.iter().enumerate() {
        let table_name = format!("table_{}", idx);
        
        // Get Q1 and Q3 using SQL approach
        let sql_query = format!(
            "SELECT 
                APPROX_PERCENTILE_CONT({}, 0.25) as q1,
                APPROX_PERCENTILE_CONT({}, 0.75) as q3
            FROM {}", 
            column, column, table_name
        );
        
        let temp_df = df.clone();
        ctx.register_table(&table_name, temp_df.into_view())?;
        
        let quantiles_df = ctx.sql(&sql_query).await?;
        let quantiles_batches = quantiles_df.collect().await?;
        
        if quantiles_batches.is_empty() {
            continue;
        }
        
        let batch = &quantiles_batches[0];
        
        // Handle different possible return types from APPROX_PERCENTILE_CONT
        let q1_value = if let Some(q1_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if q1_array.is_null(0) {
                continue; // Skip columns with null Q1
            }
            q1_array.value(0)
        } else {
            // Try other numeric types
            continue; // Skip if can't downcast to Float64
        };
        
        let q3_value = if let Some(q3_array) = batch.column(1).as_any().downcast_ref::<Float64Array>() {
            if q3_array.is_null(0) {
                continue; // Skip columns with null Q3
            }
            q3_array.value(0)
        } else {
            // Try other numeric types
            continue; // Skip if can't downcast to Float64
        };
        
        let q1 = q1_value;
        let q3 = q3_value;
        let iqr = q3 - q1;
        let lower_bound = q1 - (multiplier * iqr);
        let upper_bound = q3 + (multiplier * iqr);
        
        // Find outliers
        let outlier_query = if show_values {
            format!(
                "SELECT 
                    '{}' as column_name,
                    {} as value,
                    'IQR' as method,
                    'bounds: {:.3} to {:.3}' as bounds
                FROM {} 
                WHERE {} < {} OR {} > {}",
                column, column, lower_bound, upper_bound, table_name, column, lower_bound, column, upper_bound
            )
        } else {
            format!(
                "SELECT 
                    '{}' as column_name,
                    CASE WHEN {} < {} OR {} > {} THEN true ELSE false END as is_outlier,
                    'IQR' as method
                FROM {}",
                column, column, lower_bound, column, upper_bound, table_name
            )
        };
        
        let outliers_df = ctx.sql(&outlier_query).await?;
        let outliers_batches = outliers_df.collect().await?;
        
        for batch in outliers_batches {
            all_outliers.push(batch);
        }
    }
    
    if all_outliers.is_empty() {
        return Err(NailError::Statistics("No outliers detected".to_string()));
    }
    
    // Create a simple result DataFrame
    let schema = if show_values {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Float64, false),
            Field::new("method", ArrowDataType::Utf8, false),
            Field::new("bounds", ArrowDataType::Utf8, false),
        ]))
    } else {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("is_outlier", ArrowDataType::Boolean, false),
            Field::new("method", ArrowDataType::Utf8, false),
        ]))
    };
    
    let result_batch = RecordBatch::try_new(schema, all_outliers[0].columns().to_vec())?;
    Ok(ctx.read_batch(result_batch)?)
}

async fn detect_outliers_zscore(
    df: &DataFrame,
    columns: &[String],
    threshold: f64,
    show_values: bool,
    _include_row_numbers: bool,
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    let mut all_outliers = Vec::new();
    
    for (idx, column) in columns.iter().enumerate() {
        let table_name = format!("table_{}", idx);
        
        // Get mean and stddev using SQL
        let stats_query = format!(
            "SELECT 
                AVG({}) as mean_val,
                STDDEV({}) as stddev_val
            FROM {}", 
            column, column, table_name
        );
        
        let temp_df = df.clone();
        ctx.register_table(&table_name, temp_df.into_view())?;
        
        let stats_df = ctx.sql(&stats_query).await?;
        let stats_batches = stats_df.collect().await?;
        
        if stats_batches.is_empty() {
            continue;
        }
        
        let batch = &stats_batches[0];
        
        let mean_val = if let Some(mean_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if mean_array.is_null(0) {
                continue; // Skip columns with null mean
            }
            mean_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let stddev_val = if let Some(stddev_array) = batch.column(1).as_any().downcast_ref::<Float64Array>() {
            if stddev_array.is_null(0) {
                continue; // Skip columns with null stddev
            }
            stddev_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        if stddev_val == 0.0 {
            continue;
        }
        
        // Find outliers using Z-score
        let outlier_query = if show_values {
            format!(
                "SELECT 
                    '{}' as column_name,
                    {} as value,
                    ABS(({} - {}) / {}) as z_score,
                    'Z-Score' as method
                FROM {} 
                WHERE ABS(({} - {}) / {}) > {}",
                column, column, column, mean_val, stddev_val, table_name, column, mean_val, stddev_val, threshold
            )
        } else {
            format!(
                "SELECT 
                    '{}' as column_name,
                    CASE WHEN ABS(({} - {}) / {}) > {} THEN true ELSE false END as is_outlier,
                    'Z-Score' as method
                FROM {}",
                column, column, mean_val, stddev_val, threshold, table_name
            )
        };
        
        let outliers_df = ctx.sql(&outlier_query).await?;
        let outliers_batches = outliers_df.collect().await?;
        
        for batch in outliers_batches {
            all_outliers.push(batch);
        }
    }
    
    if all_outliers.is_empty() {
        return Err(NailError::Statistics("No outliers detected".to_string()));
    }
    
    // Create result DataFrame
    let schema = if show_values {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Float64, false),
            Field::new("z_score", ArrowDataType::Float64, false),
            Field::new("method", ArrowDataType::Utf8, false),
        ]))
    } else {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("is_outlier", ArrowDataType::Boolean, false),
            Field::new("method", ArrowDataType::Utf8, false),
        ]))
    };
    
    let result_batch = RecordBatch::try_new(schema, all_outliers[0].columns().to_vec())?;
    Ok(ctx.read_batch(result_batch)?)
}

async fn detect_outliers_modified_zscore(
    df: &DataFrame,
    columns: &[String],
    threshold: f64,
    show_values: bool,
    _include_row_numbers: bool,
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    let mut all_outliers = Vec::new();
    
    for (idx, column) in columns.iter().enumerate() {
        let table_name = format!("table_{}", idx);
        
        // Get median using SQL
        let median_query = format!(
            "SELECT 
                APPROX_PERCENTILE_CONT({}, 0.5) as median_val
            FROM {}", 
            column, table_name
        );
        
        let temp_df = df.clone();
        ctx.register_table(&table_name, temp_df.into_view())?;
        
        let median_df = ctx.sql(&median_query).await?;
        let median_batches = median_df.collect().await?;
        
        if median_batches.is_empty() {
            continue;
        }
        
        let batch = &median_batches[0];
        
        let median_val = if let Some(median_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if median_array.is_null(0) {
                continue; // Skip columns with null median
            }
            median_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        // Get MAD (Median Absolute Deviation)
        let mad_query = format!(
            "SELECT 
                APPROX_PERCENTILE_CONT(ABS({} - {}), 0.5) as mad_val
            FROM {}", 
            column, median_val, table_name
        );
        
        let mad_df = ctx.sql(&mad_query).await?;
        let mad_batches = mad_df.collect().await?;
        
        if mad_batches.is_empty() {
            continue;
        }
        
        let mad_batch = &mad_batches[0];
        
        let mad_val = if let Some(mad_array) = mad_batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if mad_array.is_null(0) {
                continue; // Skip columns with null MAD
            }
            mad_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        if mad_val == 0.0 {
            continue;
        }
        
        // Find outliers using Modified Z-score
        let outlier_query = if show_values {
            format!(
                "SELECT 
                    '{}' as column_name,
                    {} as value,
                    0.6745 * ({} - {}) / {} as modified_z_score,
                    'Modified Z-Score' as method
                FROM {} 
                WHERE ABS(0.6745 * ({} - {}) / {}) > {}",
                column, column, column, median_val, mad_val, table_name, column, median_val, mad_val, threshold
            )
        } else {
            format!(
                "SELECT 
                    '{}' as column_name,
                    CASE WHEN ABS(0.6745 * ({} - {}) / {}) > {} THEN true ELSE false END as is_outlier,
                    'Modified Z-Score' as method
                FROM {}",
                column, column, median_val, mad_val, threshold, table_name
            )
        };
        
        let outliers_df = ctx.sql(&outlier_query).await?;
        let outliers_batches = outliers_df.collect().await?;
        
        for batch in outliers_batches {
            all_outliers.push(batch);
        }
    }
    
    if all_outliers.is_empty() {
        return Err(NailError::Statistics("No outliers detected".to_string()));
    }
    
    // Create result DataFrame
    let schema = if show_values {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Float64, false),
            Field::new("modified_z_score", ArrowDataType::Float64, false),
            Field::new("method", ArrowDataType::Utf8, false),
        ]))
    } else {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("is_outlier", ArrowDataType::Boolean, false),
            Field::new("method", ArrowDataType::Utf8, false),
        ]))
    };
    
    let result_batch = RecordBatch::try_new(schema, all_outliers[0].columns().to_vec())?;
    Ok(ctx.read_batch(result_batch)?)
}

async fn detect_outliers_isolation_forest(
    df: &DataFrame,
    columns: &[String],
    show_values: bool,
    _include_row_numbers: bool,
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    let mut all_outliers = Vec::new();
    
    for (idx, column) in columns.iter().enumerate() {
        let table_name = format!("table_{}", idx);
        
        // Get basic statistics
        let stats_query = format!(
            "SELECT 
                AVG({}) as mean_val,
                STDDEV({}) as stddev_val,
                MIN({}) as min_val,
                MAX({}) as max_val
            FROM {}", 
            column, column, column, column, table_name
        );
        
        let temp_df = df.clone();
        ctx.register_table(&table_name, temp_df.into_view())?;
        
        let stats_df = ctx.sql(&stats_query).await?;
        let stats_batches = stats_df.collect().await?;
        
        if stats_batches.is_empty() {
            continue;
        }
        
        let batch = &stats_batches[0];
        
        let mean_val = if let Some(mean_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if mean_array.is_null(0) {
                continue; // Skip columns with null mean
            }
            mean_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let stddev_val = if let Some(stddev_array) = batch.column(1).as_any().downcast_ref::<Float64Array>() {
            if stddev_array.is_null(0) {
                continue; // Skip columns with null stddev
            }
            stddev_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let min_val = if let Some(min_array) = batch.column(2).as_any().downcast_ref::<Float64Array>() {
            if min_array.is_null(0) {
                continue; // Skip columns with null min
            }
            min_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let max_val = if let Some(max_array) = batch.column(3).as_any().downcast_ref::<Float64Array>() {
            if max_array.is_null(0) {
                continue; // Skip columns with null max
            }
            max_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        if stddev_val == 0.0 {
            continue;
        }
        
        let range_10th = min_val + 0.1 * (max_val - min_val);
        let range_90th = max_val - 0.1 * (max_val - min_val);
        
        // Simplified isolation forest heuristic
        let outlier_query = if show_values {
            format!(
                "SELECT 
                    '{}' as column_name,
                    {} as value,
                    ABS({} - {}) / {} as isolation_score,
                    'Isolation Forest' as method
                FROM {} 
                WHERE (ABS({} - {}) / {} > 2.5) AND ({} < {} OR {} > {})",
                column, column, column, mean_val, stddev_val, table_name, column, mean_val, stddev_val, column, range_10th, column, range_90th
            )
        } else {
            format!(
                "SELECT 
                    '{}' as column_name,
                    CASE WHEN (ABS({} - {}) / {} > 2.5) AND ({} < {} OR {} > {}) THEN true ELSE false END as is_outlier,
                    'Isolation Forest' as method
                FROM {}",
                column, column, mean_val, stddev_val, column, range_10th, column, range_90th, table_name
            )
        };
        
        let outliers_df = ctx.sql(&outlier_query).await?;
        let outliers_batches = outliers_df.collect().await?;
        
        for batch in outliers_batches {
            all_outliers.push(batch);
        }
    }
    
    if all_outliers.is_empty() {
        return Err(NailError::Statistics("No outliers detected".to_string()));
    }
    
    // Create result DataFrame
    let schema = if show_values {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Float64, false),
            Field::new("isolation_score", ArrowDataType::Float64, false),
            Field::new("method", ArrowDataType::Utf8, false),
        ]))
    } else {
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("is_outlier", ArrowDataType::Boolean, false),
            Field::new("method", ArrowDataType::Utf8, false),
        ]))
    };
    
    let result_batch = RecordBatch::try_new(schema, all_outliers[0].columns().to_vec())?;
    Ok(ctx.read_batch(result_batch)?)
}

// Functions to remove outliers from the dataset

async fn remove_outliers_iqr(
    df: &DataFrame,
    columns: &[String],
    multiplier: f64,
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    ctx.register_table("data", df.clone().into_view())?;
    
    let mut where_conditions = Vec::new();
    
    for column in columns.iter() {
        // Get Q1 and Q3 using SQL approach
        let sql_query = format!(
            "SELECT 
                APPROX_PERCENTILE_CONT({}, 0.25) as q1,
                APPROX_PERCENTILE_CONT({}, 0.75) as q3
            FROM data", 
            column, column
        );
        
        let quantiles_df = ctx.sql(&sql_query).await?;
        let quantiles_batches = quantiles_df.collect().await?;
        
        if quantiles_batches.is_empty() {
            continue;
        }
        
        let batch = &quantiles_batches[0];
        
        let q1_value = if let Some(q1_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if q1_array.is_null(0) {
                continue; // Skip columns with null Q1
            }
            q1_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let q3_value = if let Some(q3_array) = batch.column(1).as_any().downcast_ref::<Float64Array>() {
            if q3_array.is_null(0) {
                continue; // Skip columns with null Q3
            }
            q3_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let q1 = q1_value;
        let q3 = q3_value;
        let iqr = q3 - q1;
        let lower_bound = q1 - (multiplier * iqr);
        let upper_bound = q3 + (multiplier * iqr);
        
        // Add condition to keep non-outliers
        where_conditions.push(format!("({} >= {} AND {} <= {})", column, lower_bound, column, upper_bound));
    }
    
    if where_conditions.is_empty() {
        return Ok(df.clone()); // Return original data if no conditions
    }
    
    let where_clause = where_conditions.join(" AND ");
    let filter_query = format!("SELECT * FROM data WHERE {}", where_clause);
    
    ctx.sql(&filter_query).await.map_err(NailError::DataFusion)
}

async fn remove_outliers_zscore(
    df: &DataFrame,
    columns: &[String],
    threshold: f64,
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    ctx.register_table("data", df.clone().into_view())?;
    
    let mut where_conditions = Vec::new();
    
    for column in columns.iter() {
        // Get mean and stddev using SQL
        let stats_query = format!(
            "SELECT 
                AVG({}) as mean_val,
                STDDEV({}) as stddev_val
            FROM data", 
            column, column
        );
        
        let stats_df = ctx.sql(&stats_query).await?;
        let stats_batches = stats_df.collect().await?;
        
        if stats_batches.is_empty() {
            continue;
        }
        
        let batch = &stats_batches[0];
        
        let mean_val = if let Some(mean_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if mean_array.is_null(0) {
                continue; // Skip columns with null mean
            }
            mean_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let stddev_val = if let Some(stddev_array) = batch.column(1).as_any().downcast_ref::<Float64Array>() {
            if stddev_array.is_null(0) {
                continue; // Skip columns with null stddev
            }
            stddev_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        if stddev_val == 0.0 {
            continue;
        }
        
        // Add condition to keep non-outliers (Z-score <= threshold)
        where_conditions.push(format!("ABS(({} - {}) / {}) <= {}", column, mean_val, stddev_val, threshold));
    }
    
    if where_conditions.is_empty() {
        return Ok(df.clone()); // Return original data if no conditions
    }
    
    let where_clause = where_conditions.join(" AND ");
    let filter_query = format!("SELECT * FROM data WHERE {}", where_clause);
    
    ctx.sql(&filter_query).await.map_err(NailError::DataFusion)
}

async fn remove_outliers_modified_zscore(
    df: &DataFrame,
    columns: &[String],
    threshold: f64,
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    ctx.register_table("data", df.clone().into_view())?;
    
    let mut where_conditions = Vec::new();
    
    for column in columns.iter() {
        // Get median using SQL
        let median_query = format!(
            "SELECT 
                APPROX_PERCENTILE_CONT({}, 0.5) as median_val
            FROM data", 
            column
        );
        
        let median_df = ctx.sql(&median_query).await?;
        let median_batches = median_df.collect().await?;
        
        if median_batches.is_empty() {
            continue;
        }
        
        let batch = &median_batches[0];
        
        let median_val = if let Some(median_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if median_array.is_null(0) {
                continue; // Skip columns with null median
            }
            median_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        // Get MAD (Median Absolute Deviation)
        let mad_query = format!(
            "SELECT 
                APPROX_PERCENTILE_CONT(ABS({} - {}), 0.5) as mad_val
            FROM data", 
            column, median_val
        );
        
        let mad_df = ctx.sql(&mad_query).await?;
        let mad_batches = mad_df.collect().await?;
        
        if mad_batches.is_empty() {
            continue;
        }
        
        let mad_batch = &mad_batches[0];
        
        let mad_val = if let Some(mad_array) = mad_batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if mad_array.is_null(0) {
                continue; // Skip columns with null MAD
            }
            mad_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        if mad_val == 0.0 {
            continue;
        }
        
        // Add condition to keep non-outliers (Modified Z-score <= threshold)
        where_conditions.push(format!("ABS(0.6745 * ({} - {}) / {}) <= {}", column, median_val, mad_val, threshold));
    }
    
    if where_conditions.is_empty() {
        return Ok(df.clone()); // Return original data if no conditions
    }
    
    let where_clause = where_conditions.join(" AND ");
    let filter_query = format!("SELECT * FROM data WHERE {}", where_clause);
    
    ctx.sql(&filter_query).await.map_err(NailError::DataFusion)
}

async fn remove_outliers_isolation_forest(
    df: &DataFrame,
    columns: &[String],
) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    ctx.register_table("data", df.clone().into_view())?;
    
    let mut where_conditions = Vec::new();
    
    for column in columns.iter() {
        // Get basic statistics
        let stats_query = format!(
            "SELECT 
                AVG({}) as mean_val,
                STDDEV({}) as stddev_val,
                MIN({}) as min_val,
                MAX({}) as max_val
            FROM data", 
            column, column, column, column
        );
        
        let stats_df = ctx.sql(&stats_query).await?;
        let stats_batches = stats_df.collect().await?;
        
        if stats_batches.is_empty() {
            continue;
        }
        
        let batch = &stats_batches[0];
        
        let mean_val = if let Some(mean_array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            if mean_array.is_null(0) {
                continue; // Skip columns with null mean
            }
            mean_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let stddev_val = if let Some(stddev_array) = batch.column(1).as_any().downcast_ref::<Float64Array>() {
            if stddev_array.is_null(0) {
                continue; // Skip columns with null stddev
            }
            stddev_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let min_val = if let Some(min_array) = batch.column(2).as_any().downcast_ref::<Float64Array>() {
            if min_array.is_null(0) {
                continue; // Skip columns with null min
            }
            min_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        let max_val = if let Some(max_array) = batch.column(3).as_any().downcast_ref::<Float64Array>() {
            if max_array.is_null(0) {
                continue; // Skip columns with null max
            }
            max_array.value(0)
        } else {
            continue; // Skip if can't downcast to Float64
        };
        
        if stddev_val == 0.0 {
            continue;
        }
        
        let range_10th = min_val + 0.1 * (max_val - min_val);
        let range_90th = max_val - 0.1 * (max_val - min_val);
        
        // Simplified isolation forest heuristic - keep non-outliers
        where_conditions.push(format!("NOT ((ABS({} - {}) / {} > 2.5) AND ({} < {} OR {} > {}))", 
            column, mean_val, stddev_val, column, range_10th, column, range_90th));
    }
    
    if where_conditions.is_empty() {
        return Ok(df.clone()); // Return original data if no conditions
    }
    
    let where_clause = where_conditions.join(" AND ");
    let filter_query = format!("SELECT * FROM data WHERE {}", where_clause);
    
    ctx.sql(&filter_query).await.map_err(NailError::DataFusion)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, DataType as ArrowDataType};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use parquet::arrow::AsyncArrowWriter;
    use tokio::fs::File;

    async fn create_test_data() -> NamedTempFile {
        // Create test data with some outliers
        let data = vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, // normal data
            100.0, 200.0, // clear outliers
        ];
        
        let ids: Vec<i64> = (0..data.len() as i64).collect();
        
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("value", ArrowDataType::Float64, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(data)),
            ],
        ).unwrap();

        let temp_file = tempfile::Builder::new().suffix(".parquet").tempfile().unwrap();
        let file = File::create(temp_file.path()).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
        
        temp_file
    }

    #[tokio::test]
    async fn test_outliers_iqr_method() {
        let temp_file = create_test_data().await;
        let args = OutliersArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("value".to_string()),
            method: OutlierMethod::Iqr,
            iqr_multiplier: 1.5,
            z_score_threshold: 3.0,
            show_values: true,
            include_row_numbers: false,
            remove: false,
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_outliers_zscore_method() {
        let temp_file = create_test_data().await;
        let args = OutliersArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("value".to_string()),
            method: OutlierMethod::ZScore,
            iqr_multiplier: 1.5,
            z_score_threshold: 2.0,
            show_values: true,
            include_row_numbers: false,
            remove: false,
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_outliers_no_numeric_columns() {
        // Create test data with only string columns
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["test1", "test2"])),
            ],
        ).unwrap();

        let temp_file = tempfile::Builder::new().suffix(".parquet").tempfile().unwrap();
        let file = File::create(temp_file.path()).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let args = OutliersArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: None,
            method: OutlierMethod::Iqr,
            iqr_multiplier: 1.5,
            z_score_threshold: 3.0,
            show_values: false,
            include_row_numbers: false,
            remove: false,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No numeric columns found"));
    }

    #[tokio::test]
    async fn test_outliers_modified_zscore() {
        let temp_file = create_test_data().await;
        let args = OutliersArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("value".to_string()),
            method: OutlierMethod::ModifiedZScore,
            iqr_multiplier: 1.5,
            z_score_threshold: 3.0,
            show_values: false,
            include_row_numbers: false,
            remove: false,
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_outliers_isolation_forest() {
        let temp_file = create_test_data().await;
        let args = OutliersArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            columns: Some("value".to_string()),
            method: OutlierMethod::IsolationForest,
            iqr_multiplier: 1.5,
            z_score_threshold: 3.0,
            show_values: false,
            include_row_numbers: false,
            remove: false,
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }
}