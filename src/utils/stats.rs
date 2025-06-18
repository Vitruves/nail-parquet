use datafusion::prelude::*;
use datafusion::common::DFSchemaRef;
use regex::Regex;
use crate::error::{NailError, NailResult};
use arrow::array::{StringArray, Float64Array, ArrayRef};
use arrow::datatypes::{Field, Schema as ArrowSchema, DataType as ArrowDataType};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use statrs::distribution::{Normal, StudentsT, ChiSquared};
use statrs::distribution::ContinuousCDF;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum CorrelationType {
	Pearson,
	Kendall,
	Spearman,
}

pub fn select_columns_by_pattern(schema: DFSchemaRef, pattern: &str) -> NailResult<Vec<String>> {
	let patterns: Vec<&str> = pattern.split(',').map(|s| s.trim()).collect();
	let mut selected = Vec::new();
	let mut not_found = Vec::new();
	
	for pattern in &patterns {
		let mut found = false;
		
		for field in schema.fields() {
			let field_name = field.name();
			
			if pattern.contains('*') || pattern.contains('^') || pattern.contains('$') {
				let regex = Regex::new(pattern)?;
				if regex.is_match(field_name) {
					selected.push(field_name.clone());
					found = true;
				}
			} else if field_name == *pattern {
				selected.push(field_name.clone());
				found = true;
				break;
			}
		}
		
		if !found {
			for field in schema.fields() {
				let field_name = field.name();
				
				if pattern.contains('*') || pattern.contains('^') || pattern.contains('$') {
					let case_insensitive_pattern = format!("(?i){}", pattern);
					if let Ok(regex) = Regex::new(&case_insensitive_pattern) {
						if regex.is_match(field_name) {
							selected.push(field_name.clone());
							found = true;
						}
					}
				} else if field_name.to_lowercase() == pattern.to_lowercase() {
					selected.push(field_name.clone());
					found = true;
					break;
				}
			}
		}
		
		if !found {
			not_found.push(*pattern);
		}
	}
	
	if !not_found.is_empty() {
		let available_columns: Vec<String> = schema.fields().iter()
			.map(|f| f.name().clone())
			.collect();
		return Err(NailError::ColumnNotFound(format!(
			"Columns not found: {:?}. Available columns: {:?}", 
			not_found, available_columns
		)));
	}
	
	let mut unique_selected = Vec::new();
	for col in selected {
		if !unique_selected.contains(&col) {
			unique_selected.push(col);
		}
	}
	
	if unique_selected.is_empty() {
		return Err(NailError::ColumnNotFound(format!("No columns matched pattern: {}", pattern)));
	}
	
	Ok(unique_selected)
}

pub async fn calculate_basic_stats(df: &DataFrame, columns: &[String]) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let mut stats_rows = Vec::new();
	
	for column in columns {
		let field = df.schema().field_with_name(None, column)
			.map_err(|_| NailError::ColumnNotFound(column.clone()))?;
		
		match field.data_type() {
			datafusion::arrow::datatypes::DataType::Int64 | 
			datafusion::arrow::datatypes::DataType::Float64 | 
			datafusion::arrow::datatypes::DataType::Int32 | 
			datafusion::arrow::datatypes::DataType::Float32 => {
				let stats_sql = format!(
					"SELECT 
						'{}' as column_name,
						COUNT(\"{}\") as count,
						AVG(\"{}\") as mean,
						APPROX_PERCENTILE_CONT(\"{}\", 0.25) as q25,
						APPROX_PERCENTILE_CONT(\"{}\", 0.5) as q50,
						APPROX_PERCENTILE_CONT(\"{}\", 0.75) as q75,
						COUNT(DISTINCT \"{}\") as num_classes
					FROM {}",
					column, column, column, column, column, column, column, table_name
				);
				
				let stats_df = ctx.sql(&stats_sql).await?;
				stats_rows.push(stats_df);
			},
			datafusion::arrow::datatypes::DataType::Utf8 => {
				let stats_sql = format!(
					"SELECT 
						'{}' as column_name,
						COUNT(\"{}\") as count,
						NULL as mean,
						NULL as q25,
						NULL as q50,
						NULL as q75,
						COUNT(DISTINCT \"{}\") as num_classes
					FROM {}",
					column, column, column, table_name
				);
				
				let stats_df = ctx.sql(&stats_sql).await?;
				stats_rows.push(stats_df);
			},
			_ => continue,
		}
	}
	
	if stats_rows.is_empty() {
		return Err(NailError::Statistics("No suitable columns for statistics".to_string()));
	}
	
	let mut iter = stats_rows.into_iter();
	let mut combined = iter.next().unwrap();
	for df in iter {
		combined = combined.union(df)?;
	}
	
	Ok(combined)
}

pub async fn calculate_exhaustive_stats(df: &DataFrame, columns: &[String]) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let mut stats_rows = Vec::new();
	
	for column in columns {
		let field = df.schema().field_with_name(None, column)
			.map_err(|_| NailError::ColumnNotFound(column.clone()))?;
		
		match field.data_type() {
			datafusion::arrow::datatypes::DataType::Int64 | 
			datafusion::arrow::datatypes::DataType::Float64 | 
			datafusion::arrow::datatypes::DataType::Int32 | 
			datafusion::arrow::datatypes::DataType::Float32 => {
				let stats_sql = format!(
					"SELECT 
						'{}' as column_name,
						COUNT(\"{}\") as count,
						AVG(\"{}\") as mean,
						STDDEV(\"{}\") as std_dev,
						MIN(\"{}\") as min_val,
						APPROX_PERCENTILE_CONT(\"{}\", 0.25) as q25,
						APPROX_PERCENTILE_CONT(\"{}\", 0.5) as median,
						APPROX_PERCENTILE_CONT(\"{}\", 0.75) as q75,
						MAX(\"{}\") as max_val,
						VAR_POP(\"{}\") as variance,
						COUNT(DISTINCT \"{}\") as num_classes,
						(COUNT(\"{}\") - COUNT(DISTINCT \"{}\")) as duplicates
					FROM {}",
					column, column, column, column, column, column, column, column, 
					column, column, column, column, column, table_name
				);
				
				let stats_df = ctx.sql(&stats_sql).await?;
				stats_rows.push(stats_df);
			},
			datafusion::arrow::datatypes::DataType::Utf8 => {
				let stats_sql = format!(
					"SELECT 
						'{}' as column_name,
						COUNT(\"{}\") as count,
						NULL as mean,
						NULL as std_dev,
						NULL as min_val,
						NULL as q25,
						NULL as median,
						NULL as q75,
						NULL as max_val,
						NULL as variance,
						COUNT(DISTINCT \"{}\") as num_classes,
						(COUNT(\"{}\") - COUNT(DISTINCT \"{}\")) as duplicates
					FROM {}",
					column, column, column, column, column, table_name
				);
				
				let stats_df = ctx.sql(&stats_sql).await?;
				stats_rows.push(stats_df);
			},
			_ => continue,
		}
	}
	
	if stats_rows.is_empty() {
		return Err(NailError::Statistics("No suitable columns for statistics".to_string()));
	}
	
	let mut iter = stats_rows.into_iter();
	let mut combined = iter.next().unwrap();
	for df in iter {
		combined = combined.union(df)?;
	}
	
	Ok(combined)
}

pub async fn calculate_hypothesis_tests(_df: &DataFrame, _columns: &[String]) -> NailResult<DataFrame> {
	Err(NailError::Statistics("Hypothesis tests not yet implemented".to_string()))
}

pub async fn calculate_correlations(
    df: &DataFrame,
    columns: &[String],
    correlation_type: &CorrelationType,
    matrix_format: bool,
    include_tests: bool,
    digits: usize,
) -> NailResult<DataFrame> {
    let ctx = crate::utils::create_context().await?;
    let table_name = "temp_table";
    ctx.register_table(table_name, df.clone().into_view())?;

    // Compute correlations
    let corr_df = if matrix_format {
        calculate_correlation_matrix(ctx.clone(), table_name, columns, correlation_type, digits).await?
    } else {
        calculate_correlation_pairs(ctx.clone(), table_name, columns, correlation_type, digits).await?
    };

    // If no tests or in matrix mode, return as is
    if !include_tests || matrix_format {
        return Ok(corr_df);
    }

    // Calculate number of observations once
    let total_batches = df.clone().collect().await.map_err(NailError::DataFusion)?;
    let n: usize = total_batches.iter().map(|b| b.num_rows()).sum();

    // Collect correlation pairs and pre-allocate
    let batches = corr_df.collect().await.map_err(NailError::DataFusion)?;
    let total_pairs: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut col1_vec = Vec::with_capacity(total_pairs);
    let mut col2_vec = Vec::with_capacity(total_pairs);
    let mut corr_vec = Vec::with_capacity(total_pairs);
    let mut p_fisher = Vec::with_capacity(total_pairs);
    let mut p_t = Vec::with_capacity(total_pairs);
    let mut p_chi2 = Vec::with_capacity(total_pairs);


    for batch in batches {
        let schema = batch.schema();
        let col1_idx = schema.index_of("column1").unwrap();
        let col2_idx = schema.index_of("column2").unwrap();
        let corr_idx = schema.index_of("correlation").unwrap();
        let col1_arr = batch.column(col1_idx).as_any().downcast_ref::<StringArray>().unwrap();
        let col2_arr = batch.column(col2_idx).as_any().downcast_ref::<StringArray>().unwrap();
        let corr_arr = batch.column(corr_idx).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..batch.num_rows() {
            let r = corr_arr.value(i);
            col1_vec.push(col1_arr.value(i).to_string());
            col2_vec.push(col2_arr.value(i).to_string());
            corr_vec.push(r);
            // Fisher Z-test
            let z = 0.5 * ((1.0 + r) / (1.0 - r)).ln() * ((n as f64 - 3.0).sqrt());
            let p_z = 2.0 * (1.0 - <Normal as ContinuousCDF<f64, f64>>::cdf(&Normal::new(0.0, 1.0).unwrap(), z.abs()));
            p_fisher.push(p_z);
            // T-test
            let t = r * ((n as f64 - 2.0) / (1.0 - r * r)).sqrt();
            let student = StudentsT::new(0.0, 1.0, (n - 2) as f64).unwrap();
            let p_tval = 2.0 * (1.0 - <StudentsT as ContinuousCDF<f64, f64>>::cdf(&student, t.abs()));
            p_t.push(p_tval);
            // Chi-squared
            let chi2 = t * t;
            let chisq = ChiSquared::new(1.0).unwrap();
            let p_chival = 1.0 - <ChiSquared as ContinuousCDF<f64, f64>>::cdf(&chisq, chi2);
            p_chi2.push(p_chival);
        }
    }

    // Build new RecordBatch
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("column1", ArrowDataType::Utf8, false),
        Field::new("column2", ArrowDataType::Utf8, false),
        Field::new("correlation", ArrowDataType::Float64, false),
        Field::new("p_fisher", ArrowDataType::Float64, false),
        Field::new("p_t", ArrowDataType::Float64, false),
        Field::new("p_chi2", ArrowDataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(StringArray::from(col1_vec)) as ArrayRef,
            Arc::new(StringArray::from(col2_vec)) as ArrayRef,
            Arc::new(Float64Array::from(corr_vec)) as ArrayRef,
            Arc::new(Float64Array::from(p_fisher)) as ArrayRef,
            Arc::new(Float64Array::from(p_t)) as ArrayRef,
            Arc::new(Float64Array::from(p_chi2)) as ArrayRef,
        ],
    ).map_err(NailError::Arrow)?;

    Ok(ctx.read_batch(batch).map_err(NailError::DataFusion)?)
}

async fn calculate_correlation_matrix(
	ctx: SessionContext,
	table_name: &str,
	columns: &[String],
	correlation_type: &CorrelationType,
	digits: usize,
) -> NailResult<DataFrame> {
	let mut correlation_queries = Vec::new();
	
	for col1 in columns {
		let mut row_values = Vec::new();
		row_values.push(format!("'{}' as variable", col1));
		
		for col2 in columns {
			if col1 == col2 {
				row_values.push(format!("1.0 as corr_with_{}", col2.replace(".", "_")));
			} else {
				let corr_expr = match correlation_type {
					CorrelationType::Pearson => {
						format!("ROUND((SELECT CORR(\"{}\", \"{}\") FROM {}), {})", col1, col2, table_name, digits)
					},
					CorrelationType::Spearman => {
						format!("ROUND((SELECT CORR(\"{}\", \"{}\") FROM {}), {})", col1, col2, table_name, digits)
					},
					CorrelationType::Kendall => {
						format!("ROUND((SELECT CORR(\"{}\", \"{}\") * 0.816 FROM {}), {})", col1, col2, table_name, digits)
					}
				};
				row_values.push(format!("{} as corr_with_{}", corr_expr, col2.replace(".", "_")));
			}
		}
		
		let row_sql = format!("SELECT {}", row_values.join(", "));
		correlation_queries.push(row_sql);
	}
	
	if correlation_queries.is_empty() {
		return Err(NailError::Statistics("No columns for correlation".to_string()));
	}
	
	let mut combined = ctx.sql(&correlation_queries[0]).await?;
	for query in correlation_queries.into_iter().skip(1) {
		let df = ctx.sql(&query).await?;
		combined = combined.union(df)?;
	}
	
	Ok(combined)
}

async fn calculate_correlation_pairs(
	ctx: SessionContext,
	table_name: &str,
	columns: &[String],
	correlation_type: &CorrelationType,
	digits: usize,
) -> NailResult<DataFrame> {
	let mut pair_queries = Vec::new();
	
	for (i, col1) in columns.iter().enumerate() {
		for col2 in columns.iter().skip(i + 1) {
			let pair_sql = match correlation_type {
				CorrelationType::Pearson => {
					format!(
						"SELECT '{}' as column1, '{}' as column2, ROUND(CORR(\"{}\", \"{}\"), {}) as correlation FROM {}",
						col1, col2, col1, col2, digits, table_name
					)
				},
				CorrelationType::Spearman => {
					format!(
						"WITH ranked_data AS (
							SELECT 
								ROW_NUMBER() OVER (ORDER BY \"{}\") as rank1,
								ROW_NUMBER() OVER (ORDER BY \"{}\") as rank2
							FROM {}
						) 
						SELECT '{}' as column1, '{}' as column2, ROUND(CORR(rank1, rank2), {}) as correlation FROM ranked_data",
						col1, col2, table_name, col1, col2, digits
					)
				},
				CorrelationType::Kendall => {
					format!(
						"WITH ranked_data AS (
							SELECT 
								ROW_NUMBER() OVER (ORDER BY \"{}\") as rank1,
								ROW_NUMBER() OVER (ORDER BY \"{}\") as rank2
							FROM {}
						) 
						SELECT '{}' as column1, '{}' as column2, ROUND(CORR(rank1, rank2) * 0.816, {}) as correlation FROM ranked_data",
						col1, col2, table_name, col1, col2, digits
					)
				}
			};
			pair_queries.push(pair_sql);
		}
	}
	
	if pair_queries.is_empty() {
		return Err(NailError::Statistics("Need at least 2 columns for correlation".to_string()));
	}
	
	let mut combined = ctx.sql(&pair_queries[0]).await?;
	for query in pair_queries.into_iter().skip(1) {
		let df = ctx.sql(&query).await?;
		combined = combined.union(df)?;
	}
	
	Ok(combined)
}