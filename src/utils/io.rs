use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use std::path::Path;
use crate::error::{NailError, NailResult};
use crate::utils::{create_context, detect_file_format, FileFormat};
use datafusion::arrow::array::{ArrayRef, StringArray, Float64Array, Int64Array, BooleanArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use calamine::{Reader, Xlsx, open_workbook, Data};
use std::sync::Arc;

pub async fn read_data(path: &Path) -> NailResult<DataFrame> {
	let ctx = create_context().await?;
	let format = detect_file_format(path)?;
	
	let result = match format {
		FileFormat::Parquet => {
			ctx.read_parquet(path.to_str().unwrap(), ParquetReadOptions::default()).await
		},
		FileFormat::Csv => {
			ctx.read_csv(path.to_str().unwrap(), CsvReadOptions::default()).await
		},
		FileFormat::Json => {
			ctx.read_json(path.to_str().unwrap(), NdJsonReadOptions::default()).await
		},
		FileFormat::Excel => {
			read_excel_file(path, &ctx).await
		},
	};
	
	result.map_err(NailError::DataFusion)
}

async fn read_excel_file(path: &Path, ctx: &SessionContext) -> Result<DataFrame, datafusion::error::DataFusionError> {
	let mut workbook: Xlsx<_> = open_workbook(path)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	
	// Get the first worksheet
	let sheet_names = workbook.sheet_names();
	if sheet_names.is_empty() {
		return Err(datafusion::error::DataFusionError::External(
			"No worksheets found in Excel file".into()
		));
	}
	
	let sheet_name = &sheet_names[0];
	let range = workbook.worksheet_range(sheet_name)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	
	if range.is_empty() {
		return Err(datafusion::error::DataFusionError::External(
			"Empty worksheet".into()
		));
	}
	
	// Extract headers from first row
	let mut headers = Vec::new();
	let (rows, cols) = range.get_size();
	
	for col in 0..cols {
		let cell_value = range.get_value((0, col as u32)).unwrap_or(&Data::Empty);
		let header = match cell_value {
			Data::String(s) => s.clone(),
			Data::Int(i) => i.to_string(),
			Data::Float(f) => f.to_string(),
			_ => format!("Column_{}", col + 1),
		};
		headers.push(header);
	}
	
	// Determine column types by sampling data
	let mut column_types = vec![DataType::Utf8; cols];
	for col in 0..cols {
		let mut has_int = false;
		let mut has_float = false;
		let mut has_bool = false;
		
		// Sample up to 10 rows to determine type
		let sample_rows = std::cmp::min(rows, 11);
		for row in 1..sample_rows {
			let cell_value = range.get_value((row as u32, col as u32)).unwrap_or(&Data::Empty);
			match cell_value {
				Data::Int(_) => has_int = true,
				Data::Float(_) => has_float = true,
				Data::Bool(_) => has_bool = true,
				_ => {}
			}
		}
		
		// Determine type priority: Bool > Float > Int > String
		if has_bool && !has_int && !has_float {
			column_types[col] = DataType::Boolean;
		} else if has_float {
			column_types[col] = DataType::Float64;
		} else if has_int && !has_float {
			column_types[col] = DataType::Int64;
		}
	}
	
	// Create schema
	let fields: Vec<Field> = headers.iter().zip(column_types.iter())
		.map(|(name, dtype)| Field::new(name, dtype.clone(), true))
		.collect();
	let schema = Arc::new(Schema::new(fields));
	
	// Extract data
	let mut columns: Vec<ArrayRef> = Vec::new();
	
	for col in 0..cols {
		let dtype = &column_types[col];
		let mut values = Vec::new();
		
		for row in 1..rows {
			let cell_value = range.get_value((row as u32, col as u32)).unwrap_or(&Data::Empty);
			values.push(cell_value.clone());
		}
		
		let array: ArrayRef = match dtype {
			DataType::Boolean => {
				let bool_values: Vec<Option<bool>> = values.iter().map(|v| match v {
					Data::Bool(b) => Some(*b),
					Data::String(s) => {
						match s.to_lowercase().as_str() {
							"true" | "1" | "yes" => Some(true),
							"false" | "0" | "no" => Some(false),
							_ => None,
						}
					},
					_ => None,
				}).collect();
				Arc::new(BooleanArray::from(bool_values))
			},
			DataType::Int64 => {
				let int_values: Vec<Option<i64>> = values.iter().map(|v| match v {
					Data::Int(i) => Some(*i),
					Data::Float(f) => Some(*f as i64),
					Data::String(s) => s.parse().ok(),
					_ => None,
				}).collect();
				Arc::new(Int64Array::from(int_values))
			},
			DataType::Float64 => {
				let float_values: Vec<Option<f64>> = values.iter().map(|v| match v {
					Data::Float(f) => Some(*f),
					Data::Int(i) => Some(*i as f64),
					Data::String(s) => s.parse().ok(),
					_ => None,
				}).collect();
				Arc::new(Float64Array::from(float_values))
			},
			_ => { // String
				let string_values: Vec<Option<String>> = values.iter().map(|v| match v {
					Data::String(s) => Some(s.clone()),
					Data::Int(i) => Some(i.to_string()),
					Data::Float(f) => Some(f.to_string()),
					Data::Bool(b) => Some(b.to_string()),
					Data::DateTime(dt) => Some(format!("{}", dt)),
					Data::DateTimeIso(dt) => Some(dt.clone()),
					Data::DurationIso(d) => Some(d.clone()),
					Data::Error(e) => Some(format!("Error: {:?}", e)),
					Data::Empty => None,
				}).collect();
				Arc::new(StringArray::from(string_values))
			}
		};
		
		columns.push(array);
	}
	
	// Create record batch
	let batch = RecordBatch::try_new(schema.clone(), columns)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	
	// Convert to DataFrame
	ctx.read_batch(batch)
}

pub async fn write_data(df: &DataFrame, path: &Path, format: Option<&FileFormat>) -> NailResult<()> {
	let output_format = format.map(|f| f.clone()).unwrap_or_else(|| detect_file_format(path).unwrap_or(FileFormat::Parquet));
	
	match output_format {
		FileFormat::Parquet => {
			df.clone().write_parquet(
				path.to_str().unwrap(),
				DataFrameWriteOptions::new(),
				None,
			).await.map_err(NailError::DataFusion)?;
		},
		FileFormat::Csv => {
			df.clone().write_csv(
				path.to_str().unwrap(),
				DataFrameWriteOptions::new(),
				None,
			).await.map_err(NailError::DataFusion)?;
		},
		FileFormat::Json => {
			df.clone().write_json(
				path.to_str().unwrap(),
				DataFrameWriteOptions::new(),
				None,
			).await.map_err(NailError::DataFusion)?;
		},
		FileFormat::Excel => {
			return Err(NailError::UnsupportedFormat("Writing to Excel format is not yet supported".to_string()));
		},
	};
	
	Ok(())
}