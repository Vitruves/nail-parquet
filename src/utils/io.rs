use datafusion::prelude::{SessionContext, CsvReadOptions as DataFusionCsvReadOptions, ParquetReadOptions, NdJsonReadOptions};
use datafusion::dataframe::{DataFrame as DataFusionDataFrame, DataFrameWriteOptions};
use std::path::Path;
use crate::error::{NailError, NailResult};
use crate::utils::{create_context, detect_file_format, FileFormat};
use datafusion::arrow::array::{Array, ArrayRef, StringArray, Float64Array, Int64Array, BooleanArray, Date32Array, Date64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use calamine::{Reader, Xlsx, open_workbook, Data};
use rust_xlsxwriter::{Workbook, Format};
use std::sync::Arc;

pub async fn read_data(path: &Path) -> NailResult<DataFusionDataFrame> {
	let ctx = create_context().await?;
	let format = detect_file_format(path)?;
	
	let result = match format {
		FileFormat::Parquet => {
			ctx.read_parquet(path.to_str().unwrap(), ParquetReadOptions::default()).await
		},
		FileFormat::Csv => {
			ctx.read_csv(path.to_str().unwrap(), DataFusionCsvReadOptions::default()).await
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

async fn read_excel_file(path: &Path, ctx: &SessionContext) -> Result<DataFusionDataFrame, datafusion::error::DataFusionError> {
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
	
	// Infer column types by sampling some rows
	let mut column_types = Vec::new();
	for col_idx in 0..headers.len() {
		let sample_values: Vec<_> = (1..std::cmp::min(rows, 101))
			.filter_map(|row_idx| range.get_value((row_idx as u32, col_idx as u32)))
			.collect();
		
		let data_type = if sample_values.iter().all(|v| matches!(v, Data::Empty)) {
			DataType::Utf8
		} else if sample_values.iter().all(|v| matches!(v, Data::String(_))) {
			DataType::Utf8
		} else if sample_values.iter().all(|v| matches!(v, Data::Int(_))) {
			DataType::Int64
		} else if sample_values.iter().all(|v| matches!(v, Data::Float(_))) {
			DataType::Float64
		} else {
			DataType::Utf8
		};
		column_types.push(data_type);
	}
	
	// Create Arrow arrays for each column
	let mut arrays: Vec<ArrayRef> = Vec::new();
	for (col_idx, data_type) in column_types.iter().enumerate() {
		match data_type {
			DataType::Utf8 => {
				let values: Vec<Option<String>> = (1..rows)
					.map(|row_idx| {
						range.get_value((row_idx as u32, col_idx as u32))
							.and_then(|cell| match cell {
								Data::String(s) => Some(s.clone()),
								Data::Int(i) => Some(i.to_string()),
								Data::Float(f) => Some(f.to_string()),
								Data::Bool(b) => Some(b.to_string()),
								Data::Empty => None,
								_ => Some(format!("{:?}", cell)),
							})
					})
					.collect();
				arrays.push(Arc::new(StringArray::from(values)));
			},
			DataType::Int64 => {
				let values: Vec<Option<i64>> = (1..rows)
					.map(|row_idx| {
						range.get_value((row_idx as u32, col_idx as u32))
							.and_then(|cell| match cell {
								Data::Int(i) => Some(*i),
								Data::Float(f) => Some(*f as i64),
								_ => None,
							})
					})
					.collect();
				arrays.push(Arc::new(Int64Array::from(values)));
			},
			DataType::Float64 => {
				let values: Vec<Option<f64>> = (1..rows)
					.map(|row_idx| {
						range.get_value((row_idx as u32, col_idx as u32))
							.and_then(|cell| match cell {
								Data::Float(f) => Some(*f),
								Data::Int(i) => Some(*i as f64),
								_ => None,
							})
					})
					.collect();
				arrays.push(Arc::new(Float64Array::from(values)));
			},
			_ => {
				let values: Vec<Option<String>> = (1..rows)
					.map(|row_idx| {
						range.get_value((row_idx as u32, col_idx as u32))
							.map(|cell| format!("{:?}", cell))
					})
					.collect();
				arrays.push(Arc::new(StringArray::from(values)));
			},
		}
	}
	
	// Create schema
	let fields: Vec<Field> = headers.iter()
		.zip(column_types.iter())
		.map(|(name, data_type)| Field::new(name, data_type.clone(), true))
		.collect();
	let schema = Arc::new(Schema::new(fields));
	
	// Create RecordBatch
	let batch = RecordBatch::try_new(schema, arrays)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	
	// Register as DataFrame
	ctx.read_batch(batch)
}

pub async fn write_data(df: &DataFusionDataFrame, path: &Path, format: Option<&FileFormat>) -> NailResult<()> {
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
			write_excel_file(df, path).await?;
		},
	};
	
	Ok(())
}

async fn write_excel_file(df: &DataFusionDataFrame, path: &Path) -> Result<(), datafusion::error::DataFusionError> {
	// Collect the data from DataFusion DataFrame
	let batches = df.clone().collect().await?;
	
	if batches.is_empty() {
		return Ok(());
	}
	
	// Create a new Excel workbook
	let mut workbook = Workbook::new();
	
	// Create date format for Excel
	let date_format = Format::new().set_num_format("yyyy-mm-dd");
	
	// Add a worksheet to the workbook
	let worksheet = workbook.add_worksheet();
	
	// Write the header row
	let schema = batches[0].schema();
	for (col_idx, field) in schema.fields().iter().enumerate() {
		worksheet.write_string(0, col_idx as u16, field.name().as_str())
			.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	}
	
	// Write the data rows
	let mut current_row = 1u32;
	for batch in &batches {
		for row_idx in 0..batch.num_rows() {
			for (col_idx, field) in schema.fields().iter().enumerate() {
				match field.data_type() {
					DataType::Utf8 => {
						let array = batch.column(col_idx).as_any().downcast_ref::<StringArray>().unwrap();
						if !array.is_null(row_idx) {
							worksheet.write_string(current_row, col_idx as u16, array.value(row_idx))
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					DataType::Int64 => {
						let array = batch.column(col_idx).as_any().downcast_ref::<Int64Array>().unwrap();
						if !array.is_null(row_idx) {
							worksheet.write_number(current_row, col_idx as u16, array.value(row_idx) as f64)
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					DataType::Float64 => {
						let array = batch.column(col_idx).as_any().downcast_ref::<Float64Array>().unwrap();
						if !array.is_null(row_idx) {
							worksheet.write_number(current_row, col_idx as u16, array.value(row_idx))
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					DataType::Boolean => {
						let array = batch.column(col_idx).as_any().downcast_ref::<BooleanArray>().unwrap();
						if !array.is_null(row_idx) {
							worksheet.write_boolean(current_row, col_idx as u16, array.value(row_idx))
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					DataType::Date32 => {
						let array = batch.column(col_idx).as_any().downcast_ref::<Date32Array>().unwrap();
						if !array.is_null(row_idx) {
							let days_since_epoch = array.value(row_idx);
							// Convert days since epoch to Excel date (Excel uses days since 1900-01-01, but accounting for leap year bug)
							// Arrow uses days since 1970-01-01, so we need to add the offset
							let excel_date = days_since_epoch as f64 + 25569.0; // Days between 1900-01-01 and 1970-01-01
							
							worksheet.write_with_format(current_row, col_idx as u16, excel_date, &date_format)
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					DataType::Date64 => {
						let array = batch.column(col_idx).as_any().downcast_ref::<Date64Array>().unwrap();
						if !array.is_null(row_idx) {
							let millis_since_epoch = array.value(row_idx);
							// Convert to days since epoch and then to Excel date
							let days_since_epoch = millis_since_epoch as f64 / (1000.0 * 60.0 * 60.0 * 24.0);
							let excel_date = days_since_epoch + 25569.0;
							
							worksheet.write_with_format(current_row, col_idx as u16, excel_date, &date_format)
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					_ => {
						// For other unsupported types, convert to string
						let array = batch.column(col_idx);
						if !array.is_null(row_idx) {
							let value = format!("{:?}", array.slice(row_idx, 1));
							worksheet.write_string(current_row, col_idx as u16, &value)
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
				}
			}
			current_row += 1;
		}
	}
	
	// Save the workbook to a file
	workbook.save(path)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	
	Ok(())
}