use datafusion::prelude::{SessionContext, CsvReadOptions as DataFusionCsvReadOptions, ParquetReadOptions, NdJsonReadOptions};
use datafusion::dataframe::{DataFrame as DataFusionDataFrame, DataFrameWriteOptions};
use std::path::Path;
use crate::error::{NailError, NailResult};
use crate::utils::{create_context, create_context_with_opts, detect_file_format, FileFormat};
use datafusion::arrow::array::{Array, ArrayRef, StringArray, Float64Array, Int64Array, BooleanArray, Date32Array, Date64Array, RecordBatch};
use datafusion::arrow::array::{Float64Builder, Int64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use calamine::{Reader, Xlsx, open_workbook, Data};
use rust_xlsxwriter::{Workbook, Format};
use std::sync::Arc;
use std::fs::File;
use std::io::Write;
use futures::StreamExt;

pub async fn read_data(path: &Path) -> NailResult<DataFusionDataFrame> {
	read_data_with_opts(path, None, None).await
}

pub async fn read_data_with_opts(
	path: &Path,
	jobs: Option<usize>,
	batch_size: Option<usize>,
) -> NailResult<DataFusionDataFrame> {
	let ctx = if jobs.is_some() || batch_size.is_some() {
		create_context_with_opts(jobs, batch_size).await?
	} else {
		create_context().await?
	};
	read_data_in(path, &ctx).await
}

async fn read_data_in(path: &Path, ctx: &SessionContext) -> NailResult<DataFusionDataFrame> {
	let format = detect_file_format(path)?;
	let path_str = path.to_str().ok_or_else(|| {
		NailError::InvalidArgument(format!("Non-UTF8 path: {}", path.display()))
	})?;

	let result = match format {
		FileFormat::Parquet => {
			// Enable pruning + statistics-driven skipping.
			let opts = ParquetReadOptions::default();
			ctx.read_parquet(path_str, opts).await
		},
		FileFormat::Csv => {
			ctx.read_csv(path_str, DataFusionCsvReadOptions::default()).await
		},
		FileFormat::Json => {
			ctx.read_json(path_str, NdJsonReadOptions::default()).await
		},
		FileFormat::Excel => {
			read_excel_file(path, ctx).await
		},
	};

	result.map_err(NailError::DataFusion)
}

async fn read_excel_file(path: &Path, ctx: &SessionContext) -> Result<DataFusionDataFrame, datafusion::error::DataFusionError> {
	let mut workbook: Xlsx<_> = open_workbook(path)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

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

	let (rows, cols) = range.get_size();

	// Extract headers from first row (single pass)
	let mut headers: Vec<String> = Vec::with_capacity(cols);
	for col in 0..cols {
		let header = match range.get_value((0, col as u32)).unwrap_or(&Data::Empty) {
			Data::String(s) => s.clone(),
			Data::Int(i) => i.to_string(),
			Data::Float(f) => f.to_string(),
			_ => format!("Column_{}", col + 1),
		};
		headers.push(header);
	}

	// Infer column types by sampling (scan ≤100 rows)
	let sample_end = std::cmp::min(rows, 101);
	let mut column_types: Vec<DataType> = Vec::with_capacity(cols);
	for col_idx in 0..cols {
		let mut saw_any = false;
		let mut all_int = true;
		let mut all_float = true;
		let mut all_string = true;
		for row_idx in 1..sample_end {
			match range.get_value((row_idx as u32, col_idx as u32)).unwrap_or(&Data::Empty) {
				Data::Empty => continue,
				Data::Int(_) => { saw_any = true; all_string = false; all_float = false; },
				Data::Float(_) => { saw_any = true; all_int = false; all_string = false; },
				Data::String(_) => { saw_any = true; all_int = false; all_float = false; },
				_ => { saw_any = true; all_int = false; all_float = false; },
			}
		}
		let dt = if !saw_any || all_string { DataType::Utf8 }
			else if all_int { DataType::Int64 }
			else if all_float { DataType::Float64 }
			else { DataType::Utf8 };
		column_types.push(dt);
	}

	// Build arrays using typed Arrow builders (single column-wise pass,
	// no per-cell heap allocation for numeric types).
	let data_rows = rows.saturating_sub(1);
	let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols);
	for (col_idx, data_type) in column_types.iter().enumerate() {
		match data_type {
			DataType::Int64 => {
				let mut b = Int64Builder::with_capacity(data_rows);
				for row_idx in 1..rows {
					match range.get_value((row_idx as u32, col_idx as u32)).unwrap_or(&Data::Empty) {
						Data::Int(i) => b.append_value(*i),
						Data::Float(f) => b.append_value(*f as i64),
						_ => b.append_null(),
					}
				}
				arrays.push(Arc::new(b.finish()));
			},
			DataType::Float64 => {
				let mut b = Float64Builder::with_capacity(data_rows);
				for row_idx in 1..rows {
					match range.get_value((row_idx as u32, col_idx as u32)).unwrap_or(&Data::Empty) {
						Data::Float(f) => b.append_value(*f),
						Data::Int(i) => b.append_value(*i as f64),
						_ => b.append_null(),
					}
				}
				arrays.push(Arc::new(b.finish()));
			},
			_ => {
				let mut b = StringBuilder::with_capacity(data_rows, data_rows * 8);
				for row_idx in 1..rows {
					match range.get_value((row_idx as u32, col_idx as u32)).unwrap_or(&Data::Empty) {
						Data::String(s) => b.append_value(s),
						Data::Int(i) => b.append_value(i.to_string()),
						Data::Float(f) => b.append_value(f.to_string()),
						Data::Bool(v) => b.append_value(v.to_string()),
						Data::Empty => b.append_null(),
						other => b.append_value(format!("{:?}", other)),
					}
				}
				arrays.push(Arc::new(b.finish()));
			},
		}
	}

	let fields: Vec<Field> = headers.iter()
		.zip(column_types.iter())
		.map(|(name, data_type)| Field::new(name, data_type.clone(), true))
		.collect();
	let schema = Arc::new(Schema::new(fields));

	let batch = RecordBatch::try_new(schema, arrays)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

	ctx.read_batch(batch)
}

pub async fn write_data(df: &DataFusionDataFrame, path: &Path, format: Option<&FileFormat>) -> NailResult<()> {
	let output_format = format.cloned().unwrap_or_else(|| detect_file_format(path).unwrap_or(FileFormat::Parquet));

	match output_format {
		FileFormat::Parquet => write_parquet_streaming(df, path).await?,
		FileFormat::Csv => write_csv_streaming(df, path).await?,
		FileFormat::Json => {
			// Let DataFusion stream JSON directly; it already writes batch-by-batch.
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

async fn write_parquet_streaming(df: &DataFusionDataFrame, path: &Path) -> NailResult<()> {
	use parquet::arrow::ArrowWriter;
	use parquet::file::properties::WriterProperties;
	use parquet::basic::Compression;

	let arrow_schema = Arc::new(df.schema().as_arrow().clone());
	let file = File::create(path).map_err(NailError::Io)?;
	let props = WriterProperties::builder()
		.set_compression(Compression::SNAPPY)
		.build();
	let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))
		.map_err(|e| NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e))))?;

	let mut stream = df.clone().execute_stream().await.map_err(NailError::DataFusion)?;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res.map_err(NailError::DataFusion)?;
		writer.write(&batch).map_err(|e| NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e))))?;
	}
	writer.close().map_err(|e| NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e))))?;
	Ok(())
}

async fn write_csv_streaming(df: &DataFusionDataFrame, path: &Path) -> NailResult<()> {
	use arrow::csv::WriterBuilder;

	let file = File::create(path).map_err(NailError::Io)?;
	let mut writer = WriterBuilder::new().with_header(true).build(file);

	let mut stream = df.clone().execute_stream().await.map_err(NailError::DataFusion)?;
	let mut wrote_any = false;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res.map_err(NailError::DataFusion)?;
		wrote_any = true;
		writer.write(&batch).map_err(NailError::Arrow)?;
	}

	// Empty DataFrame: emit a header-only CSV so consumers still see the schema.
	if !wrote_any {
		drop(writer);
		let mut f = File::create(path).map_err(NailError::Io)?;
		let header: Vec<&str> = df.schema().fields().iter()
			.map(|f| f.name().as_str())
			.collect();
		writeln!(f, "{}", header.join(","))
			.map_err(NailError::Io)?;
	}
	Ok(())
}

async fn write_excel_file(df: &DataFusionDataFrame, path: &Path) -> Result<(), datafusion::error::DataFusionError> {
	// Excel library is not streaming by design; collect in bounded batches.
	let mut stream = df.clone().execute_stream().await?;
	let mut workbook = Workbook::new();
	let date_format = Format::new().set_num_format("yyyy-mm-dd");
	let worksheet = workbook.add_worksheet();

	// Write header row from DataFrame schema (no need to peek a batch)
	for (col_idx, field) in df.schema().fields().iter().enumerate() {
		worksheet.write_string(0, col_idx as u16, field.name().as_str())
			.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	}

	let mut current_row = 1u32;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res?;
		for row_idx in 0..batch.num_rows() {
			for (col_idx, field) in batch.schema().fields().iter().enumerate() {
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
							let days = array.value(row_idx);
							let excel_date = days as f64 + 25569.0;
							worksheet.write_with_format(current_row, col_idx as u16, excel_date, &date_format)
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					DataType::Date64 => {
						let array = batch.column(col_idx).as_any().downcast_ref::<Date64Array>().unwrap();
						if !array.is_null(row_idx) {
							let millis = array.value(row_idx);
							let days = millis as f64 / (1000.0 * 60.0 * 60.0 * 24.0);
							let excel_date = days + 25569.0;
							worksheet.write_with_format(current_row, col_idx as u16, excel_date, &date_format)
								.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
						}
					},
					_ => {
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

	workbook.save(path)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

	Ok(())
}

pub(crate) async fn write_empty_parquet_file(df: &DataFusionDataFrame, path: &Path) -> NailResult<()> {
	use parquet::arrow::ArrowWriter;

	let arrow_schema = Arc::new(df.schema().as_arrow().clone());
	let empty_arrays: Vec<Arc<dyn arrow::array::Array>> = arrow_schema.fields().iter()
		.map(|field| {
			use arrow::array::*;
			match field.data_type() {
				DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>,
				DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
				DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
				DataType::Float32 => Arc::new(Float32Array::from(Vec::<f32>::new())),
				DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
				DataType::Utf8 => Arc::new(StringArray::from(Vec::<String>::new())),
				_ => Arc::new(StringArray::from(Vec::<String>::new())),
			}
		})
		.collect();

	let empty_batch = RecordBatch::try_new(arrow_schema.clone(), empty_arrays)
		.map_err(|e| NailError::DataFusion(datafusion::error::DataFusionError::ArrowError(e, None)))?;

	let file = File::create(path).map_err(NailError::Io)?;
	let mut writer = ArrowWriter::try_new(file, empty_batch.schema(), None)
		.map_err(|e| NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e))))?;
	writer.write(&empty_batch)
		.map_err(|e| NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e))))?;
	writer.close()
		.map_err(|e| NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e))))?;
	Ok(())
}
