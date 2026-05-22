use crate::error::{NailError, NailResult};
use crate::utils::{create_context, create_context_with_opts, detect_file_format, FileFormat};
use calamine::{open_workbook, Data, Reader, Xlsx};
use datafusion::arrow::array::{
	Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float64Array, Int64Array, RecordBatch,
	StringArray,
};
use datafusion::arrow::array::{Float64Builder, Int64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::{DataFrame as DataFusionDataFrame, DataFrameWriteOptions};
use datafusion::datasource::MemTable;
use datafusion::prelude::{
	CsvReadOptions as DataFusionCsvReadOptions, NdJsonReadOptions, ParquetReadOptions,
	SessionContext,
};
use futures::StreamExt;
use rust_xlsxwriter::{Format, Workbook};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

/// Returns true when the path is the conventional stdio marker `-`.
pub fn is_stdio(path: &Path) -> bool {
	path.as_os_str() == "-"
}

pub async fn read_data(path: &Path) -> NailResult<DataFusionDataFrame> {
	read_data_with_opts(path, None, None).await
}

pub async fn read_data_with_opts(
	path: &Path,
	jobs: Option<usize>,
	batch_size: Option<usize>,
) -> NailResult<DataFusionDataFrame> {
	read_data_with_opts_and_format(path, jobs, batch_size, None).await
}

/// Like `read_data_with_opts` but accepts an explicit format hint (used for stdin
/// when the format can't be inferred from a file extension).
pub async fn read_data_with_opts_and_format(
	path: &Path,
	jobs: Option<usize>,
	batch_size: Option<usize>,
	format_hint: Option<FileFormat>,
) -> NailResult<DataFusionDataFrame> {
	let ctx = if jobs.is_some() || batch_size.is_some() {
		create_context_with_opts(jobs, batch_size).await?
	} else {
		create_context().await?
	};

	if is_stdio(path) {
		read_stdin(&ctx, format_hint).await
	} else {
		read_data_in(path, &ctx).await
	}
}

async fn read_data_in(path: &Path, ctx: &SessionContext) -> NailResult<DataFusionDataFrame> {
	let format = detect_file_format(path)?;
	let path_str = path
		.to_str()
		.ok_or_else(|| NailError::InvalidArgument(format!("Non-UTF8 path: {}", path.display())))?;

	let result = match format {
		FileFormat::Parquet => {
			// Enable pruning + statistics-driven skipping.
			let opts = ParquetReadOptions::default();
			ctx.read_parquet(path_str, opts).await
		}
		FileFormat::Csv => {
			ctx.read_csv(path_str, DataFusionCsvReadOptions::default())
				.await
		}
		FileFormat::Json => ctx.read_json(path_str, NdJsonReadOptions::default()).await,
		FileFormat::Excel => read_excel_file(path, ctx).await,
	};

	result.map_err(NailError::DataFusion)
}

async fn read_stdin(
	ctx: &SessionContext,
	format_hint: Option<FileFormat>,
) -> NailResult<DataFusionDataFrame> {
	let mut buf = Vec::new();
	std::io::stdin()
		.read_to_end(&mut buf)
		.map_err(NailError::Io)?;
	if buf.is_empty() {
		return Err(NailError::InvalidArgument(
			"stdin is empty — pipe data into nail or pass a file path".to_string(),
		));
	}

	let format = format_hint.unwrap_or_else(|| sniff_format(&buf));

	let batches = match format {
		FileFormat::Parquet => parse_parquet_bytes(&buf)?,
		FileFormat::Csv => parse_csv_bytes(&buf)?,
		FileFormat::Json => parse_json_bytes(&buf)?,
		FileFormat::Excel => {
			return Err(NailError::UnsupportedFormat(
				"Excel (xlsx) cannot be read from stdin; pass a file path".to_string(),
			))
		}
	};

	let schema = batches
		.first()
		.map(|b| b.schema())
		.ok_or_else(|| NailError::InvalidArgument("Empty input on stdin".to_string()))?;
	let mem = MemTable::try_new(schema, vec![batches]).map_err(NailError::DataFusion)?;
	ctx.read_table(Arc::new(mem)).map_err(NailError::DataFusion)
}

fn sniff_format(buf: &[u8]) -> FileFormat {
	// Parquet files start (and end) with the "PAR1" magic.
	if buf.len() >= 4 && &buf[0..4] == b"PAR1" {
		return FileFormat::Parquet;
	}
	// First non-whitespace byte tells JSON from CSV.
	for &b in buf {
		match b {
			b' ' | b'\t' | b'\r' | b'\n' => continue,
			b'{' | b'[' => return FileFormat::Json,
			_ => return FileFormat::Csv,
		}
	}
	FileFormat::Csv
}

fn parse_parquet_bytes(buf: &[u8]) -> NailResult<Vec<RecordBatch>> {
	use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
	let bytes = bytes::Bytes::copy_from_slice(buf);
	let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(NailError::Parquet)?;
	let reader = builder.build().map_err(NailError::Parquet)?;
	let mut batches = Vec::new();
	for b in reader {
		batches.push(b.map_err(NailError::Arrow)?);
	}
	Ok(batches)
}

fn parse_csv_bytes(buf: &[u8]) -> NailResult<Vec<RecordBatch>> {
	use arrow::csv::ReaderBuilder;
	// Infer schema from a copy of the bytes, then re-read for data.
	let (schema, _) = arrow::csv::reader::Format::default()
		.with_header(true)
		.infer_schema(std::io::Cursor::new(buf), Some(1000))
		.map_err(NailError::Arrow)?;
	let reader = ReaderBuilder::new(Arc::new(schema))
		.with_header(true)
		.build(std::io::Cursor::new(buf))
		.map_err(NailError::Arrow)?;
	let mut batches = Vec::new();
	for b in reader {
		batches.push(b.map_err(NailError::Arrow)?);
	}
	Ok(batches)
}

fn parse_json_bytes(buf: &[u8]) -> NailResult<Vec<RecordBatch>> {
	use arrow::json::ReaderBuilder;
	let (schema, _) =
		arrow::json::reader::infer_json_schema_from_seekable(std::io::Cursor::new(buf), Some(1000))
			.map_err(NailError::Arrow)?;
	let reader = ReaderBuilder::new(Arc::new(schema))
		.build(std::io::Cursor::new(buf))
		.map_err(NailError::Arrow)?;
	let mut batches = Vec::new();
	for b in reader {
		batches.push(b.map_err(NailError::Arrow)?);
	}
	Ok(batches)
}

async fn read_excel_file(
	path: &Path,
	ctx: &SessionContext,
) -> Result<DataFusionDataFrame, datafusion::error::DataFusionError> {
	let mut workbook: Xlsx<_> = open_workbook(path)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

	let sheet_names = workbook.sheet_names();
	if sheet_names.is_empty() {
		return Err(datafusion::error::DataFusionError::External(
			"No worksheets found in Excel file".into(),
		));
	}

	let sheet_name = &sheet_names[0];
	let range = workbook
		.worksheet_range(sheet_name)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

	if range.is_empty() {
		return Err(datafusion::error::DataFusionError::External(
			"Empty worksheet".into(),
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
			match range
				.get_value((row_idx as u32, col_idx as u32))
				.unwrap_or(&Data::Empty)
			{
				Data::Empty => continue,
				Data::Int(_) => {
					saw_any = true;
					all_string = false;
					all_float = false;
				}
				Data::Float(_) => {
					saw_any = true;
					all_int = false;
					all_string = false;
				}
				Data::String(_) => {
					saw_any = true;
					all_int = false;
					all_float = false;
				}
				_ => {
					saw_any = true;
					all_int = false;
					all_float = false;
				}
			}
		}
		let dt = if !saw_any || all_string {
			DataType::Utf8
		} else if all_int {
			DataType::Int64
		} else if all_float {
			DataType::Float64
		} else {
			DataType::Utf8
		};
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
					match range
						.get_value((row_idx as u32, col_idx as u32))
						.unwrap_or(&Data::Empty)
					{
						Data::Int(i) => b.append_value(*i),
						Data::Float(f) => b.append_value(*f as i64),
						_ => b.append_null(),
					}
				}
				arrays.push(Arc::new(b.finish()));
			}
			DataType::Float64 => {
				let mut b = Float64Builder::with_capacity(data_rows);
				for row_idx in 1..rows {
					match range
						.get_value((row_idx as u32, col_idx as u32))
						.unwrap_or(&Data::Empty)
					{
						Data::Float(f) => b.append_value(*f),
						Data::Int(i) => b.append_value(*i as f64),
						_ => b.append_null(),
					}
				}
				arrays.push(Arc::new(b.finish()));
			}
			_ => {
				let mut b = StringBuilder::with_capacity(data_rows, data_rows * 8);
				for row_idx in 1..rows {
					match range
						.get_value((row_idx as u32, col_idx as u32))
						.unwrap_or(&Data::Empty)
					{
						Data::String(s) => b.append_value(s),
						Data::Int(i) => b.append_value(i.to_string()),
						Data::Float(f) => b.append_value(f.to_string()),
						Data::Bool(v) => b.append_value(v.to_string()),
						Data::Empty => b.append_null(),
						other => b.append_value(format!("{:?}", other)),
					}
				}
				arrays.push(Arc::new(b.finish()));
			}
		}
	}

	let fields: Vec<Field> = headers
		.iter()
		.zip(column_types.iter())
		.map(|(name, data_type)| Field::new(name, data_type.clone(), true))
		.collect();
	let schema = Arc::new(Schema::new(fields));

	let batch = RecordBatch::try_new(schema, arrays)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

	ctx.read_batch(batch)
}

pub async fn write_data(
	df: &DataFusionDataFrame,
	path: &Path,
	format: Option<&FileFormat>,
) -> NailResult<()> {
	if is_stdio(path) {
		// Stdout: default to CSV when no explicit format is given (most pipe-friendly).
		let out_format = format.cloned().unwrap_or(FileFormat::Csv);
		return write_data_to_stdout(df, &out_format).await;
	}

	let output_format = format
		.cloned()
		.unwrap_or_else(|| detect_file_format(path).unwrap_or(FileFormat::Parquet));

	match output_format {
		FileFormat::Parquet => write_parquet_streaming(df, path).await?,
		FileFormat::Csv => write_csv_streaming(df, path).await?,
		FileFormat::Json => {
			// Let DataFusion stream JSON directly; it already writes batch-by-batch.
			df.clone()
				.write_json(path.to_str().unwrap(), DataFrameWriteOptions::new(), None)
				.await
				.map_err(NailError::DataFusion)?;
		}
		FileFormat::Excel => {
			write_excel_file(df, path).await?;
		}
	};

	Ok(())
}

pub async fn write_data_to_stdout(df: &DataFusionDataFrame, format: &FileFormat) -> NailResult<()> {
	use std::io::BufWriter;
	// `Stdout` is Send+Sync; `StdoutLock` is not. We need Send because the
	// arrow/parquet writers cross an await point.
	let writer = BufWriter::new(std::io::stdout());

	match format {
		FileFormat::Parquet => write_parquet_to_writer(df, writer).await,
		FileFormat::Csv => write_csv_to_writer(df, writer).await,
		FileFormat::Json => write_json_to_writer(df, writer).await,
		FileFormat::Excel => Err(NailError::UnsupportedFormat(
			"Excel (xlsx) cannot be written to stdout".to_string(),
		)),
	}
}

async fn write_parquet_to_writer<W: Write + Send>(
	df: &DataFusionDataFrame,
	writer: W,
) -> NailResult<()> {
	use parquet::arrow::ArrowWriter;
	use parquet::basic::Compression;
	use parquet::file::properties::WriterProperties;

	let arrow_schema = Arc::new(df.schema().as_arrow().clone());
	let props = WriterProperties::builder()
		.set_compression(Compression::SNAPPY)
		.build();
	let mut w = ArrowWriter::try_new(writer, arrow_schema, Some(props)).map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
	})?;
	let mut stream = df
		.clone()
		.execute_stream()
		.await
		.map_err(NailError::DataFusion)?;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res.map_err(NailError::DataFusion)?;
		w.write(&batch).map_err(|e| {
			NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
		})?;
	}
	w.close().map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
	})?;
	Ok(())
}

async fn write_csv_to_writer<W: Write>(df: &DataFusionDataFrame, writer: W) -> NailResult<()> {
	use arrow::csv::WriterBuilder;
	let mut w = WriterBuilder::new().with_header(true).build(writer);
	let mut stream = df
		.clone()
		.execute_stream()
		.await
		.map_err(NailError::DataFusion)?;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res.map_err(NailError::DataFusion)?;
		w.write(&batch).map_err(NailError::Arrow)?;
	}
	Ok(())
}

async fn write_json_to_writer<W: Write>(df: &DataFusionDataFrame, writer: W) -> NailResult<()> {
	use arrow::json::LineDelimitedWriter;
	let mut w = LineDelimitedWriter::new(writer);
	let mut stream = df
		.clone()
		.execute_stream()
		.await
		.map_err(NailError::DataFusion)?;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res.map_err(NailError::DataFusion)?;
		w.write(&batch).map_err(NailError::Arrow)?;
	}
	w.finish().map_err(NailError::Arrow)?;
	Ok(())
}

async fn write_parquet_streaming(df: &DataFusionDataFrame, path: &Path) -> NailResult<()> {
	use parquet::arrow::ArrowWriter;
	use parquet::basic::Compression;
	use parquet::file::properties::WriterProperties;

	let arrow_schema = Arc::new(df.schema().as_arrow().clone());
	let file = File::create(path).map_err(NailError::Io)?;
	let props = WriterProperties::builder()
		.set_compression(Compression::SNAPPY)
		.build();
	let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
	})?;

	let mut stream = df
		.clone()
		.execute_stream()
		.await
		.map_err(NailError::DataFusion)?;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res.map_err(NailError::DataFusion)?;
		writer.write(&batch).map_err(|e| {
			NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
		})?;
	}
	writer.close().map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
	})?;
	Ok(())
}

async fn write_csv_streaming(df: &DataFusionDataFrame, path: &Path) -> NailResult<()> {
	use arrow::csv::WriterBuilder;

	let file = File::create(path).map_err(NailError::Io)?;
	let mut writer = WriterBuilder::new().with_header(true).build(file);

	let mut stream = df
		.clone()
		.execute_stream()
		.await
		.map_err(NailError::DataFusion)?;
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
		let header: Vec<&str> = df
			.schema()
			.fields()
			.iter()
			.map(|f| f.name().as_str())
			.collect();
		writeln!(f, "{}", header.join(",")).map_err(NailError::Io)?;
	}
	Ok(())
}

async fn write_excel_file(
	df: &DataFusionDataFrame,
	path: &Path,
) -> Result<(), datafusion::error::DataFusionError> {
	// Excel library is not streaming by design; collect in bounded batches.
	let mut stream = df.clone().execute_stream().await?;
	let mut workbook = Workbook::new();
	let date_format = Format::new().set_num_format("yyyy-mm-dd");
	let worksheet = workbook.add_worksheet();

	// Write header row from DataFrame schema (no need to peek a batch)
	for (col_idx, field) in df.schema().fields().iter().enumerate() {
		worksheet
			.write_string(0, col_idx as u16, field.name().as_str())
			.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
	}

	let mut current_row = 1u32;
	while let Some(batch_res) = stream.next().await {
		let batch = batch_res?;
		for row_idx in 0..batch.num_rows() {
			for (col_idx, field) in batch.schema().fields().iter().enumerate() {
				match field.data_type() {
					DataType::Utf8 => {
						let array = batch
							.column(col_idx)
							.as_any()
							.downcast_ref::<StringArray>()
							.unwrap();
						if !array.is_null(row_idx) {
							worksheet
								.write_string(current_row, col_idx as u16, array.value(row_idx))
								.map_err(|e| {
									datafusion::error::DataFusionError::External(Box::new(e))
								})?;
						}
					}
					DataType::Int64 => {
						let array = batch
							.column(col_idx)
							.as_any()
							.downcast_ref::<Int64Array>()
							.unwrap();
						if !array.is_null(row_idx) {
							worksheet
								.write_number(
									current_row,
									col_idx as u16,
									array.value(row_idx) as f64,
								)
								.map_err(|e| {
									datafusion::error::DataFusionError::External(Box::new(e))
								})?;
						}
					}
					DataType::Float64 => {
						let array = batch
							.column(col_idx)
							.as_any()
							.downcast_ref::<Float64Array>()
							.unwrap();
						if !array.is_null(row_idx) {
							worksheet
								.write_number(current_row, col_idx as u16, array.value(row_idx))
								.map_err(|e| {
									datafusion::error::DataFusionError::External(Box::new(e))
								})?;
						}
					}
					DataType::Boolean => {
						let array = batch
							.column(col_idx)
							.as_any()
							.downcast_ref::<BooleanArray>()
							.unwrap();
						if !array.is_null(row_idx) {
							worksheet
								.write_boolean(current_row, col_idx as u16, array.value(row_idx))
								.map_err(|e| {
									datafusion::error::DataFusionError::External(Box::new(e))
								})?;
						}
					}
					DataType::Date32 => {
						let array = batch
							.column(col_idx)
							.as_any()
							.downcast_ref::<Date32Array>()
							.unwrap();
						if !array.is_null(row_idx) {
							let days = array.value(row_idx);
							let excel_date = days as f64 + 25569.0;
							worksheet
								.write_with_format(
									current_row,
									col_idx as u16,
									excel_date,
									&date_format,
								)
								.map_err(|e| {
									datafusion::error::DataFusionError::External(Box::new(e))
								})?;
						}
					}
					DataType::Date64 => {
						let array = batch
							.column(col_idx)
							.as_any()
							.downcast_ref::<Date64Array>()
							.unwrap();
						if !array.is_null(row_idx) {
							let millis = array.value(row_idx);
							let days = millis as f64 / (1000.0 * 60.0 * 60.0 * 24.0);
							let excel_date = days + 25569.0;
							worksheet
								.write_with_format(
									current_row,
									col_idx as u16,
									excel_date,
									&date_format,
								)
								.map_err(|e| {
									datafusion::error::DataFusionError::External(Box::new(e))
								})?;
						}
					}
					_ => {
						let array = batch.column(col_idx);
						if !array.is_null(row_idx) {
							let value = format!("{:?}", array.slice(row_idx, 1));
							worksheet
								.write_string(current_row, col_idx as u16, &value)
								.map_err(|e| {
									datafusion::error::DataFusionError::External(Box::new(e))
								})?;
						}
					}
				}
			}
			current_row += 1;
		}
	}

	workbook
		.save(path)
		.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

	Ok(())
}

pub(crate) async fn write_empty_parquet_file(
	df: &DataFusionDataFrame,
	path: &Path,
) -> NailResult<()> {
	use parquet::arrow::ArrowWriter;

	let arrow_schema = Arc::new(df.schema().as_arrow().clone());
	let empty_arrays: Vec<Arc<dyn arrow::array::Array>> = arrow_schema
		.fields()
		.iter()
		.map(|field| {
			use arrow::array::*;
			match field.data_type() {
				DataType::Int64 => {
					Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>
				}
				DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
				DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
				DataType::Float32 => Arc::new(Float32Array::from(Vec::<f32>::new())),
				DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
				DataType::Utf8 => Arc::new(StringArray::from(Vec::<String>::new())),
				_ => Arc::new(StringArray::from(Vec::<String>::new())),
			}
		})
		.collect();

	let empty_batch = RecordBatch::try_new(arrow_schema.clone(), empty_arrays).map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::ArrowError(e, None))
	})?;

	let file = File::create(path).map_err(NailError::Io)?;
	let mut writer = ArrowWriter::try_new(file, empty_batch.schema(), None).map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
	})?;
	writer.write(&empty_batch).map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
	})?;
	writer.close().map_err(|e| {
		NailError::DataFusion(datafusion::error::DataFusionError::External(Box::new(e)))
	})?;
	Ok(())
}
