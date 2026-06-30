use clap::Args;
use std::sync::Arc;

use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::create_context_with_jobs;
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

/// Transpose materializes one output column per input row, so guard against
/// flipping a large table into an unwieldy (and slow) wide frame.
const MAX_TRANSPOSE_COLS: usize = 10_000;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail transpose matrix.parquet
  nail transpose stats.csv --header-column metric -o wide.parquet")]
pub struct TransposeArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	/// Use this column's values as the transposed column headers (instead of row_1..N).
	#[arg(
		short = 'H',
		long,
		help = "Column whose values become the new column headers"
	)]
	pub header_column: Option<String>,

	/// Name of the leading output column that holds the original column names.
	#[arg(
		long,
		default_value = "column",
		help = "Name of the leading column holding the original column names"
	)]
	pub name_column: String,
}

pub async fn execute(args: TransposeArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;

	let field_names: Vec<String> = df
		.schema()
		.fields()
		.iter()
		.map(|f| f.name().to_string())
		.collect();

	// Resolve the optional header column against the input schema up front.
	let header_col_idx = match &args.header_column {
		Some(name) => Some(field_names.iter().position(|n| n == name).ok_or_else(|| {
			NailError::ColumnNotFound(format!(
				"Column '{}' not found. Available columns: {}",
				name,
				field_names.join(", ")
			))
		})?),
		None => None,
	};

	let batches = df.collect().await?;
	let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

	if num_rows > MAX_TRANSPOSE_COLS {
		return Err(NailError::InvalidArgument(format!(
			"transpose would create {} columns (one per input row), exceeding the limit of {}. Filter or limit rows first.",
			num_rows, MAX_TRANSPOSE_COLS
		)));
	}

	// Render every cell to a string. A transposed row mixes the original column
	// types, so text is the only lossless common output type. cells[col][row].
	let opts = FormatOptions::default().with_null("");
	let mut cells: Vec<Vec<String>> = vec![Vec::with_capacity(num_rows); field_names.len()];
	for batch in &batches {
		let formatters = batch
			.columns()
			.iter()
			.map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
			.collect::<Result<Vec<_>, _>>()
			.map_err(NailError::Arrow)?;
		for row in 0..batch.num_rows() {
			for (ci, fmt) in formatters.iter().enumerate() {
				cells[ci].push(fmt.value(row).to_string());
			}
		}
	}

	// New column headers: from the chosen header column's values, or row_1..N.
	let mut headers: Vec<String> = match header_col_idx {
		Some(idx) => cells[idx].clone(),
		None => (1..=num_rows).map(|i| format!("row_{}", i)).collect(),
	};
	dedupe_names(&mut headers);

	// Body = every original column except the one consumed as the header.
	let body_cols: Vec<usize> = (0..field_names.len())
		.filter(|i| Some(*i) != header_col_idx)
		.collect();

	// Column 0 holds the original field names; columns 1..=num_rows hold each
	// original row's values laid out vertically.
	let name_array = StringArray::from(
		body_cols
			.iter()
			.map(|&i| field_names[i].clone())
			.collect::<Vec<_>>(),
	);
	let mut out_arrays: Vec<ArrayRef> = vec![Arc::new(name_array)];
	let mut out_fields: Vec<Field> =
		vec![Field::new(args.name_column.as_str(), DataType::Utf8, false)];

	for (row, header) in headers.iter().enumerate() {
		let col_values: Vec<String> = body_cols.iter().map(|&ci| cells[ci][row].clone()).collect();
		out_arrays.push(Arc::new(StringArray::from(col_values)));
		out_fields.push(Field::new(header.as_str(), DataType::Utf8, true));
	}

	let out_schema = Arc::new(Schema::new(out_fields));
	let batch = RecordBatch::try_new(out_schema, out_arrays).map_err(NailError::Arrow)?;

	let ctx = create_context_with_jobs(args.common.jobs).await?;
	let result_df = ctx.read_batch(batch).map_err(NailError::DataFusion)?;

	args.common
		.log_if_verbose(&format!("Transposed into {} columns", num_rows));

	let output_handler = OutputHandler::new(&args.common);
	output_handler
		.handle_output(&result_df, "transpose")
		.await?;

	Ok(())
}

/// Make generated column names unique by suffixing collisions with `_1`, `_2`, …
fn dedupe_names(names: &mut [String]) {
	let mut seen = std::collections::HashSet::new();
	for n in names.iter_mut() {
		let base = n.clone();
		let mut candidate = base.clone();
		let mut k = 1usize;
		while !seen.insert(candidate.clone()) {
			candidate = format!("{}_{}", base, k);
			k += 1;
		}
		*n = candidate;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	fn common(input: &str) -> CommonArgs {
		CommonArgs {
			input: PathBuf::from(input),
			output: None,
			format: None,
			random: None,
			batch_size: None,
			jobs: None,
			table: false,
			level: 1,
			verbose: false,
		}
	}

	#[test]
	fn test_dedupe_names() {
		let mut names = vec![
			"a".to_string(),
			"a".to_string(),
			"b".to_string(),
			"a".to_string(),
		];
		dedupe_names(&mut names);
		assert_eq!(names, vec!["a", "a_1", "b", "a_2"]);
	}

	#[test]
	fn test_transpose_args() {
		let args = TransposeArgs {
			common: common("data.parquet"),
			header_column: Some("metric".to_string()),
			name_column: "column".to_string(),
		};
		assert_eq!(args.header_column.as_deref(), Some("metric"));
		assert_eq!(args.name_column, "column");
	}
}
