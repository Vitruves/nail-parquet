use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::output::OutputHandler;
use crate::utils::{create_context_with_jobs, io::read_data_with_opts};
use clap::Args;
use datafusion::arrow::array::ArrayRef;
use datafusion::prelude::*;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail pivot sales.parquet --index date --columns region --values revenue --agg sum
  nail pivot data.csv --index user --columns event --values count --agg sum -o -")]
pub struct PivotArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	/// Row index columns (comma-separated)
	#[arg(short, long, help = "Row index columns (comma-separated)")]
	pub index: String,

	/// Column pivot columns (comma-separated)
	#[arg(short, long, help = "Column pivot columns (comma-separated)")]
	pub columns: String,

	/// Value columns to aggregate (comma-separated)
	#[arg(
		short = 'l',
		long = "values",
		help = "Value columns to aggregate (comma-separated)"
	)]
	pub values: Option<String>,

	/// Aggregation function
	#[arg(short, long, default_value = "sum", help = "Aggregation function")]
	#[arg(value_enum)]
	pub agg: AggregationFunction,

	/// Fill missing values
	#[arg(long, default_value = "0", help = "Fill missing values")]
	pub fill: String,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum AggregationFunction {
	Sum,
	Mean,
	Count,
	Min,
	Max,
}

impl AggregationFunction {
	/// SQL aggregate function name used when building the pivot query.
	fn sql_function(&self) -> &'static str {
		match self {
			AggregationFunction::Sum => "sum",
			AggregationFunction::Mean => "avg",
			AggregationFunction::Count => "count",
			AggregationFunction::Min => "min",
			AggregationFunction::Max => "max",
		}
	}
}

pub async fn execute(args: PivotArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	// Read input data
	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;

	// Parse columns
	let index_cols: Vec<&str> = args.index.split(',').map(|s| s.trim()).collect();
	let pivot_cols: Vec<&str> = args.columns.split(',').map(|s| s.trim()).collect();

	// Validate columns exist
	let temp_df = df.clone();
	let schema = temp_df.schema();
	for col in index_cols.iter().chain(pivot_cols.iter()) {
		if schema.field_with_name(None, col).is_err() {
			let available_cols: Vec<String> = schema
				.fields()
				.iter()
				.map(|f| f.name().to_string())
				.collect();
			return Err(NailError::ColumnNotFound(format!(
				"Column '{}' not found. Available columns: {}",
				col,
				available_cols.join(", ")
			)));
		}
	}

	// Determine value columns
	let value_cols: Vec<&str> = if let Some(values_str) = &args.values {
		let cols: Vec<&str> = values_str.split(',').map(|s| s.trim()).collect();
		// Validate value columns exist and are numeric
		for col in &cols {
			match schema.field_with_name(None, col) {
				Ok(field) => match field.data_type() {
					datafusion::arrow::datatypes::DataType::Int8
					| datafusion::arrow::datatypes::DataType::Int16
					| datafusion::arrow::datatypes::DataType::Int32
					| datafusion::arrow::datatypes::DataType::Int64
					| datafusion::arrow::datatypes::DataType::UInt8
					| datafusion::arrow::datatypes::DataType::UInt16
					| datafusion::arrow::datatypes::DataType::UInt32
					| datafusion::arrow::datatypes::DataType::UInt64
					| datafusion::arrow::datatypes::DataType::Float32
					| datafusion::arrow::datatypes::DataType::Float64 => {}
					_ => {
						return Err(NailError::InvalidArgument(format!(
							"Value column '{}' must be numeric (type: {:?})",
							col,
							field.data_type()
						)));
					}
				},
				Err(_) => {
					let available_cols: Vec<String> = schema
						.fields()
						.iter()
						.map(|f| f.name().to_string())
						.collect();
					return Err(NailError::ColumnNotFound(format!(
						"Column '{}' not found. Available columns: {}",
						col,
						available_cols.join(", ")
					)));
				}
			}
		}
		cols
	} else {
		// If no value columns specified, find all numeric columns not in index or pivot columns
		schema
			.fields()
			.iter()
			.filter(|field| {
				let name = field.name();
				!index_cols.contains(&name.as_str())
					&& !pivot_cols.contains(&name.as_str())
					&& matches!(
						field.data_type(),
						datafusion::arrow::datatypes::DataType::Int8
							| datafusion::arrow::datatypes::DataType::Int16
							| datafusion::arrow::datatypes::DataType::Int32
							| datafusion::arrow::datatypes::DataType::Int64
							| datafusion::arrow::datatypes::DataType::UInt8
							| datafusion::arrow::datatypes::DataType::UInt16
							| datafusion::arrow::datatypes::DataType::UInt32
							| datafusion::arrow::datatypes::DataType::UInt64
							| datafusion::arrow::datatypes::DataType::Float32
							| datafusion::arrow::datatypes::DataType::Float64
					)
			})
			.map(|field| field.name().as_str())
			.collect::<Vec<_>>()
			.into_iter()
			.collect()
	};

	if value_cols.is_empty() {
		return Err(NailError::InvalidArgument(
            "No numeric value columns found to aggregate. Please specify value columns with --values".to_string()
        ));
	}

	args.common
		.log_if_verbose(&format!("Index columns: {:?}", index_cols));
	args.common
		.log_if_verbose(&format!("Pivot columns: {:?}", pivot_cols));
	args.common
		.log_if_verbose(&format!("Value columns: {:?}", value_cols));
	args.common
		.log_if_verbose(&format!("Aggregation: {:?}", args.agg));

	// Spread the pivot columns into one aggregated column per distinct key.
	let result_df = create_pivot_table(
		&df,
		&index_cols,
		&pivot_cols,
		&value_cols,
		&args.agg,
		&args.fill,
		args.common.jobs,
	)
	.await?;

	// Display or write the results
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "pivot").await?;

	Ok(())
}

async fn create_pivot_table(
	df: &DataFrame,
	index_cols: &[&str],
	pivot_cols: &[&str],
	value_cols: &[&str],
	agg: &AggregationFunction,
	fill_value: &str,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = create_context_with_jobs(jobs).await?;
	let src = "__nail_pivot_src";
	ctx.register_table(src, df.clone().into_view())?;

	// 1. Find the distinct combinations of pivot-key values that actually occur.
	//    NULL keys are dropped, matching conventional pivot/crosstab semantics.
	let pivot_select = pivot_cols
		.iter()
		.map(|c| quote_ident(c))
		.collect::<Vec<_>>()
		.join(", ");
	let not_null = pivot_cols
		.iter()
		.map(|c| format!("{} IS NOT NULL", quote_ident(c)))
		.collect::<Vec<_>>()
		.join(" AND ");
	let combos_sql = format!(
		"SELECT DISTINCT {sel} FROM {src} WHERE {nn} ORDER BY {sel}",
		sel = pivot_select,
		nn = not_null,
		src = src,
	);
	let combo_batches = ctx.sql(&combos_sql).await?.collect().await?;

	// Each combination becomes (match predicate, human-readable label).
	let mut combos: Vec<(String, String)> = Vec::new();
	for batch in &combo_batches {
		for row in 0..batch.num_rows() {
			let mut preds = Vec::with_capacity(pivot_cols.len());
			let mut label_parts = Vec::with_capacity(pivot_cols.len());
			for (ci, pcol) in pivot_cols.iter().enumerate() {
				let (lit_sql, label) = sql_literal_and_label(batch.column(ci), row)?;
				preds.push(format!("{} = {}", quote_ident(pcol), lit_sql));
				label_parts.push(label);
			}
			combos.push((preds.join(" AND "), label_parts.join("_")));
		}
	}

	if combos.is_empty() {
		return Err(NailError::InvalidArgument(
			"No non-null pivot key values found to spread into columns".to_string(),
		));
	}

	// 2. Build one aggregated column per (value column × pivot key).
	let agg_fn = agg.sql_function();
	let multi_value = value_cols.len() > 1;
	let mut select_parts: Vec<String> = index_cols.iter().map(|c| quote_ident(c)).collect();
	let mut seen_names = std::collections::HashSet::new();
	for &vcol in value_cols {
		for (pred, label) in &combos {
			let mut col_name = if multi_value {
				format!("{}_{}", vcol, label)
			} else {
				label.clone()
			};
			// Guard against collisions (e.g. a pivot label equal to an index name).
			while !seen_names.insert(col_name.clone()) {
				col_name.push('_');
			}
			let inner = format!(
				"{}(CASE WHEN {} THEN {} END)",
				agg_fn,
				pred,
				quote_ident(vcol)
			);
			let expr = apply_fill(&inner, fill_value);
			select_parts.push(format!("{} AS {}", expr, quote_ident(&col_name)));
		}
	}

	let group_by = index_cols
		.iter()
		.map(|c| quote_ident(c))
		.collect::<Vec<_>>()
		.join(", ");
	let sql = format!(
		"SELECT {select} FROM {src} GROUP BY {group} ORDER BY {group}",
		select = select_parts.join(", "),
		src = src,
		group = group_by,
	);

	let result = ctx.sql(&sql).await?;
	Ok(result)
}

/// Quote a SQL identifier, escaping embedded double quotes.
fn quote_ident(name: &str) -> String {
	format!("\"{}\"", name.replace('"', "\"\""))
}

/// Wrap a pivoted aggregate in COALESCE so empty cells take the fill value.
/// `null`/`none`/`nan`/empty leaves cells NULL; numeric fills inject unquoted,
/// anything else is treated as a string literal.
fn apply_fill(inner: &str, fill: &str) -> String {
	let f = fill.trim();
	if f.is_empty()
		|| f.eq_ignore_ascii_case("null")
		|| f.eq_ignore_ascii_case("none")
		|| f.eq_ignore_ascii_case("nan")
	{
		return inner.to_string();
	}
	if f.parse::<i64>().is_ok() || f.parse::<f64>().is_ok() {
		format!("COALESCE({}, {})", inner, f)
	} else {
		format!("COALESCE({}, '{}')", inner, f.replace('\'', "''"))
	}
}

/// Render a single array cell as a typed SQL literal (for the CASE predicate)
/// plus a display label (for the generated column name).
fn sql_literal_and_label(array: &ArrayRef, row: usize) -> NailResult<(String, String)> {
	use datafusion::arrow::array::*;
	use datafusion::arrow::datatypes::DataType;

	if array.is_null(row) {
		return Ok(("NULL".to_string(), "null".to_string()));
	}

	macro_rules! num {
		($ty:ty) => {{
			let a = array.as_any().downcast_ref::<$ty>().unwrap();
			let v = a.value(row).to_string();
			(v.clone(), v)
		}};
	}

	let pair = match array.data_type() {
		DataType::Utf8 => {
			let v = array
				.as_any()
				.downcast_ref::<StringArray>()
				.unwrap()
				.value(row);
			(format!("'{}'", v.replace('\'', "''")), v.to_string())
		}
		DataType::LargeUtf8 => {
			let v = array
				.as_any()
				.downcast_ref::<LargeStringArray>()
				.unwrap()
				.value(row);
			(format!("'{}'", v.replace('\'', "''")), v.to_string())
		}
		DataType::Boolean => {
			let v = array
				.as_any()
				.downcast_ref::<BooleanArray>()
				.unwrap()
				.value(row);
			(v.to_string(), v.to_string())
		}
		DataType::Int8 => num!(Int8Array),
		DataType::Int16 => num!(Int16Array),
		DataType::Int32 => num!(Int32Array),
		DataType::Int64 => num!(Int64Array),
		DataType::UInt8 => num!(UInt8Array),
		DataType::UInt16 => num!(UInt16Array),
		DataType::UInt32 => num!(UInt32Array),
		DataType::UInt64 => num!(UInt64Array),
		DataType::Float32 => num!(Float32Array),
		DataType::Float64 => num!(Float64Array),
		_ => {
			// Fallback: stringify via Arrow's formatter and treat as text.
			use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
			let fmt = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
				.map_err(NailError::Arrow)?;
			let s = fmt.value(row).to_string();
			(format!("'{}'", s.replace('\'', "''")), s)
		}
	};
	Ok(pair)
}

impl std::fmt::Display for AggregationFunction {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			AggregationFunction::Sum => write!(f, "Sum"),
			AggregationFunction::Mean => write!(f, "Mean"),
			AggregationFunction::Count => write!(f, "Count"),
			AggregationFunction::Min => write!(f, "Min"),
			AggregationFunction::Max => write!(f, "Max"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_pivot_args_parsing() {
		let args = PivotArgs {
			common: CommonArgs {
				input: PathBuf::from("data.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			index: "category".to_string(),
			columns: "month".to_string(),
			values: Some("sales".to_string()),
			agg: AggregationFunction::Sum,
			fill: "0".to_string(),
		};

		assert_eq!(args.index, "category");
		assert_eq!(args.columns, "month");
		assert_eq!(args.values, Some("sales".to_string()));
		assert!(matches!(args.agg, AggregationFunction::Sum));
		assert_eq!(args.fill, "0");
	}

	#[test]
	fn test_pivot_args_with_multiple_columns() {
		let args = PivotArgs {
			common: CommonArgs {
				input: PathBuf::from("sales.csv"),
				output: Some(PathBuf::from("pivot.parquet")),
				format: Some(crate::cli::OutputFormat::Parquet),
				random: Some(456),
				batch_size: None,
				jobs: Some(4),
				table: false,
				level: 1,
				verbose: true,
			},
			index: "region,product".to_string(),
			columns: "quarter,year".to_string(),
			values: Some("revenue,units".to_string()),
			agg: AggregationFunction::Mean,
			fill: "null".to_string(),
		};

		assert_eq!(args.index, "region,product");
		assert_eq!(args.columns, "quarter,year");
		assert_eq!(args.values, Some("revenue,units".to_string()));
		assert!(matches!(args.agg, AggregationFunction::Mean));
		assert_eq!(args.fill, "null");
		assert_eq!(args.common.jobs, Some(4));
		assert!(args.common.verbose);
	}

	#[test]
	fn test_pivot_args_with_count_aggregation() {
		let args = PivotArgs {
			common: CommonArgs {
				input: PathBuf::from("events.json"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			index: "user_id".to_string(),
			columns: "event_type".to_string(),
			values: None,
			agg: AggregationFunction::Count,
			fill: "0".to_string(),
		};

		assert_eq!(args.index, "user_id");
		assert_eq!(args.columns, "event_type");
		assert_eq!(args.values, None);
		assert!(matches!(args.agg, AggregationFunction::Count));
		assert_eq!(args.fill, "0");
	}

	#[test]
	fn test_pivot_args_with_min_max_aggregation() {
		let args_min = PivotArgs {
			common: CommonArgs {
				input: PathBuf::from("temperature.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			index: "location".to_string(),
			columns: "month".to_string(),
			values: Some("temperature".to_string()),
			agg: AggregationFunction::Min,
			fill: "-999".to_string(),
		};

		let args_max = PivotArgs {
			common: CommonArgs {
				input: PathBuf::from("temperature.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			index: "location".to_string(),
			columns: "month".to_string(),
			values: Some("temperature".to_string()),
			agg: AggregationFunction::Max,
			fill: "-999".to_string(),
		};

		assert!(matches!(args_min.agg, AggregationFunction::Min));
		assert!(matches!(args_max.agg, AggregationFunction::Max));
		assert_eq!(args_min.fill, "-999");
		assert_eq!(args_max.fill, "-999");
	}

	#[test]
	fn test_aggregation_function_debug() {
		let sum_func = AggregationFunction::Sum;
		let mean_func = AggregationFunction::Mean;
		let count_func = AggregationFunction::Count;
		let min_func = AggregationFunction::Min;
		let max_func = AggregationFunction::Max;

		assert_eq!(format!("{:?}", sum_func), "Sum");
		assert_eq!(format!("{:?}", mean_func), "Mean");
		assert_eq!(format!("{:?}", count_func), "Count");
		assert_eq!(format!("{:?}", min_func), "Min");
		assert_eq!(format!("{:?}", max_func), "Max");
	}

	#[test]
	fn test_pivot_args_clone() {
		let args = PivotArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			index: "category".to_string(),
			columns: "month".to_string(),
			values: Some("sales".to_string()),
			agg: AggregationFunction::Sum,
			fill: "0".to_string(),
		};

		let cloned = args.clone();
		assert_eq!(args.index, cloned.index);
		assert_eq!(args.columns, cloned.columns);
		assert_eq!(args.values, cloned.values);
		assert_eq!(args.fill, cloned.fill);
		assert!(matches!(cloned.agg, AggregationFunction::Sum));
	}

	#[test]
	fn test_pivot_args_parsing_columns() {
		let args = PivotArgs {
			common: CommonArgs {
				input: PathBuf::from("test.parquet"),
				output: None,
				format: None,
				random: None,
				batch_size: None,
				jobs: None,
				table: false,
				level: 1,
				verbose: false,
			},
			index: "col_a,col_b,col_c".to_string(),
			columns: "pivot_col".to_string(),
			values: Some("value1,value2".to_string()),
			agg: AggregationFunction::Sum,
			fill: "0".to_string(),
		};

		let index_cols: Vec<&str> = args.index.split(',').map(|s| s.trim()).collect();
		let value_cols: Vec<&str> = args
			.values
			.as_ref()
			.unwrap()
			.split(',')
			.map(|s| s.trim())
			.collect();

		assert_eq!(index_cols, vec!["col_a", "col_b", "col_c"]);
		assert_eq!(value_cols, vec!["value1", "value2"]);
	}
}
