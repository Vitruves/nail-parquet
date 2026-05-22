use crate::cli::CommonArgs;
use crate::error::NailResult;
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use crate::utils::stats::select_columns_by_pattern;
use arrow::array::Array;
use clap::Args;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::prelude::*;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const BORDER_COLOR: &str = "\x1b[90m";
const FIELD_COLORS: [&str; 6] = [
	"\x1b[92m", // Green
	"\x1b[93m", // Yellow
	"\x1b[94m", // Blue
	"\x1b[95m", // Magenta
	"\x1b[96m", // Cyan
	"\x1b[97m", // White
];

#[derive(Args)]
#[command(after_help = "Examples:
  nail frequency data.parquet -c category
  nail frequency logs.csv -c status --head 10 -o -")]
pub struct FrequencyArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Comma-separated column names to analyze")]
	pub columns: String,

	#[arg(long, help = "Show only the top N most frequent entries")]
	pub head: Option<usize>,

	#[arg(long, help = "Show only the bottom N least frequent entries")]
	pub tail: Option<usize>,
}

pub async fn execute(args: FrequencyArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));
	args.common.log_if_verbose(&format!(
		"Analyzing frequency for columns: {}",
		args.columns
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;

	// Parse and resolve column names using the standard utility
	let schema = df.schema().clone().into();
	let resolved_column_names = select_columns_by_pattern(schema, &args.columns)?;

	if resolved_column_names.is_empty() {
		return Err(crate::error::NailError::InvalidArgument(
			"No column names provided".to_string(),
		));
	}

	args.common.log_if_verbose(&format!(
		"Computing frequency table for {} column(s)",
		resolved_column_names.len()
	));

	// Build the frequency query using resolved column names
	let mut group_by_cols = Vec::new();

	for col_name in &resolved_column_names {
		// Use qualified column name to avoid case sensitivity issues on Linux
		group_by_cols.push(col(format!("\"{}\"", col_name)));
	}

	// Execute the frequency query
	let frequency_df = df
		.aggregate(group_by_cols, vec![count(lit(1)).alias("frequency")])?
		.sort(vec![
			// Sort by frequency descending
			col("frequency").sort(false, true),
		])?;

	// Calculate sum of all frequencies for percentage calculation (before head/tail filtering)
	let sum_freq_df = frequency_df.clone().aggregate(
		vec![],
		vec![
			datafusion::functions_aggregate::expr_fn::sum(col("frequency"))
				.alias("total_frequency"),
		],
	)?;

	// Apply --head or --tail limit
	let frequency_df = if let Some(n) = args.tail {
		// Reverse sort (ascending) to get least frequent, take N, then re-sort descending
		frequency_df
			.sort(vec![col("frequency").sort(true, true)])?
			.limit(0, Some(n))?
			.sort(vec![col("frequency").sort(false, true)])?
	} else if let Some(n) = args.head {
		frequency_df.limit(0, Some(n))?
	} else {
		frequency_df
	};

	let sum_batches = sum_freq_df.collect().await?;
	let total_frequency: i64 = if !sum_batches.is_empty() && sum_batches[0].num_rows() > 0 {
		if let Some(array) = sum_batches[0].column_by_name("total_frequency") {
			if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
				int_array.value(0)
			} else {
				0
			}
		} else {
			0
		}
	} else {
		0
	};

	// Display results — add percentage column for table/file output
	if args.common.output.is_some() || args.common.format.is_some() || args.common.table {
		let frequency_df = if total_frequency > 0 {
			frequency_df.with_column(
				"%",
				datafusion::prelude::round(vec![
					cast(col("frequency"), DataType::Float64) / lit(total_frequency as f64)
						* lit(100.0),
					lit(1),
				]),
			)?
		} else {
			frequency_df
		};
		let output_handler = OutputHandler::new(&args.common);
		output_handler
			.handle_output(&frequency_df, "frequency")
			.await?;
	} else {
		// Display to console with condensed format including percentages
		display_frequency_table(&frequency_df, &resolved_column_names, total_frequency).await?;
	}

	Ok(())
}

async fn display_frequency_table(
	df: &DataFrame,
	column_names: &[String],
	total_frequency: i64,
) -> NailResult<()> {
	let batches = df.clone().collect().await?;

	if batches.is_empty() {
		println!("No frequency data to display");
		return Ok(());
	}

	// Get terminal width for proper formatting
	let terminal_width = if let Some((w, _)) = term_size::dimensions() {
		w.clamp(60, 200)
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

	println!(
		"{}┌{}{}{}{}",
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

				display_parts.push(format!(
					"{}{}{}: {}{}{}",
					field_color, col_name, RESET, field_color, value, RESET
				));
			}

			// Add frequency with special highlighting and percentage
			let freq_column = batch.column_by_name("frequency").unwrap();
			let frequency_str = format_array_value(freq_column, row_idx);

			// Calculate percentage
			let frequency_value: i64 = frequency_str.parse().unwrap_or(0);
			let percentage = if total_frequency > 0 {
				(frequency_value as f64 / total_frequency as f64) * 100.0
			} else {
				0.0
			};

			display_parts.push(format!(
				"{}frequency{}: {}{}{} {}({:.1}%){}",
				"\x1b[93m", RESET, BOLD, "\x1b[93m", frequency_str, "\x1b[2;37m", percentage, RESET
			));

			// Join all parts with comma separation
			let content = display_parts.join(", ");

			// Print the frequency entry within the card
			println!("{}│{} {}", BORDER_COLOR, RESET, content);
		}
	}

	// Print card footer
	println!("{}│{}", BORDER_COLOR, RESET);
	println!("{}└{}{}", BORDER_COLOR, "─".repeat(header_width), RESET);

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
			}
			DataType::Int64 => {
				if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
					array.value(row_idx).to_string()
				} else {
					"0".to_string()
				}
			}
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
			}
			DataType::Int32 => {
				if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
					array.value(row_idx).to_string()
				} else {
					"0".to_string()
				}
			}
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
			}
			DataType::Boolean => {
				if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
					array.value(row_idx).to_string()
				} else {
					"false".to_string()
				}
			}
			DataType::Date32 => {
				if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
					let days_since_epoch = array.value(row_idx);
					let date =
						chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
							.unwrap_or_else(|| {
								chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default()
							});
					date.format("%Y-%m-%d").to_string()
				} else {
					"1970-01-01".to_string()
				}
			}
			DataType::Date64 => {
				if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
					let millis_since_epoch = array.value(row_idx);
					let datetime = chrono::DateTime::from_timestamp_millis(millis_since_epoch)
						.unwrap_or_else(|| {
							chrono::DateTime::from_timestamp(0, 0)
								.unwrap_or(chrono::DateTime::UNIX_EPOCH)
						});
					datetime.format("%Y-%m-%d").to_string()
				} else {
					"1970-01-01".to_string()
				}
			}
			DataType::Timestamp(_, _) => "timestamp".to_string(),
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
			}
		}
	}
}
