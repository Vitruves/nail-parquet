use crate::cli::OutputFormat;
use crate::error::NailResult;
use crate::utils::io::write_data;
use crate::utils::FileFormat;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use std::path::Path;

// ANSI color codes
const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";

const NULL_COLOR: &str = "\x1b[2;37m"; // Dim white
const BORDER_COLOR: &str = "\x1b[90m"; // Gray

// Bat-style field colors (cycling through different colors for variety)
const FIELD_COLORS: &[&str] = &[
	"\x1b[32m", // Green
	"\x1b[33m", // Yellow
	"\x1b[34m", // Blue
	"\x1b[35m", // Magenta
	"\x1b[36m", // Cyan
	"\x1b[91m", // Bright red
	"\x1b[92m", // Bright green
	"\x1b[93m", // Bright yellow
	"\x1b[94m", // Bright blue
	"\x1b[95m", // Bright magenta
	"\x1b[96m", // Bright cyan
	"\x1b[31m", // Red
];

pub async fn display_dataframe_with_mode(
	df: &DataFrame,
	output_path: Option<&Path>,
	format: Option<&OutputFormat>,
	table_mode: bool,
) -> NailResult<()> {
	match output_path {
		Some(path) => {
			let file_format = match format {
				Some(OutputFormat::Json) => Some(FileFormat::Json),
				Some(OutputFormat::Csv) => Some(FileFormat::Csv),
				Some(OutputFormat::Parquet) => Some(FileFormat::Parquet),
				Some(OutputFormat::Xlsx) => Some(FileFormat::Excel),
				Some(OutputFormat::Text) | None => {
					match path.extension().and_then(|s| s.to_str()) {
						Some("json") => Some(FileFormat::Json),
						Some("csv") => Some(FileFormat::Csv),
						Some("parquet") => Some(FileFormat::Parquet),
						Some("xlsx") => Some(FileFormat::Excel),
						_ => Some(FileFormat::Parquet),
					}
				}
			};

			write_data(df, path, file_format.as_ref()).await
		}
		None => {
			match format {
				Some(OutputFormat::Json) => {
					display_as_json(df).await?;
				}
				Some(OutputFormat::Text) | None => {
					if table_mode {
						display_as_columnar_table(df).await?;
					} else if is_correlation_matrix(df) {
						display_correlation_matrix(df).await?;
					} else {
						display_as_table(df).await?;
					}
				}
				_ => {
					return Err(crate::error::NailError::InvalidArgument(
						"CSV and Parquet formats require an output file".to_string(),
					));
				}
			}

			Ok(())
		}
	}
}

async fn display_as_json(df: &DataFrame) -> NailResult<()> {
	let batches = df.clone().collect().await?;
	let schema = df.schema();

	println!("[");
	let mut first_record = true;

	for batch in &batches {
		for row_idx in 0..batch.num_rows() {
			if !first_record {
				println!(",");
			}
			first_record = false;

			print!("  {{");
			let mut first_field = true;

			for (col_idx, field) in schema.fields().iter().enumerate() {
				if !first_field {
					print!(", ");
				}
				first_field = false;

				let column = batch.column(col_idx);
				let value = format_json_value(column, row_idx, field.data_type());
				print!("\"{}\": {}", field.name(), value);
			}
			print!("}}");
		}
	}

	println!("\n]");
	Ok(())
}

async fn display_as_table(df: &DataFrame) -> NailResult<()> {
	let batches = df.clone().collect().await?;
	let schema = df.schema();

	if batches.is_empty() {
		println!("{}No data to display{}", DIM, RESET);
		return Ok(());
	}

	// Get terminal width for proper wrapping
	let terminal_width = if let Some((w, _)) = term_size::dimensions() {
		w.clamp(60, 200)
	} else {
		120
	};

	// Calculate available width for content (accounting for borders and field name)
	let field_name_width = 20;
	let border_width = 4; // "│ " + " : "
	let content_width = terminal_width.saturating_sub(field_name_width + border_width + 2);
	let header_width = terminal_width.saturating_sub(4); // Account for "┌─ " and " ─"

	// Print data in card format
	let mut row_count = 0;
	for batch in &batches {
		for row_idx in 0..batch.num_rows() {
			row_count += 1;

			// Card header with dynamic width
			let record_text = format!(" Record {} ", row_count);
			let remaining_width = header_width.saturating_sub(record_text.len());
			let left_dashes = remaining_width / 2;
			let right_dashes = remaining_width - left_dashes;

			println!(
				"{}┌{}{}{}{}",
				BORDER_COLOR,
				"─".repeat(left_dashes),
				record_text,
				"─".repeat(right_dashes),
				RESET
			);
			println!("{}│{}", BORDER_COLOR, RESET);

			// Print each field as a key-value pair
			for (col_idx, field) in schema.fields().iter().enumerate() {
				let column = batch.column(col_idx);
				let field_color = FIELD_COLORS[col_idx % FIELD_COLORS.len()];
				let value = format_cell_value_with_field_color(
					column,
					row_idx,
					field.data_type(),
					field_color,
				);

				// Format field name with the same color as the value
				let field_name = format!(
					"{}{}{:<width$}{}",
					BOLD,
					field_color,
					field.name(),
					RESET,
					width = field_name_width
				);

				// Handle long values by wrapping them properly
				let wrapped_value = wrap_text_with_color(&value, content_width, field_color);
				let lines: Vec<&str> = wrapped_value.lines().collect();

				if lines.len() == 1 {
					println!("{}│{} {} : {}", BORDER_COLOR, RESET, field_name, lines[0]);
				} else {
					println!("{}│{} {} : {}", BORDER_COLOR, RESET, field_name, lines[0]);
					for line in &lines[1..] {
						println!(
							"{}│{} {:<width$} : {}",
							BORDER_COLOR,
							RESET,
							"",
							line,
							width = field_name_width
						);
					}
				}
			}

			// Card footer with dynamic width
			println!("{}│{}", BORDER_COLOR, RESET);
			println!(
				"{}└{}{}",
				BORDER_COLOR,
				"─".repeat(terminal_width.saturating_sub(2)),
				RESET
			);

			// Add spacing between records
			if row_count < batches.iter().map(|b| b.num_rows()).sum::<usize>() {
				println!();
			}
		}
	}

	// Print summary
	println!("{}Total records: {}{}{}", DIM, BOLD, row_count, RESET);

	Ok(())
}

async fn display_as_columnar_table(df: &DataFrame) -> NailResult<()> {
	let batches = df.clone().collect().await?;
	let schema = df.schema();

	if batches.is_empty() {
		println!("{}No data to display{}", DIM, RESET);
		return Ok(());
	}

	let fields = schema.fields();
	let num_cols = fields.len();

	// Collect all cell values as plain strings (no ANSI)
	let mut columns_values: Vec<Vec<String>> = vec![Vec::new(); num_cols];
	for batch in &batches {
		for row_idx in 0..batch.num_rows() {
			for (col_idx, field) in fields.iter().enumerate() {
				let column = batch.column(col_idx);
				let val = format_cell_value_plain(column, row_idx, field.data_type());
				columns_values[col_idx].push(val);
			}
		}
	}

	// Calculate column widths (header name vs max cell width)
	let mut col_widths: Vec<usize> = Vec::with_capacity(num_cols);
	for (col_idx, field) in fields.iter().enumerate() {
		let header_len = field.name().len();
		let max_cell = columns_values[col_idx]
			.iter()
			.map(|v| v.len())
			.max()
			.unwrap_or(0);
		col_widths.push(header_len.max(max_cell).max(3)); // minimum 3 chars
	}

	// Get terminal width to potentially truncate
	let terminal_width = if let Some((w, _)) = term_size::dimensions() {
		w.max(60)
	} else {
		120
	};

	// Build the separator and header lines
	let sep_parts: Vec<String> = col_widths.iter().map(|w| "─".repeat(w + 2)).collect();
	let top_border = format!("{}┌{}┐{}", BORDER_COLOR, sep_parts.join("┬"), RESET);
	let mid_border = format!("{}├{}┤{}", BORDER_COLOR, sep_parts.join("┼"), RESET);
	let bot_border = format!("{}└{}┘{}", BORDER_COLOR, sep_parts.join("┴"), RESET);

	// Header row
	let header_cells: Vec<String> = fields
		.iter()
		.enumerate()
		.map(|(col_idx, field)| {
			let color = FIELD_COLORS[col_idx % FIELD_COLORS.len()];
			format!(
				" {}{}{:<width$}{} ",
				BOLD,
				color,
				field.name(),
				RESET,
				width = col_widths[col_idx]
			)
		})
		.collect();
	let header_line = format!(
		"{}│{}{}│{}",
		BORDER_COLOR,
		RESET,
		header_cells.join(&format!("{}│{}", BORDER_COLOR, RESET)),
		BORDER_COLOR,
	);

	// Truncate display to terminal width if needed
	let print_line = |line: &str| {
		let visible_len = strip_ansi_codes(line).len();
		if visible_len > terminal_width {
			// Simple truncation — print as-is and let the terminal handle it
			println!("{}", line);
		} else {
			println!("{}", line);
		}
	};

	print_line(&top_border);
	print_line(&header_line);
	print_line(&mid_border);

	// Data rows
	let total_rows = columns_values.first().map(|c| c.len()).unwrap_or(0);
	#[allow(clippy::needless_range_loop)]
	for row_idx in 0..total_rows {
		let row_cells: Vec<String> = (0..num_cols)
			.map(|col_idx| {
				let color = FIELD_COLORS[col_idx % FIELD_COLORS.len()];
				let val = &columns_values[col_idx][row_idx];
				if val == "NULL" {
					format!(
						" {}{:<width$}{} ",
						NULL_COLOR,
						val,
						RESET,
						width = col_widths[col_idx]
					)
				} else {
					format!(
						" {}{:<width$}{} ",
						color,
						val,
						RESET,
						width = col_widths[col_idx]
					)
				}
			})
			.collect();
		let row_line = format!(
			"{}│{}{}│{}",
			BORDER_COLOR,
			RESET,
			row_cells.join(&format!("{}│{}", BORDER_COLOR, RESET)),
			BORDER_COLOR
		);
		print_line(&row_line);
	}

	print_line(&bot_border);
	println!("{}Total records: {}{}{}", DIM, BOLD, total_rows, RESET);

	Ok(())
}

fn format_cell_value_plain(column: &dyn Array, row_idx: usize, data_type: &DataType) -> String {
	if column.is_null(row_idx) {
		"NULL".to_string()
	} else {
		match data_type {
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
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::Float64 => {
				if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
					let val = array.value(row_idx);
					if val.abs() < 0.001 && val != 0.0 {
						format!("{:.2e}", val)
					} else {
						format_float_trimmed(val)
					}
				} else {
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::Int32 => {
				if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
					array.value(row_idx).to_string()
				} else {
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::UInt64 => {
				if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
					array.value(row_idx).to_string()
				} else {
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::Float32 => {
				if let Some(array) = column.as_any().downcast_ref::<Float32Array>() {
					format!("{:.2}", array.value(row_idx))
				} else {
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
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
			_ => format!("{:?}", column.slice(row_idx, 1))
				.lines()
				.next()
				.unwrap_or("unknown")
				.trim_start_matches('[')
				.trim_end_matches(']')
				.trim_start_matches("\"")
				.trim_end_matches("\"")
				.trim()
				.to_string(),
		}
	}
}

fn wrap_text_with_color(text: &str, max_width: usize, field_color: &str) -> String {
	let clean_text = strip_ansi_codes(text);

	// First, normalize the text by replacing all newlines with spaces
	// This handles cases where the original data has embedded newlines
	let normalized_text = clean_text.replace(['\n', '\r'], " ");

	// Remove multiple consecutive spaces
	let normalized_text = normalized_text
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ");

	if normalized_text.len() <= max_width {
		// For short text, return with proper color formatting
		return format!("{}{}{}", field_color, normalized_text, RESET);
	}

	let mut result = Vec::new();
	let mut current_line = String::new();

	for word in normalized_text.split_whitespace() {
		if current_line.is_empty() {
			current_line = word.to_string();
		} else if current_line.len() + word.len() < max_width {
			current_line.push(' ');
			current_line.push_str(word);
		} else {
			result.push(current_line);
			current_line = word.to_string();
		}
	}

	if !current_line.is_empty() {
		result.push(current_line);
	}

	// Apply color to all lines
	let colored_lines: Vec<String> = result
		.iter()
		.map(|line| format!("{}{}{}", field_color, line, RESET))
		.collect();

	colored_lines.join("\n")
}

fn strip_ansi_codes(text: &str) -> String {
	let mut result = String::new();
	let mut in_escape = false;

	for ch in text.chars() {
		if ch == '\x1b' {
			in_escape = true;
		} else if in_escape && ch == 'm' {
			in_escape = false;
		} else if !in_escape {
			result.push(ch);
		}
	}

	result
}

/// Format a float with up to 3 decimals, trimming trailing zeros but keeping at least one.
fn format_float_trimmed(val: f64) -> String {
	let s = format!("{:.3}", val);
	let s = s.trim_end_matches('0');
	let s = if s.ends_with('.') {
		format!("{}0", s)
	} else {
		s.to_string()
	};
	s
}

fn extract_numeric_from_debug(debug_str: &str) -> String {
	// Try to extract numeric value from debug representation like "PrimitiveArray<Float64>\n[\n  4.5,\n]"
	if let Some(start) = debug_str.find('[') {
		if let Some(end) = debug_str.find(']') {
			let content = &debug_str[start + 1..end];
			// Look for numeric values in the content
			for line in content.lines() {
				let trimmed = line.trim().trim_end_matches(',');
				if let Ok(val) = trimmed.parse::<f64>() {
					return format!("{:.2}", val);
				}
				if let Ok(val) = trimmed.parse::<i64>() {
					return val.to_string();
				}
			}
		}
	}

	// Fallback to original approach
	debug_str
		.lines()
		.next()
		.unwrap_or("unknown")
		.trim_start_matches('[')
		.trim_end_matches(']')
		.trim()
		.to_string()
}

fn format_cell_value_with_field_color(
	column: &dyn Array,
	row_idx: usize,
	data_type: &DataType,
	_field_color: &str,
) -> String {
	if column.is_null(row_idx) {
		format!("{}{}{}", NULL_COLOR, "NULL", RESET)
	} else {
		let value = match data_type {
			DataType::Utf8 => {
				let array = column.as_any().downcast_ref::<StringArray>().unwrap();
				array.value(row_idx).to_string()
			}
			DataType::Int64 => {
				if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
					array.value(row_idx).to_string()
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::Float64 => {
				if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
					let val = array.value(row_idx);
					if val.abs() < 0.001 && val != 0.0 {
						format!("{:.2e}", val)
					} else {
						format_float_trimmed(val)
					}
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::Int32 => {
				if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
					array.value(row_idx).to_string()
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::UInt64 => {
				if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
					array.value(row_idx).to_string()
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::Float32 => {
				if let Some(array) = column.as_any().downcast_ref::<Float32Array>() {
					format!("{:.2}", array.value(row_idx))
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			}
			DataType::Boolean => {
				let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
				array.value(row_idx).to_string()
			}
			DataType::Date32 => {
				let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
				let days_since_epoch = array.value(row_idx);
				// Convert days since epoch to a readable date
				let date = chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
					.unwrap_or_else(|| {
						chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default()
					});
				date.format("%Y-%m-%d").to_string()
			}
			DataType::Date64 => {
				let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
				let millis_since_epoch = array.value(row_idx);
				let datetime = chrono::DateTime::from_timestamp_millis(millis_since_epoch)
					.unwrap_or_else(|| {
						chrono::DateTime::from_timestamp(0, 0)
							.unwrap_or(chrono::DateTime::UNIX_EPOCH)
					});
				datetime.format("%Y-%m-%d").to_string()
			}
			DataType::Timestamp(_, _) => {
				// Handle timestamp types
				"timestamp".to_string() // Simplified for now
			}
			_ => {
				// Fallback for other types - try to get a string representation
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
		};
		// Don't apply color here - it will be applied during wrapping
		value
	}
}

fn format_json_value(column: &dyn Array, row_idx: usize, data_type: &DataType) -> String {
	if column.is_null(row_idx) {
		"null".to_string()
	} else {
		match data_type {
			DataType::Utf8 => {
				if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
					format!("\"{}\"", array.value(row_idx).replace("\"", "\\\""))
				} else {
					"\"unknown\"".to_string()
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
						"null".to_string()
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
			DataType::UInt64 => {
				if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
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
						"null".to_string()
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
					format!("\"{}\"", date.format("%Y-%m-%d"))
				} else {
					"\"1970-01-01\"".to_string()
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
					format!("\"{}\"", datetime.format("%Y-%m-%d"))
				} else {
					"\"1970-01-01\"".to_string()
				}
			}
			DataType::Timestamp(_, _) => "\"timestamp\"".to_string(),
			_ => {
				// Safe fallback for any other type
				let debug_str = format!("{:?}", column.slice(row_idx, 1));
				let val = debug_str
					.lines()
					.next()
					.unwrap_or("unknown")
					.trim_start_matches('[')
					.trim_end_matches(']')
					.trim_start_matches("\"")
					.trim_end_matches("\"")
					.trim()
					.to_string();
				format!("\"{}\"", val.replace("\"", "\\\""))
			}
		}
	}
}

pub fn is_correlation_matrix(df: &DataFrame) -> bool {
	let schema = df.schema();
	if let Some(first_field) = schema.fields().first() {
		// Check if first column is named 'variable' and other columns start with 'corr_with_'
		first_field.name() == "variable"
			&& schema
				.fields()
				.iter()
				.skip(1)
				.all(|f| f.name().starts_with("corr_with_"))
	} else {
		false
	}
}

pub async fn display_correlation_matrix(df: &DataFrame) -> NailResult<()> {
	let batches = df.clone().collect().await?;
	let schema = df.schema();

	if batches.is_empty() {
		println!("{}No correlation data to display{}", DIM, RESET);
		return Ok(());
	}

	// Extract column names from schema - skip 'variable' column, get actual column names from corr_with_ prefixes
	let mut col_names = Vec::new();
	for field in schema.fields().iter().skip(1) {
		if let Some(col_name) = field.name().strip_prefix("corr_with_") {
			col_names.push(col_name.replace("_", "."));
		}
	}

	if col_names.is_empty() {
		return display_as_table(df).await; // Fallback to regular table if not a proper matrix
	}

	// Calculate column widths - minimum 8 characters for each correlation value
	let col_width = 8;
	let var_col_width = col_names.iter().map(|s| s.len()).max().unwrap_or(8).max(8);

	// Print header
	print!("{:<width$}", "", width = var_col_width + 2);
	for col_name in &col_names {
		print!("{:>width$}", col_name, width = col_width);
	}
	println!();

	// Print each row
	for batch in &batches {
		for row_idx in 0..batch.num_rows() {
			// Get row variable name
			let var_column = batch.column(0);
			let var_name =
				if let Some(string_array) = var_column.as_any().downcast_ref::<StringArray>() {
					string_array.value(row_idx).to_string()
				} else {
					format!("row_{}", row_idx)
				};

			// Print row name
			print!("{:<width$}  ", var_name, width = var_col_width);

			// Print correlation values
			for col_idx in 1..batch.num_columns() {
				let column = batch.column(col_idx);
				if column.is_null(row_idx) {
					print!("{:>width$}", "NULL", width = col_width);
				} else if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
					let val = float_array.value(row_idx);
					print!("{:>width$.3}", val, width = col_width);
				} else if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
					let val = int_array.value(row_idx) as f64;
					print!("{:>width$.3}", val, width = col_width);
				} else if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
					let val = int_array.value(row_idx) as f64;
					print!("{:>width$.3}", val, width = col_width);
				} else {
					// Fallback: try to extract from debug representation
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					if let Ok(val) = extract_numeric_from_debug(&debug_str).parse::<f64>() {
						print!("{:>width$.3}", val, width = col_width);
					} else {
						print!("{:>width$}", "N/A", width = col_width);
					}
				}
			}
			println!();
		}
	}

	Ok(())
}
