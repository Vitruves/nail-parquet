use datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use std::path::Path;
use crate::error::NailResult;
use crate::cli::OutputFormat;
use crate::utils::io::write_data;
use crate::utils::FileFormat;

// ANSI color codes
const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";

const NULL_COLOR: &str = "\x1b[2;37m";   // Dim white
const BORDER_COLOR: &str = "\x1b[2;90m"; // Dim gray

// Bat-style field colors (cycling through different colors for variety)
const FIELD_COLORS: &[&str] = &[
	"\x1b[32m",   // Green
	"\x1b[33m",   // Yellow
	"\x1b[34m",   // Blue
	"\x1b[35m",   // Magenta
	"\x1b[36m",   // Cyan
	"\x1b[91m",   // Bright red
	"\x1b[92m",   // Bright green
	"\x1b[93m",   // Bright yellow
	"\x1b[94m",   // Bright blue
	"\x1b[95m",   // Bright magenta
	"\x1b[96m",   // Bright cyan
	"\x1b[31m",   // Red
];

pub async fn display_dataframe(
	df: &DataFrame,
	output_path: Option<&Path>,
	format: Option<&OutputFormat>,
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
				},
			};
			
			write_data(df, path, file_format.as_ref()).await
		},
		None => {
			match format {
				Some(OutputFormat::Json) => {
					display_as_json(df).await?;
				},
				Some(OutputFormat::Text) | None => {
					display_as_table(df).await?;
				},
				_ => {
					return Err(crate::error::NailError::InvalidArgument(
						"CSV and Parquet formats require an output file".to_string()
					));
				},
			}
			
			Ok(())
		},
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
		w.max(60).min(200)
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
			
			println!("{}┌{}{}{}{}",
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
				let value = format_cell_value_with_field_color(column, row_idx, field.data_type(), field_color);
				
				// Format field name with the same color as the value
				let field_name = format!("{}{}{:<width$}{}", BOLD, field_color, field.name(), RESET, width = field_name_width);
				
				// Handle long values by wrapping them properly
				let wrapped_value = wrap_text_with_color(&value, content_width, field_color);
				let lines: Vec<&str> = wrapped_value.lines().collect();
				
				if lines.len() == 1 {
					println!("{}│{} {} : {}", BORDER_COLOR, RESET, field_name, lines[0]);
				} else {
					println!("{}│{} {} : {}", BORDER_COLOR, RESET, field_name, lines[0]);
					for line in &lines[1..] {
						println!("{}│{} {:<width$} : {}", BORDER_COLOR, RESET, "", line, width = field_name_width);
					}
				}
			}
			
			// Card footer with dynamic width
			println!("{}│{}", BORDER_COLOR, RESET);
			println!("{}└{}{}", BORDER_COLOR, "─".repeat(terminal_width.saturating_sub(2)), RESET);
			
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

fn wrap_text_with_color(text: &str, max_width: usize, field_color: &str) -> String {
	let clean_text = strip_ansi_codes(text);
	
	// First, normalize the text by replacing all newlines with spaces
	// This handles cases where the original data has embedded newlines
	let normalized_text = clean_text.replace('\n', " ").replace('\r', " ");
	
	// Remove multiple consecutive spaces
	let normalized_text = normalized_text.split_whitespace().collect::<Vec<_>>().join(" ");
	
	if normalized_text.len() <= max_width {
		// For short text, return with proper color formatting
		return format!("{}{}{}", field_color, normalized_text, RESET);
	}
	
	let mut result = Vec::new();
	let mut current_line = String::new();
	
	for word in normalized_text.split_whitespace() {
		if current_line.is_empty() {
			current_line = word.to_string();
		} else if current_line.len() + word.len() + 1 <= max_width {
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
	let colored_lines: Vec<String> = result.iter()
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



fn extract_numeric_from_debug(debug_str: &str) -> String {
	// Try to extract numeric value from debug representation like "PrimitiveArray<Float64>\n[\n  4.5,\n]"
	if let Some(start) = debug_str.find('[') {
		if let Some(end) = debug_str.find(']') {
			let content = &debug_str[start+1..end];
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

fn format_cell_value_with_field_color(column: &dyn Array, row_idx: usize, data_type: &DataType, _field_color: &str) -> String {
	if column.is_null(row_idx) {
		format!("{}{}{}", NULL_COLOR, "NULL", RESET)
	} else {
		let value = match data_type {
			DataType::Utf8 => {
				let array = column.as_any().downcast_ref::<StringArray>().unwrap();
				array.value(row_idx).to_string()
			},
			DataType::Int64 => {
				if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
					array.value(row_idx).to_string()
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			},
			DataType::Float64 => {
                if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                    let val = array.value(row_idx);
                    if val.abs() < 0.001 {
                        format!("{:.2e}", val)
                    } else {
                        format!("{:.3}", val)
                    }
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			},
			DataType::Int32 => {
				if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
					array.value(row_idx).to_string()
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			},
			DataType::Float32 => {
				if let Some(array) = column.as_any().downcast_ref::<Float32Array>() {
					format!("{:.2}", array.value(row_idx))
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					extract_numeric_from_debug(&debug_str)
				}
			},
			DataType::Boolean => {
				let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
				array.value(row_idx).to_string()
			},
			DataType::Date32 => {
				let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
				let days_since_epoch = array.value(row_idx);
				// Convert days since epoch to a readable date
				let date = chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
					.unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
						.unwrap_or_else(|| chrono::NaiveDate::default()));
				date.format("%Y-%m-%d").to_string()
			},
			DataType::Date64 => {
				let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
				let millis_since_epoch = array.value(row_idx);
				let datetime = chrono::DateTime::from_timestamp_millis(millis_since_epoch)
					.unwrap_or_else(|| {
						chrono::DateTime::from_timestamp(0, 0)
							.unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH)
					});
				datetime.format("%Y-%m-%d").to_string()
			},
			DataType::Timestamp(_, _) => {
				// Handle timestamp types
				"timestamp".to_string() // Simplified for now
			},
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
			},
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
			},
			DataType::Int64 => {
				if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
					array.value(row_idx).to_string()
				} else {
					"0".to_string()
				}
			},
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
			},
			DataType::Int32 => {
				if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
					array.value(row_idx).to_string()
				} else {
					"0".to_string()
				}
			},
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
			},
			DataType::Boolean => {
				if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
					array.value(row_idx).to_string()
				} else {
					"false".to_string()
				}
			},
			DataType::Date32 => {
				if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
					let days_since_epoch = array.value(row_idx);
					let date = chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
						.unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
							.unwrap_or_else(|| chrono::NaiveDate::default()));
					format!("\"{}\"", date.format("%Y-%m-%d"))
				} else {
					"\"1970-01-01\"".to_string()
				}
			},
			DataType::Date64 => {
				if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
					let millis_since_epoch = array.value(row_idx);
					let datetime = chrono::DateTime::from_timestamp_millis(millis_since_epoch)
						.unwrap_or_else(|| {
							chrono::DateTime::from_timestamp(0, 0)
								.unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH)
						});
					format!("\"{}\"", datetime.format("%Y-%m-%d"))
				} else {
					"\"1970-01-01\"".to_string()
				}
			},
			DataType::Timestamp(_, _) => {
				"\"timestamp\"".to_string()
			},
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
			},
		}
	}
}

