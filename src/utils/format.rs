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
const HEADER_COLOR: &str = "\x1b[1;36m"; // Bold cyan
const NUMERIC_COLOR: &str = "\x1b[33m";  // Yellow
const STRING_COLOR: &str = "\x1b[32m";   // Green
const NULL_COLOR: &str = "\x1b[2;37m";   // Dim white
const BORDER_COLOR: &str = "\x1b[2;90m"; // Dim gray

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
				Some(OutputFormat::Text) | None => {
					match path.extension().and_then(|s| s.to_str()) {
						Some("json") => Some(FileFormat::Json),
						Some("csv") => Some(FileFormat::Csv),
						Some("parquet") => Some(FileFormat::Parquet),
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
	
	// Print data in card format
	let mut row_count = 0;
	for batch in &batches {
		for row_idx in 0..batch.num_rows() {
			row_count += 1;
			
			// Card header
			println!("{}┌─ Record {} ─{}", BORDER_COLOR, row_count, "─".repeat(60));
			println!("│{}", RESET);
			
			// Print each field as a key-value pair
			for (col_idx, field) in schema.fields().iter().enumerate() {
				let column = batch.column(col_idx);
				let value = format_cell_value(column, row_idx, field.data_type(), true);
				
				// Format field name
				let field_name = format!("{}{:<20}{}", HEADER_COLOR, field.name(), RESET);
				
				// Handle long values by wrapping them
				let wrapped_value = wrap_text(&value, 80, 22);
				let lines: Vec<&str> = wrapped_value.lines().collect();
				
				if lines.len() == 1 {
					println!("{}│{} {} : {}", BORDER_COLOR, RESET, field_name, lines[0]);
				} else {
					println!("{}│{} {} : {}", BORDER_COLOR, RESET, field_name, lines[0]);
					for line in &lines[1..] {
						println!("{}│{} {:<20} : {}", BORDER_COLOR, RESET, "", line);
					}
				}
			}
			
			// Card footer
			println!("{}│{}", BORDER_COLOR, RESET);
			println!("{}└{}{}", BORDER_COLOR, "─".repeat(70), RESET);
			
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

fn wrap_text(text: &str, max_width: usize, _indent: usize) -> String {
	let clean_text = strip_ansi_codes(text);
	
	// First, normalize the text by replacing all newlines with spaces
	// This handles cases where the original data has embedded newlines
	let normalized_text = clean_text.replace('\n', " ").replace('\r', " ");
	
	// Remove multiple consecutive spaces
	let normalized_text = normalized_text.split_whitespace().collect::<Vec<_>>().join(" ");
	
	if normalized_text.len() <= max_width {
		// If the original text had colors, preserve them for short text
		if text.contains('\x1b') {
			return text.replace('\n', " ").replace('\r', " ");
		} else {
			return normalized_text;
		}
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
	
	// Reconstruct with original formatting for first line, plain for wrapped lines
	if result.len() == 1 {
		// For single line, preserve original colors but normalize whitespace
		if text.contains('\x1b') {
			text.replace('\n', " ").replace('\r', " ")
		} else {
			result[0].clone()
		}
	} else {
		let mut output = Vec::new();
		output.push(result[0].clone());
		
		for line in &result[1..] {
			// For wrapped lines, apply the same color as the original if it had color
			if text.contains('\x1b') {
				// Extract color from original text
				let color = extract_color_code(text);
				output.push(format!("{}{}{}", color, line, RESET));
			} else {
				output.push(line.clone());
			}
		}
		
		output.join("\n")
	}
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

fn extract_color_code(text: &str) -> &str {
	if text.contains(NUMERIC_COLOR) {
		NUMERIC_COLOR
	} else if text.contains(STRING_COLOR) {
		STRING_COLOR
	} else if text.contains(NULL_COLOR) {
		NULL_COLOR
	} else {
		""
	}
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

fn format_cell_value(column: &dyn Array, row_idx: usize, data_type: &DataType, with_color: bool) -> String {
	if column.is_null(row_idx) {
		let value = "NULL";
		if with_color {
			format!("{}{}{}", NULL_COLOR, value, RESET)
		} else {
			value.to_string()
		}
	} else {
		let value = match data_type {
			DataType::Utf8 => {
				let array = column.as_any().downcast_ref::<StringArray>().unwrap();
				let val = array.value(row_idx);
				if with_color {
					format!("{}{}{}", STRING_COLOR, val, RESET)
				} else {
					val.to_string()
				}
			},
			DataType::Int64 => {
				if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
					let val = array.value(row_idx).to_string();
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					let val = extract_numeric_from_debug(&debug_str);
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				}
			},
			DataType::Float64 => {
				if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
					let val = format!("{:.2}", array.value(row_idx));
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					let val = extract_numeric_from_debug(&debug_str);
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				}
			},
			DataType::Int32 => {
				if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
					let val = array.value(row_idx).to_string();
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					let val = extract_numeric_from_debug(&debug_str);
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				}
			},
			DataType::Float32 => {
				if let Some(array) = column.as_any().downcast_ref::<Float32Array>() {
					let val = format!("{:.2}", array.value(row_idx));
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				} else {
					// Fallback for when the actual type doesn't match the schema type
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					let val = extract_numeric_from_debug(&debug_str);
					if with_color {
						format!("{}{}{}", NUMERIC_COLOR, val, RESET)
					} else {
						val
					}
				}
			},
			DataType::Boolean => {
				let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
				let val = array.value(row_idx).to_string();
				if with_color {
					format!("{}{}{}", NUMERIC_COLOR, val, RESET)
				} else {
					val
				}
			},
			DataType::Date32 => {
				let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
				let days_since_epoch = array.value(row_idx);
				// Convert days since epoch to a readable date
				let date = chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
					.unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
				let val = date.format("%Y-%m-%d").to_string();
				if with_color {
					format!("{}{}{}", STRING_COLOR, val, RESET)
				} else {
					val
				}
			},
			DataType::Date64 => {
				let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
				let millis_since_epoch = array.value(row_idx);
				let datetime = chrono::DateTime::from_timestamp_millis(millis_since_epoch)
					.unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
				let val = datetime.format("%Y-%m-%d").to_string();
				if with_color {
					format!("{}{}{}", STRING_COLOR, val, RESET)
				} else {
					val
				}
			},
			DataType::Timestamp(_, _) => {
				// Handle timestamp types
				let val = "timestamp"; // Simplified for now
				if with_color {
					format!("{}{}{}", STRING_COLOR, val, RESET)
				} else {
					val.to_string()
				}
			},
			_ => {
				// Fallback for other types - try to get a string representation
				let val = format!("{:?}", column.slice(row_idx, 1))
					.lines()
					.next()
					.unwrap_or("unknown")
					.trim_start_matches('[')
					.trim_end_matches(']')
					.trim_start_matches("\"")
					.trim_end_matches("\"")
					.trim()
					.to_string();
				if with_color {
					format!("{}{}{}", STRING_COLOR, val, RESET)
				} else {
					val
				}
			},
		};
		value
	}
}

fn format_json_value(column: &dyn Array, row_idx: usize, data_type: &DataType) -> String {
	if column.is_null(row_idx) {
		"null".to_string()
	} else {
		match data_type {
			DataType::Utf8 => {
				let array = column.as_any().downcast_ref::<StringArray>().unwrap();
				format!("\"{}\"", array.value(row_idx).replace("\"", "\\\""))
			},
			DataType::Int64 => {
				let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
				array.value(row_idx).to_string()
			},
			DataType::Float64 => {
				let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
				array.value(row_idx).to_string()
			},
			DataType::Int32 => {
				let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
				array.value(row_idx).to_string()
			},
			DataType::Float32 => {
				let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
				array.value(row_idx).to_string()
			},
			DataType::Boolean => {
				let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
				array.value(row_idx).to_string()
			},
			DataType::Date32 => {
				let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
				let days_since_epoch = array.value(row_idx);
				let date = chrono::NaiveDate::from_num_days_from_ce_opt(days_since_epoch + 719163)
					.unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
				format!("\"{}\"", date.format("%Y-%m-%d"))
			},
			DataType::Date64 => {
				let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
				let millis_since_epoch = array.value(row_idx);
				let datetime = chrono::DateTime::from_timestamp_millis(millis_since_epoch)
					.unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
				format!("\"{}\"", datetime.format("%Y-%m-%d"))
			},
			DataType::Timestamp(_, _) => {
				format!("\"timestamp\"")
			},
			_ => {
				let val = format!("{:?}", column.slice(row_idx, 1))
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

