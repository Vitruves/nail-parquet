use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use datafusion::prelude::*;

#[derive(Args, Clone)]
pub struct SizeArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Show per-column sizes")]
	pub columns: bool,
	
	#[arg(short, long, help = "Show per-row analysis")]
	pub rows: bool,
	
	#[arg(long, help = "Show raw bits without human-friendly conversion")]
	pub bits: bool,
}

pub async fn execute(args: SizeArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Analyzing size of: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let schema = df.schema();
	
	let row_count = df.clone().count().await?;
	let col_count = schema.fields().len();
	
	// For memory usage calculation, we'll use an approximation based on schema and row count
	// rather than loading the entire dataset into memory
	let mut total_memory = 0usize;
	let mut column_sizes = Vec::new();
	
	// Estimate memory usage based on data types and row count
	for field in schema.fields().iter() {
		let estimated_size = estimate_column_memory_size(field.data_type(), row_count);
		total_memory += estimated_size;
		
		if args.columns {
			column_sizes.push((field.name().clone(), estimated_size));
		}
	}
	
	let file_size = std::fs::metadata(&args.common.input)?.len();
	
	args.common.log_if_verbose(&format!("Analysis complete: {} rows, {} columns", row_count, col_count));
	
	// For size command, provide formatted text output for console
	match &args.common.output {
		Some(_output_path) => {
			// Create a DataFrame with the size information for file output
			let ctx = SessionContext::new();
			let mut size_data = vec![
				format!("'Total columns' as metric, '{}' as value", col_count),
				format!("'Total rows' as metric, '{}' as value", row_count),
				format!("'File size' as metric, '{}' as value", if args.bits { format!("{} bits", file_size * 8) } else { human_bytes(file_size as usize) }),
			];
			
			if args.rows && row_count > 0 {
				let avg_per_row = if args.bits { 
					format!("{} bits", (total_memory * 8) / row_count)
				} else {
					human_bytes(total_memory / row_count)
				};
				size_data.push(format!("'Average per row' as metric, '{}' as value", avg_per_row));
			}
			
			if args.columns {
				for (name, size) in &column_sizes {
					let size_str = if args.bits {
						format!("{} bits", size * 8)
					} else {
						human_bytes(*size)
					};
					size_data.push(format!("'Column: {}' as metric, '{}' as value", name.replace("'", "''"), size_str));
				}
			}
			
			size_data.push(format!("'Estimated uncompressed memory usage' as metric, '{}' as value", if args.bits { format!("{} bits", total_memory * 8) } else { human_bytes(total_memory) }));
			
			let sql = format!("SELECT {} {}", 
				size_data.first().unwrap(),
				if size_data.len() > 1 {
					format!(" UNION ALL SELECT {}", size_data[1..].join(" UNION ALL SELECT "))
				} else {
					String::new()
				}
			);
			
			let result_df = ctx.sql(&sql).await.map_err(crate::error::NailError::DataFusion)?;
			
			let output_handler = OutputHandler::new(&args.common);
			output_handler.handle_output(&result_df, "size").await?;
		}
		None => {
			// Console output with test-expected text
			println!("Total rows: {}", row_count);
			println!("Total columns: {}", col_count);
			println!("File size: {}", if args.bits { format!("{} bits", file_size * 8) } else { human_bytes(file_size as usize) });
			
			if args.rows && row_count > 0 {
				let avg_per_row = if args.bits { 
					format!("{} bits", (total_memory * 8) / row_count)
				} else {
					human_bytes(total_memory / row_count)
				};
				println!("Average bits per row: {}", avg_per_row);
			}
			
			if args.columns {
				println!("Per-column sizes:");
				for (name, size) in &column_sizes {
					let size_str = if args.bits {
						format!("{} bits", size * 8)
					} else {
						human_bytes(*size)
					};
					println!("  {}: {}", name, size_str);
				}
			}
			
			println!("Estimated uncompressed memory usage: {}", if args.bits { format!("{} bits", total_memory * 8) } else { human_bytes(total_memory) });
		}
	}
	
	Ok(())
}


fn estimate_column_memory_size(data_type: &datafusion::arrow::datatypes::DataType, row_count: usize) -> usize {
	use datafusion::arrow::datatypes::DataType;
	
	match data_type {
		DataType::Boolean => row_count / 8 + 1, // 1 bit per value, rounded up
		DataType::Int8 | DataType::UInt8 => row_count,
		DataType::Int16 | DataType::UInt16 => row_count * 2,
		DataType::Int32 | DataType::UInt32 | DataType::Float32 => row_count * 4,
		DataType::Int64 | DataType::UInt64 | DataType::Float64 => row_count * 8,
		DataType::Date32 => row_count * 4,
		DataType::Date64 | DataType::Timestamp(_, _) => row_count * 8,
		DataType::Utf8 => {
			// Estimate average string length as 20 characters
			// Plus overhead for string length tracking
			row_count * (20 + 4)
		},
		DataType::LargeUtf8 => {
			// Similar to Utf8 but with 8-byte length tracking
			row_count * (20 + 8)
		},
		DataType::Binary => {
			// Estimate average binary length as 50 bytes
			row_count * (50 + 4)
		},
		DataType::LargeBinary => {
			row_count * (50 + 8)
		},
		DataType::List(_) | DataType::LargeList(_) => {
			// Very rough estimate for lists
			row_count * 100
		},
		DataType::Struct(_) => {
			// Rough estimate for structs
			row_count * 50
		},
		_ => {
			// Default estimate for unknown types
			row_count * 8
		}
	}
}

fn human_bytes(bytes: usize) -> String {
	const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
	let mut size = bytes as f64;
	let mut unit_idx = 0;
	
	while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
		size /= 1024.0;
		unit_idx += 1;
	}
	
	if unit_idx == 0 {
		format!("{} {}", size as usize, UNITS[unit_idx])
	} else {
		format!("{:.2} {}", size, UNITS[unit_idx])
	}
}