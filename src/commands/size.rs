use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use datafusion::arrow::array::Array;
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
	let batches = df.clone().collect().await?;
	let schema = df.schema();
	
	let row_count = df.clone().count().await?;
	let col_count = schema.fields().len();
	
	let mut total_memory = 0usize;
	let mut column_sizes = Vec::new();
	
	for batch in &batches {
		for (col_idx, field) in schema.fields().iter().enumerate() {
			let column = batch.column(col_idx);
			let size = column.get_buffer_memory_size();
			total_memory += size;
			
			if args.columns {
				if let Some(existing) = column_sizes.iter_mut().find(|(name, _)| name == field.name()) {
					existing.1 += size;
				} else {
					column_sizes.push((field.name().clone(), size));
				}
			}
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
			
			size_data.push(format!("'Memory usage' as metric, '{}' as value", if args.bits { format!("{} bits", total_memory * 8) } else { human_bytes(total_memory) }));
			
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
			
			println!("Memory usage: {}", if args.bits { format!("{} bits", total_memory * 8) } else { human_bytes(total_memory) });
		}
	}
	
	Ok(())
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