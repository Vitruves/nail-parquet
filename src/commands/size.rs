use clap::Args;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;
use datafusion::arrow::array::Array;

#[derive(Args, Clone)]
pub struct SizeArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Show per-column sizes")]
	pub columns: bool,
	
	#[arg(short, long, help = "Show per-row analysis")]
	pub rows: bool,
	
	#[arg(long, help = "Show raw bits without human-friendly conversion")]
	pub bits: bool,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: SizeArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Analyzing size of: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
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
	
	let file_size = std::fs::metadata(&args.input)?.len();
	
	if args.verbose {
		eprintln!("Analysis complete: {} rows, {} columns", row_count, col_count);
	}
	
	let output = if args.bits {
		format_size_bits(row_count, col_count, total_memory, file_size, &column_sizes, args.columns, args.rows)
	} else {
		format_size_human(row_count, col_count, total_memory, file_size, &column_sizes, args.columns, args.rows)
	};
	
	match &args.output {
		Some(output_path) => {
			std::fs::write(output_path, output)?;
			if args.verbose {
				eprintln!("Size analysis written to: {}", output_path.display());
			}
		},
		None => {
			println!("{}", output);
		},
	}
	
	Ok(())
}

fn format_size_bits(row_count: usize, col_count: usize, memory: usize, file_size: u64, 
				   column_sizes: &[(String, usize)], show_columns: bool, show_rows: bool) -> String {
	let mut output = String::new();
	
	output.push_str(&format!("Total rows: {}\n", row_count));
	output.push_str(&format!("Total columns: {}\n", col_count));
	output.push_str(&format!("File size (bytes): {}\n", file_size));
	output.push_str(&format!("Memory usage (bytes): {}\n", memory));
	output.push_str(&format!("File size (bits): {}\n", file_size * 8));
	output.push_str(&format!("Memory usage (bits): {}\n", memory * 8));
	
	if show_rows && row_count > 0 {
		output.push_str(&format!("Average bytes per row: {}\n", memory / row_count));
		output.push_str(&format!("Average bits per row: {}\n", (memory * 8) / row_count));
	}
	
	if show_columns {
		output.push_str("\nPer-column sizes (bytes):\n");
		for (name, size) in column_sizes {
			output.push_str(&format!("  {}: {} bytes ({} bits)\n", name, size, size * 8));
		}
	}
	
	output
}

fn format_size_human(row_count: usize, col_count: usize, memory: usize, file_size: u64, 
					column_sizes: &[(String, usize)], show_columns: bool, show_rows: bool) -> String {
	let mut output = String::new();
	
	output.push_str(&format!("Total rows: {}\n", row_count));
	output.push_str(&format!("Total columns: {}\n", col_count));
	output.push_str(&format!("File size: {}\n", human_bytes(file_size as usize)));
	output.push_str(&format!("Memory usage: {}\n", human_bytes(memory)));
	
	if show_rows && row_count > 0 {
		output.push_str(&format!("Average per row: {}\n", human_bytes(memory / row_count)));
	}
	
	if show_columns {
		output.push_str("\nPer-column sizes:\n");
		for (name, size) in column_sizes {
			output.push_str(&format!("  {}: {}\n", name, human_bytes(*size)));
		}
	}
	
	output
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