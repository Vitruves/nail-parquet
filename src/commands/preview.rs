use clap::Args;
use std::path::PathBuf;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand::rngs::StdRng;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::format::display_dataframe;
use crossterm::{
	event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
	execute,
	terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
	cursor::{Hide, Show},
};
use std::io::{self, Write};
use datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;

#[derive(Args, Clone)]
pub struct PreviewArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Number of rows to display", default_value = "5")]
	pub number: usize,
	
	#[arg(short, long, help = "Random seed for reproducible results")]
	pub random: Option<u64>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
	
	#[arg(short = 'I', long, help = "Interactive mode with scrolling (use arrow keys, q to quit)")]
	pub interactive: bool,
}

pub async fn execute(args: PreviewArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let total_rows = df.clone().count().await?;
	
	// If interactive mode is requested, handle it separately
	if args.interactive {
		return execute_interactive(args, df, total_rows).await;
	}
	
	// Non-interactive mode (original behavior)
	if total_rows <= args.number {
		display_dataframe(&df, args.output.as_deref(), args.format.as_ref()).await?;
		return Ok(());
	}
	
	let mut rng = match args.random {
		Some(seed) => StdRng::seed_from_u64(seed),
		None => StdRng::from_entropy(),
	};
	
	let mut indices: Vec<usize> = (0..total_rows).collect();
	indices.shuffle(&mut rng);
	indices.truncate(args.number);
	indices.sort();
	
	if args.verbose {
		eprintln!("Randomly sampling {} rows from {} total rows", args.number, total_rows);
	}
	
	let ctx = crate::utils::create_context_with_jobs(args.jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let indices_str = indices.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");
	
	// Get the original column names and quote them to preserve case
	let original_columns: Vec<String> = df.schema().fields().iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();
	
	let sql = format!(
		"SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn IN ({})",
		original_columns.join(", "),
		original_columns.join(", "),
		table_name, 
		indices_str
	);
	
	if args.verbose {
		eprintln!("Executing SQL: {}", sql);
	}
	
	let result = ctx.sql(&sql).await?;
	
	display_dataframe(&result, args.output.as_deref(), args.format.as_ref()).await?;
	
	Ok(())
}

async fn execute_interactive(_args: PreviewArgs, df: DataFrame, _total_rows: usize) -> NailResult<()> {
	// Collect all data for interactive browsing
	let batches = df.clone().collect().await?;
	
	if batches.is_empty() {
		println!("No data to display");
		return Ok(());
	}
	
	// Calculate total records
	let total_records: usize = batches.iter().map(|b| b.num_rows()).sum();
	
	// Interactive viewer state
	let mut current_record = 0;
	
	// Setup terminal
	enable_raw_mode().map_err(|e| crate::error::NailError::Io(e))?;
	execute!(io::stdout(), EnterAlternateScreen, Hide).map_err(|e| crate::error::NailError::Io(e))?;
	
	let result = run_simple_interactive_viewer(&batches, total_records, &mut current_record);
	
	// Cleanup terminal
	execute!(io::stdout(), Show, LeaveAlternateScreen).map_err(|e| crate::error::NailError::Io(e))?;
	disable_raw_mode().map_err(|e| crate::error::NailError::Io(e))?;
	
	result
}

fn run_simple_interactive_viewer(
	batches: &[datafusion::arrow::record_batch::RecordBatch],
	total_records: usize,
	current_record: &mut usize,
) -> NailResult<()> {
	loop {
		// Clear screen
		execute!(io::stdout(), crossterm::terminal::Clear(crossterm::terminal::ClearType::All))
			.map_err(|e| crate::error::NailError::Io(e))?;
		execute!(io::stdout(), crossterm::cursor::MoveTo(0, 0))
			.map_err(|e| crate::error::NailError::Io(e))?;
		
		// Display current record using the same format as non-interactive mode
		display_single_record_card(batches, *current_record)?;
		
		// Get terminal height to position status at bottom
		let (_, height) = crossterm::terminal::size().unwrap_or((80, 24));
		
		// Move cursor to bottom and display status
		execute!(io::stdout(), crossterm::cursor::MoveTo(0, height - 1))
			.map_err(|e| crate::error::NailError::Io(e))?;
		
		// Dense status and controls display anchored at bottom
		print!("{}Record {} of {} | ↑↓←→ hjkl | quit: q | top: Home | end: End | jump: PgUp/PgDn{}", 
			"\x1b[2;37m", // Dim white
			*current_record + 1, 
			total_records,
			"\x1b[0m" // Reset
		);
		
		// Flush output
		io::stdout().flush().map_err(|e| crate::error::NailError::Io(e))?;
		
		// Handle input
		if let Event::Key(key_event) = event::read().map_err(|e| crate::error::NailError::Io(e))? {
			match key_event {
				KeyEvent { code: KeyCode::Char('q'), .. } |
				KeyEvent { code: KeyCode::Char('Q'), .. } |
				KeyEvent { code: KeyCode::Esc, .. } => {
					break;
				},
				KeyEvent { code: KeyCode::Char('c'), modifiers: KeyModifiers::CONTROL, .. } => {
					break;
				},
				KeyEvent { code: KeyCode::Right, .. } |
				KeyEvent { code: KeyCode::Char('l'), .. } |
				KeyEvent { code: KeyCode::Down, .. } |
				KeyEvent { code: KeyCode::Char('j'), .. } => {
					if *current_record < total_records - 1 {
						*current_record += 1;
					}
				},
				KeyEvent { code: KeyCode::Left, .. } |
				KeyEvent { code: KeyCode::Char('h'), .. } |
				KeyEvent { code: KeyCode::Up, .. } |
				KeyEvent { code: KeyCode::Char('k'), .. } => {
					if *current_record > 0 {
						*current_record -= 1;
					}
				},
				KeyEvent { code: KeyCode::PageDown, .. } => {
					*current_record = (*current_record + 10).min(total_records - 1);
				},
				KeyEvent { code: KeyCode::PageUp, .. } => {
					*current_record = current_record.saturating_sub(10);
				},
				KeyEvent { code: KeyCode::Home, .. } => {
					*current_record = 0;
				},
				KeyEvent { code: KeyCode::End, .. } => {
					*current_record = total_records - 1;
				},
				_ => {}
			}
		}
	}
	
	Ok(())
}

fn display_single_record_card(
	batches: &[datafusion::arrow::record_batch::RecordBatch],
	record_index: usize,
) -> NailResult<()> {
	// Find which batch and row within batch
	let mut current_index = 0;
	let mut target_batch_idx = 0;
	let mut target_row_idx = 0;
	
	for (batch_idx, batch) in batches.iter().enumerate() {
		if current_index + batch.num_rows() > record_index {
			target_batch_idx = batch_idx;
			target_row_idx = record_index - current_index;
			break;
		}
		current_index += batch.num_rows();
	}
	
	let batch = &batches[target_batch_idx];
	let schema = batch.schema();
	
	// Use the same formatting as non-interactive mode
	display_single_record_with_format(batch, target_row_idx, record_index + 1, &schema)?;
	
	Ok(())
}

fn display_single_record_with_format(
	batch: &datafusion::arrow::record_batch::RecordBatch,
	row_idx: usize,
	record_number: usize,
	schema: &datafusion::arrow::datatypes::SchemaRef,
) -> NailResult<()> {
	// Constants for colors
	const RESET: &str = "\x1b[0m";
	const BOLD: &str = "\x1b[1m";
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
	
	// Simple record header
	let record_text = format!("=== Record {} ===", record_number);
	
	// Simple header without box
	let mut output = String::new();
	output.push_str(&format!("\n{}{}{}\n\n", 
		"\x1b[1;36m",  // Bold cyan
		record_text, 
		RESET
	));
	
	// Print each field as a simple list with colors
	for (col_idx, field) in schema.fields().iter().enumerate() {
		let column = batch.column(col_idx);
		let field_color = FIELD_COLORS[col_idx % FIELD_COLORS.len()];
		let value = format_cell_value_for_interactive(column, row_idx, field.data_type());
		
		// Ultra-simple format: no alignment at all
		output.push_str(&format!("{}{}{}{}: {}\n", 
			field_color,
			BOLD,
			field.name(), 
			RESET,
			value
		));
	}
	
	output.push('\n');
	
	// Print the entire output at once
	print!("{}", output);
	
	Ok(())
}

fn format_cell_value_for_interactive(column: &dyn Array, row_idx: usize, data_type: &DataType) -> String {
	const NULL_COLOR: &str = "\x1b[2;37m";   // Dim white
	const RESET: &str = "\x1b[0m";
	
	if column.is_null(row_idx) {
		format!("{}{}{}", NULL_COLOR, "NULL", RESET)
	} else {
		match data_type {
			DataType::Utf8 => {
				let array = column.as_any().downcast_ref::<StringArray>().unwrap();
				array.value(row_idx).to_string()
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
					format!("{:.2}", array.value(row_idx))
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
					format!("{:.2}", array.value(row_idx))
				} else {
					"0.0".to_string()
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
			},
		}
	}
}





