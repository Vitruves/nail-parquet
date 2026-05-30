use crate::cli::CommonArgs;
use crate::commands::select::{parse_row_specification, select_rows_by_indices};
use crate::error::NailResult;
use crate::utils::format::display_dataframe_with_mode;
use crate::utils::io::read_data_with_opts;
use clap::Args;
use crossterm::{
	cursor::{Hide, Show},
	event::{self, Event, KeyCode, KeyEvent},
	execute,
	terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use datafusion::prelude::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use ratatui::{
	backend::CrosstermBackend,
	layout::{Constraint, Direction, Layout},
	style::{Color, Modifier, Style},
	text::{Line, Span},
	widgets::{Paragraph, Wrap},
	Terminal,
};
use std::io;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail preview data.parquet -n 20
  nail preview data.csv --interactive")]
pub struct PreviewArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(short, long, help = "Number of rows to display", default_value = "5")]
	pub number: usize,

	#[arg(
		short = 'I',
		long,
		help = "Interactive mode with scrolling (use arrow keys, q to quit)"
	)]
	pub interactive: bool,

	#[arg(short, long, help = "Specific row numbers or ranges (e.g., 1,3,5-10)")]
	pub rows: Option<String>,
}

pub async fn execute(args: PreviewArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let total_rows = df.clone().count().await?;

	// If specific rows are requested, handle them first
	if let Some(row_spec) = &args.rows {
		let row_indices = parse_row_specification(row_spec)?;
		args.common
			.log_if_verbose(&format!("Selecting {} specific rows", row_indices.len()));

		let result_df = select_rows_by_indices(&df, &row_indices, args.common.jobs).await?;

		// If interactive mode is also requested, use it with the selected rows
		if args.interactive {
			let selected_total = result_df.clone().count().await?;
			return execute_interactive(args, result_df, selected_total).await;
		}

		display_dataframe_with_mode(
			&result_df,
			args.common.output.as_deref(),
			args.common.format.as_ref(),
			args.common.table,
			args.common.level,
		)
		.await?;
		return Ok(());
	}

	// If interactive mode is requested, handle it separately
	if args.interactive {
		return execute_interactive(args, df, total_rows).await;
	}

	// Non-interactive mode (original behavior)
	if total_rows <= args.number {
		display_dataframe_with_mode(
			&df,
			args.common.output.as_deref(),
			args.common.format.as_ref(),
			args.common.table,
			args.common.level,
		)
		.await?;
		return Ok(());
	}

	let mut rng = match args.common.random {
		Some(seed) => StdRng::seed_from_u64(seed),
		None => StdRng::from_entropy(),
	};

	let mut indices: Vec<usize> = (0..total_rows).collect();
	indices.shuffle(&mut rng);
	indices.truncate(args.number);
	indices.sort();

	args.common.log_if_verbose(&format!(
		"Randomly sampling {} rows from {} total rows",
		args.number, total_rows
	));

	let ctx = crate::utils::create_context_with_jobs(args.common.jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;

	let indices_str = indices
		.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");

	// Get the original column names and quote them to preserve case
	let original_columns: Vec<String> = df
		.schema()
		.fields()
		.iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();

	let sql = format!(
		"SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn IN ({})",
		original_columns.join(", "),
		original_columns.join(", "),
		table_name,
		indices_str
	);

	args.common
		.log_if_verbose(&format!("Executing SQL: {}", sql));

	let result = ctx.sql(&sql).await?;

	display_dataframe_with_mode(
		&result,
		args.common.output.as_deref(),
		args.common.format.as_ref(),
		args.common.table,
		args.common.level,
	)
	.await?;

	Ok(())
}

async fn execute_interactive(
	args: PreviewArgs,
	df: DataFrame,
	total_rows: usize,
) -> NailResult<()> {
	if total_rows == 0 {
		println!("No data to display");
		return Ok(());
	}

	// For interactive mode, we'll use a paging approach to avoid loading everything into memory
	// Start with the first page of data
	const PAGE_SIZE: usize = 1000; // Load data in chunks of 1000 rows
	let mut current_page = 0;
	let total_pages = total_rows.div_ceil(PAGE_SIZE);

	// Load first page
	let first_page_df = df.clone().limit(0, Some(PAGE_SIZE.min(total_rows)))?;
	let mut current_batches = first_page_df.collect().await?;

	if current_batches.is_empty() {
		println!("No data to display");
		return Ok(());
	}

	let total_records = total_rows;

	// Interactive viewer state
	let mut current_record = 0;
	let mut row_offset = 0;

	// Setup terminal
	enable_raw_mode().map_err(crate::error::NailError::Io)?;
	execute!(io::stdout(), EnterAlternateScreen, Hide).map_err(crate::error::NailError::Io)?;

	let result = run_ratatui_viewer_paged(
		&df,
		&mut current_batches,
		&mut current_page,
		total_pages,
		PAGE_SIZE,
		total_records,
		&mut current_record,
		&mut row_offset,
		&args,
	)
	.await;

	// Cleanup terminal
	execute!(io::stdout(), Show, LeaveAlternateScreen).map_err(crate::error::NailError::Io)?;
	disable_raw_mode().map_err(crate::error::NailError::Io)?;

	result
}

const FIELD_COLORS_TUI: [Color; 8] = [
	Color::Green,
	Color::Yellow,
	Color::Blue,
	Color::Magenta,
	Color::Cyan,
	Color::Red,
	Color::LightGreen,
	Color::LightYellow,
];

#[allow(clippy::too_many_arguments)]
async fn run_ratatui_viewer_paged(
	df: &DataFrame,
	current_batches: &mut Vec<datafusion::arrow::record_batch::RecordBatch>,
	current_page: &mut usize,
	_total_pages: usize,
	page_size: usize,
	total_records: usize,
	current_record: &mut usize,
	row_offset: &mut usize,
	_args: &PreviewArgs,
) -> NailResult<()> {
	let mut stdout = io::stdout();
	enable_raw_mode().map_err(crate::error::NailError::Io)?;
	execute!(stdout, EnterAlternateScreen, Hide).map_err(crate::error::NailError::Io)?;

	let backend = CrosstermBackend::new(stdout);
	let mut terminal = Terminal::new(backend).map_err(crate::error::NailError::Io)?;

	loop {
		// Check if we need to load a new page for the current record
		let needed_page = *current_record / page_size;
		if needed_page != *current_page {
			*current_page = needed_page;
			let offset = needed_page * page_size;
			let limit = page_size.min(total_records - offset);

			if offset < total_records {
				let page_df = df.clone().limit(offset, Some(limit))?;
				*current_batches = page_df
					.collect()
					.await
					.map_err(crate::error::NailError::DataFusion)?;
			}
		}

		terminal
			.draw(|f| {
				let size = f.size();
				let chunks = Layout::default()
					.direction(Direction::Vertical)
					.constraints([
						Constraint::Min(0),    // table
						Constraint::Length(1), // status
					])
					.split(size);

				// build table rows for current record within the current page
				let record_in_page = *current_record % page_size;
				let (batch_idx, row_idx) = {
					let mut idx = record_in_page;
					let mut b_idx = 0;
					for b in current_batches.iter() {
						if idx < b.num_rows() {
							break;
						}
						idx -= b.num_rows();
						b_idx += 1;
					}
					(b_idx, idx)
				};

				if batch_idx >= current_batches.len() {
					return; // No data available
				}

				let batch = &current_batches[batch_idx];
				let schema = batch.schema();
				let level = _args.common.level;
				// Match the card layout: a fixed field-name column, " : " separator,
				// then value lines (tree or wrapped) in the remaining width.
				const NAME_WIDTH: usize = 20;
				let total_width = (chunks[0].width as usize).max(NAME_WIDTH + 8);
				let value_width = total_width.saturating_sub(NAME_WIDTH + 3).max(10);
				let content_iter = schema.fields().iter().enumerate().skip(*row_offset);
				let lines: Vec<Line> = content_iter
					.flat_map(|(col_idx, field)| {
						let column = batch.column(col_idx);
						let field_color = FIELD_COLORS_TUI[col_idx % FIELD_COLORS_TUI.len()];
						let name_style = Style::default()
							.fg(field_color)
							.add_modifier(Modifier::BOLD);
						let value_style = Style::default().fg(field_color);
						let vlines = crate::utils::format::render_field_value_lines(
							column,
							row_idx,
							field.data_type(),
							level,
							value_width,
						);
						vlines
							.into_iter()
							.enumerate()
							.map(|(i, vline)| {
								if i == 0 {
									Line::from(vec![
										Span::styled(
											format!("{:<width$}", field.name(), width = NAME_WIDTH),
											name_style,
										),
										Span::styled(" : ".to_string(), name_style),
										Span::styled(vline, value_style),
									])
								} else {
									Line::from(vec![
										Span::styled(
											format!("{:<width$} : ", "", width = NAME_WIDTH),
											value_style,
										),
										Span::styled(vline, value_style),
									])
								}
							})
							.collect::<Vec<Line>>()
					})
					.collect();

				let table = Paragraph::new(lines).wrap(Wrap { trim: false });

				f.render_widget(table, chunks[0]);

				// status
				let status_text = format!(
					"Record {} of {} | ↑↓←→ hjkl | quit: q",
					*current_record + 1,
					total_records
				);
				let status =
					Paragraph::new(status_text).style(Style::default().fg(Color::DarkGray));
				f.render_widget(status, chunks[1]);
			})
			.map_err(crate::error::NailError::Io)?;

		// handle keys
		if let Event::Key(ke) = event::read().map_err(crate::error::NailError::Io)? {
			// Get schema for key handling
			let record_in_page = *current_record % page_size;
			let (batch_idx, _) = {
				let mut idx = record_in_page;
				let mut b_idx = 0;
				for b in current_batches.iter() {
					if idx < b.num_rows() {
						break;
					}
					idx -= b.num_rows();
					b_idx += 1;
				}
				(b_idx, idx)
			};

			if batch_idx >= current_batches.len() {
				continue; // Skip if no data available
			}

			let batch = &current_batches[batch_idx];
			let schema = batch.schema();

			match ke {
				KeyEvent {
					code: KeyCode::Char('q'),
					..
				}
				| KeyEvent {
					code: KeyCode::Esc, ..
				} => break,
				KeyEvent {
					code: KeyCode::Right,
					..
				}
				| KeyEvent {
					code: KeyCode::Char('l'),
					..
				} if *current_record + 1 < total_records => {
					*current_record += 1;
					*row_offset = 0;
				}
				KeyEvent {
					code: KeyCode::Left,
					..
				}
				| KeyEvent {
					code: KeyCode::Char('h'),
					..
				} if *current_record > 0 => {
					*current_record -= 1;
					*row_offset = 0;
				}
				KeyEvent {
					code: KeyCode::Down,
					..
				}
				| KeyEvent {
					code: KeyCode::Char('j'),
					..
				} => {
					let total_fields = schema.fields().len();
					if *row_offset + 1 < total_fields {
						*row_offset += 1;
					}
				}
				KeyEvent {
					code: KeyCode::Up, ..
				}
				| KeyEvent {
					code: KeyCode::Char('k'),
					..
				} if *row_offset > 0 => {
					*row_offset -= 1;
				}
				_ => {}
			}
		}
	}

	// restore terminal
	disable_raw_mode().map_err(crate::error::NailError::Io)?;
	execute!(terminal.backend_mut(), Show, LeaveAlternateScreen)
		.map_err(crate::error::NailError::Io)?;
	terminal.show_cursor().ok();

	Ok(())
}
