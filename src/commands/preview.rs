use clap::Args;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand::rngs::StdRng;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::format::display_dataframe;
use crate::cli::CommonArgs;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    cursor::{Hide, Show},
};
use datafusion::arrow::{
    array::*,
    datatypes::DataType,
};
use datafusion::prelude::*;
use ratatui::{
    Terminal, 
    backend::CrosstermBackend, 
    widgets::{Paragraph, Wrap}, 
    layout::{Layout, Constraint, Direction}, 
    style::{Style, Color, Modifier},
    text::{Line, Span}
};
use std::io;

#[derive(Args, Clone)]
pub struct PreviewArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    
    #[arg(short, long, help = "Number of rows to display", default_value = "5")]
    pub number: usize,
    
    #[arg(short = 'I', long, help = "Interactive mode with scrolling (use arrow keys, q to quit)")]
    pub interactive: bool,
}

pub async fn execute(args: PreviewArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
    
    let df = read_data(&args.common.input).await?;
    let total_rows = df.clone().count().await?;
    
    // If interactive mode is requested, handle it separately
    if args.interactive {
        return execute_interactive(args, df, total_rows).await;
    }
    
    // Non-interactive mode (original behavior)
    if total_rows <= args.number {
        display_dataframe(&df, args.common.output.as_deref(), args.common.format.as_ref()).await?;
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
    
    args.common.log_if_verbose(&format!("Randomly sampling {} rows from {} total rows", args.number, total_rows));
    
    let ctx = crate::utils::create_context_with_jobs(args.common.jobs).await?;
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
    
    args.common.log_if_verbose(&format!("Executing SQL: {}", sql));
    
    let result = ctx.sql(&sql).await?;
    
    display_dataframe(&result, args.common.output.as_deref(), args.common.format.as_ref()).await?;
    
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
    let mut row_offset = 0;
    
    // Setup terminal
    enable_raw_mode().map_err(|e| crate::error::NailError::Io(e))?;
    execute!(io::stdout(), EnterAlternateScreen, Hide).map_err(|e| crate::error::NailError::Io(e))?;
    
    let result = run_ratatui_viewer(&batches, total_records, &mut current_record, &mut row_offset);
    
    // Cleanup terminal
    execute!(io::stdout(), Show, LeaveAlternateScreen).map_err(|e| crate::error::NailError::Io(e))?;
    disable_raw_mode().map_err(|e| crate::error::NailError::Io(e))?;
    
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

fn run_ratatui_viewer(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    total_records: usize,
    current_record: &mut usize,
    row_offset: &mut usize,
) -> NailResult<()> {
    let mut stdout = io::stdout();
    enable_raw_mode().map_err(|e| crate::error::NailError::Io(e))?;
    execute!(stdout, EnterAlternateScreen, Hide).map_err(|e| crate::error::NailError::Io(e))?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).map_err(|e| crate::error::NailError::Io(e))?;

    loop {
        terminal.draw(|f| {
            let size = f.size();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(0),    // table
                    Constraint::Length(1), // status
                ])
                .split(size);

            // build table rows for current record
            let (batch_idx, row_idx) = {
                let mut idx = *current_record;
                let mut b_idx = 0;
                for b in batches {
                    if idx < b.num_rows() { break; }
                    idx -= b.num_rows();
                    b_idx += 1;
                }
                (b_idx, idx)
            };
            let batch = &batches[batch_idx];
            let schema = batch.schema();
            let content_iter = schema.fields().iter().enumerate().skip(*row_offset);
            let lines: Vec<Line> = content_iter.map(|(col_idx, field)| {
                let column = batch.column(col_idx);
                let is_null = column.is_null(row_idx);
                let raw_value = if is_null {
                    "NULL".to_string()
                } else {
                    format_cell_value_for_interactive(column, row_idx, field.data_type())
                };
                
                let field_color = FIELD_COLORS_TUI[col_idx % FIELD_COLORS_TUI.len()];
                let field_span = Span::styled(format!("{}: ", field.name()), Style::default().fg(field_color).add_modifier(Modifier::BOLD));
                
                let value_style = if is_null {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default().fg(Color::White)
                };
                let value_span = Span::styled(raw_value, value_style);
                
                Line::from(vec![field_span, value_span])
            }).collect();
            
            let table = Paragraph::new(lines)
                .wrap(Wrap { trim: true });
                
            f.render_widget(table, chunks[0]);

            // status
            let status_text = format!("Record {} of {} | ↑↓←→ hjkl | quit: q", *current_record + 1, total_records);
            let status = Paragraph::new(status_text).style(Style::default().fg(Color::DarkGray));
            f.render_widget(status, chunks[1]);
        }).map_err(|e| crate::error::NailError::Io(e))?;

        // handle keys
        if let Event::Key(ke) = event::read().map_err(|e| crate::error::NailError::Io(e))? {
            // Get schema for key handling
            let (batch_idx, _) = {
                let mut idx = *current_record;
                let mut b_idx = 0;
                for b in batches {
                    if idx < b.num_rows() { break; }
                    idx -= b.num_rows();
                    b_idx += 1;
                }
                (b_idx, idx)
            };
            let batch = &batches[batch_idx];
            let schema = batch.schema();
            
            match ke {
                KeyEvent { code: KeyCode::Char('q'), .. } | KeyEvent { code: KeyCode::Esc, .. } => break,
                KeyEvent { code: KeyCode::Right, .. } | KeyEvent { code: KeyCode::Char('l'), .. } => {
                    if *current_record + 1 < total_records { *current_record += 1; *row_offset = 0; }
                }
                KeyEvent { code: KeyCode::Left, .. } | KeyEvent { code: KeyCode::Char('h'), .. } => {
                    if *current_record > 0 { *current_record -= 1; *row_offset = 0; }
                }
                KeyEvent { code: KeyCode::Down, .. } | KeyEvent { code: KeyCode::Char('j'), .. } => {
                    let total_fields = schema.fields().len();
                    if *row_offset + 1 < total_fields { *row_offset += 1; }
                }
                KeyEvent { code: KeyCode::Up, .. } | KeyEvent { code: KeyCode::Char('k'), .. } => {
                    if *row_offset > 0 { *row_offset -= 1; }
                }
                _ => {}
            }
        }
    }

    // restore terminal
    disable_raw_mode().map_err(|e| crate::error::NailError::Io(e))?;
    execute!(terminal.backend_mut(), Show, LeaveAlternateScreen).map_err(|e| crate::error::NailError::Io(e))?;
    terminal.show_cursor().ok();

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
