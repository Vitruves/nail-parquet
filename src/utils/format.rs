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
	level: usize,
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
					display_as_json(df, level).await?;
				}
				Some(OutputFormat::Text) | None => {
					if table_mode {
						display_as_columnar_table(df, level).await?;
					} else if is_correlation_matrix(df) {
						display_correlation_matrix(df).await?;
					} else {
						display_as_table(df, level).await?;
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

async fn display_as_json(df: &DataFrame, level: usize) -> NailResult<()> {
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
				let value = format_json_value(column, row_idx, field.data_type(), level);
				print!("\"{}\": {}", field.name(), value);
			}
			print!("}}");
		}
	}

	println!("\n]");
	Ok(())
}

async fn display_as_table(df: &DataFrame, level: usize) -> NailResult<()> {
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
					level,
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
				// The key/value separator colon is tinted with the field color so
				// wrapped continuation lines can be traced back to their column.
				let colon = format!("{}{}:{}", BOLD, field_color, RESET);

				// Nested containers (Struct / List-of-containers / Map) expand into
				// an indented multi-line tree, laid out within the value column so
				// it stays aligned with the rest of the card.
				if !column.is_null(row_idx) && type_expands(field.data_type(), level) {
					let tree = render_tree(column, row_idx, level, 0, content_width);
					if !tree.is_empty() {
						println!(
							"{}│{} {} {} {}{}{}",
							BORDER_COLOR, RESET, field_name, colon, field_color, tree[0], RESET
						);
						for line in &tree[1..] {
							println!(
								"{}│{} {:<width$} {} {}{}{}",
								BORDER_COLOR,
								RESET,
								"",
								colon,
								field_color,
								line,
								RESET,
								width = field_name_width
							);
						}
						continue;
					}
				}

				// Handle long values by wrapping them properly
				let wrapped_value = wrap_text_with_color(&value, content_width, field_color);
				let lines: Vec<&str> = wrapped_value.lines().collect();

				if lines.len() == 1 {
					println!(
						"{}│{} {} {} {}",
						BORDER_COLOR, RESET, field_name, colon, lines[0]
					);
				} else {
					println!(
						"{}│{} {} {} {}",
						BORDER_COLOR, RESET, field_name, colon, lines[0]
					);
					for line in &lines[1..] {
						println!(
							"{}│{} {:<width$} {} {}",
							BORDER_COLOR,
							RESET,
							"",
							colon,
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

async fn display_as_columnar_table(df: &DataFrame, level: usize) -> NailResult<()> {
	let batches = df.clone().collect().await?;
	let schema = df.schema();

	if batches.is_empty() {
		println!("{}No data to display{}", DIM, RESET);
		return Ok(());
	}

	let fields = schema.fields();
	let num_cols = fields.len();

	// Collect all cell values as plain strings (no ANSI).
	let mut columns_values: Vec<Vec<String>> = vec![Vec::new(); num_cols];
	for batch in &batches {
		for row_idx in 0..batch.num_rows() {
			for (col_idx, field) in fields.iter().enumerate() {
				let column = batch.column(col_idx);
				let val = format_cell_value_plain(column, row_idx, field.data_type(), level);
				// The grid is one physical line per cell; collapse any embedded
				// newlines/tabs (common in nested string leaves) to single spaces
				// so they can't break the row borders.
				let val = val.split_whitespace().collect::<Vec<_>>().join(" ");
				columns_values[col_idx].push(val);
			}
		}
	}

	let total_rows = columns_values.first().map(|c| c.len()).unwrap_or(0);

	// Per-column max content width (header name vs. widest cell), capped.
	const MAX_COL_CONTENT: usize = 30;
	let mut col_widths: Vec<usize> = Vec::with_capacity(num_cols);
	for (col_idx, field) in fields.iter().enumerate() {
		let header_len = field.name().chars().count();
		let max_cell = columns_values[col_idx]
			.iter()
			.map(|v| v.chars().count())
			.max()
			.unwrap_or(0);
		col_widths.push(header_len.max(max_cell).clamp(3, MAX_COL_CONTENT));
	}

	// Truncate cell strings down to the chosen column width.
	for col_idx in 0..num_cols {
		let w = col_widths[col_idx];
		for v in columns_values[col_idx].iter_mut() {
			*v = truncate_with_ellipsis(v, w);
		}
	}

	let terminal_width = if let Some((w, _)) = term_size::dimensions() {
		w.max(60)
	} else {
		120
	};

	// Row-index column repeats on every page block so rows stay readable
	// when columns are paginated horizontally.
	let row_idx_width = total_rows.to_string().len().max(1);

	// Split columns into pages that each fit terminal_width.
	// Page layout per column: "│ <content> ", where content is col_widths[i].
	// Plus closing "│" at end of the row, plus the row-index column "│ <idx> ".
	let row_idx_segment = row_idx_width + 3; // "│ <idx> "
	let trailing = 1; // closing "│"
	let pages = split_columns_into_pages(&col_widths, terminal_width, row_idx_segment, trailing);

	let total_pages = pages.len();
	let vbar = format!("{}│{}", BORDER_COLOR, RESET);

	for (page_idx, page) in pages.iter().enumerate() {
		// Segment widths for this page = row-idx col + each data col.
		let mut seg_widths: Vec<usize> = Vec::with_capacity(1 + page.len());
		seg_widths.push(row_idx_width);
		for &c in page {
			seg_widths.push(col_widths[c]);
		}
		let sep_parts: Vec<String> = seg_widths.iter().map(|w| "─".repeat(w + 2)).collect();
		let top_border = format!("{}┌{}┐{}", BORDER_COLOR, sep_parts.join("┬"), RESET);
		let mid_border = format!("{}├{}┤{}", BORDER_COLOR, sep_parts.join("┼"), RESET);
		let bot_border = format!("{}└{}┘{}", BORDER_COLOR, sep_parts.join("┴"), RESET);

		// Header row.
		let mut header_cells: Vec<String> = Vec::with_capacity(1 + page.len());
		header_cells.push(format!(
			" {}{}{:>width$}{} ",
			BOLD,
			DIM,
			"#",
			RESET,
			width = row_idx_width
		));
		for &col_idx in page {
			let color = FIELD_COLORS[col_idx % FIELD_COLORS.len()];
			header_cells.push(format!(
				" {}{}{:<width$}{} ",
				BOLD,
				color,
				truncate_with_ellipsis(fields[col_idx].name(), col_widths[col_idx]),
				RESET,
				width = col_widths[col_idx]
			));
		}
		let header_line = format!("{vbar}{}{vbar}", header_cells.join(&vbar));

		if page_idx > 0 {
			println!(
				"{}cols {}–{} of {} (page {}/{}){}",
				DIM,
				page.first().copied().unwrap_or(0) + 1,
				page.last().copied().unwrap_or(0) + 1,
				num_cols,
				page_idx + 1,
				total_pages,
				RESET
			);
		}
		println!("{}", top_border);
		println!("{}", header_line);
		println!("{}", mid_border);

		// row_idx indexes the per-column value vectors and the printed row number.
		#[allow(clippy::needless_range_loop)]
		for row_idx in 0..total_rows {
			let mut cells: Vec<String> = Vec::with_capacity(1 + page.len());
			cells.push(format!(
				" {}{:>width$}{} ",
				DIM,
				row_idx + 1,
				RESET,
				width = row_idx_width
			));
			for &col_idx in page {
				let color = FIELD_COLORS[col_idx % FIELD_COLORS.len()];
				let val = &columns_values[col_idx][row_idx];
				let cell = if val == "NULL" {
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
				};
				cells.push(cell);
			}
			println!("{vbar}{}{vbar}", cells.join(&vbar));
		}

		println!("{}", bot_border);
	}

	println!(
		"{}Total records: {}{}{} · columns: {}{}{}",
		DIM, BOLD, total_rows, RESET, BOLD, num_cols, RESET
	);

	Ok(())
}

fn truncate_with_ellipsis(s: &str, max_chars: usize) -> String {
	let n = s.chars().count();
	if n <= max_chars {
		return s.to_string();
	}
	if max_chars == 0 {
		return String::new();
	}
	if max_chars == 1 {
		return "…".to_string();
	}
	let take = max_chars - 1;
	let mut out: String = s.chars().take(take).collect();
	out.push('…');
	out
}

fn split_columns_into_pages(
	col_widths: &[usize],
	terminal_width: usize,
	row_idx_segment: usize,
	trailing: usize,
) -> Vec<Vec<usize>> {
	let mut pages: Vec<Vec<usize>> = Vec::new();
	let budget = terminal_width.saturating_sub(row_idx_segment + trailing);
	let mut current: Vec<usize> = Vec::new();
	let mut used: usize = 0;
	for (i, &w) in col_widths.iter().enumerate() {
		let seg = w + 3; // "│ <content> "
		if !current.is_empty() && used + seg > budget {
			pages.push(std::mem::take(&mut current));
			used = 0;
		}
		current.push(i);
		used += seg;
	}
	if !current.is_empty() {
		pages.push(current);
	}
	if pages.is_empty() {
		pages.push(Vec::new());
	}
	pages
}

fn format_cell_value_plain(
	column: &dyn Array,
	row_idx: usize,
	data_type: &DataType,
	level: usize,
) -> String {
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
			_ => render_nested(column, row_idx, level, NestMode::Text),
		}
	}
}

// Maximum number of elements rendered for a single list/map before the rest
// are summarized as "… +K more". Independent of the depth budget.
const NEST_MAX_ELEMS: usize = 20;

#[derive(Clone, Copy, PartialEq)]
enum NestMode {
	Text,
	Json,
}

/// Recursively render a nested value (List/Struct/Map/Binary) for one row up to
/// a depth budget. `depth` is the remaining number of container levels to
/// expand: at 0 a container collapses to a compact type tag. Binary always
/// renders as a byte-count summary regardless of depth so large blobs (e.g.
/// image bytes) are never dumped. Scalar leaves are delegated to Arrow's
/// `ArrayFormatter`, which covers timestamps, decimals, dictionaries, etc.
fn render_nested(array: &dyn Array, row: usize, depth: usize, mode: NestMode) -> String {
	if array.is_null(row) {
		return "null".to_string();
	}
	match array.data_type() {
		DataType::Struct(fields) => {
			let sa = match array.as_any().downcast_ref::<StructArray>() {
				Some(s) => s,
				None => return render_leaf(array, row, mode),
			};
			if depth == 0 {
				return collapsed_tag(format!("…{} fields", fields.len()), '{', '}', mode);
			}
			let parts: Vec<String> = fields
				.iter()
				.enumerate()
				.map(|(i, f)| {
					let v = render_nested(sa.column(i).as_ref(), row, depth - 1, mode);
					match mode {
						NestMode::Json => format!("{}: {}", json_quote(f.name()), v),
						NestMode::Text => format!("{}: {}", f.name(), v),
					}
				})
				.collect();
			format!("{{{}}}", parts.join(", "))
		}
		DataType::List(_) => match array.as_any().downcast_ref::<ListArray>() {
			Some(la) => render_seq(la.value(row).as_ref(), depth, mode),
			None => render_leaf(array, row, mode),
		},
		DataType::LargeList(_) => match array.as_any().downcast_ref::<LargeListArray>() {
			Some(la) => render_seq(la.value(row).as_ref(), depth, mode),
			None => render_leaf(array, row, mode),
		},
		DataType::FixedSizeList(_, _) => {
			match array.as_any().downcast_ref::<FixedSizeListArray>() {
				Some(la) => render_seq(la.value(row).as_ref(), depth, mode),
				None => render_leaf(array, row, mode),
			}
		}
		DataType::Map(_, _) => match array.as_any().downcast_ref::<MapArray>() {
			Some(ma) => {
				if depth == 0 {
					return collapsed_tag(
						format!("…{} entries", ma.value(row).len()),
						'{',
						'}',
						mode,
					);
				}
				let entries = ma.value(row); // StructArray of {key, value}
				let n = entries.len();
				let parts: Vec<String> = (0..n)
					.map(|i| {
						let k = render_nested(entries.column(0).as_ref(), i, depth - 1, mode);
						let v = render_nested(entries.column(1).as_ref(), i, depth - 1, mode);
						format!("{}: {}", k, v)
					})
					.collect();
				format!("{{{}}}", parts.join(", "))
			}
			None => render_leaf(array, row, mode),
		},
		DataType::Binary => match array.as_any().downcast_ref::<BinaryArray>() {
			Some(a) => byte_summary(a.value(row).len(), mode),
			None => render_leaf(array, row, mode),
		},
		DataType::LargeBinary => match array.as_any().downcast_ref::<LargeBinaryArray>() {
			Some(a) => byte_summary(a.value(row).len(), mode),
			None => render_leaf(array, row, mode),
		},
		DataType::FixedSizeBinary(_) => {
			match array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
				Some(a) => byte_summary(a.value(row).len(), mode),
				None => render_leaf(array, row, mode),
			}
		}
		_ => render_leaf(array, row, mode),
	}
}

/// Produce the plain value-column lines for one field, laid out exactly like the
/// card view: an indented tree for expanded containers, otherwise the inline
/// value wrapped to `width`. The caller is responsible for the field-name column
/// and any coloring. Shared by the interactive preview so it matches the card.
pub(crate) fn render_field_value_lines(
	column: &dyn Array,
	row: usize,
	data_type: &DataType,
	level: usize,
	width: usize,
) -> Vec<String> {
	if column.is_null(row) {
		return vec!["NULL".to_string()];
	}
	if type_expands(data_type, level) {
		let tree = render_tree(column, row, level, 0, width);
		if !tree.is_empty() {
			return tree;
		}
	}
	let value = format_cell_value_with_field_color(column, row, data_type, "", level);
	wrap_label_value("", &value, width)
}

/// Render the elements of one list row: `[e0, e1, …]`, capped at NEST_MAX_ELEMS.
/// `depth` is the parent container's budget; at 0 the list collapses, otherwise
/// elements are rendered at `depth - 1` (the list itself consumed one level).
fn render_seq(values: &dyn Array, depth: usize, mode: NestMode) -> String {
	let n = values.len();
	if depth == 0 {
		return collapsed_tag(format!("…{} items", n), '[', ']', mode);
	}
	let shown = n.min(NEST_MAX_ELEMS);
	let mut parts: Vec<String> = (0..shown)
		.map(|i| render_nested(values, i, depth - 1, mode))
		.collect();
	if n > shown {
		let more = format!("… +{} more", n - shown);
		parts.push(match mode {
			NestMode::Json => json_quote(&more),
			NestMode::Text => more,
		});
	}
	format!("[{}]", parts.join(", "))
}

/// A collapsed container placeholder, e.g. `{…2 fields}` or `[…3 items]`.
/// In JSON mode it is emitted as a quoted string so the output stays valid.
fn collapsed_tag(inner: String, open: char, close: char, mode: NestMode) -> String {
	let tag = format!("{}{}{}", open, inner, close);
	match mode {
		NestMode::Json => json_quote(&tag),
		NestMode::Text => tag,
	}
}

fn byte_summary(len: usize, mode: NestMode) -> String {
	let s = format!("<{} bytes>", len);
	match mode {
		NestMode::Json => json_quote(&s),
		NestMode::Text => s,
	}
}

fn json_quote(s: &str) -> String {
	let mut out = String::with_capacity(s.len() + 2);
	out.push('"');
	for ch in s.chars() {
		match ch {
			'"' => out.push_str("\\\""),
			'\\' => out.push_str("\\\\"),
			'\n' => out.push_str("\\n"),
			'\r' => out.push_str("\\r"),
			'\t' => out.push_str("\\t"),
			c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
			c => out.push(c),
		}
	}
	out.push('"');
	out
}

/// Render a scalar leaf value. Text mode delegates to Arrow's `ArrayFormatter`
/// (covers timestamps, decimals, dictionaries, …). JSON mode emits typed JSON:
/// numbers/bools bare, everything else quoted.
fn render_leaf(array: &dyn Array, row: usize, mode: NestMode) -> String {
	use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

	let text = match ArrayFormatter::try_new(array, &FormatOptions::default()) {
		Ok(fmt) => fmt.value(row).to_string(),
		Err(_) => {
			return match mode {
				NestMode::Json => "null".to_string(),
				NestMode::Text => "?".to_string(),
			}
		}
	};

	match mode {
		NestMode::Text => text,
		NestMode::Json => match array.data_type() {
			DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64 => text,
			DataType::Float16 | DataType::Float32 | DataType::Float64 => {
				if text.parse::<f64>().map(|v| v.is_finite()).unwrap_or(false) {
					text
				} else {
					"null".to_string()
				}
			}
			DataType::Boolean => text,
			_ => json_quote(&text),
		},
	}
}

/// Whether a value of this type, given a `depth` budget, should be rendered as
/// a multi-line indented block (vs. a compact inline string). Purely type- and
/// depth-driven: a list only expands when its elements would themselves expand,
/// so a vector of scalars stays inline as `[1, 2, 3]`.
fn type_expands(dt: &DataType, depth: usize) -> bool {
	if depth == 0 {
		return false;
	}
	match dt {
		DataType::Struct(fields) => !fields.is_empty(),
		DataType::Map(_, _) => true,
		DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
			type_expands(f.data_type(), depth - 1)
		}
		_ => false,
	}
}

/// Render a nested value as indented, multi-line "tree" lines for the card view.
/// `indent` is the leading space count for this level; `width` is the usable
/// line width. Containers whose children also expand recurse deeper; scalar
/// children are rendered inline and wrapped with a hanging indent.
fn render_tree(
	array: &dyn Array,
	row: usize,
	depth: usize,
	indent: usize,
	width: usize,
) -> Vec<String> {
	let pad = " ".repeat(indent);
	match array.data_type() {
		DataType::Struct(fields) => {
			let sa = match array.as_any().downcast_ref::<StructArray>() {
				Some(s) => s,
				None => return vec![render_nested(array, row, depth, NestMode::Text)],
			};
			let key_width = fields
				.iter()
				.map(|f| f.name().chars().count())
				.max()
				.unwrap_or(0)
				.min(20);
			let mut lines = Vec::new();
			for (i, f) in fields.iter().enumerate() {
				let child = sa.column(i).as_ref();
				if !child.is_null(row) && type_expands(f.data_type(), depth.saturating_sub(1)) {
					lines.push(format!("{}{}:", pad, f.name()));
					lines.extend(render_tree(child, row, depth - 1, indent + 2, width));
				} else {
					let label = format!("{}{:<kw$} : ", pad, f.name(), kw = key_width);
					let val = render_nested(child, row, depth.saturating_sub(1), NestMode::Text);
					lines.extend(wrap_label_value(&label, &val, width));
				}
			}
			lines
		}
		DataType::List(f) => render_list_tree(array, row, depth, indent, width, f.data_type()),
		DataType::LargeList(f) => render_list_tree(array, row, depth, indent, width, f.data_type()),
		DataType::FixedSizeList(f, _) => {
			render_list_tree(array, row, depth, indent, width, f.data_type())
		}
		DataType::Map(_, _) => match array.as_any().downcast_ref::<MapArray>() {
			Some(ma) => {
				let entries = ma.value(row);
				let mut lines = Vec::new();
				for i in 0..entries.len() {
					let k = render_nested(entries.column(0).as_ref(), i, 0, NestMode::Text);
					let vchild = entries.column(1);
					if !vchild.is_null(i)
						&& type_expands(vchild.data_type(), depth.saturating_sub(1))
					{
						lines.push(format!("{}{}:", pad, k));
						lines.extend(render_tree(
							vchild.as_ref(),
							i,
							depth - 1,
							indent + 2,
							width,
						));
					} else {
						let label = format!("{}{} : ", pad, k);
						let val = render_nested(
							vchild.as_ref(),
							i,
							depth.saturating_sub(1),
							NestMode::Text,
						);
						lines.extend(wrap_label_value(&label, &val, width));
					}
				}
				lines
			}
			None => vec![render_nested(array, row, depth, NestMode::Text)],
		},
		_ => vec![render_nested(array, row, depth, NestMode::Text)],
	}
}

fn render_list_tree(
	array: &dyn Array,
	row: usize,
	depth: usize,
	indent: usize,
	width: usize,
	elem_type: &DataType,
) -> Vec<String> {
	let values: ArrayRef = if let Some(la) = array.as_any().downcast_ref::<ListArray>() {
		la.value(row)
	} else if let Some(la) = array.as_any().downcast_ref::<LargeListArray>() {
		la.value(row)
	} else if let Some(la) = array.as_any().downcast_ref::<FixedSizeListArray>() {
		la.value(row)
	} else {
		return vec![render_nested(array, row, depth, NestMode::Text)];
	};
	let n = values.len();
	let shown = n.min(NEST_MAX_ELEMS);
	// A single-element list reads cleaner without a bullet; multi-element lists
	// keep "- " markers so element boundaries stay visible.
	let bullets = n > 1;
	let child_indent = if bullets { indent + 2 } else { indent };
	let mut lines = Vec::new();
	for idx in 0..shown {
		if !values.is_null(idx) && type_expands(elem_type, depth.saturating_sub(1)) {
			let mut block = render_tree(values.as_ref(), idx, depth - 1, child_indent, width);
			if bullets {
				// Pull the element's first line out to a "- " bullet.
				if let Some(first) = block.first_mut() {
					if first.chars().count() >= indent + 2 {
						first.replace_range(indent..indent + 2, "- ");
					}
				}
			}
			lines.extend(block);
		} else {
			let label = if bullets {
				format!("{}- ", " ".repeat(indent))
			} else {
				" ".repeat(indent)
			};
			let val = render_nested(
				values.as_ref(),
				idx,
				depth.saturating_sub(1),
				NestMode::Text,
			);
			lines.extend(wrap_label_value(&label, &val, width));
		}
	}
	if n > shown {
		lines.push(format!("{}… +{} more", " ".repeat(indent), n - shown));
	}
	lines
}

/// Wrap `value` after `label`, aligning continuation lines under the value start
/// (hanging indent). Whitespace in `value` is normalized to single spaces.
fn wrap_label_value(label: &str, value: &str, width: usize) -> Vec<String> {
	let label_w = label.chars().count();
	let avail = width.saturating_sub(label_w).max(10);
	let normalized: String = value
		.replace(['\n', '\r', '\t'], " ")
		.split_whitespace()
		.collect::<Vec<_>>()
		.join(" ");
	if normalized.is_empty() {
		return vec![label.to_string()];
	}
	let mut chunks: Vec<String> = Vec::new();
	let mut cur = String::new();
	let mut cur_w = 0usize;
	for word in normalized.split(' ') {
		let w = word.chars().count();
		if cur.is_empty() {
			cur = word.to_string();
			cur_w = w;
		} else if cur_w + 1 + w <= avail {
			cur.push(' ');
			cur.push_str(word);
			cur_w += 1 + w;
		} else {
			chunks.push(std::mem::take(&mut cur));
			cur = word.to_string();
			cur_w = w;
		}
	}
	if !cur.is_empty() {
		chunks.push(cur);
	}
	let hang = " ".repeat(label_w);
	chunks
		.into_iter()
		.enumerate()
		.map(|(i, c)| {
			if i == 0 {
				format!("{}{}", label, c)
			} else {
				format!("{}{}", hang, c)
			}
		})
		.collect()
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
	level: usize,
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
			_ => render_nested(column, row_idx, level, NestMode::Text),
		};
		// Don't apply color here - it will be applied during wrapping
		value
	}
}

fn format_json_value(
	column: &dyn Array,
	row_idx: usize,
	data_type: &DataType,
	level: usize,
) -> String {
	if column.is_null(row_idx) {
		"null".to_string()
	} else {
		match data_type {
			DataType::Utf8 => {
				if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
					json_quote(array.value(row_idx))
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
			_ => render_nested(column, row_idx, level, NestMode::Json),
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

	if batches.is_empty() {
		println!("{}No correlation data to display{}", DIM, RESET);
		return Ok(());
	}

	// Collect row variable names and correlation values.
	// The matrix is square: row variable names in order ARE the column headers,
	// so we derive the headers from the rows rather than the munged
	// `corr_with_<...>` schema column names.
	let mut row_names: Vec<String> = Vec::new();
	let mut values: Vec<Vec<Option<f64>>> = Vec::new();

	for batch in &batches {
		let var_column = batch.column(0);
		for row_idx in 0..batch.num_rows() {
			let var_name =
				if let Some(string_array) = var_column.as_any().downcast_ref::<StringArray>() {
					string_array.value(row_idx).to_string()
				} else {
					format!("row_{}", row_names.len())
				};
			row_names.push(var_name);

			let mut row_vals: Vec<Option<f64>> = Vec::with_capacity(batch.num_columns() - 1);
			for col_idx in 1..batch.num_columns() {
				let column = batch.column(col_idx);
				if column.is_null(row_idx) {
					row_vals.push(None);
				} else if let Some(a) = column.as_any().downcast_ref::<Float64Array>() {
					row_vals.push(Some(a.value(row_idx)));
				} else if let Some(a) = column.as_any().downcast_ref::<Float32Array>() {
					row_vals.push(Some(a.value(row_idx) as f64));
				} else if let Some(a) = column.as_any().downcast_ref::<Int64Array>() {
					row_vals.push(Some(a.value(row_idx) as f64));
				} else if let Some(a) = column.as_any().downcast_ref::<Int32Array>() {
					row_vals.push(Some(a.value(row_idx) as f64));
				} else {
					let debug_str = format!("{:?}", column.slice(row_idx, 1));
					row_vals.push(extract_numeric_from_debug(&debug_str).parse::<f64>().ok());
				}
			}
			values.push(row_vals);
		}
	}

	let n = row_names.len();
	if n == 0 {
		return display_as_table(df, 1).await; // correlation data is numeric; depth is irrelevant
	}

	// Headers come from row labels (square matrix).
	let headers: Vec<String> = row_names.clone();

	// Format every cell as a string first so we can compute widths.
	let cell_strings: Vec<Vec<String>> = values
		.iter()
		.map(|row| {
			row.iter()
				.map(|v| match v {
					None => "NULL".to_string(),
					Some(val) => format_float_trimmed(*val),
				})
				.collect()
		})
		.collect();

	// Variable (row label) column width.
	let var_col_width = row_names.iter().map(|s| s.len()).max().unwrap_or(8).max(8);

	// Per-column data widths: max of header name and any cell value, min 6.
	let mut data_col_widths: Vec<usize> = Vec::with_capacity(headers.len());
	for (i, h) in headers.iter().enumerate() {
		let mut w = h.len();
		for row in &cell_strings {
			if let Some(c) = row.get(i) {
				w = w.max(c.len());
			}
		}
		data_col_widths.push(w.max(6));
	}

	// Build borders: first segment is the row-label column, then each data column.
	let mut seg_widths: Vec<usize> = Vec::with_capacity(1 + data_col_widths.len());
	seg_widths.push(var_col_width);
	seg_widths.extend(data_col_widths.iter().copied());
	let sep_parts: Vec<String> = seg_widths.iter().map(|w| "─".repeat(w + 2)).collect();
	let top_border = format!("{}┌{}┐{}", BORDER_COLOR, sep_parts.join("┬"), RESET);
	let mid_border = format!("{}├{}┤{}", BORDER_COLOR, sep_parts.join("┼"), RESET);
	let bot_border = format!("{}└{}┘{}", BORDER_COLOR, sep_parts.join("┴"), RESET);
	let vbar = format!("{}│{}", BORDER_COLOR, RESET);

	// Header row: empty top-left cell, then header names right-aligned.
	let mut header_cells: Vec<String> = vec![format!(" {:<width$} ", "", width = var_col_width)];
	for (i, h) in headers.iter().enumerate() {
		let color = FIELD_COLORS[i % FIELD_COLORS.len()];
		header_cells.push(format!(
			" {}{}{:>width$}{} ",
			BOLD,
			color,
			h,
			RESET,
			width = data_col_widths[i]
		));
	}
	let header_line = format!("{vbar}{}{vbar}", header_cells.join(&vbar));

	println!("{}", top_border);
	println!("{}", header_line);
	println!("{}", mid_border);

	for (r, row_name) in row_names.iter().enumerate() {
		let row_color = FIELD_COLORS[r % FIELD_COLORS.len()];
		let mut cells: Vec<String> = Vec::with_capacity(1 + data_col_widths.len());
		cells.push(format!(
			" {}{}{:<width$}{} ",
			BOLD,
			row_color,
			row_name,
			RESET,
			width = var_col_width
		));
		for (c, w) in data_col_widths.iter().enumerate() {
			let s = &cell_strings[r][c];
			let cell = if s == "NULL" {
				format!(" {}{:>width$}{} ", NULL_COLOR, s, RESET, width = w)
			} else {
				let val = values[r][c].unwrap_or(0.0);
				let color = corr_color(val, r == c);
				format!(" {}{:>width$}{} ", color, s, RESET, width = w)
			};
			cells.push(cell);
		}
		println!("{vbar}{}{vbar}", cells.join(&vbar));
	}

	println!("{}", bot_border);
	Ok(())
}

fn corr_color(val: f64, is_diagonal: bool) -> &'static str {
	if is_diagonal {
		return "\x1b[1;37m"; // bold white for diagonal (=1.0)
	}
	let a = val.abs();
	if val >= 0.0 {
		if a >= 0.7 {
			"\x1b[1;32m" // bold green: strong positive
		} else if a >= 0.3 {
			"\x1b[32m" // green: moderate positive
		} else {
			"\x1b[2;32m" // dim green: weak positive
		}
	} else if a >= 0.7 {
		"\x1b[1;31m" // bold red: strong negative
	} else if a >= 0.3 {
		"\x1b[31m" // red: moderate negative
	} else {
		"\x1b[2;31m" // dim red: weak negative
	}
}
