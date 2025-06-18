use clap::Args;

use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;

// ANSI color codes (matching format.rs style)
const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const BORDER_COLOR: &str = "\x1b[2;90m"; // Dim gray

// Field colors for schema fields
const FIELD_COLORS: &[&str] = &[
	"\x1b[32m",   // Green
	"\x1b[33m",   // Yellow
	"\x1b[34m",   // Blue
	"\x1b[35m",   // Magenta
	"\x1b[36m",   // Cyan
	"\x1b[91m",   // Bright red
];

#[derive(Args, Clone)]
pub struct SchemaArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: SchemaArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading schema from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let schema = df.schema();
	
	let schema_info: Vec<SchemaField> = schema.fields().iter()
		.map(|field| SchemaField {
			name: field.name().clone(),
			data_type: format!("{:?}", field.data_type()),
			nullable: field.is_nullable(),
		})
		.collect();
	
	if args.verbose {
		eprintln!("Schema contains {} fields", schema_info.len());
	}
	
	match &args.output {
		Some(output_path) => {
			let content = match args.format {
				Some(crate::cli::OutputFormat::Json) => {
					serde_json::to_string_pretty(&schema_info)?
				},
				_ => {
					format_schema_as_table(&schema_info)
				}
			};
			std::fs::write(output_path, content)?;
		},
		None => {
			match args.format {
				Some(crate::cli::OutputFormat::Json) => {
					println!("{}", serde_json::to_string_pretty(&schema_info)?);
				},
				_ => {
					print!("{}", format_schema_as_table(&schema_info));
				}
			}
		},
	}
	
	Ok(())
}

fn format_schema_as_table(schema_info: &[SchemaField]) -> String {
	let mut output = String::new();
	
	// Get terminal width for proper formatting
	let terminal_width = if let Some((w, _)) = term_size::dimensions() {
		w.max(60).min(120)
	} else {
		80
	};
	
	// Schema header
	let header_text = " Parquet Schema ";
	let remaining_width = terminal_width.saturating_sub(header_text.len() + 4);
	let left_dashes = remaining_width / 2;
	let right_dashes = remaining_width - left_dashes;
	
	output.push_str(&format!("{}┌{}{}{}{}",
		BORDER_COLOR,
		"─".repeat(left_dashes),
		header_text,
		"─".repeat(right_dashes),
		RESET
	));
	output.push('\n');
	output.push_str(&format!("{}│{}", BORDER_COLOR, RESET));
	output.push('\n');
	
	// Field information
	let field_name_width = 20;
	for (idx, field) in schema_info.iter().enumerate() {
		let field_color = FIELD_COLORS[idx % FIELD_COLORS.len()];
		
		// Field name
		let field_name = format!("{}{}{:<width$}{}", BOLD, field_color, field.name, RESET, width = field_name_width);
		
		// Type and nullable info
		let type_info = format!("{}{}{}", field_color, field.data_type, RESET);
		let nullable_info = if field.nullable {
			format!("{}nullable{}", DIM, RESET)
		} else {
			format!("{}NOT NULL{}", BOLD, RESET)
		};
		
		output.push_str(&format!("{}│{} {} : {} ({})", 
			BORDER_COLOR, RESET, field_name, type_info, nullable_info));
		output.push('\n');
	}
	
	// Schema footer
	output.push_str(&format!("{}│{}", BORDER_COLOR, RESET));
	output.push('\n');
	output.push_str(&format!("{}└{}{}", BORDER_COLOR, "─".repeat(terminal_width.saturating_sub(2)), RESET));
	output.push('\n');
	
	// Summary
	output.push_str(&format!("{}Total fields: {}{}{}", DIM, BOLD, schema_info.len(), RESET));
	output.push('\n');
	
	output
}

#[derive(serde::Serialize)]
struct SchemaField {
	name: String,
	data_type: String,
	nullable: bool,
}