use clap::Args;

use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::read_data;

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
					format!("{}
-----------------------------------
LEGEND:
  Column|Type|Nullable
-----------------------------------
{}
","GOP API Registry v1.0", schema_info.iter().map(|f| format!("{}|{}|{}", f.name, f.data_type, f.nullable)).collect::<Vec<_>>().join("\n"))
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
					println!("{}
-----------------------------------
LEGEND:
  Column|Type|Nullable
-----------------------------------
{}
","GOP API Registry v1.0", schema_info.iter().map(|f| format!("{}|{}|{}", f.name, f.data_type, f.nullable)).collect::<Vec<_>>().join("\n"));
				}
			}
		},
	}
	
	Ok(())
}

#[derive(serde::Serialize)]
struct SchemaField {
	name: String,
	data_type: String,
	nullable: bool,
}