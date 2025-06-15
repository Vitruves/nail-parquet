use clap::Args;
use std::path::PathBuf;
use regex::Regex;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;

#[derive(Args, Clone)]
pub struct HeadersArgs {
	#[arg(help = "Input file")]
	pub input: PathBuf,
	
	#[arg(long, help = "Filter headers with regex pattern")]
	pub filter: Option<String>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: HeadersArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading schema from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let schema = df.schema();
	let field_names: Vec<String> = schema.fields().iter()
		.map(|f| f.name().clone())
		.collect();
	
	let filtered_names = if let Some(pattern) = &args.filter {
		let regex = Regex::new(pattern)
			.map_err(|e| NailError::InvalidArgument(format!("Invalid regex pattern: {}", e)))?;
		
		field_names.into_iter()
			.filter(|name| regex.is_match(name))
			.collect()
	} else {
		field_names
	};
	
	if args.verbose {
		eprintln!("Found {} headers", filtered_names.len());
	}
	
	match &args.output {
		Some(output_path) => {
			let content = match args.format {
				Some(crate::cli::OutputFormat::Json) => {
					serde_json::to_string_pretty(&filtered_names)?
				},
				_ => filtered_names.join("\n"),
			};
			std::fs::write(output_path, content)?;
		},
		None => {
			for name in filtered_names {
				println!("{}", name);
			}
		},
	}
	
	Ok(())
}