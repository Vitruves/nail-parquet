use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::NailResult;
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct IdArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(long, help = "Create new ID column")]
	pub create: bool,
	
	#[arg(long, help = "Prefix for ID values", default_value = "id")]
	pub prefix: String,
	
	#[arg(long, help = "ID column name", default_value = "id")]
	pub id_col_name: String,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: IdArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	
	let result_df = if args.create {
		if args.verbose {
			eprintln!("Creating ID column '{}' with prefix '{}'", args.id_col_name, args.prefix);
		}
		add_id_column(&df, &args.id_col_name, &args.prefix).await?
	} else {
		df
	};
	
	if let Some(output_path) = &args.output {
		let file_format = match args.format {
			Some(crate::cli::OutputFormat::Json) => Some(crate::utils::FileFormat::Json),
			Some(crate::cli::OutputFormat::Csv) => Some(crate::utils::FileFormat::Csv),
			Some(crate::cli::OutputFormat::Parquet) => Some(crate::utils::FileFormat::Parquet),
			_ => None,
		};
		write_data(&result_df, output_path, file_format.as_ref()).await?;
	} else {
		display_dataframe(&result_df, None, args.format.as_ref()).await?;
	}
	
	Ok(())
}

async fn add_id_column(df: &DataFrame, col_name: &str, prefix: &str) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	// Check if column already exists
	let schema = df.schema();
	if schema.field_with_name(None, col_name).is_ok() {
		return Err(crate::error::NailError::InvalidArgument(
			format!("Column '{}' already exists. Use --id-col-name to specify a different name.", col_name)
		));
	}
	
	let id_col = if prefix.is_empty() {
		"ROW_NUMBER() OVER()".to_string()
	} else {
		format!("CONCAT('{}', ROW_NUMBER() OVER())", prefix)
	};
	
	let columns: Vec<String> = df.schema().fields().iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();
	
	let sql = format!(
		"SELECT {} as \"{}\", {} FROM {}",
		id_col,
		col_name,
		columns.join(", "),
		table_name
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}