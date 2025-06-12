use clap::Args;
use datafusion::prelude::*;
use datafusion::common::DFSchemaRef;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct AppendArgs {
	#[arg(short, long, help = "Input file (base table)")]
	pub input: PathBuf,
	
	#[arg(long, help = "Files to append (comma-separated)")]
	pub files: String,
	
	#[arg(long, help = "Ignore schema mismatches")]
	pub ignore_schema: bool,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: AppendArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading base table from: {}", args.input.display());
	}
	
	let mut base_df = read_data(&args.input).await?;
	let base_schema: DFSchemaRef = base_df.schema().clone().into();
	
	let append_files: Vec<&str> = args.files.split(',').map(|s| s.trim()).collect();
	
	if args.verbose {
		eprintln!("Appending {} files", append_files.len());
	}
	
	for file_path in append_files {
		let path = PathBuf::from(file_path);
		
		if args.verbose {
			eprintln!("Appending: {}", path.display());
		}
		
		let append_df = read_data(&path).await?;
		let append_schema: DFSchemaRef = append_df.schema().clone().into();
		
		if !args.ignore_schema && !schemas_compatible(&base_schema, &append_schema) {
			return Err(NailError::InvalidArgument(format!(
				"Schema mismatch in file: {}. Use --ignore-schema to force append.",
				path.display()
			)));
		}
		
		let aligned_df = if args.ignore_schema {
			align_schemas(&append_df, &base_schema).await?
		} else {
			append_df
		};
		
		base_df = base_df.union(aligned_df)?;
	}
	
	if args.verbose {
		let total_rows = base_df.clone().count().await?;
		eprintln!("Final dataset contains {} rows", total_rows);
	}
	
	if let Some(output_path) = &args.output {
		let file_format = match args.format {
			Some(crate::cli::OutputFormat::Json) => Some(crate::utils::FileFormat::Json),
			Some(crate::cli::OutputFormat::Csv) => Some(crate::utils::FileFormat::Csv),
			Some(crate::cli::OutputFormat::Parquet) => Some(crate::utils::FileFormat::Parquet),
			_ => None,
		};
		write_data(&base_df, output_path, file_format.as_ref()).await?;
	} else {
		display_dataframe(&base_df, None, args.format.as_ref()).await?;
	}
	
	Ok(())
}

fn schemas_compatible(schema1: &datafusion::common::DFSchemaRef, schema2: &datafusion::common::DFSchemaRef) -> bool {
	if schema1.fields().len() != schema2.fields().len() {
		return false;
	}
	
	for (field1, field2) in schema1.fields().iter().zip(schema2.fields().iter()) {
		if field1.name() != field2.name() || field1.data_type() != field2.data_type() {
			return false;
		}
	}
	
	true
}

async fn align_schemas(df: &DataFrame, target_schema: &datafusion::common::DFSchemaRef) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context().await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let current_schema = df.schema();
	let mut select_exprs = Vec::new();
	
	for target_field in target_schema.fields() {
		let target_name = target_field.name();
		
		if let Ok(_current_field) = current_schema.field_with_name(None, target_name) {
			select_exprs.push(col(target_name));
		} else {
			let null_expr = match target_field.data_type() {
				datafusion::arrow::datatypes::DataType::Int64 => lit(0i64).alias(target_name),
				datafusion::arrow::datatypes::DataType::Float64 => lit(0.0f64).alias(target_name),
				datafusion::arrow::datatypes::DataType::Utf8 => lit("").alias(target_name),
				datafusion::arrow::datatypes::DataType::Boolean => lit(false).alias(target_name),
				_ => lit("").alias(target_name),
			};
			select_exprs.push(null_expr);
		}
	}
	
	let result = ctx.table(table_name).await?.select(select_exprs)?;
	Ok(result)
}