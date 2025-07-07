use clap::Args;
use datafusion::prelude::*;
use datafusion::common::DFSchemaRef;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct AppendArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(long, help = "Files to append (comma-separated)")]
	pub files: String,
	
	#[arg(long, help = "Ignore schema mismatches")]
	pub ignore_schema: bool,
}

pub async fn execute(args: AppendArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading base table from: {}", args.common.input.display()));
	
	let mut base_df = read_data(&args.common.input).await?;
	let base_schema: DFSchemaRef = base_df.schema().clone().into();
	
	let append_files: Vec<&str> = args.files.split(',').map(|s| s.trim()).collect();
	
	args.common.log_if_verbose(&format!("Appending {} files", append_files.len()));
	
	for file_path in append_files {
		let path = PathBuf::from(file_path);
		
		args.common.log_if_verbose(&format!("Appending: {}", path.display()));
		
		let append_df = read_data(&path).await?;
		let append_schema: DFSchemaRef = append_df.schema().clone().into();
		
		if !args.ignore_schema && !schemas_compatible(&base_schema, &append_schema) {
			return Err(NailError::InvalidArgument(format!(
				"Schema mismatch in file: {}. Use --ignore-schema to force append.",
				path.display()
			)));
		}
		
		let aligned_df = if args.ignore_schema {
			align_schemas(&append_df, &base_schema, args.common.jobs).await?
		} else {
			append_df
		};
		
		base_df = base_df.union(aligned_df)?;
	}
	
	let total_rows = base_df.clone().count().await?;
	args.common.log_if_verbose(&format!("Final dataset contains {} rows", total_rows));
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&base_df, "append").await?;
	
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

async fn align_schemas(df: &DataFrame, target_schema: &datafusion::common::DFSchemaRef, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let current_schema = df.schema();
	let mut select_exprs = Vec::new();
	
	for target_field in target_schema.fields() {
		let target_name = target_field.name();
		
		if let Ok(_current_field) = current_schema.field_with_name(None, target_name) {
			select_exprs.push(Expr::Column(datafusion::common::Column::new(None::<String>, target_name)));
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