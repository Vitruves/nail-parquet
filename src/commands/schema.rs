use clap::Args;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use datafusion::prelude::*;


#[derive(Args, Clone)]
pub struct SchemaArgs {
	#[command(flatten)]
	pub common: CommonArgs,
}

pub async fn execute(args: SchemaArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading schema from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let schema = df.schema();
	
	let schema_info: Vec<SchemaField> = schema.fields().iter()
		.map(|field| SchemaField {
			name: field.name().clone(),
			data_type: format!("{:?}", field.data_type()),
			nullable: field.is_nullable(),
		})
		.collect();
	
	args.common.log_if_verbose(&format!("Schema contains {} fields", schema_info.len()));
	
	// Handle JSON output specially to match test expectations
	if let Some(output_path) = &args.common.output {
		if matches!(args.common.format, Some(crate::cli::OutputFormat::Json)) {
			// Output JSON array directly for schema command
			let json_content = serde_json::to_string_pretty(&schema_info)
				.map_err(|e| crate::error::NailError::InvalidArgument(format!("JSON serialization error: {}", e)))?;
			std::fs::write(output_path, json_content)?;
			args.common.log_if_verbose(&format!("Schema JSON written to: {}", output_path.display()));
			return Ok(());
		}
	}
	
	// For other outputs, create a DataFrame
	let ctx = SessionContext::new();
	let schema_sql = schema_info.iter()
		.map(|field| format!("'{}' as name, '{}' as data_type, {} as nullable", 
			field.name.replace("'", "''"), 
			field.data_type.replace("'", "''"), 
			field.nullable))
		.collect::<Vec<_>>()
		.join(" UNION ALL SELECT ");
	
	let result_df = if schema_info.is_empty() {
		ctx.sql("SELECT '' as name, '' as data_type, false as nullable WHERE 1=0").await
			.map_err(crate::error::NailError::DataFusion)?
	} else {
		ctx.sql(&format!("SELECT {}", schema_sql)).await
			.map_err(crate::error::NailError::DataFusion)?
	};
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "schema").await?;
	
	Ok(())
}


#[derive(serde::Serialize)]
struct SchemaField {
	name: String,
	data_type: String,
	nullable: bool,
}