use clap::Args;
use datafusion::prelude::*;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct IdArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(long, help = "Create new ID column")]
	pub create: bool,
	
	#[arg(long, help = "Prefix for ID values", default_value = "id")]
	pub prefix: String,
	
	#[arg(long, help = "ID column name", default_value = "id")]
	pub id_col_name: String,
}

pub async fn execute(args: IdArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	
	let result_df = if args.create {
		args.common.log_if_verbose(&format!("Creating ID column '{}' with prefix '{}'", args.id_col_name, args.prefix));
		add_id_column(&df, &args.id_col_name, &args.prefix, args.common.jobs).await?
	} else {
		df
	};
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "id").await?;
	
	Ok(())
}

async fn add_id_column(df: &DataFrame, col_name: &str, prefix: &str, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
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