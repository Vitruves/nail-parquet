use clap::Args;
use datafusion::prelude::*;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::commands::select::{select_columns_by_pattern, parse_row_specification};
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct DropArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Column names or regex patterns to drop (comma-separated)")]
	pub columns: Option<String>,
	
	#[arg(short, long, help = "Row numbers or ranges to drop (e.g., 1,3,5-10)")]
	pub rows: Option<String>,
}

pub async fn execute(args: DropArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let mut result_df = df;
	
	if let Some(col_spec) = &args.columns {
		let schema = result_df.schema();
		let columns_to_drop = select_columns_by_pattern(schema.clone().into(), col_spec)?;
		
		args.common.log_if_verbose(&format!("Dropping {} columns: {:?}", columns_to_drop.len(), columns_to_drop));
		
		let remaining_columns: Vec<Expr> = result_df.schema().fields().iter()
			.filter(|f| !columns_to_drop.contains(f.name()))
			.map(|f| Expr::Column(datafusion::common::Column::new(None::<String>, f.name())))
			.collect();
		
		result_df = result_df.select(remaining_columns)?;
	}
	
	if let Some(row_spec) = &args.rows {
		let row_indices = parse_row_specification(row_spec)?;
		
		args.common.log_if_verbose(&format!("Dropping {} rows", row_indices.len()));
		
		result_df = drop_rows_by_indices(&result_df, &row_indices, args.common.jobs).await?;
	}
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "drop").await?;
	
	Ok(())
}

async fn drop_rows_by_indices(df: &DataFrame, indices: &[usize], jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	
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
		"SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn NOT IN ({})",
		original_columns.join(", "),
		original_columns.join(", "),
		table_name, 
		indices_str
	);
	
	let result = ctx.sql(&sql).await?;
	Ok(result)
}