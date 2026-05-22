use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use clap::Args;
use datafusion::prelude::*;
use regex::Regex;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail headers data.parquet
  cat data.csv | nail headers -")]
pub struct HeadersArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(long, help = "Filter headers with regex pattern")]
	pub filter: Option<String>,
}

pub async fn execute(args: HeadersArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading schema from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let schema = df.schema();
	let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

	let filtered_names = if let Some(pattern) = &args.filter {
		let regex = Regex::new(pattern)
			.map_err(|e| NailError::InvalidArgument(format!("Invalid regex pattern: {}", e)))?;

		field_names
			.into_iter()
			.filter(|name| regex.is_match(name))
			.collect()
	} else {
		field_names
	};

	args.common
		.log_if_verbose(&format!("Found {} headers", filtered_names.len()));

	// For headers command, output simple list to console or structured data to file
	match &args.common.output {
		Some(_output_path) => {
			// Create a DataFrame with the header names for file output
			let ctx = SessionContext::new();
			let headers_sql = filtered_names
				.iter()
				.map(|name| format!("'{}' as header", name.replace("'", "''")))
				.collect::<Vec<_>>()
				.join(" UNION ALL SELECT ");

			let result_df = if filtered_names.is_empty() {
				ctx.sql("SELECT '' as header WHERE 1=0")
					.await
					.map_err(crate::error::NailError::DataFusion)?
			} else {
				ctx.sql(&format!("SELECT {}", headers_sql))
					.await
					.map_err(crate::error::NailError::DataFusion)?
			};

			let output_handler = OutputHandler::new(&args.common);
			output_handler.handle_output(&result_df, "headers").await?;
		}
		None => {
			// Simple console output - one header per line
			for name in filtered_names {
				println!("{}", name);
			}
		}
	}

	Ok(())
}
