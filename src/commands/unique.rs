use clap::Args;

use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::create_context_with_jobs;
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail unique data.parquet                       # distinct whole rows
  nail unique data.parquet -c category           # distinct values of a column
  nail unique data.parquet -c category --count   # value counts, most frequent first")]
pub struct UniqueArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	/// Columns to consider (comma-separated); defaults to all columns.
	#[arg(
		short,
		long,
		help = "Columns to consider (comma-separated; default: all columns)"
	)]
	pub columns: Option<String>,

	/// Append a per-group occurrence count and sort by it descending (value counts).
	#[arg(
		long,
		help = "Append a `count` column with per-group occurrences (value counts)"
	)]
	pub count: bool,

	/// Sort the distinct rows ascending by the selected columns.
	#[arg(long, help = "Sort distinct rows by the selected columns")]
	pub sort: bool,
}

pub async fn execute(args: UniqueArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;

	let all_names: Vec<String> = df
		.schema()
		.fields()
		.iter()
		.map(|f| f.name().to_string())
		.collect();

	// Resolve and validate the working column set.
	let cols: Vec<String> = match &args.columns {
		Some(s) => {
			let requested: Vec<String> = s
				.split(',')
				.map(|c| c.trim().to_string())
				.filter(|c| !c.is_empty())
				.collect();
			for c in &requested {
				if !all_names.iter().any(|n| n == c) {
					return Err(NailError::ColumnNotFound(format!(
						"Column '{}' not found. Available columns: {}",
						c,
						all_names.join(", ")
					)));
				}
			}
			requested
		}
		None => all_names.clone(),
	};

	if cols.is_empty() {
		return Err(NailError::InvalidArgument(
			"No columns to consider for uniqueness".to_string(),
		));
	}

	let ctx = create_context_with_jobs(args.common.jobs).await?;
	let src = "__nail_unique_src";
	ctx.register_table(src, df.clone().into_view())?;

	let select_list = cols
		.iter()
		.map(|c| quote_ident(c))
		.collect::<Vec<_>>()
		.join(", ");

	let sql = if args.count {
		// Value-counts: one row per distinct combination plus its frequency.
		format!(
			"SELECT {sel}, COUNT(*) AS count FROM {src} GROUP BY {sel} ORDER BY count DESC, {sel}",
			sel = select_list,
			src = src,
		)
	} else if args.sort {
		format!(
			"SELECT DISTINCT {sel} FROM {src} ORDER BY {sel}",
			sel = select_list,
			src = src,
		)
	} else {
		format!(
			"SELECT DISTINCT {sel} FROM {src}",
			sel = select_list,
			src = src
		)
	};

	args.common
		.log_if_verbose(&format!("Unique query: {}", sql));

	let result = ctx.sql(&sql).await?;

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result, "unique").await?;

	Ok(())
}

/// Quote a SQL identifier, escaping embedded double quotes.
fn quote_ident(name: &str) -> String {
	format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_quote_ident() {
		assert_eq!(quote_ident("col"), "\"col\"");
		assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
	}
}
