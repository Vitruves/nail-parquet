//! `nail clean` — one-shot import cleanup.
//!
//! Defaults: snake_case headers, trim string whitespace, drop fully-empty rows.
//! Opt out per behaviour with `--keep-*` flags. Empty-column removal is opt-in.

use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use clap::Args;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;
use std::collections::HashMap;

#[derive(Args, Clone)]
#[command(after_help = "Examples:\n  \
    nail clean messy.csv -o clean.parquet\n  \
    nail clean data.csv --keep-headers -o -\n  \
    nail clean raw.xlsx --drop-empty-cols -o cleaned.parquet")]
pub struct CleanArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(
		long,
		help = "Keep original column names (skip snake_case normalisation)"
	)]
	pub keep_headers: bool,

	#[arg(long, help = "Keep leading/trailing whitespace in string columns")]
	pub keep_whitespace: bool,

	#[arg(long, help = "Keep rows where every column is empty")]
	pub keep_empty_rows: bool,

	#[arg(long, help = "Also drop columns where every value is empty")]
	pub drop_empty_cols: bool,
}

pub async fn execute(args: CleanArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;

	let ctx = crate::utils::create_context_with_jobs(args.common.jobs).await?;
	ctx.register_table("t", df.clone().into_view())?;

	let schema = df.schema();
	let fields: Vec<_> = schema.fields().iter().cloned().collect();
	let original_names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();

	let keep_idx: Vec<usize> = if args.drop_empty_cols {
		args.common.log_if_verbose("Scanning for empty columns…");
		find_non_empty_columns(&ctx, &original_names).await?
	} else {
		(0..fields.len()).collect()
	};

	let kept_originals: Vec<String> = keep_idx
		.iter()
		.map(|&i| original_names[i].clone())
		.collect();
	let new_names: Vec<String> = if args.keep_headers {
		kept_originals.clone()
	} else {
		snake_case_unique(&kept_originals)
	};

	let mut projections = Vec::with_capacity(keep_idx.len());
	for (k, &i) in keep_idx.iter().enumerate() {
		let field = &fields[i];
		let raw_ident = quote_ident(field.name());
		let value_expr = if !args.keep_whitespace && field.data_type() == &DataType::Utf8 {
			format!("trim({})", raw_ident)
		} else {
			raw_ident
		};
		projections.push(format!("{} AS {}", value_expr, quote_ident(&new_names[k])));
	}

	if projections.is_empty() {
		return Err(NailError::InvalidArgument(
			"clean produced an empty schema (all columns were empty)".to_string(),
		));
	}

	let mut sql = format!("SELECT {} FROM t", projections.join(", "));

	if !args.keep_empty_rows {
		let predicates: Vec<String> = keep_idx
			.iter()
			.map(|&i| {
				let f = &fields[i];
				let n = quote_ident(f.name());
				if f.data_type() == &DataType::Utf8 {
					format!("({} IS NOT NULL AND trim({}) != '')", n, n)
				} else {
					format!("({} IS NOT NULL)", n)
				}
			})
			.collect();
		if !predicates.is_empty() {
			sql.push_str(&format!(" WHERE {}", predicates.join(" OR ")));
		}
	}

	args.common.log_if_verbose(&format!("Clean SQL: {}", sql));
	let cleaned = ctx.sql(&sql).await?;

	if args.common.verbose {
		let dropped: Vec<&String> = original_names
			.iter()
			.enumerate()
			.filter_map(|(i, n)| (!keep_idx.contains(&i)).then_some(n))
			.collect();
		if !dropped.is_empty() {
			args.common
				.log_if_verbose(&format!("Dropped empty columns: {:?}", dropped));
		}
		if !args.keep_headers {
			let renames: Vec<String> = kept_originals
				.iter()
				.zip(new_names.iter())
				.filter(|(o, n)| o != n)
				.map(|(o, n)| format!("{} -> {}", o, n))
				.collect();
			if !renames.is_empty() {
				args.common
					.log_if_verbose(&format!("Renamed: {}", renames.join(", ")));
			}
		}
	}

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&cleaned, "clean").await
}

async fn find_non_empty_columns(ctx: &SessionContext, names: &[String]) -> NailResult<Vec<usize>> {
	if names.is_empty() {
		return Ok(Vec::new());
	}
	let parts: Vec<String> = names
		.iter()
		.enumerate()
		.map(|(i, n)| format!("COUNT({}) AS c{}", quote_ident(n), i))
		.collect();
	let sql = format!("SELECT {} FROM t", parts.join(", "));
	let batches = ctx.sql(&sql).await?.collect().await?;
	let batch = batches
		.into_iter()
		.next()
		.ok_or_else(|| NailError::Statistics("empty result computing column counts".to_string()))?;

	let mut keep = Vec::new();
	for i in 0..names.len() {
		let arr = batch.column(i);
		let counts = arr
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or_else(|| NailError::Statistics("unexpected COUNT result type".to_string()))?;
		if counts.value(0) > 0 {
			keep.push(i);
		}
	}
	Ok(keep)
}

fn quote_ident(s: &str) -> String {
	let escaped = s.replace('"', "\"\"");
	format!("\"{}\"", escaped)
}

fn snake_case(s: &str) -> String {
	let chars: Vec<char> = s.chars().collect();
	let mut out = String::with_capacity(chars.len() + 4);
	for (i, &c) in chars.iter().enumerate() {
		if c.is_alphanumeric() {
			if c.is_uppercase() && i > 0 {
				let prev = chars[i - 1];
				let next_is_lower = chars.get(i + 1).map(|n| n.is_lowercase()).unwrap_or(false);
				let prev_is_lower_alnum = prev.is_lowercase() || prev.is_ascii_digit();
				// Insert separators at lower→Upper and at end of ALLCAPS runs (FOOBar → foo_bar).
				if (prev_is_lower_alnum || next_is_lower) && !out.ends_with('_') {
					out.push('_');
				}
			}
			for lc in c.to_lowercase() {
				out.push(lc);
			}
		} else if !out.is_empty() && !out.ends_with('_') {
			out.push('_');
		}
	}
	out.trim_matches('_').to_string()
}

fn snake_case_unique(names: &[String]) -> Vec<String> {
	let mut seen: HashMap<String, usize> = HashMap::new();
	let mut out = Vec::with_capacity(names.len());
	for name in names {
		let mut base = snake_case(name);
		if base.is_empty() {
			base = "col".to_string();
		}
		let counter = seen.entry(base.clone()).or_insert(0);
		let final_name = if *counter == 0 {
			base.clone()
		} else {
			format!("{}_{}", base, counter)
		};
		*counter += 1;
		out.push(final_name);
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn snake_case_basic() {
		assert_eq!(snake_case("First Name"), "first_name");
		assert_eq!(snake_case("firstName"), "first_name");
		assert_eq!(snake_case("FirstName"), "first_name");
		assert_eq!(snake_case("CO2 Level"), "co2_level");
		assert_eq!(snake_case("  weird   col-NAME  "), "weird_col_name");
		assert_eq!(snake_case("ALLCAPS"), "allcaps");
		assert_eq!(snake_case("ABCThing"), "abc_thing");
		assert_eq!(snake_case("user.id"), "user_id");
		assert_eq!(snake_case("---"), "");
	}

	#[test]
	fn snake_case_unique_deduplicates() {
		let names = vec![
			"First Name".to_string(),
			"first_name".to_string(),
			"FIRST NAME".to_string(),
		];
		let unique = snake_case_unique(&names);
		assert_eq!(unique, vec!["first_name", "first_name_1", "first_name_2"]);
	}

	#[test]
	fn snake_case_unique_handles_empty_after_cleanup() {
		let names = vec!["---".to_string(), "...".to_string()];
		let unique = snake_case_unique(&names);
		assert_eq!(unique, vec!["col", "col_1"]);
	}

	#[test]
	fn quote_ident_escapes_quotes() {
		assert_eq!(quote_ident(r#"weird"name"#), r#""weird""name""#);
		assert_eq!(quote_ident("normal"), r#""normal""#);
	}
}
