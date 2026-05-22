use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use clap::Args;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail create data.parquet -c \"total=price*quantity\" -o out.parquet
  nail create sales.csv -c \"tax=revenue*0.2\"
  nail create data.parquet -c \"z=(value-mean(value))/std(value)\"
  nail create data.parquet -c \"log_price=log(price),sqrt_v=sqrt(value)\"
  nail create data.parquet -c \"a=value*2\" -c \"b=log(value)\" -c \"c=pow(value,2)\"")]
pub struct CreateArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(
		short = 'c',
		long = "column",
		action = clap::ArgAction::Append,
		help = "Column creation specs (name=expression). Repeatable: pass -c\n\
                  multiple times or comma-separate specs within one -c.\n\
                  Operators:\n\
                  • Arithmetic:  +  -  *  /  %\n\
                  • Comparison:  =  !=  <  <=  >  >=\n\
                  • Logical:     AND  OR  NOT\n\
                  • Grouping:    ( )\n\
                  Scalar math (per-row):\n\
                  • abs(x), sign(x), floor(x), ceil(x), round(x[,d]), trunc(x)\n\
                  • sqrt(x), cbrt(x), exp(x), ln(x), log(x), log10(x), log2(x), pow(a,b)\n\
                  • sin(x), cos(x), tan(x), asin(x), acos(x), atan(x), atan2(y,x)\n\
                  Aggregates (broadcast to every row — auto-windowed as OVER ()):\n\
                  • mean(x), sum(x), min(x), max(x), count(x), median(x)\n\
                  • std(x) / stddev(x), var(x) / variance(x), stddev_pop(x), var_pop(x)\n\
                  Other:\n\
                  • coalesce(a,b,...), nullif(a,b), least(a,b,...), greatest(a,b,...)\n\
                  • CASE WHEN ... THEN ... ELSE ... END\n\
                  Column references:\n\
                  • Bare identifiers reference columns (e.g., price, value)\n\
                  • Use double quotes around any column name that:\n\
                    - collides with a function name, e.g., mean(\"mean\")\n\
                    - contains operator characters (- + * / % > < =),\n\
                      e.g., \"revenue-income\"-cost  (without quotes this parses as\n\
                      three columns: revenue - income - cost)\n\
                    - contains spaces or other non-identifier characters,\n\
                      e.g., \"unit price\"*quantity"
	)]
	pub columns: Vec<String>,

	#[arg(short = 'r', long = "row", help = "Row filter expression")]
	pub row_filter: Option<String>,
}

struct FunctionAlias {
	alias: &'static str,
	target: &'static str,
	is_aggregate: bool,
}

const FUNCTION_ALIASES: &[FunctionAlias] = &[
	FunctionAlias {
		alias: "mean",
		target: "avg",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "avg",
		target: "avg",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "sum",
		target: "sum",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "min",
		target: "min",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "max",
		target: "max",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "count",
		target: "count",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "median",
		target: "median",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "std",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stdev",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stddev",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stddev_samp",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stddev_pop",
		target: "stddev_pop",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "var",
		target: "var_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "variance",
		target: "var_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "var_samp",
		target: "var_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "var_pop",
		target: "var_pop",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "pow",
		target: "power",
		is_aggregate: false,
	},
];

fn lookup_alias(name: &str) -> Option<&'static FunctionAlias> {
	let lower = name.to_ascii_lowercase();
	FUNCTION_ALIASES.iter().find(|f| f.alias == lower)
}

/// Split a column-spec string on top-level commas, ignoring commas inside
/// parentheses or quoted strings so calls like `pow(a, b)` and `round(x, 2)`
/// survive intact.
fn split_top_level_commas(input: &str) -> Vec<String> {
	let bytes = input.as_bytes();
	let mut parts = Vec::new();
	let mut start = 0;
	let mut depth = 0i32;
	let mut in_single = false;
	let mut in_double = false;
	for (i, &c) in bytes.iter().enumerate() {
		if in_single {
			if c == b'\'' {
				in_single = false;
			}
			continue;
		}
		if in_double {
			if c == b'"' {
				in_double = false;
			}
			continue;
		}
		match c {
			b'\'' => in_single = true,
			b'"' => in_double = true,
			b'(' => depth += 1,
			b')' => depth -= 1,
			b',' if depth == 0 => {
				parts.push(input[start..i].to_string());
				start = i + 1;
			}
			_ => {}
		}
	}
	parts.push(input[start..].to_string());
	parts
}

fn find_matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
	debug_assert_eq!(bytes[open], b'(');
	let mut depth = 0i32;
	let mut i = open;
	let mut in_single = false;
	let mut in_double = false;
	while i < bytes.len() {
		let c = bytes[i];
		if in_single {
			if c == b'\'' {
				in_single = false;
			}
		} else if in_double {
			if c == b'"' {
				in_double = false;
			}
		} else {
			match c {
				b'\'' => in_single = true,
				b'"' => in_double = true,
				b'(' => depth += 1,
				b')' => {
					depth -= 1;
					if depth == 0 {
						return Some(i);
					}
				}
				_ => {}
			}
		}
		i += 1;
	}
	None
}

/// Rewrite a nail expression into a DataFusion SQL expression:
/// * Apply curated function aliases (mean → avg, std → stddev_samp, pow → power, ...).
/// * Wrap aggregate calls with `OVER ()` so they broadcast across rows (unless the
///   user already supplied an `OVER` clause).
/// * Leave unknown function calls, identifiers, quoted strings, and operators
///   untouched so they reach DataFusion as written.
fn rewrite_expression(expr: &str) -> String {
	let bytes = expr.as_bytes();
	let mut out = String::new();
	let mut i = 0;
	while i < bytes.len() {
		let c = bytes[i];
		if c == b'\'' {
			out.push('\'');
			i += 1;
			while i < bytes.len() {
				let b = bytes[i];
				out.push(b as char);
				i += 1;
				if b == b'\'' {
					if i < bytes.len() && bytes[i] == b'\'' {
						out.push('\'');
						i += 1;
					} else {
						break;
					}
				}
			}
			continue;
		}
		if c == b'"' {
			out.push('"');
			i += 1;
			while i < bytes.len() {
				let b = bytes[i];
				out.push(b as char);
				i += 1;
				if b == b'"' {
					break;
				}
			}
			continue;
		}
		if c.is_ascii_alphabetic() || c == b'_' {
			let start = i;
			while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
				i += 1;
			}
			let ident = &expr[start..i];
			let mut j = i;
			while j < bytes.len() && bytes[j].is_ascii_whitespace() {
				j += 1;
			}
			if j < bytes.len() && bytes[j] == b'(' {
				if let Some(close) = find_matching_paren(bytes, j) {
					let (target_name, is_aggregate) = match lookup_alias(ident) {
						Some(a) => (a.target, a.is_aggregate),
						None => (ident, false),
					};
					let inner = &expr[j + 1..close];
					let rewritten_inner = rewrite_expression(inner);
					out.push_str(target_name);
					out.push_str(&expr[i..j]);
					out.push('(');
					out.push_str(&rewritten_inner);
					out.push(')');
					if is_aggregate {
						let mut k = close + 1;
						while k < bytes.len() && bytes[k].is_ascii_whitespace() {
							k += 1;
						}
						let has_over =
							k + 4 <= bytes.len() && expr[k..k + 4].eq_ignore_ascii_case("OVER");
						if !has_over {
							out.push_str(" OVER ()");
						}
					}
					i = close + 1;
					continue;
				}
			}
			out.push_str(ident);
			continue;
		}
		out.push(c as char);
		i += 1;
	}
	out
}

pub async fn execute(args: CreateArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let ctx = crate::utils::create_context_with_jobs(args.common.jobs).await?;
	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	ctx.register_table("t", df.clone().into_view())?;

	let mut result_df = df;

	// Apply row filter if specified
	if let Some(row_expr) = &args.row_filter {
		args.common
			.log_if_verbose(&format!("Applying row filter: {}", row_expr));
		let rewritten_filter = rewrite_expression(row_expr);
		if rewritten_filter != *row_expr {
			args.common
				.log_if_verbose(&format!("Rewritten row filter: {}", rewritten_filter));
		}
		let filter_sql = format!("SELECT * FROM t WHERE {}", rewritten_filter);
		result_df = ctx.sql(&filter_sql).await.map_err(|e| {
			NailError::InvalidArgument(format!("Invalid row filter expression: {}", e))
		})?;
		// Re-register the filtered data - need to deregister first
		ctx.deregister_table("t")?;
		ctx.register_table("t", result_df.clone().into_view())?;
	}

	// Parse and apply column creation specs
	if !args.columns.is_empty() {
		let mut column_map = Vec::new();
		for col_specs in &args.columns {
			for pair in split_top_level_commas(col_specs) {
				let pair = pair.trim();
				if pair.is_empty() {
					continue;
				}
				let eq_pos = pair.find('=').ok_or_else(|| {
					NailError::InvalidArgument(format!("Invalid column spec: {}", pair))
				})?;
				let name = pair[..eq_pos].trim();
				let expr = pair[eq_pos + 1..].trim();
				if name.is_empty() || expr.is_empty() {
					return Err(NailError::InvalidArgument(format!(
						"Invalid column spec: {}",
						pair
					)));
				}
				column_map.push((name.to_string(), expr.to_string()));
			}
		}

		args.common
			.log_if_verbose(&format!("Creating columns: {:?}", column_map));

		// Validate column names don't already exist
		let existing_columns: Vec<String> = result_df
			.schema()
			.fields()
			.iter()
			.map(|f| f.name().clone())
			.collect();

		let mut seen_new = std::collections::HashSet::new();
		for (name, _) in &column_map {
			if existing_columns.contains(name) {
				return Err(NailError::InvalidArgument(format!(
					"Column '{}' already exists",
					name
				)));
			}
			if !seen_new.insert(name.clone()) {
				return Err(NailError::InvalidArgument(format!(
					"Column '{}' is defined more than once",
					name
				)));
			}
		}

		// Build SQL select list
		let mut select_list = vec!["*".to_string()];

		for (name, expr_str) in &column_map {
			let rewritten = rewrite_expression(expr_str);
			if rewritten != *expr_str {
				args.common
					.log_if_verbose(&format!("Rewritten '{}' -> '{}'", expr_str, rewritten));
			}
			select_list.push(format!("({}) AS \"{}\"", rewritten, name));
		}

		let sql = format!("SELECT {} FROM t", select_list.join(", "));
		args.common
			.log_if_verbose(&format!("Executing SQL: {}", sql));

		result_df = ctx
			.sql(&sql)
			.await
			.map_err(|e| NailError::InvalidArgument(format!("Invalid column expression: {}", e)))?;
	}

	// Write or display result
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "create").await?;

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::cli::CommonArgs;
	use arrow::array::{Float64Array, Int64Array, StringArray};
	use arrow::record_batch::RecordBatch;
	use arrow_schema::{DataType, Field, Schema};
	use datafusion::prelude::SessionContext;
	use parquet::arrow::ArrowWriter;
	use std::fs::File;
	use std::path::PathBuf;
	use std::sync::Arc;
	use tempfile::tempdir;

	fn create_test_data() -> (tempfile::TempDir, PathBuf) {
		let temp_dir = tempdir().unwrap();
		let file_path = temp_dir.path().join("test.parquet");

		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
			Field::new("value", DataType::Float64, true),
			Field::new("category", DataType::Utf8, true),
		]));

		let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
		let name_array = StringArray::from(vec![
			Some("Alice"),
			Some("Bob"),
			Some("Charlie"),
			Some("David"),
			Some("Eve"),
		]);
		let value_array = Float64Array::from(vec![
			Some(100.0),
			Some(200.0),
			Some(300.0),
			Some(400.0),
			Some(500.0),
		]);
		let category_array =
			StringArray::from(vec![Some("A"), Some("B"), Some("A"), Some("C"), Some("B")]);

		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(id_array),
				Arc::new(name_array),
				Arc::new(value_array),
				Arc::new(category_array),
			],
		)
		.unwrap();

		let file = File::create(&file_path).unwrap();
		let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
		writer.write(&batch).unwrap();
		writer.close().unwrap();

		(temp_dir, file_path)
	}

	#[tokio::test]
	async fn test_create_arithmetic_column() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["doubled=value*2".to_string()],
			row_filter: None,
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();

		assert_eq!(df.clone().count().await.unwrap(), 5);
		assert!(df.schema().field_with_name(None, "doubled").is_ok());
		assert_eq!(df.schema().fields().len(), 5);
	}

	#[tokio::test]
	async fn test_create_comparison_column() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["high_value=value>300".to_string()],
			row_filter: None,
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();

		assert_eq!(df.clone().count().await.unwrap(), 5);
		assert!(df.schema().field_with_name(None, "high_value").is_ok());
	}

	#[tokio::test]
	async fn test_create_multiple_columns() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["doubled=value*2,id_plus_one=id+1".to_string()],
			row_filter: None,
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();

		assert_eq!(df.clone().count().await.unwrap(), 5);
		assert!(df.schema().field_with_name(None, "doubled").is_ok());
		assert!(df.schema().field_with_name(None, "id_plus_one").is_ok());
		assert_eq!(df.schema().fields().len(), 6);
	}

	#[tokio::test]
	async fn test_create_with_row_filter() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["doubled=value*2".to_string()],
			row_filter: Some("id>2".to_string()),
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();

		assert_eq!(df.clone().count().await.unwrap(), 3); // Only rows with id > 2
		assert!(df.schema().field_with_name(None, "doubled").is_ok());
	}

	#[tokio::test]
	async fn test_create_existing_column_error() {
		let (_temp_dir, input_path) = create_test_data();

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: None,
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["id=value*2".to_string()], // 'id' already exists
			row_filter: None,
		};

		let result = execute(args).await;
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("already exists"));
	}

	#[tokio::test]
	async fn test_create_invalid_column_spec() {
		let (_temp_dir, input_path) = create_test_data();

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: None,
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["invalid_spec".to_string()], // Missing '='
			row_filter: None,
		};

		let result = execute(args).await;
		assert!(result.is_err());
		assert!(result
			.unwrap_err()
			.to_string()
			.contains("Invalid column spec"));
	}

	#[tokio::test]
	async fn test_create_invalid_expression() {
		let (_temp_dir, input_path) = create_test_data();

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: None,
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["test=nonexistent_column*2".to_string()],
			row_filter: None,
		};

		let result = execute(args).await;
		assert!(result.is_err());
		assert!(result
			.unwrap_err()
			.to_string()
			.contains("Invalid column expression"));
	}

	#[tokio::test]
	async fn test_create_invalid_row_filter() {
		let (_temp_dir, input_path) = create_test_data();

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: None,
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["doubled=value*2".to_string()],
			row_filter: Some("nonexistent_column>5".to_string()),
		};

		let result = execute(args).await;
		assert!(result.is_err());
		assert!(result
			.unwrap_err()
			.to_string()
			.contains("Invalid row filter expression"));
	}

	#[test]
	fn test_rewrite_passthrough() {
		assert_eq!(rewrite_expression("value*2"), "value*2");
		assert_eq!(rewrite_expression("(a+b)/c"), "(a+b)/c");
		assert_eq!(rewrite_expression("log(value)"), "log(value)");
		assert_eq!(rewrite_expression("sqrt(value)"), "sqrt(value)");
		assert_eq!(rewrite_expression("coalesce(a, b)"), "coalesce(a, b)");
	}

	#[test]
	fn test_rewrite_scalar_aliases() {
		assert_eq!(rewrite_expression("pow(value, 2)"), "power(value, 2)");
		assert_eq!(rewrite_expression("POW(x, 3)"), "power(x, 3)");
	}

	#[test]
	fn test_rewrite_aggregate_autowindow() {
		assert_eq!(rewrite_expression("mean(value)"), "avg(value) OVER ()");
		assert_eq!(
			rewrite_expression("std(value)"),
			"stddev_samp(value) OVER ()"
		);
		assert_eq!(rewrite_expression("var(value)"), "var_samp(value) OVER ()");
		assert_eq!(rewrite_expression("median(value)"), "median(value) OVER ()");
		assert_eq!(
			rewrite_expression("(value-mean(value))/std(value)"),
			"(value-avg(value) OVER ())/stddev_samp(value) OVER ()"
		);
	}

	#[test]
	fn test_rewrite_preserves_user_over_clause() {
		assert_eq!(
			rewrite_expression("mean(value) OVER (PARTITION BY category)"),
			"avg(value) OVER (PARTITION BY category)"
		);
	}

	#[test]
	fn test_rewrite_quoted_column_named_like_function() {
		// Column literally named "mean" referenced inside mean(...) should not be
		// re-interpreted as a function call (no parens after it).
		assert_eq!(
			rewrite_expression("mean(\"mean\")"),
			"avg(\"mean\") OVER ()"
		);
	}

	#[test]
	fn test_rewrite_unknown_function_passthrough() {
		assert_eq!(
			rewrite_expression("coalesce(a, mean(b))"),
			"coalesce(a, avg(b) OVER ())"
		);
	}

	#[test]
	fn test_split_top_level_commas_respects_parens() {
		let parts = split_top_level_commas("a=pow(x, 2), b=round(y, 3)");
		assert_eq!(parts.len(), 2);
		assert_eq!(parts[0].trim(), "a=pow(x, 2)");
		assert_eq!(parts[1].trim(), "b=round(y, 3)");
	}

	#[test]
	fn test_split_top_level_commas_respects_quotes() {
		let parts = split_top_level_commas("a=\"x,y\", b=z");
		assert_eq!(parts.len(), 2);
		assert_eq!(parts[0].trim(), "a=\"x,y\"");
		assert_eq!(parts[1].trim(), "b=z");
	}

	#[tokio::test]
	async fn test_create_with_mean_broadcast() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["avg_v=mean(value)".to_string()],
			row_filter: None,
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();
		assert_eq!(df.clone().count().await.unwrap(), 5);
		assert!(df.schema().field_with_name(None, "avg_v").is_ok());

		// Every row should have the same broadcast mean (300.0).
		let batches = df.collect().await.unwrap();
		let batch = &batches[0];
		let col_idx = batch.schema().index_of("avg_v").unwrap();
		let col = batch
			.column(col_idx)
			.as_any()
			.downcast_ref::<Float64Array>()
			.unwrap();
		for i in 0..col.len() {
			assert!((col.value(i) - 300.0).abs() < 1e-9);
		}
	}

	#[tokio::test]
	async fn test_create_with_zscore() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["z=(value-mean(value))/std(value)".to_string()],
			row_filter: None,
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();
		assert!(df.schema().field_with_name(None, "z").is_ok());
		assert_eq!(df.clone().count().await.unwrap(), 5);
	}

	#[tokio::test]
	async fn test_create_multiple_c_flags() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec![
				"a=value*2".to_string(),
				"b=log(value)".to_string(),
				"c=pow(value, 2)".to_string(),
			],
			row_filter: None,
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();
		assert!(df.schema().field_with_name(None, "a").is_ok());
		assert!(df.schema().field_with_name(None, "b").is_ok());
		assert!(df.schema().field_with_name(None, "c").is_ok());
		assert_eq!(df.clone().count().await.unwrap(), 5);
	}

	#[tokio::test]
	async fn test_create_duplicate_new_column_across_c_flags() {
		let (_temp_dir, input_path) = create_test_data();

		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: None,
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["a=value*2".to_string(), "a=value+1".to_string()],
			row_filter: None,
		};

		let result = execute(args).await;
		assert!(result.is_err());
		assert!(result
			.unwrap_err()
			.to_string()
			.contains("defined more than once"));
	}

	#[tokio::test]
	async fn test_create_with_log_and_pow() {
		let (_temp_dir, input_path) = create_test_data();
		let output_dir = tempdir().unwrap();
		let output_path = output_dir.path().join("output.parquet");

		// Two columns with internal commas to exercise the smart splitter.
		let args = CreateArgs {
			common: CommonArgs {
				input: input_path,
				output: Some(output_path.clone()),
				format: None,
				random: None,
				batch_size: None,
				verbose: false,
				jobs: None,
				table: false,
			},
			columns: vec!["log_v=log(value), squared=pow(value, 2)".to_string()],
			row_filter: None,
		};

		execute(args).await.unwrap();

		let ctx = SessionContext::new();
		let df = ctx
			.read_parquet(output_path.to_str().unwrap(), Default::default())
			.await
			.unwrap();
		assert!(df.schema().field_with_name(None, "log_v").is_ok());
		assert!(df.schema().field_with_name(None, "squared").is_ok());
		assert_eq!(df.clone().count().await.unwrap(), 5);
	}
}
