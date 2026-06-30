use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data_with_opts;
use crate::utils::output::OutputHandler;
use crate::utils::predicate::normalize_predicate;
use clap::Args;
use datafusion::prelude::*;

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail filter data.parquet -c \"age>=18,status=active\"
  nail filter people.parquet -c \"18 < age < 65\"          # math-style range
  nail filter sales.csv     -c \"region IN ('EU','US')\"    # SQL operators
  nail filter users.parquet -c \"name LIKE 'A%' AND score IS NOT NULL\"
  nail filter sales.csv     -c \"region=EU|region=US\" -o filtered.csv")]
pub struct FilterArgs {
	#[command(flatten)]
	pub common: CommonArgs,

	#[arg(
		short,
		long,
		help = "Row condition. Mix plain math and SQL however you like; column\n\
		names are matched case-insensitively.\n\
		Comparisons: =  ==  !=  <>  <  <=  >  >=\n\
		Ranges (math style):  '18 < age < 65'  '0 <= score <= 100'  '8 > x > 4'\n\
		Combine conditions:\n\
		• ',' or 'AND' — all must hold:  'age>=18,status=active'\n\
		• '|'  or 'OR' — any may hold:   'region=EU|region=US'\n\
		  (',' / AND binds tighter than '|' / OR)\n\
		SQL operators also work: BETWEEN a AND b, IN (..), LIKE/ILIKE,\n\
		IS NULL, IS NOT NULL, NOT (..), CASE WHEN ..\n\
		Quote text values with spaces or capitals: name='New York'\n\
		Examples:\n\
		• 'age>25'\n\
		• 'age>=18,salary<50000,status=active'\n\
		• 'status=active|status=pending'\n\
		• 'age BETWEEN 18 AND 65 AND name LIKE \"A%\"'\n\
		• 'age>=18,salary<50000|role=admin'  => (age>=18 AND salary<50000) OR role=admin"
	)]
	pub columns: Option<String>,

	#[arg(short, long, help = "Row filter type", value_enum)]
	pub rows: Option<RowFilter>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum RowFilter {
	NoNan,
	NumericOnly,
	CharOnly,
	NoZeros,
}

pub async fn execute(args: FilterArgs) -> NailResult<()> {
	args.common.log_if_verbose(&format!(
		"Reading data from: {}",
		args.common.input.display()
	));

	let df =
		read_data_with_opts(&args.common.input, args.common.jobs, args.common.batch_size).await?;
	let mut result_df = df;

	if let Some(col_conditions) = &args.columns {
		args.common
			.log_if_verbose(&format!("Applying column filters: {}", col_conditions));
		result_df = apply_column_filters(&result_df, col_conditions, args.common.jobs).await?;
	}

	if let Some(row_filter) = &args.rows {
		args.common
			.log_if_verbose(&format!("Applying row filter: {:?}", row_filter));
		result_df = apply_row_filter(&result_df, row_filter, args.common.jobs).await?;
	}

	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "filter").await?;

	Ok(())
}

async fn apply_column_filters(
	df: &DataFrame,
	conditions: &str,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;

	let columns: Vec<String> = df
		.schema()
		.fields()
		.iter()
		.map(|f| f.name().clone())
		.collect();
	let where_sql = normalize_predicate(conditions, &columns)?;

	let sql = format!("SELECT * FROM {} WHERE {}", table_name, where_sql);
	let result = ctx.sql(&sql).await.map_err(|e| {
		NailError::InvalidArgument(format!("Invalid filter condition '{}': {}", conditions, e))
	})?;
	Ok(result)
}

async fn apply_row_filter(
	df: &DataFrame,
	filter: &RowFilter,
	jobs: Option<usize>,
) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;

	let schema = df.schema();
	let filter_expr = match filter {
		RowFilter::NoNan => {
			let conditions: Vec<Expr> = schema
				.fields()
				.iter()
				.map(|f| {
					Expr::Column(datafusion::common::Column::new(None::<String>, f.name()))
						.is_not_null()
				})
				.collect();
			conditions
				.into_iter()
				.reduce(|acc, expr| acc.and(expr))
				.unwrap()
		}
		RowFilter::NumericOnly => {
			// Filter rows where all numeric columns have valid numeric values (not null)
			let numeric_columns: Vec<String> = schema
				.fields()
				.iter()
				.filter(|f| {
					matches!(
						f.data_type(),
						datafusion::arrow::datatypes::DataType::Int64
							| datafusion::arrow::datatypes::DataType::Float64
							| datafusion::arrow::datatypes::DataType::Int32
							| datafusion::arrow::datatypes::DataType::Float32
					)
				})
				.map(|f| f.name().clone())
				.collect();

			if numeric_columns.is_empty() {
				return Err(NailError::InvalidArgument(
					"No numeric columns found".to_string(),
				));
			}

			// Create conditions that all numeric columns must not be null
			let conditions: Vec<Expr> = numeric_columns
				.iter()
				.map(|name| {
					Expr::Column(datafusion::common::Column::new(None::<String>, name))
						.is_not_null()
				})
				.collect();

			conditions
				.into_iter()
				.reduce(|acc, expr| acc.and(expr))
				.unwrap()
		}
		RowFilter::CharOnly => {
			// Filter rows where all string columns have non-null values
			let char_columns: Vec<String> = schema
				.fields()
				.iter()
				.filter(|f| matches!(f.data_type(), datafusion::arrow::datatypes::DataType::Utf8))
				.map(|f| f.name().clone())
				.collect();

			if char_columns.is_empty() {
				return Err(NailError::InvalidArgument(
					"No string columns found".to_string(),
				));
			}

			// Create conditions that all string columns must not be null and not empty
			let conditions: Vec<Expr> = char_columns
				.iter()
				.map(|name| {
					let col_expr =
						Expr::Column(datafusion::common::Column::new(None::<String>, name));
					col_expr.clone().is_not_null().and(col_expr.not_eq(lit("")))
				})
				.collect();

			conditions
				.into_iter()
				.reduce(|acc, expr| acc.and(expr))
				.unwrap()
		}
		RowFilter::NoZeros => {
			let conditions: Vec<Expr> = schema
				.fields()
				.iter()
				.filter_map(|f| match f.data_type() {
					datafusion::arrow::datatypes::DataType::Int64
					| datafusion::arrow::datatypes::DataType::Int32 => Some(
						Expr::Column(datafusion::common::Column::new(None::<String>, f.name()))
							.not_eq(lit(0)),
					),
					datafusion::arrow::datatypes::DataType::Float64
					| datafusion::arrow::datatypes::DataType::Float32 => Some(
						Expr::Column(datafusion::common::Column::new(None::<String>, f.name()))
							.not_eq(lit(0.0)),
					),
					_ => None,
				})
				.collect();

			if conditions.is_empty() {
				return Ok(df.clone());
			}

			conditions
				.into_iter()
				.reduce(|acc, expr| acc.and(expr))
				.unwrap()
		}
	};

	let result = ctx.table(table_name).await?.filter(filter_expr)?;
	Ok(result)
}
