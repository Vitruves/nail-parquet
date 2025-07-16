use clap::Args;
use datafusion::prelude::*;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::utils::column::resolve_column_name;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct FilterArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Column filter conditions (comma-separated).\n\
		Supported operators:\n\
		• = (equals): 'status=active'\n\
		• != (not equals): 'status!=inactive'\n\
		• > (greater than): 'age>25'\n\
		• >= (greater or equal): 'score>=80'\n\
		• < (less than): 'salary<50000'\n\
		• <= (less or equal): 'price<=100'\n\
		Examples:\n\
		• Single: 'age>25'\n\
		• Multiple: 'age>=18,salary<50000,status=active'\n\
		• Mixed types: 'score>80,name!=test,active=true'")]
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
	args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
	
	let df = read_data(&args.common.input).await?;
	let mut result_df = df;
	
	if let Some(col_conditions) = &args.columns {
		args.common.log_if_verbose(&format!("Applying column filters: {}", col_conditions));
		result_df = apply_column_filters(&result_df, col_conditions, args.common.jobs).await?;
	}
	
	if let Some(row_filter) = &args.rows {
		args.common.log_if_verbose(&format!("Applying row filter: {:?}", row_filter));
		result_df = apply_row_filter(&result_df, row_filter, args.common.jobs).await?;
	}
	
	let output_handler = OutputHandler::new(&args.common);
	output_handler.handle_output(&result_df, "filter").await?;
	
	Ok(())
}

async fn apply_column_filters(df: &DataFrame, conditions: &str, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema().clone().into();
	let mut filter_conditions = Vec::new();
	
	for condition in conditions.split(',') {
		let condition = condition.trim();
		let filter_expr = parse_condition_with_schema(condition, &schema).await?;
		filter_conditions.push(filter_expr);
	}
	
	let combined_filter = filter_conditions.into_iter()
		.reduce(|acc, expr| acc.and(expr))
		.unwrap();
	
	let result = ctx.table(table_name).await?.filter(combined_filter)?;
	Ok(result)
}

async fn parse_condition_with_schema(condition: &str, schema: &datafusion::common::DFSchemaRef) -> NailResult<Expr> {
	let operators = [">=", "<=", "!=", "=", ">", "<"];
	
	for op in &operators {
		if let Some(pos) = condition.find(op) {
			let column_name_input = condition[..pos].trim();
			let value_str = condition[pos + op.len()..].trim();
			
			// Use the centralized column resolution utility
			let actual_column_name = resolve_column_name(schema, column_name_input)?;
			
			let value_expr = if let Ok(int_val) = value_str.parse::<i64>() {
				lit(int_val)
			} else if let Ok(float_val) = value_str.parse::<f64>() {
				lit(float_val)
			} else {
				lit(value_str)
			};
			
			// Use quoted column name to preserve case sensitivity
			let column_expr = Expr::Column(datafusion::common::Column::new(None::<String>, &actual_column_name));
			
			return Ok(match *op {
				"=" => column_expr.eq(value_expr),
				"!=" => column_expr.not_eq(value_expr),
				">" => column_expr.gt(value_expr),
				">=" => column_expr.gt_eq(value_expr),
				"<" => column_expr.lt(value_expr),
				"<=" => column_expr.lt_eq(value_expr),
				_ => unreachable!(),
			});
		}
	}
	
	Err(NailError::InvalidArgument(format!("Invalid condition: {}", condition)))
}

async fn apply_row_filter(df: &DataFrame, filter: &RowFilter, jobs: Option<usize>) -> NailResult<DataFrame> {
	let ctx = crate::utils::create_context_with_jobs(jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let schema = df.schema();
	let filter_expr = match filter {
		RowFilter::NoNan => {
			let conditions: Vec<Expr> = schema.fields().iter()
				.map(|f| Expr::Column(datafusion::common::Column::new(None::<String>, f.name())).is_not_null())
				.collect();
			conditions.into_iter().reduce(|acc, expr| acc.and(expr)).unwrap()
		},
		RowFilter::NumericOnly => {
			// Filter rows where all numeric columns have valid numeric values (not null)
			let numeric_columns: Vec<String> = schema.fields().iter()
				.filter(|f| matches!(f.data_type(), 
					datafusion::arrow::datatypes::DataType::Int64 | 
					datafusion::arrow::datatypes::DataType::Float64 | 
					datafusion::arrow::datatypes::DataType::Int32 | 
					datafusion::arrow::datatypes::DataType::Float32
				))
				.map(|f| f.name().clone())
				.collect();
			
			if numeric_columns.is_empty() {
				return Err(NailError::InvalidArgument("No numeric columns found".to_string()));
			}
			
			// Create conditions that all numeric columns must not be null
			let conditions: Vec<Expr> = numeric_columns.iter()
				.map(|name| Expr::Column(datafusion::common::Column::new(None::<String>, name)).is_not_null())
				.collect();
			
			conditions.into_iter().reduce(|acc, expr| acc.and(expr)).unwrap()
		},
		RowFilter::CharOnly => {
			// Filter rows where all string columns have non-null values
			let char_columns: Vec<String> = schema.fields().iter()
				.filter(|f| matches!(f.data_type(), datafusion::arrow::datatypes::DataType::Utf8))
				.map(|f| f.name().clone())
				.collect();
			
			if char_columns.is_empty() {
				return Err(NailError::InvalidArgument("No string columns found".to_string()));
			}
			
			// Create conditions that all string columns must not be null and not empty
			let conditions: Vec<Expr> = char_columns.iter()
				.map(|name| {
					let col_expr = Expr::Column(datafusion::common::Column::new(None::<String>, name));
					col_expr.clone().is_not_null().and(col_expr.not_eq(lit("")))
				})
				.collect();
			
			conditions.into_iter().reduce(|acc, expr| acc.and(expr)).unwrap()
		},
		RowFilter::NoZeros => {
			let conditions: Vec<Expr> = schema.fields().iter()
				.filter_map(|f| {
					match f.data_type() {
						datafusion::arrow::datatypes::DataType::Int64 | 
						datafusion::arrow::datatypes::DataType::Int32 => {
							Some(Expr::Column(datafusion::common::Column::new(None::<String>, f.name())).not_eq(lit(0)))
						},
						datafusion::arrow::datatypes::DataType::Float64 | 
						datafusion::arrow::datatypes::DataType::Float32 => {
							Some(Expr::Column(datafusion::common::Column::new(None::<String>, f.name())).not_eq(lit(0.0)))
						},
						_ => None,
					}
				})
				.collect();
			
			if conditions.is_empty() {
				return Ok(df.clone());
			}
			
			conditions.into_iter().reduce(|acc, expr| acc.and(expr)).unwrap()
		},
	};
	
	let result = ctx.table(table_name).await?.filter(filter_expr)?;
	Ok(result)
}