use clap::Args;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use crate::utils::stats::{calculate_correlations, CorrelationType, select_columns_by_pattern};
use clap::ValueEnum;

#[derive(ValueEnum, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum CorrTest {
    fisher_exact,
    chi_sqr,
    t_test,
}

#[derive(Args, Clone)]
pub struct CorrelationsArgs {
	#[command(flatten)]
	pub common: CommonArgs,
	
	#[arg(short, long, help = "Comma-separated column names or regex patterns")]
	pub columns: Option<String>,
	
	#[arg(short = 't', long, help = "Correlation type", value_enum, default_value = "pearson")]
	pub correlation_type: CorrelationType,
	
	#[arg(long, help = "Output correlation matrix format")]
	pub correlation_matrix: bool,
	
	#[arg(long, help = "Statistical tests to include (comma-separated)", value_enum, num_args = 1.., value_delimiter = ',')]
	pub stats_tests: Option<Vec<CorrTest>>,
	
	#[arg(long, help = "Number of decimal places for correlation values", default_value = "4")]
	pub digits: usize,
}

pub async fn execute(args: CorrelationsArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));
    
    let df = read_data(&args.common.input).await?;
    let schema = df.schema();
    
    let target_columns = if let Some(col_spec) = &args.columns {
        let selected = select_columns_by_pattern(schema.clone().into(), col_spec)?;
        
        let mut numeric_columns = Vec::new();
        let mut non_numeric_columns = Vec::new();
        
        for col_name in &selected {
            if let Ok(field) = schema.field_with_name(None, col_name) {
                match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Int64 | 
                    datafusion::arrow::datatypes::DataType::Float64 | 
                    datafusion::arrow::datatypes::DataType::Int32 | 
                    datafusion::arrow::datatypes::DataType::Float32 |
                    datafusion::arrow::datatypes::DataType::Int16 |
                    datafusion::arrow::datatypes::DataType::Int8 |
                    datafusion::arrow::datatypes::DataType::UInt64 |
                    datafusion::arrow::datatypes::DataType::UInt32 |
                    datafusion::arrow::datatypes::DataType::UInt16 |
                    datafusion::arrow::datatypes::DataType::UInt8 => {
                        numeric_columns.push(col_name.clone());
                    },
                    _ => {
                        non_numeric_columns.push((col_name.clone(), field.data_type().clone()));
                    }
                }
            }
        }
        
        if !non_numeric_columns.is_empty() {
            let non_numeric_info: Vec<String> = non_numeric_columns.iter()
                .map(|(name, dtype)| format!("'{}' ({:?})", name, dtype))
                .collect();
            return Err(NailError::Statistics(
                format!("Correlation requires numeric columns only. Non-numeric columns found: {}. Available numeric columns: {:?}", 
                    non_numeric_info.join(", "), numeric_columns)
            ));
        }
        
        if numeric_columns.len() < 2 {
            return Err(NailError::Statistics(
                format!("Need at least 2 numeric columns for correlation. Found {} numeric columns: {:?}", 
                    numeric_columns.len(), numeric_columns)
            ));
        }
        
        numeric_columns
    } else {
        let numeric_columns: Vec<String> = schema.fields().iter()
            .filter(|f| matches!(f.data_type(), 
                datafusion::arrow::datatypes::DataType::Int64 | 
                datafusion::arrow::datatypes::DataType::Float64 | 
                datafusion::arrow::datatypes::DataType::Int32 | 
                datafusion::arrow::datatypes::DataType::Float32 |
                datafusion::arrow::datatypes::DataType::Int16 |
                datafusion::arrow::datatypes::DataType::Int8 |
                datafusion::arrow::datatypes::DataType::UInt64 |
                datafusion::arrow::datatypes::DataType::UInt32 |
                datafusion::arrow::datatypes::DataType::UInt16 |
                datafusion::arrow::datatypes::DataType::UInt8
            ))
            .map(|f| f.name().clone())
            .collect();
            
        if numeric_columns.len() < 2 {
            return Err(NailError::Statistics(
                format!("Need at least 2 numeric columns for correlation. Found {} numeric columns: {:?}", 
                    numeric_columns.len(), numeric_columns)
            ));
        }
        
        numeric_columns
    };
    
    args.common.log_if_verbose(&format!("Computing {:?} correlations for {} numeric columns: {:?}", 
        args.correlation_type, target_columns.len(), target_columns));
    args.common.log_if_verbose(&format!("Using {} decimal places for correlation values", args.digits));
    
    let include_tests = args.stats_tests.as_ref().map(|v| !v.is_empty()).unwrap_or(false);

let corr_df = calculate_correlations(
        &df, 
        &target_columns, 
        &args.correlation_type,
        args.correlation_matrix,
        include_tests,
        args.digits
    ).await?;
    
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&corr_df, "correlations").await?;
    
    Ok(())
}