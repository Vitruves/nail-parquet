use clap::Args;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::format::display_dataframe;
use crate::utils::stats::{calculate_correlations, CorrelationType, select_columns_by_pattern};

#[derive(Args, Clone)]
pub struct CorrelationsArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Comma-separated column names or regex patterns")]
	pub columns: Option<String>,
	
	#[arg(short = 't', long, help = "Correlation type", value_enum, default_value = "pearson")]
	pub correlation_type: CorrelationType,
	
	#[arg(long, help = "Output correlation matrix format")]
	pub correlation_matrix: bool,
	
	#[arg(long, help = "Include statistical significance tests")]
	pub stats_tests: bool,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: CorrelationsArgs) -> NailResult<()> {
    // Only Pearson correlation implemented
    if args.correlation_type != CorrelationType::Pearson {
        return Err(NailError::Statistics(
            format!("{:?} correlations not yet implemented", args.correlation_type)
        ));
    }
    if args.verbose {
        eprintln!("Reading data from: {}", args.input.display());
    }
    
    let df = read_data(&args.input).await?;
    let schema = df.schema();
    
    let target_columns = if let Some(col_spec) = &args.columns {
        select_columns_by_pattern(schema.clone().into(), col_spec)?
    } else {
        schema.fields().iter()
            .filter(|f| matches!(f.data_type(), 
                datafusion::arrow::datatypes::DataType::Int64 | 
                datafusion::arrow::datatypes::DataType::Float64 | 
                datafusion::arrow::datatypes::DataType::Int32 | 
                datafusion::arrow::datatypes::DataType::Float32
            ))
            .map(|f| f.name().clone())
            .collect()
    };
    
    if args.verbose {
        eprintln!("Computing {:?} correlations for {} numeric columns", args.correlation_type, target_columns.len());
    }
    
    let corr_df = calculate_correlations(
        &df, 
        &target_columns, 
        &args.correlation_type,
        args.correlation_matrix,
        args.stats_tests
    ).await?;
    
    display_dataframe(&corr_df, args.output.as_deref(), args.format.as_ref()).await?;
    
    Ok(())
}