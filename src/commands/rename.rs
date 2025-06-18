use clap::Args;
use datafusion::prelude::*;
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;
use crate::utils::FileFormat;

#[derive(Args, Clone)]
pub struct RenameArgs {
    #[arg(help = "Input file")]
    pub input: PathBuf,

    #[arg(short = 'c', long = "column", help = "Column rename specs (before=after), comma-separated")]
    pub columns: String,

    #[arg(short, long, help = "Output file (if not specified, prints to console)")]
    pub output: Option<PathBuf>,

    #[arg(short, long, help = "Output format", value_enum)]
    pub format: Option<crate::cli::OutputFormat>,

    #[arg(short, long, help = "Number of parallel jobs")]
    pub jobs: Option<usize>,

    #[arg(short, long, help = "Enable verbose output")]
    pub verbose: bool,
}

pub async fn execute(args: RenameArgs) -> NailResult<()> {
    if args.verbose {
        eprintln!("Reading data from: {}", args.input.display());
    }

    let df = read_data(&args.input).await?;
    let mut result_df = df;

    // Parse rename specifications
    let mut rename_map = Vec::new();
    for pair in args.columns.split(',') {
        let parts: Vec<&str> = pair.split('=').collect();
        if parts.len() != 2 {
            return Err(NailError::InvalidArgument(format!("Invalid column spec: {}", pair)));
        }
        let before = parts[0].trim();
        let after = parts[1].trim();
        rename_map.push((before.to_string(), after.to_string()));
    }

    if args.verbose {
        eprintln!("Renaming columns: {:?}", rename_map);
    }

    // Validate source columns exist
    let schema = result_df.schema();
    let existing_columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    
    for (source, _) in &rename_map {
        if !existing_columns.contains(source) {
            return Err(NailError::InvalidArgument(format!("Source column '{}' does not exist", source)));
        }
    }

    // Validate no duplicate target column names
    let mut target_names = Vec::new();
    let mut final_column_names = Vec::new();
    
    for field in schema.fields() {
        let name = field.name();
        if let Some(new_name) = rename_map.iter().find(|(b, _)| b == name).map(|(_, a)| a) {
            target_names.push(new_name.clone());
            final_column_names.push(new_name.clone());
        } else {
            final_column_names.push(name.clone());
        }
    }
    
    // Check for duplicates in target names
    for (i, name) in target_names.iter().enumerate() {
        if target_names.iter().skip(i + 1).any(|other| other == name) {
            return Err(NailError::InvalidArgument(format!("Duplicate target column name: '{}'", name)));
        }
    }
    
    // Check for conflicts between renamed columns and existing columns
    for target in &target_names {
        if final_column_names.iter().filter(|&n| n == target).count() > 1 {
            return Err(NailError::InvalidArgument(format!("Target column name '{}' conflicts with existing column", target)));
        }
    }

    // Build select expressions with aliases
    let select_exprs: Vec<Expr> = result_df.schema().fields().iter()
        .map(|f| {
            let name = f.name();
            if let Some(new_name) = rename_map.iter().find(|(b, _)| b == name).map(|(_, a)| a) {
                Expr::Column(datafusion::common::Column::new(None::<String>, name)).alias(new_name)
            } else {
                Expr::Column(datafusion::common::Column::new(None::<String>, name))
            }
        })
        .collect();
    result_df = result_df.select(select_exprs)?;

    // Write or display result
    if let Some(output_path) = &args.output {
        let file_format = match args.format {
            Some(crate::cli::OutputFormat::Json) => Some(FileFormat::Json),
            Some(crate::cli::OutputFormat::Csv) => Some(FileFormat::Csv),
            Some(crate::cli::OutputFormat::Parquet) => Some(FileFormat::Parquet),
            _ => None,
        };
        write_data(&result_df, output_path, file_format.as_ref()).await?;
    } else {
        display_dataframe(&result_df, None, args.format.as_ref()).await?;
    }

    Ok(())
}
