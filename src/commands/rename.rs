use clap::Args;
use datafusion::prelude::*;
use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;

#[derive(Args, Clone)]
pub struct RenameArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    #[arg(short = 'c', long = "column", help = "Column rename specs (before=after), comma-separated")]
    pub columns: String,
}

pub async fn execute(args: RenameArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

    let df = read_data(&args.common.input).await?;
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

    args.common.log_if_verbose(&format!("Renaming columns: {:?}", rename_map));

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
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&result_df, "rename").await?;

    Ok(())
}
