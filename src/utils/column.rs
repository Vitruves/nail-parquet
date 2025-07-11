use datafusion::common::DFSchemaRef;
use crate::error::{NailError, NailResult};

pub fn resolve_column_name(
    schema: &DFSchemaRef,
    column_input: &str,
) -> NailResult<String> {
    // Strip quotes from column name if present
    let clean_column = if column_input.starts_with('"') && column_input.ends_with('"') && column_input.len() > 1 {
        &column_input[1..column_input.len()-1]
    } else {
        column_input
    };
    
    schema.fields().iter()
        .find(|f| f.name().to_lowercase() == clean_column.to_lowercase())
        .map(|f| f.name().clone())
        .ok_or_else(|| {
            let available_cols: Vec<String> = schema.fields().iter()
                .map(|f| f.name().clone())
                .collect();
            NailError::ColumnNotFound(format!(
                "Column '{}' not found. Available columns: {:?}",
                clean_column, available_cols
            ))
        })
}

