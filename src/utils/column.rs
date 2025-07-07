use datafusion::common::DFSchemaRef;
use crate::error::{NailError, NailResult};

pub fn resolve_column_name(
    schema: &DFSchemaRef,
    column_input: &str,
) -> NailResult<String> {
    schema.fields().iter()
        .find(|f| f.name().to_lowercase() == column_input.to_lowercase())
        .map(|f| f.name().clone())
        .ok_or_else(|| {
            let available_cols: Vec<String> = schema.fields().iter()
                .map(|f| f.name().clone())
                .collect();
            NailError::ColumnNotFound(format!(
                "Column '{}' not found. Available columns: {:?}",
                column_input, available_cols
            ))
        })
}

