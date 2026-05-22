use crate::error::{NailError, NailResult};
use crate::utils::suggest::did_you_mean_suffix;
use datafusion::common::DFSchemaRef;

pub fn resolve_column_name(schema: &DFSchemaRef, column_input: &str) -> NailResult<String> {
	// Strip surrounding quotes if present.
	let clean_column =
		if column_input.starts_with('"') && column_input.ends_with('"') && column_input.len() > 1 {
			&column_input[1..column_input.len() - 1]
		} else {
			column_input
		};

	schema
		.fields()
		.iter()
		.find(|f| f.name().to_lowercase() == clean_column.to_lowercase())
		.map(|f| f.name().clone())
		.ok_or_else(|| {
			let available: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
			let suggestion = did_you_mean_suffix(clean_column, available.iter());
			NailError::ColumnNotFound(format!(
				"Column '{}' not found.{} Available columns: {:?}",
				clean_column, suggestion, available
			))
		})
}
