use std::path::Path;
use crate::error::{NailError, NailResult};

/// Fast row count for Parquet files using metadata without scanning data
pub async fn get_parquet_row_count_fast(path: &Path) -> NailResult<usize> {
    use std::fs::File;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    
    let file = File::open(path)
        .map_err(|e| NailError::Io(e))?;
    
    let reader = SerializedFileReader::new(file)
        .map_err(|e| NailError::InvalidArgument(format!("Failed to read Parquet metadata: {}", e)))?;
    
    let metadata = reader.metadata();
    let mut total_rows = 0;
    
    // Sum up rows from all row groups
    for i in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(i);
        total_rows += row_group.num_rows() as usize;
    }
    
    Ok(total_rows)
}

/// Check if we can use fast metadata reading for this file
pub fn can_use_fast_metadata(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_lowercase() == "parquet")
        .unwrap_or(false)
}
