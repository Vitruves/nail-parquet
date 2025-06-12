use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

// Creates sample.parquet
pub fn create_sample_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), Some("Charlie"), None, Some("Eve")]);
    let value_array = Float64Array::from(vec![Some(100.0), None, Some(300.0), Some(400.0), Some(500.0)]);
    let category_array = StringArray::from(vec![Some("A"), Some("B"), Some("A"), Some("B"), Some("A")]);

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(id_array),
        Arc::new(name_array),
        Arc::new(value_array),
        Arc::new(category_array),
    ])?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

// Creates sample2.parquet for joins
pub fn create_sample2_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, true),
    ]));

    let id_array = Int64Array::from(vec![4, 5, 6, 7]);
    let score_array = Float64Array::from(vec![Some(88.0), Some(92.5), None, Some(88.0)]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array), Arc::new(score_array)])?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

// Creates sample3.parquet for key-mapping joins
pub fn create_sample3_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let id_array = Int64Array::from(vec![1, 3, 5]);
    let status_array = StringArray::from(vec!["active", "inactive", "active"]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array), Arc::new(status_array)])?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

// Creates sample.csv for conversion tests
pub fn create_sample_csv(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let content = "id,name,value\n10,Frank,1000.0\n11,Grace,1100.0\n";
    fs::write(path, content)?;
    Ok(())
}

// Creates sample.xlsx for Excel conversion tests  
pub fn create_sample_excel(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple CSV file as Excel placeholder since we can't easily create Excel files
    let content = "sample_item,sample_value\nitem1,100\nitem2,200\n";
    let csv_path = path.with_extension("csv");
    fs::write(&csv_path, content)?;
    
    // Copy it to .xlsx extension for the test (it will still be read as CSV)
    fs::copy(&csv_path, path)?;
    fs::remove_file(&csv_path)?;
    Ok(())
}