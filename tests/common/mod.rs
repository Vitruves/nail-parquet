// File: tests/common/mod.rs

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

/// A struct to hold all test fixtures for a single test.
/// It manages the temporary directory and paths to all created files,
/// ensuring automatic cleanup when it goes out of scope.
pub struct TestFixtures {
    pub _temp_dir: TempDir, // Held for RAII cleanup
    pub sample_parquet: PathBuf,
    pub sample2_parquet: PathBuf,
    pub sample_with_nulls_parquet: PathBuf,
    pub sample_with_duplicates_parquet: PathBuf,
    pub sample_for_stratify_parquet: PathBuf,
    pub sample_for_col_dedup_parquet: PathBuf,
    pub sample_mixed_types_parquet: PathBuf,
    pub sample_csv: PathBuf,
    pub empty_parquet: PathBuf,
    pub output_dir: PathBuf,
}

impl TestFixtures {
    /// Creates a new set of test fixtures in a temporary directory.
    pub fn new() -> Self {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_path = temp_dir.path();

        let output_dir = base_path.join("outputs");
        fs::create_dir(&output_dir).expect("Failed to create output dir");

        let sample_parquet = base_path.join("sample.parquet");
        create_sample_parquet(&sample_parquet).unwrap();

        let sample2_parquet = base_path.join("sample2.parquet");
        create_sample2_parquet(&sample2_parquet).unwrap();

        let sample_with_nulls_parquet = base_path.join("sample_with_nulls.parquet");
        create_sample_with_nulls_parquet(&sample_with_nulls_parquet).unwrap();

        let sample_with_duplicates_parquet = base_path.join("sample_with_duplicates.parquet");
        create_sample_with_duplicates_parquet(&sample_with_duplicates_parquet).unwrap();
        
        let sample_for_stratify_parquet = base_path.join("sample_for_stratify.parquet");
        create_for_stratify_parquet(&sample_for_stratify_parquet).unwrap();

        let sample_for_col_dedup_parquet = base_path.join("sample_for_col_dedup.parquet");
        create_for_col_dedup_parquet(&sample_for_col_dedup_parquet).unwrap();

        let sample_mixed_types_parquet = base_path.join("sample_mixed_types.parquet");
        create_mixed_types_parquet(&sample_mixed_types_parquet).unwrap();

        let sample_csv = base_path.join("sample.csv");
        create_sample_csv(&sample_csv).unwrap();

        let empty_parquet = base_path.join("empty.parquet");
        create_empty_parquet(&empty_parquet).unwrap();

        Self {
            _temp_dir: temp_dir,
            sample_parquet,
            sample2_parquet,
            sample_with_nulls_parquet,
            sample_with_duplicates_parquet,
            sample_for_stratify_parquet,
            sample_for_col_dedup_parquet,
            sample_mixed_types_parquet,
            sample_csv,
            empty_parquet,
            output_dir,
        }
    }

    /// Helper to get a path for a new output file within the fixture's output directory.
    pub fn get_output_path(&self, filename: &str) -> PathBuf {
        self.output_dir.join(filename)
    }
}

/// Helper to read a Parquet file and get its row count using DataFusion.
pub async fn get_row_count(path: &Path) -> usize {
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(path.to_str().unwrap(), ParquetReadOptions::default())
        .await
        .expect("Failed to read parquet file for row count");
    df.count().await.expect("Failed to count rows")
}

// --- Data Creation Functions (private to this module) ---

fn create_sample_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), Some("Charlie"), Some("David"), Some("Eve")])),
        Arc::new(Float64Array::from(vec![Some(100.0), Some(250.5), Some(300.0), Some(450.5), Some(500.0)])),
        Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("A"), Some("B"), Some("C")])),
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_sample2_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
        Arc::new(Float64Array::from(vec![Some(88.0), Some(92.5), None, Some(75.0)])),
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_sample_with_nulls_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Charlie"), None, Some("Eve")])),
        Arc::new(Float64Array::from(vec![Some(100.0), Some(200.0), None, Some(400.0), None])),
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_sample_with_duplicates_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("val", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2, 2, 3, 4, 4, 4])),
        Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("B"), Some("C"), Some("D"), Some("D"), Some("D")])),
        Arc::new(Int64Array::from(vec![10, 20, 20, 30, 40, 41, 40])), // Note the 41 to test subset dedup
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_for_stratify_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("strat_key", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from( (0..20).collect::<Vec<i64>>() )),
        Arc::new(StringArray::from(
            vec!["A","A","A","A","A","A","A","A","A","A", // 10 'A'
                 "B","B","B","B","B",                      // 5 'B'
                 "C","C","C",                              // 3 'C'
                 "D","D"]                                  // 2 'D'
        )),
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_for_col_dedup_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Int64, false),
        Field::new("col_b", DataType::Int64, false),
        Field::new("col_a", DataType::Int64, false), // Duplicate name
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(Int64Array::from(vec![3, 4])),
        Arc::new(Int64Array::from(vec![5, 6])),
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_mixed_types_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int_col", DataType::Int64, true),
        Field::new("float_col", DataType::Float64, true),
        Field::new("string_col", DataType::Utf8, true),
        Field::new("bool_col", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![Some(1), Some(0), None, Some(3)])),
        Arc::new(Float64Array::from(vec![Some(1.1), Some(0.0), Some(3.3), None])),
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, Some("d")])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None, Some(true)])),
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_sample_csv(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let content = "csv_id,csv_name,csv_val\n10,Frank,1000.0\n11,Grace,1100.0\n12,Heidi,1200.0\n";
    fs::write(path, content)?;
    Ok(())
}

fn create_empty_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        Arc::new(StringArray::from(Vec::<Option<String>>::new())),
    ])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}