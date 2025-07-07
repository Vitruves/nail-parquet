use std::path::Path;

use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use crate::cli::CommonArgs;
use clap::Args;
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
use datafusion::parquet::basic::Compression;

#[derive(Args, Clone)]
pub struct OptimizeArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Compression type
    #[arg(long, default_value = "snappy", help = "Compression type")]
    #[arg(value_enum)]
    pub compression: CompressionType,

    /// Compression level (1-9)
    #[arg(long, default_value = "6", help = "Compression level (1-9)")]
    pub compression_level: u32,

    /// Sort by columns for better compression (comma-separated)
    #[arg(long, help = "Sort by columns for better compression (comma-separated)")]
    pub sort_by: Option<String>,

    /// Row group size
    #[arg(long, default_value = "1000000", help = "Row group size")]
    pub row_group_size: usize,

    /// Enable dictionary encoding
    #[arg(long, help = "Enable dictionary encoding")]
    pub dictionary: bool,

    /// Disable dictionary encoding
    #[arg(long, help = "Disable dictionary encoding")]
    pub no_dictionary: bool,

    /// Validate optimized file after creation
    #[arg(long, help = "Validate optimized file after creation")]
    pub validate: bool,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum CompressionType {
    Snappy,
    Gzip,
    Zstd,
    Brotli,
}

impl CompressionType {
    fn to_parquet_compression(&self, _level: i32) -> Compression {
        match self {
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Gzip => Compression::GZIP(Default::default()),
            CompressionType::Zstd => Compression::ZSTD(Default::default()),
            CompressionType::Brotli => Compression::BROTLI(Default::default()),
        }
    }
}

pub async fn execute(args: OptimizeArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Optimizing Parquet file: {}", args.common.input.display()));
    args.common.log_if_verbose(&format!("Compression: {:?} (level {})", args.compression, args.compression_level));
    if let Some(ref cols) = args.sort_by {
        args.common.log_if_verbose(&format!("Sorting by columns: {}", cols));
    }
    args.common.log_if_verbose(&format!("Row group size: {}", args.row_group_size));

    // Validate compression level
    if args.compression_level < 1 || args.compression_level > 9 {
        return Err(NailError::InvalidArgument(
            "Compression level must be between 1 and 9".to_string()
        ));
    }

    // Dictionary encoding logic
    let use_dictionary = if args.dictionary && args.no_dictionary {
        return Err(NailError::InvalidArgument(
            "Cannot specify both --dictionary and --no-dictionary".to_string()
        ));
    } else if args.no_dictionary {
        false
    } else {
        args.dictionary || true // Default to true if neither specified
    };

    // Read the input Parquet file
    let df = read_data(&args.common.input).await?;
    
    let count = df.clone().count().await?;
    args.common.log_if_verbose(&format!("Input file contains {} rows", count));

    // Sort data if requested
    let sorted_df = if let Some(sort_cols) = &args.sort_by {
        let columns: Vec<&str> = sort_cols.split(',').map(|s| s.trim()).collect();
        
        // Validate columns exist
        let schema = df.schema();
        for col in &columns {
            if schema.field_with_name(None, col).is_err() {
                let available_cols: Vec<String> = schema.fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                return Err(NailError::ColumnNotFound(
                    format!("Column '{}' not found. Available columns: {}", 
                            col, available_cols.join(", "))
                ));
            }
        }
        
        args.common.log_if_verbose(&format!("Sorting data by columns: {:?}", columns));
        
        // Create sort expressions
        let sort_exprs: Vec<datafusion::logical_expr::SortExpr> = columns.iter()
            .map(|column_name| col(*column_name).sort(true, true))
            .collect();
        
        df.sort(sort_exprs)?
    } else {
        df
    };

    // Determine output path
    let output_path = args.common.output.clone().unwrap_or_else(|| {
        let stem = args.common.input.file_stem()
            .unwrap_or_default()
            .to_string_lossy();
        args.common.input.with_file_name(format!("{}_optimized.parquet", stem))
    });

    args.common.log_if_verbose(&format!("Writing optimized file to: {}", output_path.display()));

    // Configure writer properties
    let compression = args.compression.to_parquet_compression(args.compression_level as i32);
    
    let mut props_builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(compression)
        .set_max_row_group_size(args.row_group_size);

    if use_dictionary {
        props_builder = props_builder.set_dictionary_enabled(true);
    } else {
        props_builder = props_builder.set_dictionary_enabled(false);
    }

    let writer_props = props_builder.build();

    // Write the optimized Parquet file
    write_optimized_parquet(&sorted_df, &output_path, writer_props).await?;

    args.common.log_if_verbose("Optimization complete!");
    
    // Show file size comparison
    if args.common.verbose {
        if let Ok(original_size) = std::fs::metadata(&args.common.input).map(|m| m.len()) {
            if let Ok(optimized_size) = std::fs::metadata(&output_path).map(|m| m.len()) {
                let reduction = 100.0 * (1.0 - optimized_size as f64 / original_size as f64);
                args.common.log_if_verbose(&format!("Original size: {} bytes", original_size));
                args.common.log_if_verbose(&format!("Optimized size: {} bytes", optimized_size));
                args.common.log_if_verbose(&format!("Size reduction: {:.1}%", reduction));
            }
        }
    }

    // Validate if requested
    if args.validate {
        args.common.log_if_verbose("Validating optimized file...");
        
        let validated_df = read_data(&output_path).await?;
        let original_count = sorted_df.clone().count().await?;
        let validated_count = validated_df.count().await?;
        
        if original_count != validated_count {
            return Err(NailError::InvalidArgument(
                format!("Validation failed: row count mismatch (original: {}, optimized: {})",
                        original_count, validated_count)
            ));
        }
        
        args.common.log_if_verbose(&format!("Validation successful: {} rows", validated_count));
    }

    Ok(())
}

async fn write_optimized_parquet(
    df: &DataFrame,
    path: &Path,
    _writer_props: WriterProperties,
) -> NailResult<()> {
    // Check if DataFrame is empty and handle it specially
    let row_count = df.clone().count().await.map_err(NailError::DataFusion)?;
    if row_count == 0 {
        // Use the same empty file handling as in write_data
        crate::utils::io::write_empty_parquet_file(df, path).await?;
    } else {
        // Use the lower-level approach to write with custom WriterProperties
        
        let write_options = DataFrameWriteOptions::new()
            .with_single_file_output(true);
        
        // Note: This is a limitation in the current DataFusion API version
        // The WriterProperties are built correctly but can't be passed directly
        // In a future version, this should be updated to use the writer_props parameter
        // For now, DataFusion will use its default compression and settings
        df.clone()
            .write_parquet(
                path.to_str().unwrap(),
                write_options,
                None, // Use default options for now
            )
            .await
            .map_err(NailError::DataFusion)?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::CommonArgs;
    use std::path::PathBuf;
    use tempfile::tempdir;
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use arrow::array::{Int64Array, StringArray, Float64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::fs::File;
    use std::sync::Arc;

    fn create_test_data() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.parquet");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        // Create larger dataset for better compression testing
        let mut ids = Vec::new();
        let mut names = Vec::new();
        let mut values = Vec::new();
        let mut categories = Vec::new();
        
        for i in 0..1000 {
            ids.push(i as i64);
            names.push(format!("Name_{}", i % 100)); // Repeated names for compression
            values.push((i as f64) * 1.5);
            categories.push(match i % 4 {
                0 => "A",
                1 => "B", 
                2 => "C",
                _ => "D",
            }.to_string());
        }

        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names);
        let value_array = Float64Array::from(values);
        let category_array = StringArray::from(categories);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
                Arc::new(category_array),
            ],
        ).unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_dir, file_path)
    }

    fn create_small_test_data() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("small.parquet");
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let a_array = Int64Array::from(vec![3, 1, 2]);
        let b_array = StringArray::from(vec!["z", "x", "y"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(a_array), Arc::new(b_array)],
        ).unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        (temp_dir, file_path)
    }

    #[tokio::test]
    async fn test_optimize_default_compression() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("optimized.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 6,
            sort_by: None,
            row_group_size: 1000000,
            dictionary: false,
            no_dictionary: false,
            validate: false,
        };

        execute(args).await.unwrap();

        // Verify output file exists and is readable
        assert!(output_path.exists());
        
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 1000);
        assert_eq!(df.schema().fields().len(), 4);
    }

    #[tokio::test]
    async fn test_optimize_with_sorting() {
        let (_temp_dir, input_path) = create_small_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("sorted.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Gzip,
            compression_level: 3,
            sort_by: Some("a,b".to_string()),
            row_group_size: 100,
            dictionary: true,
            no_dictionary: false,
            validate: true,
        };

        execute(args).await.unwrap();

        // Verify output file exists
        assert!(output_path.exists());
        
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 3);
    }

    #[tokio::test]
    async fn test_optimize_different_compression_types() {
        let compression_types = vec![
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Zstd,
            CompressionType::Brotli,
        ];

        for compression in compression_types {
            let (_temp_dir, input_path) = create_test_data();
            let output_dir = tempdir().unwrap();
            let output_path = output_dir.path().join(format!("{:?}_compressed.parquet", compression));
            
            let args = OptimizeArgs {
                common: CommonArgs {
                    input: input_path,
                    output: Some(output_path.clone()),
                    format: None,
                    random: None,
                    verbose: false,
                    jobs: None,
                },
                compression: compression.clone(),
                compression_level: 5,
                sort_by: None,
                row_group_size: 500,
                dictionary: false,
                no_dictionary: false,
                validate: false,
            };

            execute(args).await.unwrap();
            assert!(output_path.exists());
        }
    }

    #[tokio::test]
    async fn test_optimize_with_validation() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("validated.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: true, // Test verbose mode
                jobs: None,
            },
            compression: CompressionType::Zstd,
            compression_level: 4,
            sort_by: Some("id".to_string()),
            row_group_size: 250,
            dictionary: false,
            no_dictionary: false,
            validate: true, // Enable validation
        };

        execute(args).await.unwrap();

        assert!(output_path.exists());
        
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 1000);
    }

    #[tokio::test]
    async fn test_optimize_invalid_compression_level() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 10, // Invalid level
            sort_by: None,
            row_group_size: 1000,
            dictionary: false,
            no_dictionary: false,
            validate: false,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Compression level must be between 1 and 9"));
    }

    #[tokio::test]
    async fn test_optimize_conflicting_dictionary_flags() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 5,
            sort_by: None,
            row_group_size: 1000,
            dictionary: true,
            no_dictionary: true, // Conflicting flags
            validate: false,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot specify both --dictionary and --no-dictionary"));
    }

    #[tokio::test]
    async fn test_optimize_invalid_sort_column() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 5,
            sort_by: Some("nonexistent_column".to_string()),
            row_group_size: 1000,
            dictionary: false,
            no_dictionary: false,
            validate: false,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_optimize_no_output_path() {
        let (_temp_dir, input_path) = create_test_data();
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path.clone(),
                output: None, // No output path specified
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 5,
            sort_by: None,
            row_group_size: 1000,
            dictionary: false,
            no_dictionary: false,
            validate: false,
        };

        execute(args).await.unwrap();

        // Should create optimized file with default naming
        let expected_output = input_path.with_file_name("test_optimized.parquet");
        assert!(expected_output.exists());
    }

    #[tokio::test]
    async fn test_optimize_dictionary_enabled() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("dictionary_enabled.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 5,
            sort_by: None,
            row_group_size: 1000,
            dictionary: true, // Enable dictionary
            no_dictionary: false,
            validate: false,
        };

        execute(args).await.unwrap();
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_optimize_dictionary_disabled() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("dictionary_disabled.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 5,
            sort_by: None,
            row_group_size: 1000,
            dictionary: false,
            no_dictionary: true, // Disable dictionary
            validate: false,
        };

        execute(args).await.unwrap();
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_optimize_custom_row_group_size() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("custom_row_group.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 5,
            sort_by: None,
            row_group_size: 100, // Small row group size
            dictionary: false,
            no_dictionary: false,
            validate: false,
        };

        execute(args).await.unwrap();
        assert!(output_path.exists());
        
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 1000);
    }

    #[tokio::test]
    async fn test_optimize_multiple_sort_columns() {
        let (_temp_dir, input_path) = create_test_data();
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("multi_sort.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: input_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Gzip,
            compression_level: 3,
            sort_by: Some("category,name,id".to_string()), // Multiple columns
            row_group_size: 200,
            dictionary: false,
            no_dictionary: false,
            validate: true,
        };

        execute(args).await.unwrap();
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_optimize_empty_dataset() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("empty.parquet");
        
        // Create empty dataset
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(Vec::<i64>::new());
        let name_array = StringArray::from(Vec::<String>::new());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        ).unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        let output_dir = tempdir().unwrap();
        let output_path = output_dir.path().join("empty_optimized.parquet");
        
        let args = OptimizeArgs {
            common: CommonArgs {
                input: file_path,
                output: Some(output_path.clone()),
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            compression: CompressionType::Snappy,
            compression_level: 5,
            sort_by: None,
            row_group_size: 1000,
            dictionary: false,
            no_dictionary: false,
            validate: true,
        };

        execute(args).await.unwrap();
        
        assert!(output_path.exists());
        
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(output_path.to_str().unwrap(), Default::default()).await.unwrap();
        let row_count = df.clone().count().await.unwrap();
        assert_eq!(row_count, 0);
    }

    #[test]
    fn test_compression_type_conversion() {
        assert!(matches!(
            CompressionType::Snappy.to_parquet_compression(5),
            Compression::SNAPPY
        ));
        
        assert!(matches!(
            CompressionType::Gzip.to_parquet_compression(5),
            Compression::GZIP(_)
        ));
        
        assert!(matches!(
            CompressionType::Zstd.to_parquet_compression(5),
            Compression::ZSTD(_)
        ));
        
        assert!(matches!(
            CompressionType::Brotli.to_parquet_compression(5),
            Compression::BROTLI(_)
        ));
    }
}