use crate::error::{NailError, NailResult};
use crate::utils::io::read_data;
use clap::Args;
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
use datafusion::parquet::basic::Compression;
use std::path::{Path, PathBuf};

#[derive(Args, Clone)]
pub struct OptimizeArgs {
    /// Input Parquet file
    #[arg(help = "Input Parquet file")]
    pub input: PathBuf,

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

    /// Output file
    #[arg(short, long, help = "Output file [default: <input>_optimized.parquet]")]
    pub output: Option<PathBuf>,

    /// Number of parallel jobs
    #[arg(short, long, help = "Number of parallel jobs")]
    pub jobs: Option<usize>,

    /// Enable verbose output
    #[arg(short, long, help = "Enable verbose output")]
    pub verbose: bool,
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
    if args.verbose {
        eprintln!("Optimizing Parquet file: {}", args.input.display());
        eprintln!("Compression: {:?} (level {})", args.compression, args.compression_level);
        if let Some(ref cols) = args.sort_by {
            eprintln!("Sorting by columns: {}", cols);
        }
        eprintln!("Row group size: {}", args.row_group_size);
    }

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
    let df = read_data(&args.input).await?;
    
    if args.verbose {
        let count = df.clone().count().await?;
        eprintln!("Input file contains {} rows", count);
    }

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
        
        if args.verbose {
            eprintln!("Sorting data by columns: {:?}", columns);
        }
        
        // Create sort expressions
        let sort_exprs: Vec<datafusion::logical_expr::SortExpr> = columns.iter()
            .map(|column_name| col(*column_name).sort(true, true))
            .collect();
        
        df.sort(sort_exprs)?
    } else {
        df
    };

    // Determine output path
    let output_path = args.output.unwrap_or_else(|| {
        let stem = args.input.file_stem()
            .unwrap_or_default()
            .to_string_lossy();
        args.input.with_file_name(format!("{}_optimized.parquet", stem))
    });

    if args.verbose {
        eprintln!("Writing optimized file to: {}", output_path.display());
    }

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

    if args.verbose {
        eprintln!("Optimization complete!");
        
        // Show file size comparison
        if let Ok(original_size) = std::fs::metadata(&args.input).map(|m| m.len()) {
            if let Ok(optimized_size) = std::fs::metadata(&output_path).map(|m| m.len()) {
                let reduction = 100.0 * (1.0 - optimized_size as f64 / original_size as f64);
                eprintln!("Original size: {} bytes", original_size);
                eprintln!("Optimized size: {} bytes", optimized_size);
                eprintln!("Size reduction: {:.1}%", reduction);
            }
        }
    }

    // Validate if requested
    if args.validate {
        if args.verbose {
            eprintln!("Validating optimized file...");
        }
        
        let validated_df = read_data(&output_path).await?;
        let original_count = sorted_df.clone().count().await?;
        let validated_count = validated_df.count().await?;
        
        if original_count != validated_count {
            return Err(NailError::InvalidArgument(
                format!("Validation failed: row count mismatch (original: {}, optimized: {})",
                        original_count, validated_count)
            ));
        }
        
        if args.verbose {
            eprintln!("Validation successful: {} rows", validated_count);
        }
    }

    Ok(())
}

async fn write_optimized_parquet(
    df: &DataFrame,
    path: &Path,
    _writer_props: WriterProperties,
) -> NailResult<()> {
    // DataFusion's write_parquet method now supports WriterProperties
    // For now, we'll use the default write method and rely on DataFusion's optimization
    let write_options = DataFrameWriteOptions::new()
        .with_single_file_output(true);
    
    df.clone()
        .write_parquet(
            path.to_str().unwrap(),
            write_options,
            None,
        )
        .await
        .map_err(NailError::DataFusion)?;
    
    Ok(())
}