use clap::Args;
use crate::error::{NailError, NailResult};
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use datafusion::prelude::*;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, DataType as ArrowDataType};
use datafusion::arrow::record_batch::RecordBatch;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::metadata::ParquetMetaData;
use std::fs::File;
use std::sync::Arc;
use std::collections::HashMap;

#[derive(Args, Clone)]
pub struct MetadataArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    
    #[arg(long, help = "Show detailed schema information")]
    pub schema: bool,
    
    #[arg(long, help = "Show row group information")]
    pub row_groups: bool,
    
    #[arg(long, help = "Show column chunk information")]
    pub column_chunks: bool,
    
    #[arg(long, help = "Show compression information")]
    pub compression: bool,
    
    #[arg(long, help = "Show encoding information")]
    pub encoding: bool,
    
    #[arg(long, help = "Show statistics information")]
    pub statistics: bool,
    
    #[arg(long, help = "Show all available metadata")]
    pub all: bool,
    
    #[arg(long, help = "Show metadata in detailed format")]
    pub detailed: bool,
}

pub async fn execute(args: MetadataArgs) -> NailResult<()> {
    // Check if input file is a parquet file
    if !args.common.input.extension().map_or(false, |ext| ext == "parquet") {
        return Err(NailError::UnsupportedFormat(
            "Metadata command only supports Parquet files".to_string()
        ));
    }
    
    args.common.log_if_verbose(&format!(
        "Reading metadata from: {}",
        args.common.input.display()
    ));
    
    let file = File::open(&args.common.input)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    
    let mut metadata_items = Vec::new();
    
    // Basic file metadata (always included)
    collect_basic_metadata(metadata, &mut metadata_items);
    
    // Schema information
    if args.schema || args.all {
        collect_schema_metadata(metadata, &mut metadata_items);
    }
    
    // Row group information
    if args.row_groups || args.all {
        collect_row_group_metadata(metadata, &mut metadata_items, args.detailed);
    }
    
    // Column chunk information
    if args.column_chunks || args.all {
        collect_column_chunk_metadata(metadata, &mut metadata_items, args.detailed);
    }
    
    // Compression information
    if args.compression || args.all {
        collect_compression_metadata(metadata, &mut metadata_items);
    }
    
    // Encoding information
    if args.encoding || args.all {
        collect_encoding_metadata(metadata, &mut metadata_items);
    }
    
    // Statistics information
    if args.statistics || args.all {
        collect_statistics_metadata(metadata, &mut metadata_items, args.detailed);
    }
    
    args.common.log_if_verbose(&format!("Collected {} metadata items", metadata_items.len()));
    
    // Output metadata items
    match &args.common.output {
        Some(_output_path) => {
            // Create DataFrame for file output
            let result_df = create_metadata_dataframe(metadata_items).await?;
            let output_handler = OutputHandler::new(&args.common);
            output_handler.handle_output(&result_df, "metadata").await?;
        }
        None => {
            // Simple console output - one item per line
            for item in metadata_items {
                println!("{}: {}", item.0, item.1);
            }
        }
    }
    
    Ok(())
}

fn collect_basic_metadata(metadata: &ParquetMetaData, items: &mut Vec<(String, String)>) {
    let file_metadata = metadata.file_metadata();
    
    items.push(("file_version".to_string(), file_metadata.version().to_string()));
    items.push(("created_by".to_string(), file_metadata.created_by().unwrap_or("Unknown").to_string()));
    items.push(("total_rows".to_string(), file_metadata.num_rows().to_string()));
    items.push(("num_row_groups".to_string(), metadata.num_row_groups().to_string()));
    items.push(("num_columns".to_string(), file_metadata.schema_descr().num_columns().to_string()));
    
    // File size calculations
    let mut total_uncompressed_size = 0i64;
    let mut total_compressed_size = 0i64;
    
    for row_group in metadata.row_groups().iter() {
        total_uncompressed_size += row_group.total_byte_size();
        total_compressed_size += row_group.compressed_size();
    }
    
    items.push(("total_uncompressed_size".to_string(), total_uncompressed_size.to_string()));
    items.push(("total_compressed_size".to_string(), total_compressed_size.to_string()));
    items.push(("compression_ratio".to_string(), 
        if total_uncompressed_size > 0 {
            format!("{:.2}%", (total_compressed_size as f64 / total_uncompressed_size as f64) * 100.0)
        } else {
            "N/A".to_string()
        }
    ));
    
    // Schema-level metadata
    let schema = file_metadata.schema_descr();
    items.push(("schema_name".to_string(), schema.name().to_string()));
    items.push(("schema_num_fields".to_string(), schema.num_columns().to_string()));
    
    // Key-value metadata if present
    if let Some(kv_metadata) = file_metadata.key_value_metadata() {
        items.push(("key_value_metadata_count".to_string(), kv_metadata.len().to_string()));
        for kv in kv_metadata.iter() {
            items.push((format!("metadata_{}", kv.key), kv.value.clone().unwrap_or_default()));
        }
    } else {
        items.push(("key_value_metadata_count".to_string(), "0".to_string()));
    }
}

fn collect_schema_metadata(metadata: &ParquetMetaData, items: &mut Vec<(String, String)>) {
    let schema = metadata.file_metadata().schema_descr();
    
    for (i, field) in schema.columns().iter().enumerate() {
        let col_prefix = format!("column_{}_", i);
        
        items.push((format!("{}name", col_prefix), field.name().to_string()));
        items.push((format!("{}physical_type", col_prefix), format!("{:?}", field.physical_type())));
        items.push((format!("{}logical_type", col_prefix), 
            field.logical_type().map_or("None".to_string(), |lt| format!("{:?}", lt))));
        items.push((format!("{}max_definition_level", col_prefix), field.max_def_level().to_string()));
        items.push((format!("{}max_repetition_level", col_prefix), field.max_rep_level().to_string()));
        items.push((format!("{}is_optional", col_prefix), (field.max_def_level() > 0).to_string()));
        items.push((format!("{}converted_type", col_prefix), format!("{:?}", field.converted_type())));
        items.push((format!("{}precision", col_prefix), 
            if field.type_precision() >= 0 { field.type_precision().to_string() } else { "N/A".to_string() }));
        items.push((format!("{}scale", col_prefix), 
            if field.type_scale() >= 0 { field.type_scale().to_string() } else { "N/A".to_string() }));
        items.push((format!("{}length", col_prefix), 
            if field.type_length() >= 0 { field.type_length().to_string() } else { "N/A".to_string() }));
    }
}

fn collect_row_group_metadata(metadata: &ParquetMetaData, items: &mut Vec<(String, String)>, detailed: bool) {
    for (i, row_group) in metadata.row_groups().iter().enumerate() {
        let rg_prefix = format!("row_group_{}_", i);
        
        items.push((format!("{}num_rows", rg_prefix), row_group.num_rows().to_string()));
        items.push((format!("{}total_byte_size", rg_prefix), row_group.total_byte_size().to_string()));
        items.push((format!("{}compressed_size", rg_prefix), row_group.compressed_size().to_string()));
        items.push((format!("{}num_columns", rg_prefix), row_group.num_columns().to_string()));
        
        let compression_ratio = if row_group.total_byte_size() > 0 {
            (row_group.compressed_size() as f64 / row_group.total_byte_size() as f64) * 100.0
        } else {
            0.0
        };
        items.push((format!("{}compression_ratio", rg_prefix), format!("{:.2}%", compression_ratio)));
        
        if detailed {
            // Add file offset and ordinal information
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                let col_prefix = format!("{}column_{}_", rg_prefix, col_idx);
                
                items.push((format!("{}file_offset", col_prefix), column.file_offset().to_string()));
                items.push((format!("{}data_page_offset", col_prefix), column.data_page_offset().to_string()));
                items.push((format!("{}index_page_offset", col_prefix), 
                    column.index_page_offset().map_or("N/A".to_string(), |o| o.to_string())));
                items.push((format!("{}dictionary_page_offset", col_prefix), 
                    column.dictionary_page_offset().map_or("N/A".to_string(), |o| o.to_string())));
            }
        }
    }
}

fn collect_column_chunk_metadata(metadata: &ParquetMetaData, items: &mut Vec<(String, String)>, detailed: bool) {
    for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
        for (col_idx, column) in row_group.columns().iter().enumerate() {
            let col_prefix = format!("rg_{}_col_{}_", rg_idx, col_idx);
            
            items.push((format!("{}path", col_prefix), column.column_path().to_string()));
            items.push((format!("{}type", col_prefix), format!("{:?}", column.column_type())));
            items.push((format!("{}encodings", col_prefix), format!("{:?}", column.encodings())));
            items.push((format!("{}compression", col_prefix), format!("{:?}", column.compression())));
            items.push((format!("{}uncompressed_size", col_prefix), column.uncompressed_size().to_string()));
            items.push((format!("{}compressed_size", col_prefix), column.compressed_size().to_string()));
            items.push((format!("{}num_values", col_prefix), column.num_values().to_string()));
            
            let compression_ratio = if column.uncompressed_size() > 0 {
                (column.compressed_size() as f64 / column.uncompressed_size() as f64) * 100.0
            } else {
                0.0
            };
            items.push((format!("{}compression_ratio", col_prefix), format!("{:.2}%", compression_ratio)));
            
            if detailed {
                // Add bloom filter and page statistics
                items.push((format!("{}has_bloom_filter", col_prefix), 
                    column.bloom_filter_offset().is_some().to_string()));
                items.push((format!("{}bloom_filter_offset", col_prefix), 
                    column.bloom_filter_offset().map_or("N/A".to_string(), |o| o.to_string())));
                items.push((format!("{}bloom_filter_length", col_prefix), 
                    column.bloom_filter_length().map_or("N/A".to_string(), |l| l.to_string())));
                
                // File offsets
                items.push((format!("{}file_offset", col_prefix), column.file_offset().to_string()));
                items.push((format!("{}data_page_offset", col_prefix), column.data_page_offset().to_string()));
                items.push((format!("{}index_page_offset", col_prefix), 
                    column.index_page_offset().map_or("N/A".to_string(), |o| o.to_string())));
                items.push((format!("{}dictionary_page_offset", col_prefix), 
                    column.dictionary_page_offset().map_or("N/A".to_string(), |o| o.to_string())));
            }
            
            // Statistics
            if let Some(stats) = column.statistics() {
                items.push((format!("{}has_min_max", col_prefix), 
                    (stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some()).to_string()));
                items.push((format!("{}null_count", col_prefix), 
                    stats.null_count_opt().map_or("N/A".to_string(), |c| c.to_string())));
                items.push((format!("{}distinct_count", col_prefix), 
                    stats.distinct_count_opt().map_or("N/A".to_string(), |c| c.to_string())));
                
                if let (Some(min_bytes), Some(max_bytes)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                    items.push((format!("{}min_value_bytes", col_prefix), format!("{:?}", min_bytes)));
                    items.push((format!("{}max_value_bytes", col_prefix), format!("{:?}", max_bytes)));
                }
            } else {
                items.push((format!("{}has_statistics", col_prefix), "false".to_string()));
            }
        }
    }
}

fn collect_compression_metadata(metadata: &ParquetMetaData, items: &mut Vec<(String, String)>) {
    let mut compression_stats = HashMap::new();
    let mut total_uncompressed = 0i64;
    let mut total_compressed = 0i64;
    
    for row_group in metadata.row_groups().iter() {
        for column in row_group.columns().iter() {
            let compression = format!("{:?}", column.compression());
            let uncompressed = column.uncompressed_size();
            let compressed = column.compressed_size();
            
            let entry = compression_stats.entry(compression.clone()).or_insert((0i64, 0i64, 0usize));
            entry.0 += uncompressed;
            entry.1 += compressed;
            entry.2 += 1; // count of chunks
            
            total_uncompressed += uncompressed;
            total_compressed += compressed;
        }
    }
    
    for (compression, (uncompressed, compressed, count)) in compression_stats {
        let comp_prefix = format!("compression_{}_", compression.to_lowercase());
        
        items.push((format!("{}uncompressed_size", comp_prefix), uncompressed.to_string()));
        items.push((format!("{}compressed_size", comp_prefix), compressed.to_string()));
        items.push((format!("{}chunk_count", comp_prefix), count.to_string()));
        items.push((format!("{}ratio", comp_prefix), 
            if uncompressed > 0 {
                format!("{:.2}%", (compressed as f64 / uncompressed as f64) * 100.0)
            } else {
                "N/A".to_string()
            }
        ));
    }
    
    items.push(("total_uncompressed_size".to_string(), total_uncompressed.to_string()));
    items.push(("total_compressed_size".to_string(), total_compressed.to_string()));
    items.push(("overall_compression_ratio".to_string(), 
        if total_uncompressed > 0 {
            format!("{:.2}%", (total_compressed as f64 / total_uncompressed as f64) * 100.0)
        } else {
            "N/A".to_string()
        }
    ));
}

fn collect_encoding_metadata(metadata: &ParquetMetaData, items: &mut Vec<(String, String)>) {
    let mut encoding_stats = HashMap::new();
    let mut total_chunks = 0;
    
    for row_group in metadata.row_groups().iter() {
        for column in row_group.columns().iter() {
            total_chunks += 1;
            for encoding in column.encodings().iter() {
                let encoding_name = format!("{:?}", encoding);
                let counter = encoding_stats.entry(encoding_name).or_insert(0);
                *counter += 1;
            }
        }
    }
    
    items.push(("total_column_chunks".to_string(), total_chunks.to_string()));
    
    for (encoding, count) in encoding_stats {
        let enc_prefix = format!("encoding_{}_", encoding.to_lowercase());
        
        items.push((format!("{}usage_count", enc_prefix), count.to_string()));
        items.push((format!("{}usage_percentage", enc_prefix), 
            format!("{:.2}%", (count as f64 / total_chunks as f64) * 100.0)));
    }
}

fn collect_statistics_metadata(metadata: &ParquetMetaData, items: &mut Vec<(String, String)>, detailed: bool) {
    let mut total_chunks_with_stats = 0;
    let mut total_chunks = 0;
    let mut total_null_count = 0i64;
    let mut total_distinct_count = 0i64;
    let mut chunks_with_null_count = 0;
    let mut chunks_with_distinct_count = 0;
    let mut chunks_with_min_max = 0;
    
    for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
        for (col_idx, column) in row_group.columns().iter().enumerate() {
            total_chunks += 1;
            
            if let Some(stats) = column.statistics() {
                total_chunks_with_stats += 1;
                
                if let Some(null_count) = stats.null_count_opt() {
                    chunks_with_null_count += 1;
                    total_null_count += null_count as i64;
                }
                
                if let Some(distinct_count) = stats.distinct_count_opt() {
                    chunks_with_distinct_count += 1;
                    total_distinct_count += distinct_count as i64;
                }
                
                if stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some() {
                    chunks_with_min_max += 1;
                }
                
                if detailed {
                    let stat_prefix = format!("stats_rg_{}_col_{}_", rg_idx, col_idx);
                    
                    items.push((format!("{}has_min_max", stat_prefix), 
                        (stats.min_bytes_opt().is_some() && stats.max_bytes_opt().is_some()).to_string()));
                    items.push((format!("{}null_count", stat_prefix), 
                        stats.null_count_opt().map_or("N/A".to_string(), |c| c.to_string())));
                    items.push((format!("{}distinct_count", stat_prefix), 
                        stats.distinct_count_opt().map_or("N/A".to_string(), |c| c.to_string())));
                }
            }
        }
    }
    
    items.push(("total_column_chunks".to_string(), total_chunks.to_string()));
    items.push(("chunks_with_statistics".to_string(), total_chunks_with_stats.to_string()));
    items.push(("chunks_with_null_count".to_string(), chunks_with_null_count.to_string()));
    items.push(("chunks_with_distinct_count".to_string(), chunks_with_distinct_count.to_string()));
    items.push(("chunks_with_min_max".to_string(), chunks_with_min_max.to_string()));
    items.push(("total_null_count".to_string(), total_null_count.to_string()));
    items.push(("total_distinct_count".to_string(), total_distinct_count.to_string()));
    items.push(("statistics_coverage".to_string(), 
        format!("{:.2}%", (total_chunks_with_stats as f64 / total_chunks as f64) * 100.0)));
}

async fn create_metadata_dataframe(items: Vec<(String, String)>) -> NailResult<DataFrame> {
    let ctx = SessionContext::new();
    
    if items.is_empty() {
        return ctx.sql("SELECT '' as key, '' as value WHERE 1=0")
            .await
            .map_err(NailError::DataFusion);
    }
    
    let keys: Vec<String> = items.iter().map(|(k, _)| k.clone()).collect();
    let values: Vec<String> = items.iter().map(|(_, v)| v.clone()).collect();
    
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("key", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Utf8, false),
    ]));
    
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(values)),
        ],
    )?;
    
    ctx.read_batch(batch).map_err(NailError::DataFusion)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, DataType as ArrowDataType};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use parquet::arrow::AsyncArrowWriter;
    use tokio::fs::File;

    async fn create_test_parquet() -> NamedTempFile {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let ids: Vec<i64> = (0..data.len() as i64).collect();
        let names = vec!["Alice", "Bob", "Charlie", "David", "Eve"];
        
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("value", ArrowDataType::Float64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(data)),
                Arc::new(StringArray::from(names)),
            ],
        ).unwrap();

        let temp_file = tempfile::Builder::new().suffix(".parquet").tempfile().unwrap();
        let file = File::create(temp_file.path()).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
        
        temp_file
    }

    async fn create_test_csv() -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), "name,value\nAlice,1\nBob,2").unwrap();
        temp_file
    }

    #[tokio::test]
    async fn test_metadata_basic_info() {
        let temp_file = create_test_parquet().await;
        let args = MetadataArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            schema: false,
            row_groups: false,
            column_chunks: false,
            compression: false,
            encoding: false,
            statistics: false,
            all: false,
            detailed: false,
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_metadata_all_info() {
        let temp_file = create_test_parquet().await;
        let args = MetadataArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            schema: false,
            row_groups: false,
            column_chunks: false,
            compression: false,
            encoding: false,
            statistics: false,
            all: true,
            detailed: true,
        };

        let result = execute(args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_metadata_non_parquet_file() {
        let temp_file = create_test_csv().await;
        let args = MetadataArgs {
            common: crate::cli::CommonArgs {
                input: temp_file.path().to_path_buf(),
                output: None,
                format: None,
                random: None,
                verbose: false,
                jobs: None,
            },
            schema: false,
            row_groups: false,
            column_chunks: false,
            compression: false,
            encoding: false,
            statistics: false,
            all: false,
            detailed: false,
        };

        let result = execute(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Metadata command only supports Parquet files"));
    }
}