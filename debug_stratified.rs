use datafusion::prelude::*;
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Debug Stratified Sampling Issue ===");
    
    // Create test DataFrame similar to runtime_tests/test_data.csv
    let ctx = SessionContext::new();
    
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    
    let id_array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "David", "Emma"]));
    let age_array = Arc::new(Int64Array::from(vec![25, 34, 28, 45, 31]));
    let category_array = Arc::new(StringArray::from(vec!["Electronics", "Books", "Electronics", "Books", "Clothing"]));
    let price_array = Arc::new(Float64Array::from(vec![129.99, 45.50, 299.99, 15.99, 75.00]));
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_array, name_array, age_array, category_array, price_array],
    )?;
    
    let df = ctx.read_batch(batch)?;
    
    println!("Created test DataFrame:");
    df.clone().show().await?;
    
    // Now test the problematic stratified sampling logic
    let table_name = "temp_table";
    ctx.register_table(table_name, df.clone().into_view())?;
    
    let stratify_col = "category";
    let actual_col_name = stratify_col; // For simplicity
    
    println!("\n=== Step 1: Get category counts ===");
    let count_sql = format!(
        "SELECT \"{}\" as category, COUNT(*) as count 
         FROM {} 
         WHERE \"{}\" IS NOT NULL 
         GROUP BY \"{}\"",
        actual_col_name, table_name, actual_col_name, actual_col_name
    );
    
    println!("Count SQL: {}", count_sql);
    let count_df = ctx.sql(&count_sql).await?;
    count_df.clone().show().await?;
    
    let count_batches = count_df.collect().await?;
    let mut category_counts = HashMap::new();
    
    for batch in &count_batches {
        let cat_array = batch.column(0);
        let count_array = batch.column(1).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
        
        for i in 0..batch.num_rows() {
            let category = match cat_array.data_type() {
                datafusion::arrow::datatypes::DataType::Utf8 => {
                    cat_array.as_any().downcast_ref::<StringArray>().unwrap().value(i).to_string()
                },
                _ => continue,
            };
            let count = count_array.value(i) as usize;
            category_counts.insert(category, count);
        }
    }
    
    println!("\nCategory counts: {:?}", category_counts);
    
    // Calculate samples per category (target: 3 samples total)
    let n = 3;
    let total_count: usize = category_counts.values().sum();
    let mut samples_per_category = HashMap::new();
    
    for (cat, count) in &category_counts {
        let proportion = *count as f64 / total_count as f64;
        let samples = ((n as f64 * proportion).round() as usize).min(*count);
        samples_per_category.insert(cat.clone(), samples);
    }
    
    println!("Samples per category: {:?}", samples_per_category);
    
    // Test the problematic SQL query for each category
    for (cat, samples) in &samples_per_category {
        if *samples == 0 {
            continue;
        }
        
        println!("\n=== Step 2: Sample from category '{}' ===", cat);
        
        // This is the problematic SQL from the original code
        let category_sql = format!(
            "SELECT * FROM {} 
             WHERE \"{}\" = '{}' 
             ORDER BY RANDOM() 
             LIMIT {}",
            table_name, actual_col_name, cat, samples
        );
        
        println!("Category SQL: {}", category_sql);
        
        match ctx.sql(&category_sql).await {
            Ok(category_df) => {
                println!("Success! Result:");
                category_df.show().await?;
            },
            Err(e) => {
                println!("ERROR: {}", e);
                
                // Try debugging the table structure
                println!("Debugging table structure...");
                let debug_sql = format!("DESCRIBE {}", table_name);
                match ctx.sql(&debug_sql).await {
                    Ok(desc_df) => {
                        println!("Table description:");
                        desc_df.show().await?;
                    },
                    Err(desc_e) => {
                        println!("Failed to describe table: {}", desc_e);
                    }
                }
                
                // Try a simpler query
                let simple_sql = format!("SELECT * FROM {} LIMIT 1", table_name);
                match ctx.sql(&simple_sql).await {
                    Ok(simple_df) => {
                        println!("Simple SELECT * works:");
                        simple_df.show().await?;
                    },
                    Err(simple_e) => {
                        println!("Even simple SELECT * fails: {}", simple_e);
                    }
                }
            }
        }
    }
    
    Ok(())
}