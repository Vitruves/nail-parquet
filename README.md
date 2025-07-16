# nail - Fast Parquet Utility

A high-performance command-line utility for working with Parquet files, built with Rust and DataFusion.

![nail_parquet](https://github.com/user-attachments/assets/0251facf-0e9b-49d0-bbd4-5dd8a288997c)


## Features

- **Fast operations** on large datasets using Apache Arrow and DataFusion
- **Multiple file formats** supported: Parquet, CSV, JSON, and Excel
- **Comprehensive data operations**: inspection, statistics, filtering, sampling, transformations
- **Data quality tools**: search, deduplication, size analysis, missing value handling
- **Advanced features**: joins, unions, schema manipulation, stratified sampling
- **File optimization**: compression, sorting, and encoding for better performance
- **Data analysis tools**: binning, pivot tables, correlation analysis
- **Flexible output**: console display or file output in multiple formats
- **Production-ready** with robust error handling and verbose logging

## Installation

```
cargo install nail-parquet
```

or

```bash
# From source
git clone https://github.com/Vitruves/nail-parquet
cd nail-parquet
cargo build --release
sudo cp target/release/nail /usr/local/bin/

# Verify installation
nail --help
```

## Global Options

All commands support these global flags:

- `-v, --verbose` - Enable verbose output with timing and progress information
- `-j, --jobs N` - Number of parallel jobs (default: half of available CPU cores)
- `-o, --output FILE` - Output file path (prints to console if not specified)
- `-f, --format FORMAT` - Output format: `json`, `csv`, `parquet`, `text` (auto-detect by default)
- `-h, --help` - Display command help

## Commands

### Data Inspection

#### `nail head`

Display the first N rows of a dataset.

```bash
# Basic usage
nail head data.parquet

# Display first 10 rows
nail head data.parquet -n 10

# Save to JSON file
nail head data.parquet -n 5 -o sample.json

# Verbose output with timing
nail head data.parquet -n 3 --verbose
```

**Options:**

- `-n, --number N` - Number of rows to display (default: 5)

#### `nail tail`

Display the last N rows of a dataset.

```bash
# Display last 5 rows
nail tail data.parquet

# Display last 20 rows with verbose logging
nail tail data.parquet -n 20 --verbose

# Save last 10 rows to CSV
nail tail data.parquet -n 10 -o tail_sample.csv
```

**Options:**

- `-n, --number N` - Number of rows to display (default: 5)

#### `nail preview`

Randomly sample and display N rows from the dataset. Supports both static display and interactive browsing mode.

```bash
# Random preview of 5 rows
nail preview data.parquet

# Reproducible random sample with seed
nail preview data.parquet -n 10 --random 42

# Interactive mode for browsing records one by one
nail preview data.parquet --interactive
```

**Options:**

- `-n, --number N` - Number of rows to display (default: 5)
- `--random SEED` - Random seed for reproducible results
- `-I, --interactive` - Interactive mode with scrolling (use arrow keys, q to quit)

**Interactive Mode Controls:**

- `←/→` or `h/l` - Navigate between records (previous/next)
- `↑/↓` or `k/j` - Navigate between fields within a record
- `q`, `Esc` - Quit interactive mode
- `Ctrl+C` - Force quit

#### `nail headers`

List column names, optionally filtered by regex patterns.

```bash
# List all column headers
nail headers data.parquet

# Filter headers with regex
nail headers data.parquet --filter "^price.*"

# Save headers to file
nail headers data.parquet -o columns.txt

# JSON format output
nail headers data.parquet -f json
```

**Options:**

- `--filter REGEX` - Filter headers with regex pattern

#### `nail schema`

Display detailed schema information including column types and nullability.

```bash
# Display schema
nail schema data.parquet

# Save schema to JSON
nail schema data.parquet -o schema.json

# Verbose schema analysis
nail schema data.parquet --verbose
```

#### `nail metadata`

Display detailed Parquet file metadata, including schema, row groups, column chunks, compression, encoding, and statistics.

```bash
# Display basic metadata
nail metadata data.parquet

# Show all available metadata
nail metadata data.parquet --all

# Show detailed schema and row group information
nail metadata data.parquet --schema --row-groups --detailed

# Save all metadata to JSON
nail metadata data.parquet --all -o metadata.json
```

**Options:**

- `--schema` - Show detailed schema information
- `--row-groups` - Show row group information
- `--column-chunks` - Show column chunk information
- `--compression` - Show compression information
- `--encoding` - Show encoding information
- `--statistics` - Show statistics information
- `--all` - Show all available metadata
- `--detailed` - Show metadata in detailed format

#### `nail size`

Analyze file and memory usage with detailed size breakdowns.

```bash
# Basic size analysis
nail size data.parquet

# Show per-column size breakdown
nail size data.parquet --columns

# Show per-row analysis
nail size data.parquet --rows

# Show all size metrics
nail size data.parquet --columns --rows

# Raw bits output (no human-friendly formatting)
nail size data.parquet --bits

# Save size analysis to file
nail size data.parquet --columns --rows -o size_report.txt
```

**Options:**

- `--columns` - Show per-column sizes
- `--rows` - Show per-row analysis
- `--bits` - Show raw bits without human-friendly conversion

#### `nail search`

Search for specific values across columns with flexible matching options.

```bash
# Basic search across all columns
nail search data.parquet --value "John"

# Search in specific columns
nail search data.parquet --value "error" -c "status,message,log"

# Case-insensitive search
nail search data.parquet --value "ACTIVE" --ignore-case

# Exact match only (no partial matches)
nail search data.parquet --value "complete" --exact

# Return row numbers only
nail search data.parquet --value "Bob" --rows

# Save search results
nail search data.parquet --value "error" -o search_results.json
```

**Options:**

- `--value VALUE` - Value to search for (required)
- `-c, --columns PATTERN` - Comma-separated column names to search in
- `--rows` - Return matching row numbers only
- `--ignore-case` - Case-insensitive search
- `--exact` - Exact match only (no partial matches)

### Data Quality Tools

#### `nail outliers`

Detect and optionally remove outliers from numeric columns using various methods.

```bash
# Detect outliers using IQR method for specific column
nail outliers data.parquet -c "price" --method iqr

# Detect outliers using Z-score with a custom threshold, showing values
nail outliers data.parquet -c "revenue" --method z-score --z-score-threshold 2.5 --show-values

# Remove outliers using Modified Z-score method from multiple columns
nail outliers data.parquet -c "age,income" --method modified-z-score --remove -o cleaned_data.parquet

# Detect outliers using Isolation Forest method (simplified)
nail outliers data.parquet -c "score" --method isolation-forest
```

**Options:**

- `-c, --columns PATTERN` - Comma-separated column names or regex patterns for outlier detection
- `--method METHOD` - Outlier detection method: `iqr`, `z-score`, `modified-z-score`, `isolation-forest` (default: iqr)
- `--iqr-multiplier VALUE` - IQR multiplier for outlier detection (default: 1.5)
- `--z-score-threshold VALUE` - Z-score threshold for outlier detection (default: 3.0)
- `--show-values` - Show outlier values instead of just flagging them
- `--include-row-numbers` - Include row numbers in output
- `--remove` - Remove outliers from dataset and save cleaned data

### Statistics & Analysis

#### `nail stats`

Compute statistical summaries for numeric and categorical columns.

```bash
# Basic statistics (mean, Q25, Q50, Q75, unique count)
nail stats data.parquet

# Exhaustive statistics
nail stats data.parquet --stats-type exhaustive

# Statistics for specific columns
nail stats data.parquet -c "price,volume,quantity"

# Statistics with regex column selection
nail stats data.parquet -c "^(price|vol).*" --stats-type exhaustive

# Save statistics to file
nail stats data.parquet --stats-type basic -o stats.json
```

**Options:**

- `-c, --columns PATTERN` - Comma-separated column names or regex patterns
- `--stats-type TYPE` - Statistics type: `basic`, `exhaustive`, `hypothesis` (default: basic)

**Statistics Types:**

- **basic**: count, mean, quartiles (q25, q50, q75), and number of unique values.
- **exhaustive**: count, mean, std dev, min, max, variance, duplicates, and unique values.
- **hypothesis**: statistical significance tests (not yet implemented).

#### `nail correlations`

Compute correlation matrices between numeric columns with optional statistical significance testing.

```bash
# Basic Pearson correlation
nail correlations data.parquet

# Specific correlation types
nail correlations data.parquet --type kendall
nail correlations data.parquet --type spearman

# Correlations for specific columns
nail correlations data.parquet -c "price,volume,quantity"

# Output as correlation matrix format
nail correlations data.parquet --matrix

# Include statistical significance tests (p-values for fisher, t-test, chi-sqr)
nail correlations data.parquet --tests fisher_exact,t_test

# Comprehensive correlation analysis with significance tests
nail correlations data.parquet --tests fisher_exact -o correlations.json
```

**Options:**

- `-c, --columns PATTERN` - Comma-separated column names or regex patterns
- `-t, --type TYPE` - Correlation type: `pearson`, `kendall`, `spearman` (default: pearson)
- `--matrix` - Output as correlation matrix format
- `--tests` - Include statistical significance tests (`fisher_exact`, `chi_sqr`, `t_test`)
- `--digits N` - Number of decimal places for correlation values (default: 4)

#### `nail frequency`

Compute frequency tables for categorical columns showing value counts, distributions, and percentages.

```bash
# Basic frequency table for a single column
nail frequency data.parquet -c "category"

# Multiple columns frequency analysis
nail frequency data.parquet -c "category,status,region"

# Save frequency table to file
nail frequency data.parquet -c "product_type" -o frequency_table.csv

# Verbose output with progress information
nail frequency data.parquet -c "category,status" --verbose
```

**Options:**

- `-c, --columns PATTERN` - Comma-separated column names to analyze (required).

**Output:** Shows frequency counts with percentages for each value, helping identify data distribution patterns.

### Data Manipulation

#### `nail select`

Select specific columns and/or rows from the dataset.

```bash
# Select specific columns
nail select data.parquet -c "id,name,price"

# Select columns with regex
nail select data.parquet -c "^(id|price).*"

# Select specific rows
nail select data.parquet -r "1,5,10-20"

# Select both columns and rows
nail select data.parquet -c "id,price" -r "1-100"

# Save selection to new file
nail select data.parquet -c "id,name" -o subset.parquet
```

**Options:**

- `-c, --columns PATTERN` - Column names or regex patterns (comma-separated)
- `-r, --rows SPEC` - Row numbers or ranges (e.g., "1,3,5-10")

#### `nail drop`

Remove specific columns and/or rows from the dataset.

```bash
# Drop specific columns
nail drop data.parquet -c "temp_col,debug_info"

# Drop columns matching pattern
nail drop data.parquet -c "^temp_.*"

# Drop specific rows
nail drop data.parquet -r "1,5,100-200"

# Drop both columns and rows
nail drop data.parquet -c "temp_col" -r "1-10"
```

**Options:**

- `-c, --columns PATTERN` - Column names or regex patterns to drop
- `-r, --rows SPEC` - Row numbers or ranges to drop

#### `nail filter`

Filter data based on column conditions or row characteristics.

```bash
# Filter by column conditions
nail filter data.parquet -c "price>100,volume<1000"

# Multiple conditions with different operators
nail filter data.parquet -c "age>=18,status=active,score!=0"

# String matching and numeric comparisons
nail filter data.parquet -c "name!=test,salary<=50000,active=true"

# Filter to numeric columns only
nail filter data.parquet --rows numeric-only

# Remove rows with NaN values
nail filter data.parquet --rows no-nan

# Remove rows with zeros
nail filter data.parquet --rows no-zeros

# String columns only
nail filter data.parquet --rows char-only
```

**Options:**

- `-c, --columns CONDITIONS` - Column filter conditions (comma-separated). Supported operators:
  - `=` (equals), `!=` (not equals)
  - `>` (greater than), `>=` (greater or equal)
  - `<` (less than), `<=` (less or equal)
  - Examples: `age>25`, `status=active`, `price<=100`
- `--rows FILTER` - Row filter type: `no-nan`, `numeric-only`, `char-only`, `no-zeros`

#### `nail fill`

Fill missing values using various strategies.

```bash
# Fill with specific value
nail fill data.parquet --method value --value 0

# Fill specific columns with value
nail fill data.parquet -c "price,quantity" --method value --value -1

# Fill with mean (for numeric columns)
nail fill data.parquet --method mean

# Fill with median
nail fill data.parquet --method median -c "price,volume"
```

**Options:**

- `--method METHOD` - Fill method: `value`, `mean`, `median`, `mode`, `forward`, `backward` (default: value)
- `--value VALUE` - Fill value (required for 'value' method)
- `-c, --columns PATTERN` - Comma-separated column names to fill

#### `nail rename`

Rename one or more columns.

```bash
# Rename a single column
nail rename data.parquet --column "old_name=new_name" -o renamed.parquet

# Rename multiple columns
nail rename data.parquet --column "id=user_id,val=value" -o renamed.parquet
```

**Options:**
- `-c, --column SPECS` - Column rename specs (`before=after`), comma-separated.

#### `nail create`

Create new columns with expressions based on existing columns.

```bash
# Create a single new column
nail create data.parquet --column "total=price*quantity" -o enhanced.parquet

# Create multiple columns
nail create data.parquet --column "total=price*quantity,margin=(price-cost)" -o enhanced.parquet

# Filter rows while creating columns
nail create data.parquet --column "category_score=score*2" --row-filter "score>50" -o filtered_enhanced.parquet
```

**Options:**

- `-c, --column SPECS` - Column creation specifications (`name=expression`), comma-separated.
- `-r, --row-filter FILTER` - Row filter expression to apply before creating columns.

#### `nail dedup`

Remove duplicate rows or columns from the dataset.

```bash
# Remove duplicate rows (all columns considered)
nail dedup data.parquet --row-wise

# Remove duplicate rows based on specific columns
nail dedup data.parquet --row-wise -c "id,email"

# Keep last occurrence instead of first
nail dedup data.parquet --row-wise --keep last

# Remove duplicate columns (by name)
nail dedup data.parquet --col-wise

# Save deduplicated data
nail dedup data.parquet --row-wise -o clean_data.parquet
```

**Options:**

- `--row-wise` - Remove duplicate rows (conflicts with --col-wise)
- `--col-wise` - Remove duplicate columns (conflicts with --row-wise)
- `-c, --columns PATTERN` - Columns to consider for row-wise deduplication
- `--keep STRATEGY` - Keep 'first' or 'last' occurrence (default: first)

### Data Sampling & Transformation

#### `nail sample`

Sample data using various strategies.

```bash
# Random sampling
nail sample data.parquet -n 1000

# Reproducible random sampling
nail sample data.parquet -n 500 --method random --random 42

# Stratified sampling
nail sample data.parquet -n 1000 --method stratified --stratify-by category

# First N rows
nail sample data.parquet -n 100 --method first

# Last N rows
nail sample data.parquet -n 100 --method last
```

**Options:**

- `-n, --number N` - Number of samples (default: 10)
- `--method METHOD` - Sampling method: `random`, `stratified`, `first`, `last` (default: random)
- `--stratify-by COLUMN` - Column name for stratified sampling
- `--random SEED` - Random seed for reproducible results

#### `nail shuffle`

Randomly shuffle the order of rows in the dataset.

```bash
# Random shuffle
nail shuffle data.parquet

# Reproducible shuffle (Note: DataFusion's RANDOM() may not be deterministic)
nail shuffle data.parquet --random 42

# Shuffle and save to new file
nail shuffle data.parquet -o shuffled.parquet --verbose
```

**Options:**

- `--random SEED` - Random seed for reproducible results

#### `nail id`

Add a unique ID column to the dataset.

```bash
# Add simple numeric ID column
nail id data.parquet --create

# Add ID with custom name and prefix
nail id data.parquet --create --id-col-name record_id --prefix "REC-"

# Save with new ID column
nail id data.parquet --create -o data_with_ids.parquet
```

**Options:**

- `--create` - Create new ID column
- `--prefix PREFIX` - Prefix for ID values (default: "id")
- `--id-col-name NAME` - ID column name (default: "id")

### Data Combination

#### `nail merge`

Join two datasets horizontally based on a common key column.

```bash
# Inner join (default)
nail merge left.parquet --right right.parquet --key id -o merged.parquet

# Left join - keep all records from left table
nail merge customers.parquet --right orders.parquet --left-join --key customer_id -o customer_orders.parquet

# Right join - keep all records from right table
nail merge orders.parquet --right customers.parquet --right-join --key customer_id -o order_customers.parquet

# Merge on columns with different names
nail merge table1.parquet --right table2.parquet --key-mapping "table1_id=table2_user_id"
```

**Options:**

- `--right FILE` - Right table file to merge with (required)
- `--key COLUMN` - Join key column name (if same in both tables)
- `--key-mapping MAPPING` - Join key mapping for different column names (`left_col=right_col`)
- `--left-join` - Perform left join
- `--right-join` - Perform right join

#### `nail append`

Append multiple datasets vertically (union operation).

```bash
# Append files with matching schemas
nail append base.parquet --files "file1.parquet,file2.parquet" -o combined.parquet

# Append with verbose logging
nail append base.parquet --files "jan.parquet,feb.parquet,mar.parquet" --verbose -o q1_data.parquet

# Force append with schema differences (fills missing columns with nulls)
nail append base.parquet --files "different_schema.parquet" --ignore-schema -o combined.parquet
```

**Options:**

- `--files FILES` - Comma-separated list of files to append (required)
- `--ignore-schema` - Ignore schema mismatches and force append

#### `nail split`

Split dataset into multiple files based on ratios or stratification.

```bash
# Split by ratio
nail split data.parquet --ratio "0.7,0.3" --names "train,test" --output-dir splits/

# Stratified split
nail split data.parquet --ratio "0.8,0.2" --stratified-by category --output-dir splits/

# Reproducible split
nail split data.parquet --ratio "0.6,0.2,0.2" --random 42 --output-dir splits/
```

**Options:**

- `--ratio RATIOS` - Comma-separated split ratios (must sum to 1.0 or 100.0)
- `--names NAMES` - Comma-separated output file names
- `--output-dir DIR` - Output directory for split files
- `--stratified-by COLUMN` - Column for stratified splitting
- `--random SEED` - Random seed for reproducible splits

### File Optimization

#### `nail optimize`

Optimize Parquet files by applying compression, sorting, and encoding techniques.

```bash
# Basic optimization with default compression (snappy)
nail optimize input.parquet -o optimized.parquet

# Optimize with specific compression type and level
nail optimize input.parquet -o optimized.parquet --compression zstd --compression-level 5

# Sort data while optimizing
nail optimize input.parquet -o optimized.parquet --sort-by "timestamp,id"

# Enable dictionary encoding for better compression
nail optimize input.parquet -o optimized.parquet --dictionary

# Comprehensive optimization and validation
nail optimize input.parquet -o optimized.parquet --compression zstd --sort-by "date,category" --dictionary --validate --verbose
```

**Options:**

- `--compression TYPE` - Compression type: `snappy`, `gzip`, `zstd`, `brotli` (default: snappy)
- `--compression-level LEVEL` - Compression level (1-9, default: 6)
- `--sort-by COLUMNS` - Comma-separated columns to sort by
- `--dictionary` - Enable dictionary encoding
- `--no-dictionary` - Disable dictionary encoding
- `--validate` - Validate optimized file after creation

### Data Analysis & Transformation

#### `nail binning`

Bin continuous variables into categorical ranges for analysis. *Note: Current implementation supports one column at a time.*

```bash
# Equal-width binning with 5 bins
nail binning data.parquet -c "age" -b 5 -o binned.parquet

# Custom bins with specific edges
nail binning data.parquet -c "score" -b "0,50,80,90,100" --method custom -o binned.parquet

# Add bin labels
nail binning data.parquet -c "temperature" -b 3 --labels "Cold,Warm,Hot" -o binned.parquet
```

**Options:**

- `-c, --columns COLUMN` - Column to bin (required)
- `-b, --bins BINS` - Number of bins or custom edges (e.g., "5" or "0,10,50,100") (default: 10)
- `--method METHOD` - Binning method: `equal-width`, `custom` (default: equal-width)
- `--labels LABELS` - Comma-separated bin labels
- `--suffix SUFFIX` - Suffix for new binned column (default: "_binned")
- `--drop-original` - Drop original column after binning

#### `nail pivot`

Create pivot tables for data aggregation and cross-tabulation. *Note: Current implementation is a simplified group-by aggregation and does not create a wide-format pivot table.*

```bash
# Basic pivot table with sum aggregation
nail pivot data.parquet -i "region" -c "product" -l "sales" -o pivot.parquet

# Pivot with different aggregation functions
nail pivot data.parquet -i "category" -c "product" -l "revenue" --agg mean -o avg_pivot.parquet
```

**Options:**

- `-i, --index COLUMNS` - Row index columns (comma-separated) (required)
- `-c, --columns COLUMNS` - Column pivot columns (comma-separated) (required)
- `-l, --values COLUMNS` - Value columns to aggregate (comma-separated)
- `--agg FUNC` - Aggregation function: `sum`, `mean`, `count`, `min`, `max` (default: sum)
- `--fill VALUE` - Fill missing values (default: "0")

### Format Conversion & Utility

#### `nail convert`

Convert between different file formats.

```bash
# Convert Parquet to CSV
nail convert data.parquet -o data.csv

# Convert CSV to Parquet
nail convert data.csv -o data.parquet

# Convert Parquet to Excel
nail convert data.parquet -o data.xlsx

# Verbose conversion with progress
nail convert large_dataset.csv -o large_dataset.parquet --verbose
```

**Supported Formats:**

- **Input**: Parquet, CSV, JSON, Excel (xlsx)
- **Output**: Parquet, CSV, JSON, Excel (xlsx)

#### `nail count`

Count the number of rows in a dataset.

```bash
# Basic row count
nail count data.parquet

# Count with verbose output
nail count data.parquet --verbose
```

#### `nail update`

Check for newer versions of the `nail` tool.

```bash
# Check for updates
nail update

# Check with verbose output
nail update --verbose
```

## Examples

### Basic Data Exploration

```bash
# Quick dataset overview
nail schema sales_data.parquet
nail size sales_data.parquet --columns --rows
nail head sales_data.parquet -n 10
nail stats sales_data.parquet --stats-type basic

# Column inspection
nail headers sales_data.parquet --filter "price"
nail correlations sales_data.parquet -c "price,quantity,discount" --stats-tests t_test

# Frequency analysis for categorical data
nail frequency sales_data.parquet -c "category,region,status"
```

### Data Quality Investigation

```bash
# Search for problematic values
nail search data.parquet --value "error" --ignore-case
nail search data.parquet --value "null" -c "critical_fields" --rows

# Find and remove duplicates
nail dedup data.parquet --row-wise -c "id" --verbose -o unique_data.parquet

# Analyze data size and memory usage
nail size data.parquet --columns --bits

# Check for specific patterns in text fields
nail search data.parquet --value "@gmail.com" -c "email" --exact

# Frequency analysis to identify data quality issues
nail frequency data.parquet -c "status" --verbose
```

### Data Enhancement and Transformation

```bash
# Create new calculated columns
nail create sales_data.parquet --column "total=price*quantity,profit=(price-cost)" -o enhanced_sales.parquet

# Rename columns for clarity
nail rename enhanced_sales.parquet --column "total=total_sale,profit=net_profit" -o final_sales.parquet

# Create complex derived metrics
nail create customer_data.parquet --column "lifetime_value=orders*avg_order" -o customer_metrics.parquet

# Filter and enhance in one step
nail create large_dataset.parquet --column "score=performance*weight" --row-filter "active=true" -o active_scored.parquet
```

### Data Optimization and Processing Pipeline

```bash
# 1. Optimize raw data files for better performance
nail optimize raw_data.parquet -o optimized_data.parquet --compression zstd --sort-by "timestamp,customer_id" --dictionary --verbose

# 2. Create analytical features with binning
nail binning optimized_data.parquet -c "age" -b "18,25,35,50,65" --method custom --labels "18-24,25-34,35-49,50-64,65+" -o aged_data.parquet

# 3. Group data for analysis (using pivot's group-by functionality)
nail pivot aged_data.parquet -i "age_binned" -c "category" -l "revenue" --agg sum -o age_revenue_summary.parquet

# 4. Add derived metrics
nail create age_revenue_summary.parquet --column "avg_revenue=sum_revenue/count_revenue" -o enhanced_summary.parquet

# 5. Statistical analysis of optimized data
nail stats enhanced_summary.parquet --stats-type exhaustive -o summary_stats.json
```

## Performance Tips

1. **Use Parquet for large datasets** - Parquet is columnar and much faster than CSV for analytical operations.
2. **Specify column patterns** - Use `-c` with regex patterns to operate only on relevant columns.
3. **Chain operations** - Use intermediate files for complex multi-step transformations.
4. **Adjust parallelism** - Use `-j` to control parallel processing based on your system.
5. **Enable verbose mode** - Use `--verbose` to monitor performance and progress on large datasets.

## Error Handling

nail provides detailed error messages for common issues:

- **File not found**: Clear indication of missing input files
- **Schema mismatches**: Detailed information about incompatible schemas in merge/append operations
- **Invalid expressions**: Specific feedback on malformed filter conditions or column patterns
- **Memory issues**: Graceful handling of large datasets with appropriate error messages

## System Requirements

- **Operating System**: Linux (Ubuntu 24.04+ recommended), macOS, Windows
- **Memory**: 4GB+ RAM (8GB+ recommended for large datasets)
- **Storage**: SSD recommended for large file operations
- **Dependencies**: None (statically linked binary)

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Support

For issues and questions:

- GitHub Issues: https://github.com/Vitruves/nail-parquet/issues
