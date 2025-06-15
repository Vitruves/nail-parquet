# nail - Fast Parquet Utility

A high-performance command-line utility for working with Parquet files, built with Rust and DataFusion.

![nailedit](https://github.com/user-attachments/assets/75f4f5c4-4123-4366-be70-85ed86739750)

## Features

- **Fast operations** on large datasets using Apache Arrow and DataFusion
- **Multiple file formats** supported: Parquet, CSV, JSON, Excel (read-only)
- **Comprehensive data operations**: inspection, statistics, filtering, sampling, transformations
- **Data quality tools**: search, deduplication, size analysis, missing value handling
- **Advanced features**: joins, unions, schema manipulation, stratified sampling
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
- `-j, --jobs N` - Number of parallel jobs (default: 4)
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
nail head data.parquet -n 5 -o sample.json -f json

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
nail tail data.parquet -n 10 -o tail_sample.csv -f csv
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

# Preview 100 random rows
nail preview data.parquet -n 100 --verbose

# Interactive mode with scrolling and navigation
nail preview data.parquet --interactive

# Interactive mode with more records to browse
nail preview data.parquet -n 1000 --interactive
```

**Options:**

- `-n, --number N` - Number of rows to display (default: 5)
- `-r, --random SEED` - Random seed for reproducible results
- `-I, --interactive` - Interactive mode with scrolling (use arrow keys, q to quit)

**Interactive Mode Controls:**

- `←/→` or `h/l` - Navigate between records (previous/next)
- `↑/↓` or `j/k` - Navigate between records (previous/next)
- `PgUp/PgDn` - Jump 10 records at a time
- `Home/End` - Jump to first/last record
- `q`, `Q`, or `Esc` - Quit interactive mode
- `Ctrl+C` - Force quit

#### `nail headers`

List column names, optionally filtered by regex patterns.

```bash
# List all column headers
nail headers data.parquet

# Filter headers with regex
nail headers data.parquet -f "^price.*"

# Save headers to file
nail headers data.parquet -o columns.txt

# JSON format output
nail headers data.parquet -f json
```

**Options:**

- `-f, --filter REGEX` - Filter headers with regex pattern

#### `nail schema`

Display detailed schema information including column types and nullability.

```bash
# Display schema
nail schema data.parquet

# Save schema to JSON
nail schema data.parquet -o schema.json -f json

# Verbose schema analysis
nail schema data.parquet --verbose
```

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

- `-c, --columns` - Show per-column sizes
- `-r, --rows` - Show per-row analysis
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

# Search with multiple options
nail search data.parquet --value "test" -c "name,description" --ignore-case --rows

# Save search results
nail search data.parquet --value "error" -o search_results.json -f json
```

**Options:**

- `--value VALUE` - Value to search for (required)
- `-c, --columns PATTERN` - Comma-separated column names to search in
- `-r, --rows` - Return matching row numbers only
- `--ignore-case` - Case-insensitive search
- `--exact` - Exact match only (no partial matches)

### Statistics & Analysis

#### `nail stats`

Compute statistical summaries for numeric and categorical columns.

```bash
# Basic statistics (mean, Q25, Q50, Q75, unique count)
nail stats data.parquet

# Exhaustive statistics
nail stats data.parquet -t exhaustive

# Statistics for specific columns
nail stats data.parquet -c "price,volume,quantity"

# Statistics with regex column selection
nail stats data.parquet -c "^(price|vol).*" -t exhaustive

# Save statistics to file
nail stats data.parquet -t basic -o stats.json -f json
```

**Options:**

- `-c, --columns PATTERN` - Comma-separated column names or regex patterns
- `-t, --type TYPE` - Statistics type: `basic`, `exhaustive`, `hypothesis` (default: basic)

**Statistics Types:**

- **basic**: mean, Q25, Q50, Q75, number of unique values
- **exhaustive**: count, mean, std dev, min, max, variance, duplicates
- **hypothesis**: statistical significance tests (not yet implemented)

#### `nail correlations`

Compute correlation matrices and pairwise correlations between numeric columns.

```bash
# Pairwise correlations for all numeric columns
nail correlations data.parquet

# Correlation matrix format
nail correlations data.parquet --correlation-matrix

# Specific correlation type
nail correlations data.parquet -t spearman

# Correlations for selected columns
nail correlations data.parquet -c "price,volume,quantity"

# Include statistical significance tests
nail correlations data.parquet --stats-tests --correlation-matrix
```

**Options:**

- `-c, --columns PATTERN` - Comma-separated column names or regex patterns
- `-t, --type TYPE` - Correlation type: `pearson`, `kendall`, `spearman` (default: pearson)
- `--correlation-matrix` - Output as correlation matrix format
- `--stats-tests` - Include statistical significance tests

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

# Multiple conditions
nail filter data.parquet -c "age>=18,status=active,score>80"

# Filter to numeric columns only
nail filter data.parquet -r numeric-only

# Remove rows with NaN values
nail filter data.parquet -r no-nan

# Remove rows with zeros
nail filter data.parquet -r no-zeros

# String columns only
nail filter data.parquet -r char-only
```

**Options:**

- `-c, --columns CONDITIONS` - Column filter conditions (e.g., 'age>25,salary<50000')
- `-r, --rows FILTER` - Row filter type: `no-nan`, `numeric-only`, `char-only`, `no-zeros`

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

#### `nail dedup`

Remove duplicate rows or columns from the dataset.

```bash
# Remove duplicate rows (all columns considered)
nail dedup data.parquet --row-wise

# Remove duplicate rows based on specific columns
nail dedup data.parquet --row-wise -c "id,email"

# Keep last occurrence instead of first
nail dedup data.parquet --row-wise --keep last

# Remove duplicate columns (same name)
nail dedup data.parquet --col-wise

# Row-wise deduplication with verbose output
nail dedup data.parquet --row-wise -c "user_id,timestamp" --verbose

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
- `-r, --random SEED` - Random seed for reproducible results

#### `nail shuffle`

Randomly shuffle the order of rows in the dataset.

```bash
# Random shuffle
nail shuffle data.parquet

# Reproducible shuffle
nail shuffle data.parquet --random 42

# Shuffle and save to new file
nail shuffle data.parquet -o shuffled.parquet --verbose
```

**Options:**

- `-r, --random SEED` - Random seed for reproducible results

#### `nail id`

Add ID columns to the dataset.

```bash
# Add simple numeric ID column
nail id data.parquet --create

# Add ID with custom name and prefix
nail id data.parquet --create --id-col-name record_id --prefix "REC"

# Save with new ID column
nail id data.parquet --create -o data_with_ids.parquet
```

**Options:**

- `--create` - Create new ID column
- `--prefix PREFIX` - Prefix for ID values (default: "id")
- `--id-col-name NAME` - ID column name (default: "id")

### Data Combination

#### `nail merge`

Join two datasets based on key columns.

```bash
# Inner join
nail merge -i left.parquet --right right.parquet --key id

# Left join
nail merge -i users.parquet --right orders.parquet --left-join --key user_id

# Right join
nail merge -i products.parquet --right sales.parquet --right-join --key product_id

# Join with different key names
nail merge -i table1.parquet --right table2.parquet --key-mapping "user_id=customer_id"

# Save merged result
nail merge -i left.parquet --right right.parquet --key id -o merged.parquet
```

**Options:**

- `-i, --input FILE` - Input file path (left table, required)
- `--right FILE` - Right table file to merge with (required)
- `--key COLUMN` - Join key column name (required)
- `--left-join` - Perform left join
- `--right-join` - Perform right join
- `--key-mapping MAPPING` - Key mapping for different column names (format: left_col=right_col)

#### `nail append`

Concatenate multiple datasets vertically.

```bash
# Append files
nail append base.parquet --files "file1.parquet,file2.parquet,file3.parquet"

# Append with schema mismatch handling
nail append base.parquet --files "data1.parquet,data2.parquet" --ignore-schema

# Verbose append operation
nail append base.parquet --files "*.parquet" -o combined.parquet --verbose
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
- `-r, --random SEED` - Random seed for reproducible splits

### Format Conversion

#### `nail convert`

Convert between different file formats.

```bash
# Convert Parquet to CSV
nail convert data.parquet -o data.csv

# Convert CSV to Parquet
nail convert data.csv -o data.parquet

# Convert to JSON
nail convert data.parquet -o data.json

# Verbose conversion with progress
nail convert large_dataset.csv -o large_dataset.parquet --verbose
```

**Options:**

- `-o, --output FILE` - Output file path (required)

**Supported Formats:**

- **Input**: Parquet, CSV, JSON, Excel (read-only)
- **Output**: Parquet, CSV, JSON

#### `nail count`

Count the number of rows in a dataset.

```bash
# Basic row count
nail count data.parquet

# Count with verbose output
nail count data.parquet --verbose
```

## Examples

### Basic Data Exploration

```bash
# Quick dataset overview
nail schema sales_data.parquet
nail size sales_data.parquet --columns --rows
nail head sales_data.parquet -n 10
nail stats sales_data.parquet -t basic

# Column inspection
nail headers sales_data.parquet -f "price"
nail correlations sales_data.parquet -c "price,quantity,discount"
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
```

### Data Cleaning Pipeline

```bash
# 1. Analyze data size and structure
nail size raw_data.parquet --columns --rows
nail search raw_data.parquet --value "null" --ignore-case

# 2. Remove duplicates
nail dedup raw_data.parquet --row-wise -c "id,timestamp" -o dedup_data.parquet

# 3. Check for missing values and data quality
nail filter dedup_data.parquet -r no-nan -o clean_step1.parquet

# 4. Remove unwanted columns
nail drop clean_step1.parquet -c "debug_,temp_" -o clean_step2.parquet

# 5. Fill remaining missing values
nail fill clean_step2.parquet --method value --value 0 -c "price,quantity" -o clean_final.parquet

# 6. Verify the result
nail stats clean_final.parquet -t exhaustive
nail size clean_final.parquet --columns
```

### Sampling and Analysis

```bash
# Create stratified sample for analysis
nail sample large_dataset.parquet -n 10000 --method stratified --stratify-by category -o sample.parquet

# Analyze the sample
nail correlations sample.parquet --correlation-matrix -o correlation_matrix.json -f json
nail stats sample.parquet -t exhaustive -o sample_stats.json -f json
```

### Data Integration

```bash
# Merge customer and order data
nail merge -i customers.parquet --right orders.parquet --left-join --key customer_id -o customer_orders.parquet

# Append monthly data files
nail append jan.parquet --files "feb.parquet,mar.parquet,apr.parquet" -o q1_data.parquet

# Convert final result to CSV for external tools
nail convert q1_data.parquet -o q1_data.csv
```

## Performance Tips

1. **Use Parquet for large datasets** - Parquet is columnar and much faster than CSV for analytical operations
2. **Specify column patterns** - Use `-c` with regex patterns to operate only on relevant columns
3. **Chain operations** - Use intermediate files for complex multi-step transformations
4. **Adjust parallelism** - Use `-j` to control parallel processing based on your system
5. **Enable verbose mode** - Use `--verbose` to monitor performance and progress on large datasets

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
