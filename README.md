# Nail - Fast Parquet Utility

A high-performance command-line utility for working with Parquet files, built with Rust and DataFusion.

## Features

- **Fast operations** on large datasets using Apache Arrow and DataFusion
- **Multiple file formats** supported: Parquet, CSV, JSON, Excel (read-only)
- **Comprehensive data operations**: inspection, statistics, filtering, sampling, transformations
- **Advanced features**: joins, unions, schema manipulation, missing value handling
- **Flexible output**: console display or file output in multiple formats
- **Production-ready** with robust error handling and verbose logging

## Installation

```bash
# From source
git clone https://github.com/yourusername/nail
cd nail
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
nail head -i data.parquet

# Display first 10 rows
nail head -i data.parquet -n 10

# Save to JSON file
nail head -i data.parquet -n 5 -o sample.json -f json

# Verbose output with timing
nail head -i data.parquet -n 3 --verbose
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-n, --number N` - Number of rows to display (default: 5)

#### `nail tail`
Display the last N rows of a dataset.

```bash
# Display last 5 rows
nail tail -i data.parquet

# Display last 20 rows with verbose logging
nail tail -i data.parquet -n 20 --verbose

# Save last 10 rows to CSV
nail tail -i data.parquet -n 10 -o tail_sample.csv -f csv
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-n, --number N` - Number of rows to display (default: 5)

#### `nail preview`
Randomly sample and display N rows from the dataset.

```bash
# Random preview of 5 rows
nail preview -i data.parquet

# Reproducible random sample with seed
nail preview -i data.parquet -n 10 --random 42

# Preview 100 random rows
nail preview -i data.parquet -n 100 --verbose
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-n, --number N` - Number of rows to display (default: 5)
- `-r, --random SEED` - Random seed for reproducible results

#### `nail headers`
List column names, optionally filtered by regex patterns.

```bash
# List all column headers
nail headers -i data.parquet

# Filter headers with regex
nail headers -i data.parquet -f "^price.*"

# Save headers to file
nail headers -i data.parquet -o columns.txt

# JSON format output
nail headers -i data.parquet -f json
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-f, --filter REGEX` - Filter headers with regex pattern

#### `nail schema`
Display detailed schema information including column types and nullability.

```bash
# Display schema
nail schema -i data.parquet

# Save schema to JSON
nail schema -i data.parquet -o schema.json -f json

# Verbose schema analysis
nail schema -i data.parquet --verbose
```

**Options:**
- `-i, --input FILE` - Input file path (required)

### Statistics & Analysis

#### `nail stats`
Compute statistical summaries for numeric and categorical columns.

```bash
# Basic statistics (mean, Q25, Q50, Q75, unique count)
nail stats -i data.parquet

# Exhaustive statistics
nail stats -i data.parquet -t exhaustive

# Statistics for specific columns
nail stats -i data.parquet -c "price,volume,quantity"

# Statistics with regex column selection
nail stats -i data.parquet -c "^(price|vol).*" -t exhaustive

# Save statistics to file
nail stats -i data.parquet -t basic -o stats.json -f json
```

**Options:**
- `-i, --input FILE` - Input file path (required)
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
nail correlations -i data.parquet

# Correlation matrix format
nail correlations -i data.parquet --correlation-matrix

# Specific correlation type
nail correlations -i data.parquet -t spearman

# Correlations for selected columns
nail correlations -i data.parquet -c "price,volume,quantity"

# Include statistical significance tests
nail correlations -i data.parquet --stats-tests --correlation-matrix
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-c, --columns PATTERN` - Comma-separated column names or regex patterns
- `-t, --type TYPE` - Correlation type: `pearson`, `kendall`, `spearman` (default: pearson)
- `--correlation-matrix` - Output as correlation matrix format
- `--stats-tests` - Include statistical significance tests

### Data Manipulation

#### `nail select`
Select specific columns and/or rows from the dataset.

```bash
# Select specific columns
nail select -i data.parquet -c "id,name,price"

# Select columns with regex
nail select -i data.parquet -c "^(id|price).*"

# Select specific rows
nail select -i data.parquet -r "1,5,10-20"

# Select both columns and rows
nail select -i data.parquet -c "id,price" -r "1-100"

# Save selection to new file
nail select -i data.parquet -c "id,name" -o subset.parquet
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-c, --columns PATTERN` - Column names or regex patterns (comma-separated)
- `-r, --rows SPEC` - Row numbers or ranges (e.g., "1,3,5-10")

#### `nail drop`
Remove specific columns and/or rows from the dataset.

```bash
# Drop specific columns
nail drop -i data.parquet -c "temp_col,debug_info"

# Drop columns matching pattern
nail drop -i data.parquet -c "^temp_.*"

# Drop specific rows
nail drop -i data.parquet -r "1,5,100-200"

# Drop both columns and rows
nail drop -i data.parquet -c "temp_col" -r "1-10"
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-c, --columns PATTERN` - Column names or regex patterns to drop
- `-r, --rows SPEC` - Row numbers or ranges to drop

#### `nail filter`
Filter data based on column conditions or row characteristics.

```bash
# Filter by column conditions
nail filter -i data.parquet -c "price>100,volume<1000"

# Multiple conditions
nail filter -i data.parquet -c "age>=18,status=active,score>80"

# Filter to numeric columns only
nail filter -i data.parquet -r numeric-only

# Remove rows with NaN values
nail filter -i data.parquet -r no-nan

# Remove rows with zeros
nail filter -i data.parquet -r no-zeros

# String columns only
nail filter -i data.parquet -r char-only
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-c, --columns CONDITIONS` - Column filter conditions (e.g., 'age>25,salary<50000')
- `-r, --rows FILTER` - Row filter type: `no-nan`, `numeric-only`, `char-only`, `no-zeros`

#### `nail fill`
Fill missing values using various strategies.

```bash
# Fill with specific value
nail fill -i data.parquet --method value --value 0

# Fill specific columns with value
nail fill -i data.parquet -c "price,quantity" --method value --value -1

# Fill with mean (for numeric columns)
nail fill -i data.parquet --method mean

# Fill with median
nail fill -i data.parquet --method median -c "price,volume"
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `--method METHOD` - Fill method: `value`, `mean`, `median`, `mode`, `forward`, `backward` (default: value)
- `--value VALUE` - Fill value (required for 'value' method)
- `-c, --columns PATTERN` - Comma-separated column names to fill

### Data Sampling & Transformation

#### `nail sample`
Sample data using various strategies.

```bash
# Random sampling
nail sample -i data.parquet -n 1000

# Reproducible random sampling
nail sample -i data.parquet -n 500 --method random --random 42

# Stratified sampling
nail sample -i data.parquet -n 1000 --method stratified --stratify-by category

# First N rows
nail sample -i data.parquet -n 100 --method first

# Last N rows
nail sample -i data.parquet -n 100 --method last
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-n, --number N` - Number of samples (default: 10)
- `--method METHOD` - Sampling method: `random`, `stratified`, `first`, `last` (default: random)
- `--stratify-by COLUMN` - Column name for stratified sampling
- `-r, --random SEED` - Random seed for reproducible results

#### `nail shuffle`
Randomly shuffle the order of rows in the dataset.

```bash
# Random shuffle
nail shuffle -i data.parquet

# Reproducible shuffle
nail shuffle -i data.parquet --random 42

# Shuffle and save to new file
nail shuffle -i data.parquet -o shuffled.parquet --verbose
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-r, --random SEED` - Random seed for reproducible results

#### `nail id`
Add ID columns to the dataset.

```bash
# Add simple numeric ID column
nail id -i data.parquet --create

# Add ID with custom name and prefix
nail id -i data.parquet --create --id-col-name record_id --prefix "REC"

# Save with new ID column
nail id -i data.parquet --create -o data_with_ids.parquet
```

**Options:**
- `-i, --input FILE` - Input file path (required)
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
nail append -i base.parquet --files "file1.parquet,file2.parquet,file3.parquet"

# Append with schema mismatch handling
nail append -i base.parquet --files "data1.parquet,data2.parquet" --ignore-schema

# Verbose append operation
nail append -i base.parquet --files "*.parquet" -o combined.parquet --verbose
```

**Options:**
- `-i, --input FILE` - Input file path (base table, required)
- `--files FILES` - Comma-separated list of files to append (required)
- `--ignore-schema` - Ignore schema mismatches and force append

### Format Conversion

#### `nail convert`
Convert between different file formats.

```bash
# Convert Parquet to CSV
nail convert -i data.parquet -o data.csv

# Convert CSV to Parquet
nail convert -i data.csv -o data.parquet

# Convert to JSON
nail convert -i data.parquet -o data.json

# Verbose conversion with progress
nail convert -i large_dataset.csv -o large_dataset.parquet --verbose
```

**Options:**
- `-i, --input FILE` - Input file path (required)
- `-o, --output FILE` - Output file path (required)

**Supported Formats:**
- **Input**: Parquet, CSV, JSON, Excel (read-only)
- **Output**: Parquet, CSV, JSON

## Testing

Run the full test suite (unit and integration tests):
```bash
cargo test
```
This will prepare sample fixtures under `tests/fixtures` and execute all integration tests defined in `tests/integration_tests.rs`.

## Examples

### Basic Data Exploration

```bash
# Quick dataset overview
nail schema -i sales_data.parquet
nail head -i sales_data.parquet -n 10
nail stats -i sales_data.parquet -t basic

# Column inspection
nail headers -i sales_data.parquet -f "price"
nail correlations -i sales_data.parquet -c "price,quantity,discount"
```

### Data Cleaning Pipeline

```bash
# 1. Check for missing values and data quality
nail filter -i raw_data.parquet -r no-nan -o clean_step1.parquet

# 2. Remove unwanted columns
nail drop -i clean_step1.parquet -c "debug_,temp_" -o clean_step2.parquet

# 3. Fill remaining missing values
nail fill -i clean_step2.parquet --method value --value 0 -c "price,quantity" -o clean_final.parquet

# 4. Verify the result
nail stats -i clean_final.parquet -t exhaustive
```

### Sampling and Analysis

```bash
# Create stratified sample for analysis
nail sample -i large_dataset.parquet -n 10000 --method stratified --stratify-by category -o sample.parquet

# Analyze the sample
nail correlations -i sample.parquet --correlation-matrix -o correlation_matrix.json -f json
nail stats -i sample.parquet -t exhaustive -o sample_stats.json -f json
```

### Data Integration

```bash
# Merge customer and order data
nail merge -i customers.parquet --right orders.parquet --left-join --key customer_id -o customer_orders.parquet

# Append monthly data files
nail append -i jan.parquet --files "feb.parquet,mar.parquet,apr.parquet" -o q1_data.parquet

# Convert final result to CSV for external tools
nail convert -i q1_data.parquet -o q1_data.csv
```

## Performance Tips

1. **Use Parquet for large datasets** - Parquet is columnar and much faster than CSV for analytical operations
2. **Specify column patterns** - Use `-c` with regex patterns to operate only on relevant columns
3. **Chain operations** - Use intermediate files for complex multi-step transformations
4. **Adjust parallelism** - Use `-j` to control parallel processing based on your system
5. **Enable verbose mode** - Use `--verbose` to monitor performance and progress on large datasets

## Error Handling

Nail provides detailed error messages for common issues:

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
- GitHub Issues: https://github.com/yourusername/nail/issues
- Documentation: https://github.com/yourusername/nail/wiki