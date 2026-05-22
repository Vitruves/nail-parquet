# nail - Lightning-Fast Data Analysis CLI

**nail** is a high-performance command-line tool for analyzing, transforming, and exploring Parquet, CSV, JSON, and Excel files. Built with Rust, Apache Arrow, and DataFusion.

Process gigabyte-scale datasets in seconds • SQL-powered • Zero configuration • Works offline • Single binary.

[![Crates.io](https://img.shields.io/crates/v/nail-parquet.svg)](https://crates.io/crates/nail-parquet)
[![Downloads](https://img.shields.io/crates/d/nail-parquet.svg)](https://crates.io/crates/nail-parquet)
[![License](https://img.shields.io/crates/l/nail-parquet.svg)](https://github.com/Vitruves/nail-parquet/blob/main/LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)

![nail_parquet](https://github.com/user-attachments/assets/0251facf-0e9b-49d0-bbd4-5dd8a288997c)

## Installation

```bash
cargo install nail-parquet
```

From source:

```bash
git clone https://github.com/Vitruves/nail-parquet
cd nail-parquet
cargo build --release
sudo cp target/release/nail /usr/local/bin/
nail --help
```

With nix:

```bash
nix shell nixpkgs#nail-parquet
```

**Dependencies:** macOS — none. Linux — `pkg-config` and `openssl`.

## Commands

```
append        Concatenate multiple datasets
binning       Bin continuous variables into categories
convert       Convert between file formats
correlations  Calculate correlation matrices
count         Count total rows
create        Create new columns with expressions
dedup         Remove duplicate rows or columns
describe      Show global file overview and metadata
diff          Compare two datasets and show differences
drop          Remove columns or rows
fill          Fill missing values
filter        Filter rows by conditions
frequency     Calculate frequency distributions
head          Display first N rows
headers       Display column headers
id            Add unique identifier column
merge         Join two datasets
metadata      Show Parquet file metadata
optimize      Optimize Parquet files for better performance
outliers      Detect outliers in data
pivot         Create pivot tables with aggregations
preview       Preview random N rows
rename        Rename columns
sample        Extract data samples
schema        Display schema information
search        Search for values in data
select        Select specific columns or rows
shuffle       Randomly shuffle rows
size          Show data size information
sort          Sort data by columns with various strategies
split         Split data into multiple files
stats         Calculate descriptive statistics
tail          Display last N rows
update        Check for newer versions
help          Print this message or the help of the given subcommand(s)
```

Run `nail <command> --help` for full usage.

## Global Options

Available on all commands:

| Flag | Description |
|------|-------------|
| `-v, --verbose` | Timing and progress output |
| `-j, --jobs N` | Parallel jobs (default: half of CPU cores) |
| `-o, --output FILE` | Output file (prints to console if omitted) |
| `-f, --format FORMAT` | Output format: `json`, `csv`, `parquet`, `text`, `xlsx` |
| `--batch-size N` | DataFusion batch size (rows per record batch) |
| `--table` | Display console output as a columnar table instead of cards |
| `--random N` | Random seed for reproducible results |
| `-h, --help` | Command help |

## Examples

Explore a dataset:

```bash
nail describe sales.parquet
nail stats sales.parquet -c "revenue,profit" --percentiles "0.5,0.9,0.99"
nail correlations sales.parquet -c "price,volume,discount" --tests t_test
nail frequency sales.parquet -c "category,region"
```

Clean and enrich:

```bash
nail dedup raw.parquet --row-wise -c "id" -o unique.parquet
nail outliers unique.parquet -c "price" --method iqr --remove -o cleaned.parquet
nail create cleaned.parquet --column "margin=(price-cost)/price" -o enriched.parquet
```

Build an analysis pipeline:

```bash
nail optimize raw.parquet -o opt.parquet --compression zstd --sort-by "ts,customer_id" --dictionary
nail binning opt.parquet -c "age" -b "18,25,35,50,65" --method custom --labels "18-24,25-34,35-49,50-64,65+" -o binned.parquet
nail pivot binned.parquet -i "age_binned" -c "category" -l "revenue" --agg sum -o summary.parquet
nail stats summary.parquet --stats-type exhaustive -o summary_stats.json
```

Compare versions:

```bash
nail diff yesterday.parquet --compare today.parquet --keys "id" --changes-only
```

## Performance Tips

- Prefer Parquet over CSV for analytical workloads.
- Scope operations with `-c` regex patterns.
- Use intermediate files for multi-step transforms.
- Tune `-j` to match your machine.
- Add `--verbose` to monitor long runs.

## License

MIT — see `LICENSE`.

## Contributing

Fork, branch, add tests, ensure `cargo test` and `cargo clippy` pass, open a PR.

## Support

Issues and questions: https://github.com/Vitruves/nail-parquet/issues
