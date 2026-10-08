<p align="center">
  <img src="assets/banner.jpg" alt="nail — lightning-fast data analysis CLI for Parquet, CSV, JSON, Arrow and Excel" width="100%">
</p>

<p align="center">
  <a href="https://crates.io/crates/nail-parquet"><img src="https://img.shields.io/crates/v/nail-parquet.svg" alt="Crates.io"></a>
  <a href="https://crates.io/crates/nail-parquet"><img src="https://img.shields.io/crates/d/nail-parquet.svg" alt="Downloads"></a>
  <a href="https://github.com/Vitruves/nail-parquet/blob/main/LICENSE"><img src="https://img.shields.io/crates/l/nail-parquet.svg" alt="License"></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/rust-1.70%2B-blue.svg" alt="Rust"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> •
  <a href="#quick-start">Quick start</a> •
  <a href="#commands">Commands</a> •
  <a href="#supported-formats">Formats</a> •
  <a href="#global-options">Options</a> •
  <a href="#examples">Examples</a> •
  <a href="#piping-stdinstdout">Piping</a>
</p>

# nail - Lightning-Fast Data Analysis CLI

**nail** is a high-performance command-line tool for analyzing, transforming, and exploring Parquet, CSV, JSON, JSON Lines, Arrow IPC, and Excel files. Built with Rust, Apache Arrow, and DataFusion.

- **Fast** — gigabyte-scale datasets in seconds, parallel across all CPU cores.
- **SQL-powered** — DataFusion under the hood, familiar filter and expression syntax.
- **Zero configuration** — formats are detected from the file extension, on input and output.
- **Composable** — every command reads stdin and writes stdout, so `nail` pipes into itself and other tools.
- **Self-contained** — a single binary that works offline.

## Installation

Prebuilt binary (macOS/Linux — auto-detects your OS and architecture):

```bash
curl -fsSL https://raw.githubusercontent.com/Vitruves/nail-parquet/main/install.sh | sh
```

It installs to `/usr/local/bin` when writable, otherwise to `~/.local/bin`. Set `BINDIR` to choose the location explicitly, and the installer tells you if the target dir is not on your `PATH`:

```bash
curl -fsSL https://raw.githubusercontent.com/Vitruves/nail-parquet/main/install.sh | BINDIR="$HOME/bin" sh
```

With Cargo:

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

## Quick Start

```bash
nail describe sales.parquet                                    # what is in this file?
nail head sales.parquet -n 5                                   # look at a few rows
nail filter sales.parquet -c "revenue > 1000" -o big.parquet   # keep what matters
nail convert big.parquet -o big.csv                            # hand it to another tool
```

The package is `nail-parquet` on crates.io; the executable is `nail`.

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
transpose     Transpose rows and columns
unique        List distinct rows or per-column value counts
update        Check for newer versions
help          Print this message or the help of the given subcommand(s)
```

Run `nail <command> --help` for full usage.

## Supported Formats

Formats are detected from the file extension on both input and output, and can be forced with `-f/--format`.

| Format | Extensions | Read | Write |
|--------|-----------|------|-------|
| Parquet | `.parquet` | yes | yes |
| CSV | `.csv` | yes | yes |
| JSON (newline-delimited) | `.json` | yes | yes |
| JSON Lines | `.jsonl`, `.ndjson` | yes | yes |
| Arrow IPC / Feather v2 | `.arrow`, `.ipc`, `.feather` | yes | yes |
| Excel | `.xlsx` | yes | yes |

Both Arrow IPC flavours are read: the file format (`ARROW1` magic + footer) and the stream format written by e.g. HuggingFace `datasets.save_to_disk`. Writes always produce the self-contained file format.

```bash
nail head dummy_dataset/data-00000-of-00001.arrow
nail convert dummy_dataset/data-00000-of-00001.arrow -o dummy.jsonl
nail filter events.jsonl -c "status == 'error'" -o errors.arrow
```

## Global Options

Available on all commands:

| Flag | Description |
|------|-------------|
| `-v, --verbose` | Timing and progress output |
| `-j, --jobs N` | Parallel jobs (default: all CPU cores) |
| `-o, --output FILE` | Output file, or `-` for stdout (prints a table to the console if omitted) |
| `-f, --format FORMAT` | Output format: `json`, `jsonl`, `csv`, `parquet`, `arrow`, `text`, `xlsx` |
| `--batch-size N` | DataFusion batch size (rows per record batch) |
| `--table` | Display console output as a columnar table instead of cards |
| `--random N` | Random seed for reproducible results |
| `--compression CODEC` | Parquet output codec: `snappy` (default), `gzip`, `zstd`, `brotli` |
| `--compression-level N` | Compression level (1-9) for gzip/zstd/brotli |
| `--color WHEN` | Colorize console output: `auto` (default), `always`, `never` (also honors `NO_COLOR`) |
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

Reshape and summarize:

```bash
nail unique sales.parquet -c "category"                 # distinct values of a column
nail unique sales.parquet -c "category,region" --count  # value counts, most frequent first
nail transpose metrics.parquet --header-column metric -o wide.parquet
```

Compare versions:

```bash
nail diff yesterday.parquet --compare today.parquet --keys "id" --changes-only
```

## Piping (stdin/stdout)

Every command can read from stdin and write to stdout, so you can chain `nail` with itself or other tools. Use `-o -` on the producer to stream out, and `-` as the input on the consumer to read in. The input format is auto-detected.

Streaming to stdout defaults to **Parquet**, which preserves the full schema (types and nested `List`/`Struct`/`Map` columns) losslessly across the pipe. Use `-f csv` or `-f json` when you want text output for other tools.

```bash
# chain nail commands (Parquet by default — keeps all types)
nail filter sales.parquet -c "revenue > 1000" -o - | nail sort - -c revenue -o - | nail head - -n 10

# stream as CSV/JSON for other tools
nail select sales.parquet -c "id,revenue" -o - -f csv | grep -v '^0,'
nail sample sales.parquet -n 100 -o - -f json | jq '.revenue'

# read from stdin produced elsewhere
cat data.parquet | nail count -
```

Pipes that close early (`| head`, `| less`) are handled cleanly — no broken-pipe errors.

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
