# Changelog

All notable changes to `nail-parquet` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/).

## [1.9.0] - 2026-06-30

### Added
- **`nail transpose` — flip rows and columns.** Turns each input column into a row and each input row into a column. By default the new columns are named `row_1..row_N`; pass `--header-column <col>` to use that column's values as the headers instead (the column is then consumed and excluded from the transposed body). The leading column holding the original field names defaults to `column` (override with `--name-column`). Because a transposed row mixes the original column types, output cells are rendered as text. Transpose materializes one output column per input row, so it refuses to flip frames with more than 10,000 rows — filter or limit first.
- **`nail unique` — list distinct rows or per-column value counts.** With no `-c` it emits distinct whole rows (`SELECT DISTINCT *`); with `-c col1,col2` it lists the distinct combinations of those columns. `--count` appends a `count` column and sorts most-frequent-first (value counts), and `--sort` orders distinct rows by the selected columns. This complements `dedup` (which removes duplicates but can't *list* them) and `frequency` (which is per-column).
- **Global `--compression` / `--compression-level`.** Every Parquet write path now honors a chosen codec (`snappy` (default), `gzip`, `zstd`, `brotli`) and level (1-9 for gzip/zstd/brotli), not just `optimize`. Previously all normal writes were hardcoded to SNAPPY. `optimize` now reads these global flags too (its per-command `--compression`/`--compression-level` flags were folded into the global ones).
- **Global `--color <auto|always|never>` with `NO_COLOR`/TTY discipline.** Console (card/table/matrix) output no longer emits ANSI escapes unconditionally. `auto` (the default) colorizes only when stdout is a terminal and `NO_COLOR` is unset; `always`/`never` force the choice. Redirecting or piping display output now produces clean, escape-free text.

- **Unified, newbie-friendly condition syntax for `filter`, `drop`, and `create`.** All three commands now share one condition engine (`src/utils/predicate.rs`), so you can mix plain math and SQL however you like and never *have* to know SQL. New capabilities on top of the old `column op value` syntax: math-style chained ranges (`18 < age < 65`, `8 > x > 4`, `0 <= score <= 100`); the full set of SQL operators that previously only worked in `create` (`BETWEEN a AND b`, `IN (…)`, `LIKE`/`ILIKE`, `IS NULL`, `IS NOT NULL`, `NOT (…)`, `CASE WHEN …`); column-vs-column comparisons (`price > cost`); and `==` as an alias for `=`. Column names are matched case-insensitively and quoted automatically, so `age > 18` works even when the column is `Age`, and an unquoted value on the right of a comparison is treated as text (`status=active` means `status = 'active'`). The existing shorthand is unchanged and still works: `,` = AND, `|` = OR, with AND binding tighter than OR. `nail filter -c` and `nail drop -r <condition>` previously had a weaker, separate parser (only `= != < <= > >=`, no OR in `drop`); they now match the power of `create`. `drop`'s condition form is the exact mirror of `filter`: rows where the condition is true are dropped, rows where it is false or null are kept.

### Changed
- **`metadata`'s compression-info toggle is now `--show-compression`** (was `--compression`). The rename was needed once `--compression` became the global output-codec flag; the metadata display toggle keeps the same behavior under the clearer name.

- **`-o -` now defaults to Parquet instead of CSV.** Streaming to stdout without an explicit `-f` previously emitted CSV, which couldn't represent nested `List`/`Struct`/`Map` columns and lost types when chaining commands. Parquet round-trips the full schema losslessly, so `nail … -o - | nail … -` just works on any data. Pass `-f csv`/`-f json` to override.

- **`nail sample --method random` no longer adds an `rn` column to its output**
  (#7). Previously seeded sampling leaked an internal `rn` column, which
  silently changed the schema and broke a subsequent `nail sample` on the
  result. The output now always has exactly the input schema (and inputs that
  already contain an `rn` column are handled correctly). Sampling was rewritten
  to be faster and lighter on memory, and seeded runs remain fully reproducible.

### Fixed
- **No more `Broken pipe` panic when a downstream consumer exits early.** `nail … -o - | head` (or any consumer that closes the pipe) aborted with `failed printing to stdout: Broken pipe` + `SIGABRT`. `SIGPIPE` is now reset to its default disposition on startup (Unix), so nail terminates quietly like a well-behaved pipeline tool.
- **`nail optimize` now actually applies the requested compression.** The command built `WriterProperties` (compression codec, level, dictionary encoding, row-group size) but then wrote the file with DataFusion's defaults via `write_parquet(..., None)`, so `--compression zstd|gzip|brotli`, `--compression-level`, and `--dictionary` were silently ignored — every "optimized" file came out SNAPPY. Output is now written through a low-level streaming `ArrowWriter` that honors the properties, so the codec, level, and dictionary settings take effect.
- **`nail diff` now compares values instead of flagging every matched row as `MODIFIED`.** The keyed diff's status only ever produced `ADDED`/`REMOVED`/`MODIFIED` (the `MODIFIED` branch was an unconditional `ELSE`) and never compared the non-key columns, so identical rows were reported as changed and `--changes-only` (which filtered `!= 'UNCHANGED'`) removed nothing. Both the keyed and positional diff paths now emit `UNCHANGED` when every compared column matches (NULL-aware via `IS DISTINCT FROM`) and `MODIFIED` only on a real difference; `--changes-only` works in both modes.
- **`nail pivot` now produces a real pivot table.** It previously returned a long-format `GROUP BY` (the pivot column was left in place, no spread happened) and ignored `--fill`. Distinct pivot-key values are now spread into one aggregated column each (`CASE WHEN <key> THEN <value> END` per key), index columns form the rows, multiple value columns are name-prefixed, NULL keys are dropped, and `--fill` fills empty cells (default `0`; `null`/`none` keeps them empty).

## [1.8.0] - 2026-05-30

### Added
- **`-L`/`--level` for nested values.** `head`/`tail`/`preview` (and any card
  output) now expand `List`/`Struct`/`Map` columns instead of printing
  `ListArray`/`StructArray`. The depth budget controls expansion: `-L 0`
  collapses to a tag (`{…2 fields}`), the default `-L 1` shows one level, higher
  values go deeper. Cards render an indented tree (single-element lists drop the
  `-` bullet); `--table` stays compact; `-f json` emits valid nested JSON.
  Binary blobs always render as `<N bytes>`, never raw — so image columns are
  safe at any depth.

### Fixed
- **Many column types displayed their Arrow type name instead of a value.**
  `head`/`tail`/`preview` now render real values for `Int8`/`Int16`/`UInt8`/
  `UInt16`/`UInt32`, `Timestamp` (incl. timezone-aware), `Time32`/`Time64`,
  `Decimal128`, `Dictionary`, `LargeUtf8`, `Binary`/`LargeBinary`, and other
  types that previously printed `PrimitiveArray<…>`, `timestamp`, `ListArray`,
  etc. Leaf values now go through Arrow's formatter, so the supported-type set
  matches Arrow's. `nail head -f json` on these columns is now valid JSON
  (strings with backslashes/control chars are properly escaped).

### Changed
- **`--table` rendering optimized for wide tables.** Cell content is capped at
  30 chars with `…` truncation, and columns paginate into multiple bordered
  table blocks that each fit the terminal width. A `#` row-index column is
  repeated on every block so rows stay aligned across pages, and a
  `cols A–B of N (page X/Y)` header annotates each subsequent block. Column
  colors remain consistent across blocks.
- **`nail correlations --matrix` redesigned.** Output is now a proper bordered
  matrix table with column headers derived from the row labels (so names
  containing dots/underscores like `__index_level_0__` no longer get mangled),
  per-column auto-sizing, and color-coded cells (green for positive, red for
  negative, intensity by magnitude; bold white for the 1.0 diagonal).

## [1.7.1] - 2026-05-21

### Fixed
- **Linux release binaries no longer require glibc ≥ 2.38** (#6).
  Releases built on `ubuntu-latest` (24.04, glibc 2.39) failed to start on
  Ubuntu 22.04, Debian 11, RHEL 8, etc. (`version 'GLIBC_2.38' not found`).
  Linux artifacts are now statically linked against musl — no glibc dependency.
- **Windows stack overflow** in many subcommands
  (`STATUS_STACK_OVERFLOW`, `0xC00000FD`). The binary now reserves an 8 MiB
  stack on Windows targets via `.cargo/config.toml`, matching Linux/macOS.
- Clippy lints under Rust 1.95: `too_many_arguments`, `collapsible_match`,
  `nonminimal_bool`, `needless_range_loop`.
- Source formatting drift (`cargo fmt`) and pinned style via committed
  `rustfmt.toml` (`hard_tabs = true`).

### Added
- **`nail create` math functions** — column expressions now support a curated
  set of nail-flavored functions:
  - Scalar (per-row): `abs`, `sign`, `floor`, `ceil`, `round`, `trunc`, `sqrt`,
    `cbrt`, `exp`, `ln`, `log`, `log10`, `log2`, `pow`, `sin`, `cos`, `tan`,
    `asin`, `acos`, `atan`, `atan2`.
  - Aggregate (broadcast to every row, auto-wrapped as `OVER ()`): `mean`,
    `sum`, `min`, `max`, `count`, `median`, `std`/`stddev`, `var`/`variance`,
    `stddev_pop`, `var_pop`.
  - Example: `-c "z=(value-mean(value))/std(value)"`.
  - Quote column names that collide with a function (`mean("mean")`) or that
    contain operator characters (`"revenue-income"-cost`).
- **`-c` is now repeatable** on `nail create`. Pass multiple `-c` flags or
  comma-separate specs within one flag; commas inside `pow(a, b)` /
  `round(x, 2)` are correctly preserved.
- **`nail clean`** — one-shot import cleanup. Snake_case headers, trim string
  whitespace, drop fully-empty rows by default; `--drop-empty-cols` opt-in.
  `--keep-headers`, `--keep-whitespace`, `--keep-empty-rows` to opt out.
- **Stdin/stdout via `-`** on every command. Read with `cat data.csv | nail head -`,
  write with `nail filter ... -o -`. Format auto-detected from `--format` or by
  sniffing input bytes (Parquet magic, JSON brace, CSV otherwise). Excel is the
  only format that still requires a file path.
- **"Did you mean …?"** suggestions on column-not-found errors. Powered by
  Levenshtein distance with length-scaled tolerance.
- **Examples in every `--help`.** Each subcommand now ends its help with 2-3
  concrete invocations.
- Multi-platform release workflow (`.github/workflows/release.yml`) producing
  artifacts for:
  - `x86_64-unknown-linux-musl` (static)
  - `aarch64-unknown-linux-musl` (static, via `cross`)
  - `x86_64-apple-darwin`, `aarch64-apple-darwin`
  - `x86_64-pc-windows-msvc`
  Each release ships a `SHA256SUMS.txt`.
  

## [1.7.0] - 2026-04-22

See `src/commands/update.rs` `RELEASE_NOTE` for the 1.7.0 release notes.
