# Changelog

All notable changes to `nail-parquet` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/).

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
