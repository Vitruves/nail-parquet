use crate::error::NailResult;
use clap::Args;
use colored::Colorize;
use serde::{Deserialize, Serialize};

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail update")]
pub struct UpdateArgs {
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

#[derive(Deserialize, Serialize, Debug)]
struct CrateInfo {
	#[serde(rename = "crate")]
	crate_info: CrateDetails,
}

#[derive(Deserialize, Serialize, Debug)]
struct CrateDetails {
	max_version: String,
	newest_version: String,
}

const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const CRATE_NAME: &str = "nail-parquet";

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
///////////////////// RELEASE NOTE HERE /////////////////////////////////////////////////
// Write your release notes using concat! for multiple lines:

const RELEASE_NOTE: &str = concat!(
	"Release note version 1.9.0:\n",
	"\n",
	"[NEW] transpose command: flip rows and columns (--header-column to use a\n",
	"      column's values as the new headers)\n",
	"[NEW] unique command: list distinct rows or per-column value counts\n",
	"      (-c cols, --count for value counts, --sort)\n",
	"[NEW] Global --compression/--compression-level: every Parquet write can now\n",
	"      emit snappy/gzip/zstd/brotli, not just optimize\n",
	"[NEW] Global --color <auto|always|never>; honors NO_COLOR and TTY detection,\n",
	"      so redirected/piped output is no longer littered with ANSI codes\n",
	"[CHG] metadata's compression-info toggle moved to --show-compression\n",
	"      (--compression is now the global output codec flag)\n",
	"[NEW] Unified condition syntax for filter/drop/create: chained ranges\n",
	"      (18 < age < 65), BETWEEN/IN/LIKE/IS NULL/CASE, column-vs-column,\n",
	"      == alias, case-insensitive names; , = AND, | = OR\n",
	"[NEW] -o - now defaults to Parquet (lossless schema across pipes;\n",
	"      use -f csv/json to override)\n",
	"[FIX] No more 'Broken pipe' panic when a consumer (| head) exits early\n",
	"[FIX] sample --method random no longer leaks an internal rn column\n",
	"[FIX] optimize now honors --compression/--compression-level/--dictionary\n",
	"[FIX] diff compares values (real UNCHANGED/MODIFIED; --changes-only works)\n",
	"[FIX] pivot produces a real spread pivot table and honors --fill"
);

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

pub async fn execute(args: UpdateArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!(
			"Checking for updates for {} v{}",
			CRATE_NAME, CURRENT_VERSION
		);
	}

	// Check crates.io API for latest version
	let url = format!("https://crates.io/api/v1/crates/{}", CRATE_NAME);

	if args.verbose {
		eprintln!("Fetching version info from: {}", url);
	}

	let crate_info: CrateInfo = ureq::get(&url)
		.set("User-Agent", &format!("{}/{}", CRATE_NAME, CURRENT_VERSION))
		.call()
		.map_err(|e| {
			crate::error::NailError::Io(std::io::Error::other(format!(
				"Failed to fetch version info: {}",
				e
			)))
		})?
		.into_json()
		.map_err(|e| {
			crate::error::NailError::Io(std::io::Error::other(format!(
				"Failed to parse version info: {}",
				e
			)))
		})?;

	let latest_version = &crate_info.crate_info.newest_version;

	if args.verbose {
		eprintln!("Current version: {}", CURRENT_VERSION);
		eprintln!("Latest version: {}", latest_version);
	}

	// Compare versions
	if is_newer_version(latest_version, CURRENT_VERSION) {
		println!("{}", "A newer version is available!".bright_green().bold());
		println!(
			"{} {}",
			"Current version:".cyan(),
			CURRENT_VERSION.to_string().yellow()
		);
		println!("{}", RELEASE_NOTE.dimmed());
		println!(
			"{} {}",
			"Latest version: ".cyan(),
			latest_version.bright_green().bold()
		);
		println!();
		println!("{}", "To update, run one of:".bright_blue());
		println!(
			"  {}",
			"curl -fsSL https://raw.githubusercontent.com/Vitruves/nail-parquet/main/install.sh | sh"
				.bright_white()
				.bold()
		);
		println!(
			"  {}",
			format!("cargo install {}", CRATE_NAME)
				.bright_white()
				.bold()
		);
		println!();
		println!(
			"{}",
			"Or if you installed via other means, check:".bright_blue()
		);
		println!(
			"  {}",
			"https://github.com/Vitruves/nail-parquet/releases"
				.bright_white()
				.underline()
		);
	} else if latest_version == CURRENT_VERSION {
		println!(
			"{}",
			format!("You are running the latest version {}!", CURRENT_VERSION)
				.bright_green()
				.bold()
		);
		println!("{}", RELEASE_NOTE.dimmed());
	} else {
		println!(
			"{}",
			format!(
				"🚀 You are running a development version {}!",
				CURRENT_VERSION
			)
			.bright_yellow()
			.bold()
		);
		println!("{}", RELEASE_NOTE.dimmed());
		println!(
			"{} {}",
			"Latest stable version:".cyan(),
			latest_version.bright_green()
		);
	}

	Ok(())
}

fn is_newer_version(latest: &str, current: &str) -> bool {
	// Simple version comparison - assumes semantic versioning
	let latest_parts: Vec<u32> = latest.split('.').filter_map(|s| s.parse().ok()).collect();
	let current_parts: Vec<u32> = current.split('.').filter_map(|s| s.parse().ok()).collect();

	// Pad with zeros if needed
	let max_len = latest_parts.len().max(current_parts.len());
	let mut latest_padded = latest_parts;
	let mut current_padded = current_parts;

	latest_padded.resize(max_len, 0);
	current_padded.resize(max_len, 0);

	// Compare version parts
	for (l, c) in latest_padded.iter().zip(current_padded.iter()) {
		if l > c {
			return true;
		} else if l < c {
			return false;
		}
	}

	false // Versions are equal
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_version_comparison() {
		assert!(is_newer_version("1.5.0", "1.4.0"));
		assert!(is_newer_version("2.0.0", "1.4.0"));
		assert!(is_newer_version("1.4.1", "1.4.0"));
		assert!(!is_newer_version("1.4.0", "1.4.0"));
		assert!(!is_newer_version("1.3.0", "1.4.0"));
		assert!(!is_newer_version("0.9.0", "1.4.0"));
	}
}
