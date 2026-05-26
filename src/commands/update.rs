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
	"Release note version 1.7.1:\n",
	"\n",
	"[NEW] nail create math functions in -c expressions:\n",
	"  Scalar: abs, sign, floor, ceil, round, trunc, sqrt, cbrt, exp,\n",
	"          ln, log, log10, log2, pow, sin/cos/tan, asin/acos/atan, atan2\n",
	"  Aggregate (broadcast via OVER ()): mean, sum, min, max, count,\n",
	"          median, std/stddev, var/variance, stddev_pop, var_pop\n",
	"  Example: -c \"z=(value-mean(value))/std(value)\"\n",
	"\n",
	"[NEW] -c is repeatable on nail create; commas inside pow(a,b) preserved\n",
	"[NEW] nail clean: snake_case headers, trim strings, drop empty rows\n",
	"[NEW] stdin/stdout via '-' on every command (format auto-sniffed)\n",
	"[NEW] \"Did you mean ...?\" suggestions on column-not-found errors\n",
	"[NEW] Examples shown in every subcommand's --help\n",
	"\n",
	"[FIX] Linux release binaries no longer require glibc >= 2.38\n",
	"      (now statically linked against musl)\n",
	"[FIX] Windows stack overflow (STATUS_STACK_OVERFLOW) in many subcommands\n",
	"[FIX] Clippy lints under Rust 1.95 and formatting drift\n",
	"\n",
	"Release artifacts now cover linux-musl (x86_64, aarch64),\n",
	"macOS (x86_64, aarch64), and windows-msvc, with SHA256SUMS.txt."
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
		println!("{}", "To update, run:".bright_blue());
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
