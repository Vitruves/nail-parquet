use clap::Args;
use crate::error::NailResult;
use serde::{Deserialize, Serialize};
use colored::Colorize;

#[derive(Args, Clone)]
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
	"Release note version 1.7.0 (major):\n",
	"\n",
	"[NEW] --table flag for all commands:\n",
	"  Display output as a columnar table instead of cards\n",
	"  Available globally via CommonArgs, works with every command\n",
	"  Colored headers and cells, auto-sized columns, box-drawing borders\n",
	"\n",
	"[NEW] frequency command enhancements:\n",
	"  Added --head <N> to show only the top N most frequent entries\n",
	"  Added --tail <N> to show only the bottom N least frequent entries\n",
	"  Percentage column (%) included in --table and file output\n",
	"\n",
	"[NEW] preview command enhancements:\n",
	"  Added --rows <spec> to select specific row numbers or ranges (e.g., 1,3,5-10)\n",
	"  Works with interactive mode and output formats\n",
	"\n",
		"[NEW] filter: OR operator via '|' separator (AND via ',', AND binds tighter)\n",
	"[NEW] select/drop: --type <numeric|integer|float|string|boolean|temporal|binary>\n",
	"[NEW] search: multi-value OR via '|' in --value (e.g. 'foo|bar')\n",
	"[NEW] completions: new subcommand generates bash/zsh/fish/powershell/elvish scripts\n",
	"\n",
	"[FIX] Improved float formatting:\n",
	"  Trailing zeros trimmed (40.000 -> 40.0), keeping at least one decimal\n",
	"[FIX] Improved border visibility on dark terminal themes",
	"Multiple optimizations and bug fixes"
);

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

pub async fn execute(args: UpdateArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Checking for updates for {} v{}", CRATE_NAME, CURRENT_VERSION);
	}

	// Check crates.io API for latest version
	let url = format!("https://crates.io/api/v1/crates/{}", CRATE_NAME);

	if args.verbose {
		eprintln!("Fetching version info from: {}", url);
	}

	let crate_info: CrateInfo = ureq::get(&url)
		.set("User-Agent", &format!("{}/{}", CRATE_NAME, CURRENT_VERSION))
		.call()
		.map_err(|e| crate::error::NailError::Io(std::io::Error::other(
			format!("Failed to fetch version info: {}", e)
		)))?
		.into_json()
		.map_err(|e| crate::error::NailError::Io(std::io::Error::other(
			format!("Failed to parse version info: {}", e)
		)))?;
	
	let latest_version = &crate_info.crate_info.newest_version;
	
	if args.verbose {
		eprintln!("Current version: {}", CURRENT_VERSION);
		eprintln!("Latest version: {}", latest_version);
	}
	
	// Compare versions
	if is_newer_version(latest_version, CURRENT_VERSION) {
		println!("{}", "A newer version is available!".bright_green().bold());
		println!("{} {}", "Current version:".cyan(), CURRENT_VERSION.to_string().yellow());
		println!("{}", RELEASE_NOTE.dimmed());
		println!("{} {}", "Latest version: ".cyan(), latest_version.bright_green().bold());
		println!();
		println!("{}", "To update, run:".bright_blue());
		println!("  {}", format!("cargo install {}", CRATE_NAME).bright_white().bold());
		println!();
		println!("{}", "Or if you installed via other means, check:".bright_blue());
		println!("  {}", "https://github.com/Vitruves/nail-parquet/releases".bright_white().underline());
	} else if latest_version == CURRENT_VERSION {
		println!("{}", format!("You are running the latest version {}!", CURRENT_VERSION).bright_green().bold());
		println!("{}", RELEASE_NOTE.dimmed());
	} else {
		println!("{}", format!("🚀 You are running a development version {}!", CURRENT_VERSION).bright_yellow().bold());
		println!("{}", RELEASE_NOTE.dimmed());
		println!("{} {}", "Latest stable version:".cyan(), latest_version.bright_green());
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