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
	"Release note version 1.6.5:\n",
    "Added sort command\n",
    "Sorting supports many date types and sorting strategies\n", 
	"Various bug fixes and improvements"
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
	
	let client = reqwest::Client::new();
	let response = client
		.get(&url)
		.header("User-Agent", format!("{}/{}", CRATE_NAME, CURRENT_VERSION))
		.send()
		.await
		.map_err(|e| crate::error::NailError::Io(std::io::Error::new(
			std::io::ErrorKind::Other,
			format!("Failed to fetch version info: {}", e)
		)))?;
	
	if !response.status().is_success() {
		return Err(crate::error::NailError::Io(std::io::Error::new(
			std::io::ErrorKind::Other,
			format!("Failed to fetch version info: HTTP {}", response.status())
		)));
	}
	
	let crate_info: CrateInfo = response
		.json()
		.await
		.map_err(|e| crate::error::NailError::Io(std::io::Error::new(
			std::io::ErrorKind::Other,
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
		println!("{} {}", "Current version:".cyan(), format!("{}", CURRENT_VERSION).yellow());
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