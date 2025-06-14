use clap::{Parser, ColorChoice, CommandFactory, FromArgMatches};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "nail")]
#[command(about = "A fast data utility written in Rust")]
#[command(long_about = "Nail, a parquet command-line tool for data manipulation, analysis, and format conversion.")]
#[command(version = "1.3.0")]
#[command(author = "Johan HG Natter")]
#[command(color = ColorChoice::Auto)]
#[command(styles = clap::builder::Styles::styled()
	.header(clap::builder::styling::AnsiColor::Yellow.on_default().bold())
	.usage(clap::builder::styling::AnsiColor::Green.on_default().bold())
	.literal(clap::builder::styling::AnsiColor::Blue.on_default().bold())
	.placeholder(clap::builder::styling::AnsiColor::Cyan.on_default())
	.error(clap::builder::styling::AnsiColor::Red.on_default().bold())
	.valid(clap::builder::styling::AnsiColor::Green.on_default().bold())
	.invalid(clap::builder::styling::AnsiColor::Red.on_default().bold())
)]
pub struct Cli {
	#[command(subcommand)]
	pub command: crate::commands::Commands,
	
	#[arg(short, long, global = true, help = "Enable verbose output")]
	pub verbose: bool,
	
	#[arg(short, long, global = true, help = "Number of parallel jobs (default: half of CPU cores)")]
	pub jobs: Option<usize>,
}

impl Cli {
	pub fn parse_with_width() -> Self {
		let width = if let Some((w, _)) = term_size::dimensions() {
			Some(w.max(80).min(200))
		} else {
			Some(120)
		};
		
		let mut cmd = Self::command();
		if let Some(w) = width {
			cmd = cmd.term_width(w);
		}
		
		Self::from_arg_matches(&cmd.get_matches()).unwrap()
	}
}

#[derive(clap::Args, Clone)]
pub struct GlobalArgs {
	#[arg(short, long, help = "Input file")]
	pub input: Option<PathBuf>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format (auto-detect by default)", value_enum)]
	pub format: Option<OutputFormat>,
	
	#[arg(long, help = "Random seed for reproducible results")]
	pub random: Option<u64>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
	Json,
	Text,
	Csv,
	Parquet,
}