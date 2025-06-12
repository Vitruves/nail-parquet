use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "nail")]
#[command(about = "A fast parquet utility written in Rust")]
#[command(version = "1.2.0")]
pub struct Cli {
	#[command(subcommand)]
	pub command: crate::commands::Commands,
	
	#[arg(short, long, global = true, help = "Enable verbose output")]
	pub verbose: bool,
	
	#[arg(short, long, global = true, help = "Number of parallel jobs", default_value = "4")]
	pub jobs: usize,
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
	
	#[arg(short, long, help = "Number of parallel jobs", default_value = "4")]
	pub jobs: usize,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
	Json,
	Text,
	Csv,
	Parquet,
}