use clap::{ColorChoice, CommandFactory, FromArgMatches, Parser};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "nail")]
#[command(about = "A fast parquet utility written in Rust")]
#[command(version = "1.8.0")]
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
}

impl Cli {
	pub fn parse_with_width() -> Self {
		let width = if let Some((w, _)) = term_size::dimensions() {
			Some(w.clamp(80, 200))
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
pub struct CommonArgs {
	#[arg(help = "Input file (use '-' to read from stdin)")]
	pub input: PathBuf,

	#[arg(
		short,
		long,
		help = "Output file (use '-' for stdout; if omitted, prints a table)"
	)]
	pub output: Option<PathBuf>,

	#[arg(
		short,
		long,
		help = "Output format (auto-detect by default)",
		value_enum
	)]
	pub format: Option<OutputFormat>,

	#[arg(long, help = "Random seed for reproducible results")]
	pub random: Option<u64>,

	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,

	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,

	#[arg(long, help = "DataFusion batch size (rows per record batch)")]
	pub batch_size: Option<usize>,

	#[arg(long, help = "Display output as a columnar table instead of cards")]
	pub table: bool,

	#[arg(
		short = 'L',
		long,
		default_value = "1",
		help = "Max depth to expand nested values (List/Struct/Map); 0 collapses to a type tag"
	)]
	pub level: usize,
}

impl CommonArgs {
	pub fn log_if_verbose(&self, message: &str) {
		if self.verbose {
			eprintln!("{}", message);
		}
	}
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
	Json,
	Text,
	Csv,
	Parquet,
	Xlsx,
}
