mod cli;
mod commands;
mod error;
mod utils;

pub use crate::commands::select::{parse_row_specification, select_columns_by_pattern};
use cli::Cli;
use error::NailResult;

#[tokio::main]
async fn main() {
	reset_sigpipe();
	if let Err(e) = run().await {
		eprintln!("Error: {}", e);
		std::process::exit(1);
	}
}

/// Restore the default `SIGPIPE` disposition on Unix.
///
/// Rust installs `SIG_IGN` for `SIGPIPE` at startup, so writing to a pipe whose
/// reader has gone away (`nail ... | head`, an early-exiting consumer, etc.)
/// surfaces as an `EPIPE` error inside the stdout print machinery, which then
/// panics with "failed printing to stdout: Broken pipe". Resetting to `SIG_DFL`
/// makes the process terminate quietly via the signal — the conventional Unix
/// behaviour for CLI tools in a pipeline.
#[cfg(unix)]
fn reset_sigpipe() {
	// SAFETY: resetting a signal handler to its default disposition is sound;
	// we touch no shared state and call this once before any threads do I/O.
	unsafe {
		libc::signal(libc::SIGPIPE, libc::SIG_DFL);
	}
}

#[cfg(not(unix))]
fn reset_sigpipe() {}

async fn run() -> NailResult<()> {
	let cli = Cli::parse_with_width();

	// Resolve global display/output settings once, before dispatch.
	utils::format::set_color_enabled(cli.color.resolve());
	if !(1..=9).contains(&cli.compression_level) {
		return Err(error::NailError::InvalidArgument(
			"Compression level must be between 1 and 9".to_string(),
		));
	}
	if let Some(codec) = cli.compression {
		utils::io::set_write_compression(
			codec.to_parquet_compression(cli.compression_level as i32),
		);
	}

	match cli.command {
		commands::Commands::Head(args) => commands::head::execute(args).await,
		commands::Commands::Tail(args) => commands::tail::execute(args).await,
		commands::Commands::Transpose(args) => commands::transpose::execute(args).await,
		commands::Commands::Unique(args) => commands::unique::execute(args).await,
		commands::Commands::Preview(args) => commands::preview::execute(args).await,
		commands::Commands::Headers(args) => commands::headers::execute(args).await,
		commands::Commands::Schema(args) => commands::schema::execute(args).await,
		commands::Commands::Count(args) => commands::count::execute(args).await,
		commands::Commands::Size(args) => commands::size::execute(args).await,
		commands::Commands::Metadata(args) => commands::metadata::execute(args).await,
		commands::Commands::Stats(args) => commands::stats::execute(args).await,
		commands::Commands::Correlations(args) => commands::correlations::execute(args).await,
		commands::Commands::Frequency(args) => commands::frequency::execute(args).await,
		commands::Commands::Outliers(args) => commands::outliers::execute(args).await,
		commands::Commands::Select(args) => commands::select::execute(args).await,
		commands::Commands::Drop(args) => commands::drop::execute(args).await,
		commands::Commands::Fill(args) => commands::fill::execute(args).await,
		commands::Commands::Filter(args) => commands::filter::execute(args).await,
		commands::Commands::Search(args) => commands::search::execute(args).await,
		commands::Commands::Rename(args) => commands::rename::execute(args).await,
		commands::Commands::Create(args) => commands::create::execute(args).await,
		commands::Commands::Id(args) => commands::id::execute(args).await,
		commands::Commands::Shuffle(args) => commands::shuffle::execute(args).await,
		commands::Commands::Sample(args) => commands::sample::execute(args).await,
		commands::Commands::Dedup(args) => commands::dedup::execute(args).await,
		commands::Commands::Describe(args) => commands::describe::execute(args).await,
		commands::Commands::Diff(args) => commands::diff::execute(args).await,
		commands::Commands::Binning(args) => commands::binning::execute(args).await,
		commands::Commands::Pivot(args) => commands::pivot::execute(args).await,
		commands::Commands::Merge(args) => commands::merge::execute(args).await,
		commands::Commands::Append(args) => commands::append::execute(args).await,
		commands::Commands::Sort(args) => commands::sort::execute(args).await,
		commands::Commands::Split(args) => commands::split::execute(args).await,
		commands::Commands::Clean(args) => commands::clean::execute(args).await,
		commands::Commands::Convert(args) => commands::convert::execute(args).await,
		commands::Commands::Optimize(args) => commands::optimize::execute(args).await,
		commands::Commands::Update(args) => commands::update::execute(args).await,
		commands::Commands::Completions(args) => commands::completions::execute(args).await,
	}
}
