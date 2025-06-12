use clap::Parser;

mod cli;
mod commands;
mod error;
mod utils;

use cli::Cli;
use error::NailResult;
pub use crate::commands::select::{select_columns_by_pattern, parse_row_specification};


#[tokio::main]
async fn main() -> NailResult<()> {
	let cli = Cli::parse();
	
	match cli.command {
		commands::Commands::Head(args) => commands::head::execute(args).await,
		commands::Commands::Tail(args) => commands::tail::execute(args).await,
		commands::Commands::Preview(args) => commands::preview::execute(args).await,
		commands::Commands::Headers(args) => commands::headers::execute(args).await,
		commands::Commands::Stats(args) => commands::stats::execute(args).await,
		commands::Commands::Correlations(args) => commands::correlations::execute(args).await,
		commands::Commands::Select(args) => commands::select::execute(args).await,
		commands::Commands::Drop(args) => commands::drop::execute(args).await,
		commands::Commands::Fill(args) => commands::fill::execute(args).await,
		commands::Commands::Filter(args) => commands::filter::execute(args).await,
		commands::Commands::Id(args) => commands::id::execute(args).await,
		commands::Commands::Merge(args) => commands::merge::execute(args).await,
		commands::Commands::Append(args) => commands::append::execute(args).await,
		commands::Commands::Schema(args) => commands::schema::execute(args).await,
		commands::Commands::Sample(args) => commands::sample::execute(args).await,
		commands::Commands::Convert(args) => commands::convert::execute(args).await,
		commands::Commands::Shuffle(args) => commands::shuffle::execute(args).await,
	}
}