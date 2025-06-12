use clap::Subcommand;

pub mod head;
pub mod tail;
pub mod preview;
pub mod headers;
pub mod stats;
pub mod correlations;
pub mod select;
pub mod drop;
pub mod fill;
pub mod filter;
pub mod id;
pub mod merge;
pub mod append;
pub mod schema;
pub mod sample;
pub mod convert;
pub mod shuffle;

#[derive(Subcommand)]
pub enum Commands {
	#[command(about = "Display first N rows")]
	Head(head::HeadArgs),
	
	#[command(about = "Display last N rows")]
	Tail(tail::TailArgs),
	
	#[command(about = "Preview random N rows")]
	Preview(preview::PreviewArgs),
	
	#[command(about = "Display column headers")]
	Headers(headers::HeadersArgs),
	
	#[command(about = "Display statistics")]
	Stats(stats::StatsArgs),
	
	#[command(about = "Display correlations")]
	Correlations(correlations::CorrelationsArgs),
	
	#[command(about = "Select columns or rows")]
	Select(select::SelectArgs),
	
	#[command(about = "Drop columns or rows")]
	Drop(drop::DropArgs),
	
	#[command(about = "Fill missing values")]
	Fill(fill::FillArgs),
	
	#[command(about = "Filter data")]
	Filter(filter::FilterArgs),
	
	#[command(about = "Add ID column")]
	Id(id::IdArgs),
	
	#[command(about = "Merge datasets")]
	Merge(merge::MergeArgs),
	
	#[command(about = "Append datasets")]
	Append(append::AppendArgs),
	
	#[command(about = "Display schema")]
	Schema(schema::SchemaArgs),
	
	#[command(about = "Sample data")]
	Sample(sample::SampleArgs),
	
	#[command(about = "Convert between formats")]
	Convert(convert::ConvertArgs),
	
	#[command(about = "Shuffle data")]
	Shuffle(shuffle::ShuffleArgs),
}