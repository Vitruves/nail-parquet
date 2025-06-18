use clap::Subcommand;

// Data Inspection
pub mod head;
pub mod tail;
pub mod preview;
pub mod headers;
pub mod schema;
pub mod count;
pub mod size;

// Data Analysis
pub mod stats;
pub mod correlations;
pub mod frequency;

// Data Manipulation
pub mod select;
pub mod drop;
pub mod fill;
pub mod filter;
pub mod search;
pub mod rename;
pub mod create;

// Data Transformation
pub mod id;
pub mod shuffle;
pub mod sample;
pub mod dedup;

// Data Combination
pub mod merge;
pub mod append;
pub mod split;

// Format Conversion
pub mod convert;

// Utility
pub mod update;

#[derive(Subcommand)]
pub enum Commands {
	// Data Inspection
	#[command(about = "Display first N rows")]
	#[command(next_help_heading = "Data Inspection")]
	Head(head::HeadArgs),
	
	#[command(about = "Display last N rows")]
	Tail(tail::TailArgs),
	
	#[command(about = "Preview random N rows")]
	Preview(preview::PreviewArgs),
	
	#[command(about = "Display column headers")]
	Headers(headers::HeadersArgs),
	
	#[command(about = "Display schema information")]
	Schema(schema::SchemaArgs),
	
	#[command(about = "Count total rows")]
	Count(count::CountArgs),
	
	#[command(about = "Show data size information")]
	Size(size::SizeArgs),
	
	// Data Analysis
	#[command(about = "Calculate descriptive statistics")]
	#[command(next_help_heading = "Data Analysis")]
	Stats(stats::StatsArgs),
	
	#[command(about = "Calculate correlation matrices")]
	Correlations(correlations::CorrelationsArgs),
	
	#[command(about = "Calculate frequency distributions")]
	Frequency(frequency::FrequencyArgs),
	
	// Data Manipulation
	#[command(about = "Select specific columns or rows")]
	#[command(next_help_heading = "Data Manipulation")]
	Select(select::SelectArgs),
	
	#[command(about = "Remove columns or rows")]
	Drop(drop::DropArgs),
	
	#[command(about = "Fill missing values")]
	Fill(fill::FillArgs),
	
	#[command(about = "Filter rows by conditions")]
	Filter(filter::FilterArgs),
	
	#[command(about = "Search for values in data")]
	Search(search::SearchArgs),
	#[command(about = "Rename columns")]
	Rename(rename::RenameArgs),
	#[command(about = "Create a new dataset")]
	Create(create::CreateArgs),
	
	// Data Transformation
	#[command(about = "Add unique identifier column")]
	#[command(next_help_heading = "Data Transformation")]
	Id(id::IdArgs),
	
	#[command(about = "Randomly shuffle rows")]
	Shuffle(shuffle::ShuffleArgs),
	
	#[command(about = "Extract data samples")]
	Sample(sample::SampleArgs),
	
	#[command(about = "Remove duplicate rows or columns")]
	Dedup(dedup::DedupArgs),
	
	// Data Combination
	#[command(about = "Join two datasets")]
	#[command(next_help_heading = "Data Combination")]
	Merge(merge::MergeArgs),
	
	#[command(about = "Concatenate multiple datasets")]
	Append(append::AppendArgs),
	
	#[command(about = "Split data into multiple files")]
	Split(split::SplitArgs),
	
	// Format Conversion
	#[command(about = "Convert between file formats")]
	#[command(next_help_heading = "Format Conversion")]
	Convert(convert::ConvertArgs),
	
	// Utility
	#[command(about = "Check for newer versions")]
	#[command(next_help_heading = "Utility")]
	Update(update::UpdateArgs),
}