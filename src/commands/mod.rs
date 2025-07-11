use clap::Subcommand;

// Data Inspection
pub mod count;
pub mod head;
pub mod headers;
pub mod metadata;
pub mod preview;
pub mod schema;
pub mod size;
pub mod tail;

// Data Analysis
pub mod correlations;
pub mod frequency;
pub mod outliers;
pub mod stats;

// Data Manipulation
pub mod create;
pub mod drop;
pub mod fill;
pub mod filter;
pub mod rename;
pub mod search;
pub mod select;

// Data Transformation
pub mod binning;
pub mod dedup;
pub mod id;
pub mod pivot;
pub mod sample;
pub mod shuffle;

// Data Combination
pub mod append;
pub mod merge;
pub mod split;

// Format Conversion
pub mod convert;

// File Optimization
pub mod optimize;

// Utility
pub mod update;

#[derive(Subcommand)]
pub enum Commands {
	#[command(about = "Concatenate multiple datasets")]
	#[command(next_help_heading = "Data Combination")]
	Append(append::AppendArgs),
	
	#[command(about = "Bin continuous variables into categories")]
	#[command(next_help_heading = "Data Transformation")]
	Binning(binning::BinningArgs),
	
	#[command(about = "Convert between file formats")]
	#[command(next_help_heading = "Format Conversion")]
	Convert(convert::ConvertArgs),
	
	#[command(about = "Calculate correlation matrices")]
	#[command(next_help_heading = "Data Analysis")]
	Correlations(correlations::CorrelationsArgs),
	
	#[command(about = "Count total rows")]
	#[command(next_help_heading = "Data Inspection")]
	Count(count::CountArgs),
	
	#[command(about = "Create a new dataset")]
	#[command(next_help_heading = "Data Manipulation")]
	Create(create::CreateArgs),
	
	#[command(about = "Remove duplicate rows or columns")]
	Dedup(dedup::DedupArgs),
	
	#[command(about = "Remove columns or rows")]
	Drop(drop::DropArgs),
	
	#[command(about = "Fill missing values")]
	Fill(fill::FillArgs),
	
	#[command(about = "Filter rows by conditions")]
	Filter(filter::FilterArgs),
	
	#[command(about = "Calculate frequency distributions")]
	Frequency(frequency::FrequencyArgs),
	
	#[command(about = "Display first N rows")]
	Head(head::HeadArgs),
	
	#[command(about = "Display column headers")]
	Headers(headers::HeadersArgs),
	
	#[command(about = "Add unique identifier column")]
	Id(id::IdArgs),
	
	#[command(about = "Join two datasets")]
	Merge(merge::MergeArgs),
	
	#[command(about = "Show Parquet file metadata")]
	Metadata(metadata::MetadataArgs),
	
	#[command(about = "Optimize Parquet files for better performance")]
	#[command(next_help_heading = "File Optimization")]
	Optimize(optimize::OptimizeArgs),
	
	#[command(about = "Detect outliers in data")]
	Outliers(outliers::OutliersArgs),
	
	#[command(about = "Create pivot tables with aggregations")]
	Pivot(pivot::PivotArgs),
	
	#[command(about = "Preview random N rows")]
	Preview(preview::PreviewArgs),
	
	#[command(about = "Rename columns")]
	Rename(rename::RenameArgs),
	
	#[command(about = "Extract data samples")]
	Sample(sample::SampleArgs),
	
	#[command(about = "Display schema information")]
	Schema(schema::SchemaArgs),
	
	#[command(about = "Search for values in data")]
	Search(search::SearchArgs),
	
	#[command(about = "Select specific columns or rows")]
	Select(select::SelectArgs),
	
	#[command(about = "Randomly shuffle rows")]
	Shuffle(shuffle::ShuffleArgs),
	
	#[command(about = "Show data size information")]
	Size(size::SizeArgs),
	
	#[command(about = "Split data into multiple files")]
	Split(split::SplitArgs),
	
	#[command(about = "Calculate descriptive statistics")]
	Stats(stats::StatsArgs),
	
	#[command(about = "Display last N rows")]
	Tail(tail::TailArgs),
	
	#[command(about = "Check for newer versions")]
	#[command(next_help_heading = "Utility")]
	Update(update::UpdateArgs),
}
