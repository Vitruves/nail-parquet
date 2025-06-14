use clap::Args;
use std::path::PathBuf;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand::rngs::StdRng;
use crate::error::NailResult;
use crate::utils::io::read_data;
use crate::utils::format::display_dataframe;

#[derive(Args, Clone)]
pub struct PreviewArgs {
	#[arg(short, long, help = "Input file")]
	pub input: PathBuf,
	
	#[arg(short, long, help = "Number of rows to display", default_value = "5")]
	pub number: usize,
	
	#[arg(short, long, help = "Random seed for reproducible results")]
	pub random: Option<u64>,
	
	#[arg(short, long, help = "Output file (if not specified, prints to console)")]
	pub output: Option<PathBuf>,
	
	#[arg(short, long, help = "Output format", value_enum)]
	pub format: Option<crate::cli::OutputFormat>,
	
	#[arg(short, long, help = "Number of parallel jobs")]
	pub jobs: Option<usize>,
	
	#[arg(short, long, help = "Enable verbose output")]
	pub verbose: bool,
}

pub async fn execute(args: PreviewArgs) -> NailResult<()> {
	if args.verbose {
		eprintln!("Reading data from: {}", args.input.display());
	}
	
	let df = read_data(&args.input).await?;
	let total_rows = df.clone().count().await?;
	
	if total_rows <= args.number {
		display_dataframe(&df, args.output.as_deref(), args.format.as_ref()).await?;
		return Ok(());
	}
	
	let mut rng = match args.random {
		Some(seed) => StdRng::seed_from_u64(seed),
		None => StdRng::from_entropy(),
	};
	
	let mut indices: Vec<usize> = (0..total_rows).collect();
	indices.shuffle(&mut rng);
	indices.truncate(args.number);
	indices.sort();
	
	if args.verbose {
		eprintln!("Randomly sampling {} rows from {} total rows", args.number, total_rows);
	}
	
	let ctx = crate::utils::create_context_with_jobs(args.jobs).await?;
	let table_name = "temp_table";
	ctx.register_table(table_name, df.clone().into_view())?;
	
	let indices_str = indices.iter()
		.map(|&i| (i + 1).to_string())
		.collect::<Vec<_>>()
		.join(",");
	
	// Get the original column names and quote them to preserve case
	let original_columns: Vec<String> = df.schema().fields().iter()
		.map(|f| format!("\"{}\"", f.name()))
		.collect();
	
	let sql = format!(
		"SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER() as rn FROM {}) WHERE rn IN ({})",
		original_columns.join(", "),
		original_columns.join(", "),
		table_name, 
		indices_str
	);
	
	if args.verbose {
		eprintln!("Executing SQL: {}", sql);
	}
	
	let result = ctx.sql(&sql).await?;
	
	display_dataframe(&result, args.output.as_deref(), args.format.as_ref()).await?;
	
	Ok(())
}