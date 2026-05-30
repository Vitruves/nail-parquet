use crate::cli::{CommonArgs, OutputFormat};
use crate::error::{NailError, NailResult};
use crate::utils::{
	format::display_dataframe_with_mode,
	io::{is_stdio, write_data},
	FileFormat,
};
use datafusion::prelude::DataFrame;
use std::path::{Path, PathBuf};

pub struct OutputHandler<'a> {
	common_args: &'a CommonArgs,
}

impl<'a> OutputHandler<'a> {
	pub fn new(common_args: &'a CommonArgs) -> Self {
		Self { common_args }
	}

	pub async fn handle_output(&self, df: &DataFrame, operation_name: &str) -> NailResult<()> {
		self.common_args
			.log_if_verbose(&format!("Completing {} operation", operation_name));

		match &self.common_args.output {
			Some(output_path) => {
				let file_format = self.map_output_format(&self.common_args.format);

				// `-o -` → stream to stdout in the chosen format (CSV default).
				if is_stdio(output_path) {
					write_data(df, output_path, file_format.as_ref()).await?;
					return Ok(());
				}

				let overwrites_input =
					paths_point_to_same_file(&self.common_args.input, output_path);

				if overwrites_input {
					let tmp_path = sibling_temp_path(output_path)?;
					self.common_args.log_if_verbose(&format!(
						"Output overwrites input; staging via temp file: {}",
						tmp_path.display()
					));
					let write_result = write_data(df, &tmp_path, file_format.as_ref()).await;
					if let Err(e) = write_result {
						let _ = std::fs::remove_file(&tmp_path);
						return Err(e);
					}
					if let Err(e) = std::fs::rename(&tmp_path, output_path) {
						let _ = std::fs::remove_file(&tmp_path);
						return Err(NailError::Io(e));
					}
				} else {
					write_data(df, output_path, file_format.as_ref()).await?;
				}
				self.common_args
					.log_if_verbose(&format!("Output written to: {}", output_path.display()));
			}
			None => {
				display_dataframe_with_mode(
					df,
					None,
					self.common_args.format.as_ref(),
					self.common_args.table,
					self.common_args.level,
				)
				.await?;
			}
		}

		Ok(())
	}

	fn map_output_format(&self, format: &Option<OutputFormat>) -> Option<FileFormat> {
		match format {
			Some(OutputFormat::Json) => Some(FileFormat::Json),
			Some(OutputFormat::Csv) => Some(FileFormat::Csv),
			Some(OutputFormat::Parquet) => Some(FileFormat::Parquet),
			Some(OutputFormat::Xlsx) => Some(FileFormat::Excel),
			Some(OutputFormat::Text) | None => None,
		}
	}
}

fn paths_point_to_same_file(input: &Path, output: &Path) -> bool {
	if input == output {
		return true;
	}
	match (std::fs::canonicalize(input), std::fs::canonicalize(output)) {
		(Ok(a), Ok(b)) => a == b,
		_ => normalize_lexical(input) == normalize_lexical(output),
	}
}

fn normalize_lexical(p: &Path) -> PathBuf {
	let abs = if p.is_absolute() {
		p.to_path_buf()
	} else {
		std::env::current_dir()
			.map(|cwd| cwd.join(p))
			.unwrap_or_else(|_| p.to_path_buf())
	};
	let mut out = PathBuf::new();
	for comp in abs.components() {
		use std::path::Component;
		match comp {
			Component::ParentDir => {
				out.pop();
			}
			Component::CurDir => {}
			other => out.push(other.as_os_str()),
		}
	}
	out
}

fn sibling_temp_path(target: &Path) -> NailResult<PathBuf> {
	let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
	let file_name = target
		.file_name()
		.ok_or_else(|| {
			NailError::InvalidArgument(format!("Invalid output path: {}", target.display()))
		})?
		.to_string_lossy()
		.into_owned();
	let nanos = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|d| d.as_nanos())
		.unwrap_or(0);
	let tmp_name = format!(".{}.nail-tmp-{}-{}", file_name, std::process::id(), nanos);
	Ok(match parent {
		Some(p) => p.join(tmp_name),
		None => PathBuf::from(tmp_name),
	})
}
