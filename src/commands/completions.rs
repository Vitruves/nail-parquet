use clap::{Args, CommandFactory};
use clap_complete::{generate, Shell};
use std::fs;
use std::io;
use std::path::PathBuf;

use crate::error::{NailError, NailResult};

#[derive(Args, Clone)]
#[command(after_help = "Examples:
  nail completions bash > ~/.local/share/bash-completion/completions/nail
  nail completions fish > ~/.config/fish/completions/nail.fish")]
pub struct CompletionsArgs {
	#[arg(value_enum, help = "Target shell")]
	pub shell: Shell,

	#[arg(
		long,
		help = "Install the completion script into the standard user location for the target shell.\n\
		Standard locations:\n\
		• bash:       ~/.local/share/bash-completion/completions/nail\n\
		• zsh:        ~/.zfunc/_nail (ensure 'fpath+=~/.zfunc' and 'autoload -Uz compinit && compinit' in ~/.zshrc)\n\
		• fish:       ~/.config/fish/completions/nail.fish\n\
		• elvish:     ~/.config/elvish/lib/nail-completions.elv\n\
		• powershell: appended to your $PROFILE"
	)]
	pub auto_install: bool,

	#[arg(
		long,
		value_name = "PATH",
		help = "Write the completion script to a custom file path instead of stdout"
	)]
	pub location: Option<PathBuf>,
}

pub async fn execute(args: CompletionsArgs) -> NailResult<()> {
	let mut cmd = crate::cli::Cli::command();
	let name = cmd.get_name().to_string();

	if args.auto_install && args.location.is_some() {
		return Err(NailError::InvalidArgument(
			"--auto-install and --location are mutually exclusive".to_string(),
		));
	}

	if args.auto_install {
		let target = standard_location(args.shell)?;
		if let Some(parent) = target.parent() {
			fs::create_dir_all(parent).map_err(NailError::Io)?;
		}
		let mut file = fs::OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(!matches!(args.shell, Shell::PowerShell))
			.append(matches!(args.shell, Shell::PowerShell))
			.open(&target)
			.map_err(NailError::Io)?;
		generate(args.shell, &mut cmd, name, &mut file);
		println!(
			"Installed {} completions to: {}",
			args.shell,
			target.display()
		);
		print_shell_activation_hint(args.shell, &target);
	} else if let Some(path) = &args.location {
		if let Some(parent) = path.parent() {
			if !parent.as_os_str().is_empty() {
				fs::create_dir_all(parent).map_err(NailError::Io)?;
			}
		}
		let mut file = fs::File::create(path).map_err(NailError::Io)?;
		generate(args.shell, &mut cmd, name, &mut file);
		println!("Wrote {} completions to: {}", args.shell, path.display());
	} else {
		generate(args.shell, &mut cmd, name, &mut io::stdout());
	}

	Ok(())
}

fn standard_location(shell: Shell) -> NailResult<PathBuf> {
	let home = dirs_home()?;
	let path = match shell {
		Shell::Bash => home.join(".local/share/bash-completion/completions/nail"),
		Shell::Zsh => home.join(".zfunc/_nail"),
		Shell::Fish => home.join(".config/fish/completions/nail.fish"),
		Shell::Elvish => home.join(".config/elvish/lib/nail-completions.elv"),
		Shell::PowerShell => powershell_profile(&home),
		_ => {
			return Err(NailError::InvalidArgument(format!(
				"--auto-install not supported for shell: {}",
				shell
			)));
		}
	};
	Ok(path)
}

fn dirs_home() -> NailResult<PathBuf> {
	std::env::var_os("HOME")
		.map(PathBuf::from)
		.or_else(|| std::env::var_os("USERPROFILE").map(PathBuf::from))
		.ok_or_else(|| NailError::InvalidArgument("Could not determine home directory".to_string()))
}

fn powershell_profile(home: &std::path::Path) -> PathBuf {
	if cfg!(windows) {
		home.join("Documents/PowerShell/Microsoft.PowerShell_profile.ps1")
	} else {
		home.join(".config/powershell/Microsoft.PowerShell_profile.ps1")
	}
}

fn print_shell_activation_hint(shell: Shell, path: &std::path::Path) {
	match shell {
		Shell::Bash => {
			println!("Restart your shell or run: source {}", path.display());
		}
		Shell::Zsh => {
			println!("Ensure ~/.zshrc contains:");
			println!("  fpath+=~/.zfunc");
			println!("  autoload -Uz compinit && compinit");
		}
		Shell::Fish => {
			println!("Completions will be loaded automatically in new fish sessions.");
		}
		Shell::Elvish => {
			println!("Add to ~/.config/elvish/rc.elv:");
			println!("  use nail-completions");
		}
		Shell::PowerShell => {
			println!(
				"Completions appended to your PowerShell $PROFILE. Restart PowerShell to activate."
			);
		}
		_ => {}
	}
}
