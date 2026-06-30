#!/bin/sh
# nail installer — downloads the right prebuilt binary for your OS/arch from
# GitHub Releases and installs it to a bin directory on your PATH.
#
#   curl -fsSL https://raw.githubusercontent.com/Vitruves/nail-parquet/main/install.sh | sh
#
# Override the version with NAIL_VERSION=v1.8.0 and the target dir with BINDIR=...
set -eu

REPO="Vitruves/nail-parquet"

err() { printf 'error: %s\n' "$1" >&2; exit 1; }

# --- detect platform → Rust target triple -----------------------------------
os="$(uname -s)"
arch="$(uname -m)"
case "$arch" in
	arm64 | aarch64) arch="aarch64" ;;
	x86_64 | amd64)  arch="x86_64" ;;
	*) err "unsupported architecture: $arch" ;;
esac
case "$os" in
	Darwin) triple="${arch}-apple-darwin" ;;
	Linux)  triple="${arch}-unknown-linux-musl" ;;
	*) err "unsupported OS: $os (Windows: download the .zip from the releases page)" ;;
esac

# --- resolve version (latest unless NAIL_VERSION is set) ---------------------
version="${NAIL_VERSION:-}"
if [ -z "$version" ]; then
	version="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
		| grep '"tag_name"' | head -1 | cut -d'"' -f4)"
	[ -n "$version" ] || err "could not determine latest version"
fi

asset="nail-${version}-${triple}.tar.gz"
url="https://github.com/${REPO}/releases/download/${version}/${asset}"

# --- pick an install dir on PATH (no sudo if avoidable) ----------------------
if [ -n "${BINDIR:-}" ]; then
	bindir="$BINDIR"
elif [ -w /usr/local/bin ]; then
	bindir="/usr/local/bin"
else
	bindir="${HOME}/.local/bin"
fi
mkdir -p "$bindir"

# --- download, extract, install ---------------------------------------------
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
printf 'Downloading %s ...\n' "$asset"
curl -fsSL "$url" -o "$tmp/nail.tar.gz" || err "download failed: $url"
tar -xzf "$tmp/nail.tar.gz" -C "$tmp"
binpath="$(find "$tmp" -type f -name nail | head -1)"
[ -n "$binpath" ] || err "nail binary not found in archive"
install -m 0755 "$binpath" "$bindir/nail"

printf 'Installed nail %s to %s/nail\n' "$version" "$bindir"
case ":$PATH:" in
	*":$bindir:"*) ;;
	*)
		printf 'Note: %s is not on your PATH. Add it with:\n' "$bindir"
		printf '  export PATH="%s:$PATH"\n' "$bindir"
		printf '(put that line in your shell profile, e.g. ~/.zshrc or ~/.bashrc)\n'
		;;
esac
