#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
version="$(awk '/^go / { print $2; exit }' "$root/go.mod")"
install_root="$root/.cache/go-toolchain"
target_dir="$install_root/go${version}"
current_link="$install_root/current"

case "$(dpkg --print-architecture)" in
  amd64) go_arch=amd64 ;;
  arm64) go_arch=arm64 ;;
  *)
    echo "unsupported architecture for Go toolchain install" >&2
    exit 1
    ;;
esac

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

ensure_command curl
mkdir -p "$install_root"

if [ ! -x "$target_dir/bin/go" ]; then
  curl -fsSL "https://go.dev/dl/go${version}.linux-${go_arch}.tar.gz" -o "$tmpdir/go.tgz"
  rm -rf "$target_dir"
  tar -C "$tmpdir" -xzf "$tmpdir/go.tgz"
  mv "$tmpdir/go" "$target_dir"
fi

ln -sfn "$target_dir" "$current_link"
"$current_link/bin/go" version
