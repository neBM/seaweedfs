#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
use_local_go_cache
use_local_rust_cache

cache_root="$(ci_cache_root)"

if ! command -v go >/dev/null 2>&1; then
  bash "$(dirname "$0")/install-go-toolchain.sh"
  export PATH="$cache_root/go-toolchain/current/bin:$PATH"
fi

if ! command -v cargo >/dev/null 2>&1; then
  export RUSTUP_HOME="${RUSTUP_HOME:-$cache_root/rustup}"
  mkdir -p "$RUSTUP_HOME"
  ensure_command curl
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --no-modify-path --default-toolchain 1.91.1 --profile minimal
  export PATH="$CARGO_HOME/bin:$PATH"
fi

cd "$root/weed"
go build -tags 5BytesOffset -o weed .
./weed version

cd "$root/seaweed-volume"
cargo build --release
