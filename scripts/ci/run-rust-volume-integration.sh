#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
log_dir="$(ci_log_dir)"
cache_root="$(ci_cache_root)"
use_local_go_cache

if [ -z "${WEED_BINARY:-}" ] && [ -x "$root/weed/weed" ]; then
  export WEED_BINARY="$root/weed/weed"
fi
if [ -z "${RUST_VOLUME_BINARY:-}" ] && [ -x "$root/seaweed-volume/target/release/weed-volume" ]; then
  export RUST_VOLUME_BINARY="$root/seaweed-volume/target/release/weed-volume"
fi

if [ -z "${WEED_BINARY:-}" ] || [ -z "${RUST_VOLUME_BINARY:-}" ] || ! command -v go >/dev/null 2>&1; then
  bash "$(dirname "$0")/build-rust-volume-artifacts.sh"
  export WEED_BINARY="${WEED_BINARY:-$root/weed/weed}"
  export RUST_VOLUME_BINARY="${RUST_VOLUME_BINARY:-$root/seaweed-volume/target/release/weed-volume}"
fi

export PATH="$cache_root/go-toolchain/current/bin:$PATH"

cd "$root"
go test -v -count=1 -timeout=15m ./test/volume_server/rust/... 2>&1 | tee "$log_dir/rust-volume-integration.log"
