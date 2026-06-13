#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
log_dir="$(ci_log_dir)"

bash "$(dirname "$0")/build-rust-volume-artifacts.sh"

export PATH="$root/.cache/go-toolchain/current/bin:$PATH"
export WEED_BINARY="$root/weed/weed"
export RUST_VOLUME_BINARY="$root/seaweed-volume/target/release/weed-volume"

cd "$root"
go test -v -count=1 -timeout=15m ./test/volume_server/rust/... 2>&1 | tee "$log_dir/rust-volume-integration.log"
