#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
log_dir="$(ci_log_dir)"

cd "$root/seaweed-volume"
cargo build --release
cargo test 2>&1 | tee "$log_dir/rust-volume-unit.log"
