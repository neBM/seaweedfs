#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <grpc|http> <1|2|3>" >&2
  exit 1
fi

test_type="$1"
shard="$2"
root="$(seaweed_repo_root)"
log_dir="$(ci_log_dir)"

case "$test_type:$shard" in
  grpc:1) test_pattern='^Test[A-H]' ;;
  grpc:2) test_pattern='^Test[I-S]' ;;
  grpc:3) test_pattern='^Test[T-Z]' ;;
  http:1) test_pattern='^Test[A-G]' ;;
  http:2) test_pattern='^Test[H-R]' ;;
  http:3) test_pattern='^Test[S-Z]' ;;
  *)
    echo "unsupported shard selection: $test_type / $shard" >&2
    exit 1
    ;;
esac

"$(dirname "$0")/build-rust-volume-artifacts.sh"

export PATH="$root/.cache/go-toolchain/current/bin:$PATH"
export WEED_BINARY="$root/weed/weed"
export RUST_VOLUME_BINARY="$root/seaweed-volume/target/release/weed-volume"
export VOLUME_SERVER_IMPL=rust

cd "$root"
go test -v -count=1 -tags 5BytesOffset -timeout=30m "./test/volume_server/${test_type}/..." -run "$test_pattern" 2>&1 | tee "$log_dir/rust-volume-${test_type}-shard${shard}.log"
