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
cache_root="$(ci_cache_root)"
use_local_go_cache

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

if [ -z "${WEED_BINARY:-}" ] || [ -z "${RUST_VOLUME_BINARY:-}" ] || ! command -v go >/dev/null 2>&1; then
  if [ -z "${WEED_BINARY:-}" ] && [ -x "$root/weed/weed" ]; then
    export WEED_BINARY="$root/weed/weed"
  fi
  if [ -z "${RUST_VOLUME_BINARY:-}" ] && [ -x "$root/seaweed-volume/target/release/weed-volume" ]; then
    export RUST_VOLUME_BINARY="$root/seaweed-volume/target/release/weed-volume"
  fi
  bash "$(dirname "$0")/build-rust-volume-artifacts.sh"
  export WEED_BINARY="${WEED_BINARY:-$root/weed/weed}"
  export RUST_VOLUME_BINARY="${RUST_VOLUME_BINARY:-$root/seaweed-volume/target/release/weed-volume}"
fi

export PATH="$cache_root/go-toolchain/current/bin:$PATH"
export VOLUME_SERVER_IMPL=rust

cd "$root"
go test -v -count=1 -tags 5BytesOffset -timeout=30m "./test/volume_server/${test_type}/..." -run "$test_pattern" 2>&1 | tee "$log_dir/rust-volume-${test_type}-shard${shard}.log"
