#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
use_local_go_cache

export PATH="$root/.cache/go-toolchain/current/bin:$PATH"

cd "$root/weed"
go build -tags 5BytesOffset -o weed .
./weed version

cd "$root/seaweed-volume"
cargo build --release
