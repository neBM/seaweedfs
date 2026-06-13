#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
log_dir="$(ci_log_dir)"
use_local_go_cache

test -c /dev/fuse
command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1

cd "$root/test/fuse_integration"
PATH="$root/weed:$PATH" go test -v -count=1 -timeout=10m -run '^TestMinimal$' ./... 2>&1 | tee "$log_dir/fuse-runner-smoke.log"
copy_fuse_logs
