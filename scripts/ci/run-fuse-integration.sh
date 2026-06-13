#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
log_dir="$(ci_log_dir)"
use_local_go_cache
ensure_command docker

cd "$root/test/fuse_integration"
PATH="$root/weed:$PATH" go test -v -count=1 -timeout=45m ./... 2>&1 | tee "$log_dir/fuse-integration.log"
copy_fuse_logs
