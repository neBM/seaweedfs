#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
log_dir="$(ci_log_dir)"
use_local_go_cache
ensure_command docker

docker version >/dev/null

cd "$root"
GOFLAGS=-buildvcs=false go test -count=1 ./weed/mount 2>&1 | tee "$log_dir/mount-unit.log"
