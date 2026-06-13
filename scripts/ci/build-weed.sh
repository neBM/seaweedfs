#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

root="$(seaweed_repo_root)"
use_local_go_cache

cd "$root/weed"
go build -tags "elastic gocdk sqlite ydb tarantool tikv rclone" -o weed .
./weed version
