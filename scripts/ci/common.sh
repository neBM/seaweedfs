#!/usr/bin/env bash

set -euo pipefail

SEAWEED_CI_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

seaweed_repo_root() {
  cd "$SEAWEED_CI_DIR/../.." && pwd -P
}

ci_log_dir() {
  local root
  root="$(seaweed_repo_root)"
  mkdir -p "$root/.artifacts/ci-logs"
  printf '%s\n' "$root/.artifacts/ci-logs"
}

use_local_go_cache() {
  local root
  root="$(seaweed_repo_root)"
  export GOMODCACHE="${GOMODCACHE:-$root/.cache/go-mod}"
  export GOCACHE="${GOCACHE:-$root/.cache/go-build}"
  mkdir -p "$GOMODCACHE" "$GOCACHE"
}

ensure_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

apt_install() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends "$@"
}

copy_fuse_logs() {
  local log_dir
  log_dir="$(ci_log_dir)"
  if [ -d /tmp/seaweedfs-fuse-logs ]; then
    rm -rf "$log_dir/seaweedfs-fuse-logs"
    cp -R /tmp/seaweedfs-fuse-logs "$log_dir/seaweedfs-fuse-logs"
  fi
}
