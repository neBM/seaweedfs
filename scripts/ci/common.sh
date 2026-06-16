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

ci_cache_root() {
  local root cache_root
  root="$(seaweed_repo_root)"
  cache_root="${SEAWEED_CI_CACHE_ROOT:-$root/.cache}"

  if mkdir -p "$cache_root" 2>/dev/null; then
    printf '%s\n' "$cache_root"
  else
    cache_root="$root/.cache"
    mkdir -p "$cache_root"
    printf '%s\n' "$cache_root"
  fi
}

use_local_go_cache() {
  local root cache_root go_version
  root="$(seaweed_repo_root)"
  cache_root="$(ci_cache_root)"

  if command -v go >/dev/null 2>&1; then
    go_version="$(go env GOVERSION 2>/dev/null || true)"
    if [ -z "$go_version" ]; then
      go_version="$(go version | awk '{ print $3 }')"
    fi
  else
    go_version="go$(awk '/^go / { print $2; exit }' "$root/go.mod")"
  fi
  go_version="${go_version//[^[:alnum:]._-]/_}"

  export GOMODCACHE="${GOMODCACHE:-$cache_root/go-mod}"
  export GOCACHE="${GOCACHE:-$cache_root/go-build-${go_version}}"
  mkdir -p "$GOMODCACHE" "$GOCACHE"
}

# Keep Cargo's registry/git cache on the shared cache root. Jobs that bootstrap
# rustup set RUSTUP_HOME explicitly so they don't interfere with the prebuilt
# rust image's toolchain layout.
use_local_rust_cache() {
  local cache_root
  cache_root="$(ci_cache_root)"

  export CARGO_HOME="${CARGO_HOME:-$cache_root/cargo}"
  mkdir -p "$CARGO_HOME"
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
