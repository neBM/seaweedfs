#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

apt_install ca-certificates docker.io
ensure_command docker
