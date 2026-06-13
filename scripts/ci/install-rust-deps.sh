#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

apt_install build-essential ca-certificates curl pkg-config protobuf-compiler
