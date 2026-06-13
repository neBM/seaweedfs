#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/common.sh"

apt_install ca-certificates fuse3 libfuse3-dev

if ! grep -qxF 'user_allow_other' /etc/fuse.conf 2>/dev/null; then
  printf 'user_allow_other\n' >> /etc/fuse.conf
fi

chmod 644 /etc/fuse.conf || true

test -c /dev/fuse
command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1
