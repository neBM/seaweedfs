#!/usr/bin/env bash
#
# Integration test: initial git push of a realistic tree snapshot to a bare
# repository hosted on a SeaweedFS FUSE mount.
#
# Verifies that git receive-pack/index-pack can ingest an initial push whose
# tree shape looks like a real source checkout instead of a tiny synthetic repo.
# The current known failure signature is:
#   remote unpack failed: index-pack abnormal exit
#   tmp_objdir-incoming-.../pack/tmp_pack_...: No such file or directory
#
# Usage:
#   bash test-git-large-initial-push.sh /path/to/mount [/path/to/source-tree]
#
# Environment:
#   EXPECT_FAILURE=1  Treat the targeted tmp_objdir/index-pack failure as a
#                     successful reproduction. Useful while iterating on the
#                     underlying SeaweedFS mount bug.
#
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEFAULT_SOURCE_DIR=$(git -C "$SCRIPT_DIR/../.." rev-parse --show-toplevel 2>/dev/null || pwd)

MOUNT_DIR="${1:?Usage: $0 <mount-dir> [source-tree]}"
SOURCE_DIR="${2:-$DEFAULT_SOURCE_DIR}"
TEST_DIR="$MOUNT_DIR/git-large-push-test-$$"
WORK_DIR=$(mktemp -d)
SNAPSHOT_DIR="$WORK_DIR/snapshot"
WORKTREE_DIR="$WORK_DIR/worktree"
PUSH_LOG="$WORK_DIR/push.log"
PASS=0
FAIL=0

cleanup() {
    if [[ "${TEST_KEEP:-}" == "1" ]]; then
        echo "TEST_KEEP=1 - leaving artifacts:"
        echo "  mount:  $TEST_DIR"
        echo "  work:   $WORK_DIR"
        echo "  source: $SOURCE_DIR"
    else
        rm -rf "$TEST_DIR" 2>/dev/null || true
        rm -rf "$WORK_DIR" 2>/dev/null || true
    fi
}
trap cleanup EXIT

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

copy_source_tree() {
    mkdir -p "$SNAPSHOT_DIR"

    if git -C "$SOURCE_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git -C "$SOURCE_DIR" archive --format=tar HEAD | tar -xf - -C "$SNAPSHOT_DIR"
    else
        tar --exclude=.git -cf - -C "$SOURCE_DIR" . | tar -xf - -C "$SNAPSHOT_DIR"
    fi
}

targeted_failure() {
    grep -qF "remote unpack failed: index-pack abnormal exit" "$PUSH_LOG" &&
        grep -q "tmp_objdir-incoming-.*tmp_pack_" "$PUSH_LOG"
}

echo "========================================"
echo "  Git large initial push integration test"
echo "========================================"
echo "Mount:        $MOUNT_DIR"
echo "Source tree:  $SOURCE_DIR"
echo "Test dir:     $TEST_DIR"
echo "Work dir:     $WORK_DIR"
echo "Expect fail:  ${EXPECT_FAILURE:-0}"
echo ""

if [[ ! -d "$MOUNT_DIR" ]]; then
    echo "ERROR: $MOUNT_DIR is not a valid directory"
    exit 1
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
    echo "ERROR: $SOURCE_DIR is not a valid directory"
    exit 1
fi

mkdir -p "$TEST_DIR" "$WORKTREE_DIR"

echo "--- Phase 1: Materialize source snapshot ---"
copy_source_tree

SOURCE_FILE_COUNT=$(find "$SNAPSHOT_DIR" -type f | wc -l | tr -d ' ')
SOURCE_DIR_COUNT=$(find "$SNAPSHOT_DIR" -type d | wc -l | tr -d ' ')
SOURCE_SIZE=$(du -sh "$SNAPSHOT_DIR" | awk '{print $1}')

echo "Snapshot files: $SOURCE_FILE_COUNT"
echo "Snapshot dirs:  $SOURCE_DIR_COUNT"
echo "Snapshot size:  $SOURCE_SIZE"
pass "source snapshot created"

echo "--- Phase 2: Create working repo from snapshot ---"
cd "$WORKTREE_DIR"
git init >/dev/null 2>&1
git config user.email "test@seaweedfs.test"
git config user.name "SeaweedFS Test"
cp -a "$SNAPSHOT_DIR"/. "$WORKTREE_DIR"/
git add -A
git commit -m "snapshot commit" >/dev/null 2>&1
OBJECT_COUNT=$(git count-objects -v | awk -F': ' '/count:/ {print $2}')
pass "snapshot committed (objects=$OBJECT_COUNT)"

echo "--- Phase 3: Push snapshot to bare repo on mount ---"
BARE_REPO="$TEST_DIR/repo.git"
git init --bare "$BARE_REPO" >/dev/null 2>&1

if git push "$BARE_REPO" HEAD:refs/heads/master >"$PUSH_LOG" 2>&1; then
    if [[ "${EXPECT_FAILURE:-0}" == "1" ]]; then
        fail "push succeeded unexpectedly; targeted failure did not reproduce"
        cat "$PUSH_LOG"
    else
        pass "initial push to mount bare repo succeeded"
    fi
else
    if targeted_failure; then
        if [[ "${EXPECT_FAILURE:-0}" == "1" ]]; then
            pass "reproduced targeted tmp_objdir/index-pack failure"
        else
            fail "initial push hit the targeted tmp_objdir/index-pack failure"
            cat "$PUSH_LOG"
        fi
    else
        fail "initial push failed with an unexpected error"
        cat "$PUSH_LOG"
    fi
fi

echo ""
echo "========================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "========================================"

if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
