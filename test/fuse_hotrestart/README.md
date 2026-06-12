# FUSE hot-restart feasibility harness

This module isolates the lowest-risk question in the SeaweedFS mount
hot-restart program:

Can a second go-fuse worker attach to an already-mounted, already-initialized
`/dev/fuse` connection that stays open in a supervisor process?

It deliberately does **not** start `weed mount` yet. The first gate is lower in
the stack than SeaweedFS state handoff:

- supervisor process mounts a real FUSE connection and keeps the fd open
- worker subprocess 1 serves a simple loopback filesystem over that inherited fd
- worker subprocess 2 tries to attach to the same live fd after worker 1 exits

This harness is intentionally pinned to the released `github.com/seaweedfs/go-fuse/v2`
module version from `go.mod`. With that baseline, worker 1 succeeds and worker 2
still reproduces the historical limitation.

The current program state is now one layer above this:

- a local go-fuse fork can adopt a live initialized fd and keep serving reopened access
- stale open file handles still fail without explicit state handoff
- generic `nodefs/pathfs` reopened reads can still crash after adoption
- SeaweedFS `WFS` state continuity is now audited in `weed/mount/hotrestart_state_test.go`

So this module is now the released-baseline harness, not the final owner-layer proof.
Use it to confirm what unpatched go-fuse does, then continue the durable work in the
go-fuse fork and `weed/mount`.

Run it with:

```bash
cd test/fuse_hotrestart
go test -v
```
