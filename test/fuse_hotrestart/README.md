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

The current expectation is that worker 1 succeeds and worker 2 does not become
ready. If worker 2 ever does come up cleanly, the design assumptions for the
durable restart path need to be revisited before the higher-level SeaweedFS
state audit continues.

Run it with:

```bash
cd test/fuse_hotrestart
go test -v
```
