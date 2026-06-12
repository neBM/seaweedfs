package mount

// HotRestartStateSnapshot captures the worker-local state that would have to be
// handed off, drained, or explicitly accepted as lost before replacing a live
// mount worker in place.
type HotRestartStateSnapshot struct {
	OpenFileHandles      int
	OpenDirectoryHandles int
	PendingAsyncFlushes  int
}

// Quiescent reports whether the mount has any worker-local runtime state that
// still needs an explicit handoff or drain before a worker swap.
func (s HotRestartStateSnapshot) Quiescent() bool {
	return s.OpenFileHandles == 0 && s.OpenDirectoryHandles == 0 && s.PendingAsyncFlushes == 0
}

func (wfs *WFS) HotRestartStateSnapshot() HotRestartStateSnapshot {
	snapshot := HotRestartStateSnapshot{
		OpenFileHandles:      wfs.fhMap.Count(),
		OpenDirectoryHandles: wfs.dhMap.Count(),
	}

	wfs.pendingAsyncFlushMu.Lock()
	snapshot.PendingAsyncFlushes = len(wfs.pendingAsyncFlush)
	wfs.pendingAsyncFlushMu.Unlock()

	return snapshot
}
