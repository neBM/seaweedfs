package mount

import "sync"

// HotRestartStatusSnapshot adds the current gate state to the worker-local
// quiescence snapshot so callers can distinguish "not quiescent yet" from
// "already frozen for takeover".
type HotRestartStatusSnapshot struct {
	HotRestartStateSnapshot
	BlockingNewHandles bool
}

func (wfs *WFS) HotRestartStatusSnapshot() HotRestartStatusSnapshot {
	status := HotRestartStatusSnapshot{
		HotRestartStateSnapshot: wfs.HotRestartStateSnapshot(),
	}

	wfs.hotRestartMu.RLock()
	status.BlockingNewHandles = wfs.hotRestartBlocked
	wfs.hotRestartMu.RUnlock()

	return status
}

// PrepareHotRestart blocks new file and directory handle creation only if the
// worker is already quiescent. If any worker-local state is still live, the
// gate is left open and the caller gets the current blockers instead.
func (wfs *WFS) PrepareHotRestartGate() HotRestartStatusSnapshot {
	wfs.hotRestartMu.Lock()
	defer wfs.hotRestartMu.Unlock()

	status := HotRestartStatusSnapshot{
		HotRestartStateSnapshot: wfs.HotRestartStateSnapshot(),
	}
	if status.Quiescent() {
		wfs.hotRestartBlocked = true
		status.BlockingNewHandles = true
		return status
	}

	wfs.hotRestartBlocked = false
	return status
}

func (wfs *WFS) CancelHotRestartGate() {
	wfs.hotRestartMu.Lock()
	wfs.hotRestartBlocked = false
	wfs.hotRestartMu.Unlock()
}

func (wfs *WFS) withHotRestartOpenGate(fn func() sync.Locker) (sync.Locker, bool) {
	locker := fn()
	locker.Lock()
	if wfs.hotRestartBlocked {
		locker.Unlock()
		return nil, false
	}
	return locker, true
}

func (wfs *WFS) beginHotRestartOpenGate() (sync.Locker, bool) {
	return wfs.withHotRestartOpenGate(func() sync.Locker {
		return wfs.hotRestartMu.RLocker()
	})
}
