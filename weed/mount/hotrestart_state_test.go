package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestHotRestartStateStableInodeCanBeReconstructed(t *testing.T) {
	const inode = uint64(0xfeedbeef)

	wfs1 := newHotRestartStateTestWFS()
	wfs2 := newHotRestartStateTestWFS()

	fullPath := util.FullPath("/mail/INBOX/cur/0001")
	got1 := wfs1.inodeToPath.Lookup(fullPath, 1710000000, false, false, inode, true)
	got2 := wfs2.inodeToPath.Lookup(fullPath, 1710000000, false, false, inode, true)

	if got1 != inode {
		t.Fatalf("worker 1 inode = %d, want %d", got1, inode)
	}
	if got2 != inode {
		t.Fatalf("worker 2 inode = %d, want %d", got2, inode)
	}
}

func TestHotRestartStateFileHandlesAreWorkerLocal(t *testing.T) {
	wfs1 := newHotRestartStateTestWFS()
	wfs2 := newHotRestartStateTestWFS()

	entry := newHotRestartStateEntry(101)
	fh1 := wfs1.fhMap.AcquireFileHandle(wfs1, 101, entry)

	if got := wfs1.GetHandle(fh1.fh); got != fh1 {
		t.Fatalf("worker 1 handle lookup = %p, want %p", got, fh1)
	}
	if got := wfs2.GetHandle(fh1.fh); got != nil {
		t.Fatalf("worker 2 unexpectedly resolved worker 1 handle %d", fh1.fh)
	}

	fh2 := wfs2.fhMap.AcquireFileHandle(wfs2, 101, entry)
	if got := wfs2.GetHandle(fh2.fh); got != fh2 {
		t.Fatalf("worker 2 handle lookup = %p, want %p", got, fh2)
	}
}

func TestHotRestartStateReopenedHandleCanReadContent(t *testing.T) {
	const inode = uint64(101)
	const contents = "hot-restart-wfs"

	wfs1 := newHotRestartStateTestWFS()
	wfs2 := newHotRestartStateTestWFS()

	fullPath := util.FullPath("/mail/INBOX/cur/0001")
	wfs1.inodeToPath.Lookup(fullPath, 1710000000, false, false, inode, true)
	wfs2.inodeToPath.Lookup(fullPath, 1710000000, false, false, inode, true)

	entry := newHotRestartStateEntryWithContent(inode, contents)
	fh1 := wfs1.fhMap.AcquireFileHandle(wfs1, inode, entry)
	if got, status := readHotRestartStateHandle(wfs1, fh1.fh, len(contents)+1); status != fuse.OK {
		t.Fatalf("worker 1 read status = %v, want %v", status, fuse.OK)
	} else if got != contents {
		t.Fatalf("worker 1 read = %q, want %q", got, contents)
	}

	fh2 := wfs2.fhMap.AcquireFileHandle(wfs2, inode, entry)
	if got, status := readHotRestartStateHandle(wfs2, fh2.fh, len(contents)+1); status != fuse.OK {
		t.Fatalf("worker 2 reopened read status = %v, want %v", status, fuse.OK)
	} else if got != contents {
		t.Fatalf("worker 2 reopened read = %q, want %q", got, contents)
	}
}

func TestHotRestartStateHeldHandleFailsOnReplacementWorker(t *testing.T) {
	const inode = uint64(101)

	wfs1 := newHotRestartStateTestWFS()
	wfs2 := newHotRestartStateTestWFS()

	fullPath := util.FullPath("/mail/INBOX/cur/0001")
	wfs1.inodeToPath.Lookup(fullPath, 1710000000, false, false, inode, true)
	wfs2.inodeToPath.Lookup(fullPath, 1710000000, false, false, inode, true)

	entry := newHotRestartStateEntryWithContent(inode, "hot-restart-wfs")
	fh1 := wfs1.fhMap.AcquireFileHandle(wfs1, inode, entry)

	if _, status := readHotRestartStateHandle(wfs2, fh1.fh, len(entry.Content)+1); status != fuse.ENOENT {
		t.Fatalf("replacement worker read via foreign handle status = %v, want %v", status, fuse.ENOENT)
	}
}

func TestHotRestartStateDirectoryHandlesResetAcrossWorkers(t *testing.T) {
	wfs1 := newHotRestartStateTestWFS()
	wfs2 := newHotRestartStateTestWFS()

	dhid, dh1 := wfs1.AcquireDirectoryHandle()
	dh1.isFinished = true
	dh1.snapshotTsNs = 12345
	dh1.entryStreamOffset = directoryStreamBaseOffset + 7

	dh2 := wfs2.GetDirectoryHandle(dhid)
	if dh2 == dh1 {
		t.Fatal("directory handle unexpectedly shared across workers")
	}
	if dh2.isFinished {
		t.Fatal("replacement worker preserved isFinished on a foreign directory handle")
	}
	if dh2.snapshotTsNs != 0 {
		t.Fatalf("replacement worker snapshotTsNs = %d, want 0", dh2.snapshotTsNs)
	}
	if dh2.entryStreamOffset != directoryStreamBaseOffset {
		t.Fatalf("replacement worker entryStreamOffset = %d, want %d", dh2.entryStreamOffset, directoryStreamBaseOffset)
	}
}

func TestHotRestartStatePendingAsyncFlushIsWorkerLocal(t *testing.T) {
	wfs1 := newHotRestartStateTestWFS()
	wfs2 := newHotRestartStateTestWFS()

	const inode = uint64(202)
	done := make(chan struct{})
	wfs1.pendingAsyncFlush[inode] = done

	if _, found := wfs1.pendingAsyncFlush[inode]; !found {
		t.Fatal("origin worker lost pending async flush marker")
	}
	if _, found := wfs2.pendingAsyncFlush[inode]; found {
		t.Fatal("replacement worker unexpectedly inherited pending async flush marker")
	}
}

func TestHotRestartStateSnapshotIsQuiescentWithoutWorkerLocalState(t *testing.T) {
	wfs := newHotRestartStateTestWFS()

	snapshot := wfs.HotRestartStateSnapshot()
	if !snapshot.Quiescent() {
		t.Fatalf("snapshot = %+v, want quiescent", snapshot)
	}
}

func TestHotRestartStateSnapshotCountsQuiesceBlockers(t *testing.T) {
	wfs := newHotRestartStateTestWFS()

	_ = wfs.fhMap.AcquireFileHandle(wfs, 101, newHotRestartStateEntry(101))
	_, _ = wfs.AcquireDirectoryHandle()
	wfs.pendingAsyncFlush[101] = make(chan struct{})

	snapshot := wfs.HotRestartStateSnapshot()
	if snapshot.OpenFileHandles != 1 {
		t.Fatalf("OpenFileHandles = %d, want 1", snapshot.OpenFileHandles)
	}
	if snapshot.OpenDirectoryHandles != 1 {
		t.Fatalf("OpenDirectoryHandles = %d, want 1", snapshot.OpenDirectoryHandles)
	}
	if snapshot.PendingAsyncFlushes != 1 {
		t.Fatalf("PendingAsyncFlushes = %d, want 1", snapshot.PendingAsyncFlushes)
	}
	if snapshot.Quiescent() {
		t.Fatalf("snapshot = %+v, want non-quiescent", snapshot)
	}
}

func newHotRestartStateTestWFS() *WFS {
	return &WFS{
		option: &Option{
			ChunkSizeLimit:     1024,
			ConcurrentReaders:  1,
			VolumeServerAccess: "filerProxy",
			FilerAddresses:     []pb.ServerAddress{"127.0.0.1:8888"},
		},
		inodeToPath:       NewInodeToPath(util.FullPath("/"), 0),
		fhMap:             NewFileHandleToInode(),
		dhMap:             NewDirectoryHandleToInode(),
		fhLockTable:       util.NewLockTable[FileHandleId](),
		pendingAsyncFlush: make(map[uint64]chan struct{}),
	}
}

func newHotRestartStateEntry(inode uint64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name: "message.eml",
		Attributes: &filer_pb.FuseAttributes{
			Inode:    inode,
			FileMode: 0o644,
		},
	}
}

func newHotRestartStateEntryWithContent(inode uint64, contents string) *filer_pb.Entry {
	entry := newHotRestartStateEntry(inode)
	entry.Attributes.FileSize = uint64(len(contents))
	entry.Content = []byte(contents)
	return entry
}

func readHotRestartStateHandle(wfs *WFS, fh FileHandleId, size int) (string, fuse.Status) {
	cancel := make(chan struct{})
	result, status := wfs.Read(cancel, &fuse.ReadIn{Fh: uint64(fh), Offset: 0}, make([]byte, size))
	if status != fuse.OK {
		return "", status
	}
	defer result.Done()
	data, status := result.Bytes(nil)
	if status != fuse.OK {
		return "", status
	}
	return string(data), fuse.OK
}
