package mount

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type createEntryTestServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu            sync.Mutex
	lastDirectory string
	lastName      string
	lastUID       uint32
	lastGID       uint32
	lastMode      uint32
	lastExpected  *int64
	entries       map[string]*filer_pb.Entry
}

type createEntrySnapshot struct {
	directory string
	name      string
	uid       uint32
	gid       uint32
	mode      uint32
	expected  *int64
}

func (s *createEntryTestServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastDirectory = req.GetDirectory()
	if req.GetEntry() != nil {
		s.lastName = req.GetEntry().GetName()
		if req.GetEntry().GetAttributes() != nil {
			s.lastUID = req.GetEntry().GetAttributes().GetUid()
			s.lastGID = req.GetEntry().GetAttributes().GetGid()
			s.lastMode = req.GetEntry().GetAttributes().GetFileMode()
		}
		if s.entries == nil {
			s.entries = make(map[string]*filer_pb.Entry)
		}
		s.entries[req.GetDirectory()+"/"+req.GetEntry().GetName()] = proto.Clone(req.GetEntry()).(*filer_pb.Entry)
	}
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *createEntryTestServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastExpected = req.ExpectedEntryRevision
	if req.GetEntry() != nil {
		key := req.GetDirectory() + "/" + req.GetEntry().GetName()
		if existing := s.entries[key]; existing != nil && req.ExpectedEntryRevision != nil {
			if existing.GetEntryRevision() != req.GetExpectedEntryRevision() {
				return nil, status.Error(codes.FailedPrecondition, "entry revision changed")
			}
		}
		if s.entries == nil {
			s.entries = make(map[string]*filer_pb.Entry)
		}
		s.entries[key] = proto.Clone(req.GetEntry()).(*filer_pb.Entry)
	}
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (s *createEntryTestServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, found := s.entries[req.GetDirectory()+"/"+req.GetName()]; found {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

func (s *createEntryTestServer) snapshot() createEntrySnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return createEntrySnapshot{
		directory: s.lastDirectory,
		name:      s.lastName,
		uid:       s.lastUID,
		gid:       s.lastGID,
		mode:      s.lastMode,
		expected:  s.lastExpected,
	}
}

func newCreateTestWFS(t *testing.T) (*WFS, *createEntryTestServer) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	server := pb.NewGrpcServer()
	testServer := &createEntryTestServer{}
	filer_pb.RegisterSeaweedFilerServer(server, testServer)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}

	root := util.FullPath("/")
	option := &Option{
		ChunkSizeLimit:     1024,
		ConcurrentReaders:  1,
		VolumeServerAccess: "filerProxy",
		FilerAddresses: []pb.ServerAddress{
			pb.NewServerAddressWithGrpcPort("127.0.0.1:1", listener.Addr().(*net.TCPAddr).Port),
		},
		GrpcDialOption:         grpc.WithTransportCredentials(insecure.NewCredentials()),
		FilerMountRootPath:     "/",
		MountUid:               99,
		MountGid:               100,
		MountMode:              0o777,
		MountMtime:             time.Now(),
		MountCtime:             time.Now(),
		UidGidMapper:           uidGidMapper,
		uniqueCacheDirForWrite: t.TempDir(),
	}

	wfs := &WFS{
		option:      option,
		signature:   1,
		inodeToPath: NewInodeToPath(root, 0),
		fhMap:       NewFileHandleToInode(),
		fhLockTable: util.NewLockTable[FileHandleId](),
	}
	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		root,
		false,
		func(path util.FullPath) {
			wfs.inodeToPath.MarkChildrenCached(path)
		},
		func(path util.FullPath) bool {
			return wfs.inodeToPath.IsChildrenCached(path)
		},
		func(util.FullPath, *filer_pb.Entry) {},
		nil,
	)
	wfs.inodeToPath.MarkChildrenCached(root)
	t.Cleanup(func() {
		wfs.metaCache.Shutdown()
	})

	return wfs, testServer
}

func TestCreateCreatesAndOpensFile(t *testing.T) {
	wfs, testServer := newCreateTestWFS(t)

	out := &fuse.CreateOut{}
	status := wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: 1,
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 123,
					Gid: 456,
				},
			},
		},
		Flags: syscall.O_WRONLY | syscall.O_CREAT,
		Mode:  0o640,
	}, "hello.txt", out)
	if status != fuse.OK {
		t.Fatalf("Create status = %v, want OK", status)
	}
	if out.NodeId == 0 {
		t.Fatal("Create returned zero inode")
	}
	if out.Fh == 0 {
		t.Fatal("Create returned zero file handle")
	}
	if out.OpenFlags != 0 {
		t.Fatalf("Create returned OpenFlags = %#x, want 0", out.OpenFlags)
	}

	fileHandle := wfs.GetHandle(FileHandleId(out.Fh))
	if fileHandle == nil {
		t.Fatal("Create did not register an open file handle")
	}
	if got := fileHandle.FullPath(); got != "/hello.txt" {
		t.Fatalf("FullPath = %q, want %q", got, "/hello.txt")
	}

	// File creation is deferred to flush time. Trigger a synchronous flush
	// so the CreateEntry gRPC call is sent to the test server.
	if flushStatus := wfs.Flush(make(chan struct{}), &fuse.FlushIn{
		InHeader: fuse.InHeader{
			NodeId: out.NodeId,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}},
		},
		Fh: out.Fh,
	}); flushStatus != fuse.OK {
		t.Fatalf("Flush status = %v, want OK", flushStatus)
	}

	snapshot := testServer.snapshot()
	if snapshot.directory != "/" {
		t.Fatalf("CreateEntry directory = %q, want %q", snapshot.directory, "/")
	}
	if snapshot.name != "hello.txt" {
		t.Fatalf("CreateEntry name = %q, want %q", snapshot.name, "hello.txt")
	}
	if snapshot.uid != 123 || snapshot.gid != 456 {
		t.Fatalf("CreateEntry uid/gid = %d/%d, want 123/456", snapshot.uid, snapshot.gid)
	}
	if snapshot.mode != 0o640 {
		t.Fatalf("CreateEntry mode = %o, want %o", snapshot.mode, 0o640)
	}
}

func TestDeferredCreateRemainsVisibleAfterDirectorySwitchesToReadThrough(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	mkdirOut := &fuse.EntryOut{}
	if status := wfs.Mkdir(make(chan struct{}), &fuse.MkdirIn{
		InHeader: fuse.InHeader{
			NodeId: 1,
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 123,
					Gid: 456,
				},
			},
		},
		Mode: 0o755,
	}, "pack", mkdirOut); status != fuse.OK {
		t.Fatalf("Mkdir status = %v, want OK", status)
	}

	createOut := &fuse.CreateOut{}
	if status := wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: mkdirOut.NodeId,
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 123,
					Gid: 456,
				},
			},
		},
		Flags: syscall.O_WRONLY | syscall.O_CREAT,
		Mode:  0o640,
	}, "tmp_pack", createOut); status != fuse.OK {
		t.Fatalf("Create status = %v, want OK", status)
	}

	fileHandle := wfs.GetHandle(FileHandleId(createOut.Fh))
	if fileHandle == nil {
		t.Fatal("Create did not register an open file handle")
	}
	if !fileHandle.dirtyMetadata {
		t.Fatal("Create should leave the deferred entry dirty before flush")
	}

	fullPath := util.FullPath("/pack/tmp_pack")
	if entry, status := wfs.maybeLoadEntry(fullPath); status != fuse.OK {
		t.Fatalf("maybeLoadEntry before hot invalidation status = %v, want OK", status)
	} else if entry.GetName() != "tmp_pack" {
		t.Fatalf("maybeLoadEntry before hot invalidation name = %q, want tmp_pack", entry.GetName())
	}

	wfs.markDirectoryReadThrough(util.FullPath("/pack"))

	entry, status := wfs.maybeLoadEntry(fullPath)
	if status != fuse.OK {
		t.Fatalf("maybeLoadEntry after hot invalidation status = %v, want OK", status)
	}
	if entry.GetName() != "tmp_pack" {
		t.Fatalf("maybeLoadEntry after hot invalidation name = %q, want tmp_pack", entry.GetName())
	}
}

func TestCachedEntryRoundTripDropsUnknownRevision(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	fullPath := util.FullPath("/cached.txt")
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath: fullPath,
		Attr: filer.Attr{
			Mode:   0o644,
			Uid:    1000,
			Gid:    1000,
			Crtime: time.Unix(123, 0),
			Mtime:  time.Unix(123, 0),
			Ctime:  time.Unix(123, 0),
		},
		Revision: 7,
	}); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}

	entry, status := wfs.maybeLoadEntry(fullPath)
	if status != fuse.OK {
		t.Fatalf("maybeLoadEntry status = %v, want OK", status)
	}
	if entry.EntryRevision != nil {
		t.Fatalf("cached entry revision = %v, want nil after cache round-trip", entry.GetEntryRevision())
	}
}

func TestSetAttrFromCachedEntryDoesNotSendZeroRevision(t *testing.T) {
	wfs, testServer := newCreateTestWFS(t)

	fullPath := util.FullPath("/cached.txt")
	inode := wfs.inodeToPath.Lookup(fullPath, 123, false, false, 0, true)
	remoteEntry := &filer_pb.Entry{
		Name: "cached.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			Uid:      0,
			Gid:      0,
			Inode:    inode,
			Crtime:   123,
			Mtime:    123,
			Ctime:    123,
		},
	}
	revision := int64(7)
	remoteEntry.EntryRevision = &revision
	testServer.entries = map[string]*filer_pb.Entry{
		"/cached.txt": proto.Clone(remoteEntry).(*filer_pb.Entry),
	}

	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", remoteEntry)); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}

	status := wfs.SetAttr(make(chan struct{}), &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{
				NodeId: inode,
				Caller: fuse.Caller{
					Owner: fuse.Owner{
						Uid: 0,
						Gid: 0,
					},
				},
			},
			Valid: fuse.FATTR_UID | fuse.FATTR_GID,
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}, &fuse.AttrOut{})
	if status != fuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", status)
	}

	snapshot := testServer.snapshot()
	if snapshot.expected != nil {
		t.Fatalf("UpdateEntry expected revision = %d, want nil for cached entry", *snapshot.expected)
	}
}

func TestTruncateEntryClearsDirtyPagesForOpenHandle(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	fullPath := util.FullPath("/truncate.txt")
	inode := wfs.inodeToPath.Lookup(fullPath, 1, false, false, 0, true)
	entry := &filer_pb.Entry{
		Name: "truncate.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			FileSize: 5,
			Inode:    inode,
			Crtime:   1,
			Mtime:    1,
		},
	}

	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, entry)
	fh.RememberPath(fullPath)

	if err := fh.dirtyPages.AddPage(0, []byte("hello"), true, time.Now().UnixNano()); err != nil {
		t.Fatalf("AddPage: %v", err)
	}
	oldDirtyPages := fh.dirtyPages

	truncatedEntry := &filer_pb.Entry{
		Name: "truncate.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			FileSize: 5,
			Inode:    inode,
			Crtime:   1,
			Mtime:    1,
		},
	}

	if status := wfs.truncateEntry(fullPath, truncatedEntry); status != fuse.OK {
		t.Fatalf("truncateEntry status = %v, want OK", status)
	}
	if fh.dirtyPages == oldDirtyPages {
		t.Fatal("truncateEntry should replace the dirtyPages writer for an open handle")
	}
	if got := fh.GetEntry().GetEntry().GetAttributes().GetFileSize(); got != 0 {
		t.Fatalf("file handle size = %d, want 0", got)
	}
	buf := make([]byte, 5)
	if maxStop := fh.dirtyPages.ReadDirtyDataAt(buf, 0, time.Now().UnixNano()); maxStop != 0 {
		t.Fatalf("dirty pages maxStop = %d, want 0 after truncate", maxStop)
	}
}

func TestAccessChecksPermissions(t *testing.T) {
	wfs := newCopyRangeTestWFS()
	oldLookupSupplementaryGroupIDs := lookupSupplementaryGroupIDs
	lookupSupplementaryGroupIDs = func(uint32) ([]string, error) {
		return nil, nil
	}
	t.Cleanup(func() {
		lookupSupplementaryGroupIDs = oldLookupSupplementaryGroupIDs
	})

	fullPath := util.FullPath("/visible.txt")
	inode := wfs.inodeToPath.Lookup(fullPath, 1, false, false, 0, true)
	handle := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name: "visible.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o640,
			Uid:      123,
			Gid:      456,
			Inode:    inode,
		},
	})
	handle.RememberPath(fullPath)

	if status := wfs.Access(make(chan struct{}), &fuse.AccessIn{
		InHeader: fuse.InHeader{
			NodeId: inode,
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 123,
					Gid: 999,
				},
			},
		},
		Mask: fuse.R_OK | fuse.W_OK,
	}); status != fuse.OK {
		t.Fatalf("owner Access status = %v, want OK", status)
	}

	if status := wfs.Access(make(chan struct{}), &fuse.AccessIn{
		InHeader: fuse.InHeader{
			NodeId: inode,
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 999,
					Gid: 999,
				},
			},
		},
		Mask: fuse.W_OK,
	}); status != fuse.EACCES {
		t.Fatalf("other-user Access status = %v, want EACCES", status)
	}

	if got := hasAccess(123, 999, 123, 456, 0o400, fuse.R_OK|fuse.W_OK); got {
		t.Fatal("owner should not get write access from a read-only owner mode")
	}

	if got := hasAccess(999, 456, 123, 456, 0o040, fuse.R_OK|fuse.W_OK); got {
		t.Fatal("group member should not get write access from a read-only group mode")
	}

	if got := hasAccess(999, 999, 123, 456, 0o004, fuse.R_OK|fuse.W_OK); got {
		t.Fatal("other users should not get write access from a read-only other mode")
	}

	if got := hasAccess(0, 0, 123, 456, 0o644, fuse.X_OK); got {
		t.Fatal("root should not get execute access when no execute bit is set")
	}

	if got := hasAccess(0, 0, 123, 456, 0o755, fuse.R_OK|fuse.X_OK); !got {
		t.Fatal("root should get execute access when at least one execute bit is set")
	}
}

func TestHasAccessUsesSupplementaryGroups(t *testing.T) {
	oldLookupSupplementaryGroupIDs := lookupSupplementaryGroupIDs
	lookupSupplementaryGroupIDs = func(uint32) ([]string, error) {
		return []string{"456"}, nil
	}
	t.Cleanup(func() {
		lookupSupplementaryGroupIDs = oldLookupSupplementaryGroupIDs
	})

	if got := hasAccess(999, 999, 123, 456, 0o060, fuse.R_OK|fuse.W_OK); !got {
		t.Fatal("supplementary group membership should grant matching group permissions")
	}
}

func TestCreateExistingFileIgnoresQuotaPreflight(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)
	wfs.option.Quota = 1
	wfs.IsOverQuota = true

	entry := &filer_pb.Entry{
		Name: "existing.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			FileSize: 7,
			Inode:    101,
			Crtime:   1,
			Mtime:    1,
			Uid:      123,
			Gid:      456,
		},
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", entry)); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	wfs.inodeToPath.Lookup(util.FullPath("/existing.txt"), entry.Attributes.Crtime, false, false, entry.Attributes.Inode, true)

	out := &fuse.CreateOut{}
	status := wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: 1,
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 123,
					Gid: 456,
				},
			},
		},
		Flags: syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL,
		Mode:  0o644,
	}, "existing.txt", out)
	if status != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Create status = %v, want EEXIST", status)
	}
}
