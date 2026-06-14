package mount

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type directoryListStream struct {
	responses []*filer_pb.ListEntriesResponse
	index     int
}

func (s *directoryListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *directoryListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *directoryListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *directoryListStream) CloseSend() error             { return nil }
func (s *directoryListStream) Context() context.Context     { return context.Background() }
func (s *directoryListStream) SendMsg(any) error            { return nil }
func (s *directoryListStream) RecvMsg(any) error            { return nil }

type directoryListClient struct {
	filer_pb.SeaweedFilerClient
	responses []*filer_pb.ListEntriesResponse
}

func (c *directoryListClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	return &directoryListStream{responses: c.responses}, nil
}

type directoryFilerAccessor struct {
	client filer_pb.SeaweedFilerClient
}

func (a *directoryFilerAccessor) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(a.client)
}

func (a *directoryFilerAccessor) AdjustedUrl(*filer_pb.Location) string { return "" }
func (a *directoryFilerAccessor) GetDataCenter() string                 { return "" }

type directoryReadTestServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu         sync.Mutex
	entries    map[string][]*filer_pb.Entry
	listCalls  map[string]int
	snapshotTs int64
}

func (s *directoryReadTestServer) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	s.mu.Lock()
	s.listCalls[req.GetDirectory()]++
	entries := append([]*filer_pb.Entry(nil), s.entries[req.GetDirectory()]...)
	snapshotTs := s.snapshotTs
	s.mu.Unlock()

	sent := 0
	for _, entry := range entries {
		if !directoryEntryMatchesRequest(entry.GetName(), req.GetStartFromFileName(), req.GetInclusiveStartFrom()) {
			continue
		}
		if req.GetLimit() > 0 && sent >= int(req.GetLimit()) {
			break
		}

		response := &filer_pb.ListEntriesResponse{
			Entry: proto.Clone(entry).(*filer_pb.Entry),
		}
		if req.GetSnapshotTsNs() != 0 {
			response.SnapshotTsNs = req.GetSnapshotTsNs()
		} else {
			response.SnapshotTsNs = snapshotTs
		}

		if err := stream.Send(response); err != nil {
			return err
		}
		sent++
	}

	return nil
}

func (s *directoryReadTestServer) listCallsForDir(dir string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalls[dir]
}

func directoryEntryMatchesRequest(name, startFrom string, inclusive bool) bool {
	if startFrom == "" {
		return true
	}
	cmp := strings.Compare(name, startFrom)
	if inclusive {
		return cmp >= 0
	}
	return cmp > 0
}

func newDirectoryReadTestWFS(t *testing.T, serverEntries map[string][]*filer_pb.Entry) (*WFS, *directoryReadTestServer) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	grpcServer := pb.NewGrpcServer()
	testServer := &directoryReadTestServer{
		entries:    serverEntries,
		listCalls:  make(map[string]int),
		snapshotTs: 100,
	}
	filer_pb.RegisterSeaweedFilerServer(grpcServer, testServer)
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)

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
		option:         option,
		signature:      1,
		inodeToPath:    NewInodeToPath(root, 0),
		fhMap:          NewFileHandleToInode(),
		dhMap:          NewDirectoryHandleToInode(),
		fhLockTable:    util.NewLockTable[FileHandleId](),
		refreshingDirs: make(map[util.FullPath]struct{}),
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

func TestLoadDirectoryEntriesDirectFiltersHiddenEntriesAndMapsIds(t *testing.T) {
	mapper, err := meta_cache.NewUidGidMapper("10:1000", "20:2000")
	if err != nil {
		t.Fatalf("uid/gid mapper: %v", err)
	}

	client := &directoryFilerAccessor{
		client: &directoryListClient{
			responses: []*filer_pb.ListEntriesResponse{
				{
					Entry: &filer_pb.Entry{
						Name: "topics",
						Attributes: &filer_pb.FuseAttributes{
							Uid: 1000,
							Gid: 2000,
						},
					},
				},
				{
					Entry: &filer_pb.Entry{
						Name: "visible",
						Attributes: &filer_pb.FuseAttributes{
							Uid: 1000,
							Gid: 2000,
						},
					},
				},
			},
		},
	}

	entries, _, err := loadDirectoryEntriesDirect(context.Background(), client, mapper, util.FullPath("/"), "", false, 10, 0, false)
	if err != nil {
		t.Fatalf("loadDirectoryEntriesDirect: %v", err)
	}
	if got := len(entries); got != 1 {
		t.Fatalf("entry count = %d, want 1", got)
	}
	if entries[0].Name() != "visible" {
		t.Fatalf("entry name = %q, want visible", entries[0].Name())
	}
	if entries[0].Attr.Uid != 10 || entries[0].Attr.Gid != 20 {
		t.Fatalf("mapped uid/gid = %d/%d, want 10/20", entries[0].Attr.Uid, entries[0].Attr.Gid)
	}
}

func TestLoadDirectoryEntriesDirectShowsSystemEntriesWhenEnabled(t *testing.T) {
	client := &directoryFilerAccessor{
		client: &directoryListClient{
			responses: []*filer_pb.ListEntriesResponse{
				{Entry: &filer_pb.Entry{Name: "topics"}},
				{Entry: &filer_pb.Entry{Name: "visible"}},
			},
		},
	}

	entries, _, err := loadDirectoryEntriesDirect(context.Background(), client, nil, util.FullPath("/"), "", false, 10, 0, true)
	if err != nil {
		t.Fatalf("loadDirectoryEntriesDirect: %v", err)
	}
	if got := len(entries); got != 2 {
		t.Fatalf("entry count = %d, want 2", got)
	}
	if entries[0].Name() != "topics" || entries[1].Name() != "visible" {
		t.Fatalf("entry names = %q, %q, want topics, visible", entries[0].Name(), entries[1].Name())
	}
}

func TestReadDirFullDirectReadPromotesDirectoryBackToCache(t *testing.T) {
	wfs, testServer := newDirectoryReadTestWFS(t, map[string][]*filer_pb.Entry{
		"/dir": {
			{Name: "alpha"},
			{Name: "beta"},
		},
	})

	dirPath := util.FullPath("/dir")
	dirInode := wfs.inodeToPath.Lookup(dirPath, time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(dirPath)
	if !wfs.inodeToPath.MarkDirectoryReadThrough(dirPath, time.Now()) {
		t.Fatal("failed to switch directory to direct-read mode")
	}

	firstHandle, _ := wfs.AcquireDirectoryHandle()
	firstOut := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := wfs.ReadDir(make(chan struct{}), &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirInode},
		Fh:       uint64(firstHandle),
		Offset:   0,
	}, firstOut); status != fuse.OK {
		t.Fatalf("ReadDir first pass status = %v, want OK", status)
	}
	wfs.ReleaseDir(&fuse.ReleaseIn{Fh: uint64(firstHandle)})

	if wfs.inodeToPath.ShouldReadDirectoryDirect(dirPath) {
		t.Fatal("directory stayed in direct-read mode after a full successful read")
	}
	if !wfs.inodeToPath.IsChildrenCached(dirPath) {
		t.Fatal("directory children were not marked cached after a full direct read")
	}
	for _, name := range []string{"alpha", "beta"} {
		entry, err := wfs.metaCache.FindEntry(context.Background(), dirPath.Child(name))
		if err != nil {
			t.Fatalf("FindEntry %s: %v", name, err)
		}
		if entry == nil {
			t.Fatalf("cached entry %s is missing after direct-read promotion", name)
		}
	}

	firstCalls := testServer.listCallsForDir("/dir")
	if firstCalls == 0 {
		t.Fatal("expected first read to hit the filer at least once")
	}

	secondHandle, _ := wfs.AcquireDirectoryHandle()
	secondOut := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := wfs.ReadDir(make(chan struct{}), &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirInode},
		Fh:       uint64(secondHandle),
		Offset:   0,
	}, secondOut); status != fuse.OK {
		t.Fatalf("ReadDir second pass status = %v, want OK", status)
	}
	wfs.ReleaseDir(&fuse.ReleaseIn{Fh: uint64(secondHandle)})

	if got := testServer.listCallsForDir("/dir"); got != firstCalls {
		t.Fatalf("second read triggered %d filer list calls, want %d total", got, firstCalls)
	}
}

func TestReleaseDirAbortsIncompleteDirectReadRefresh(t *testing.T) {
	wfs, _ := newDirectoryReadTestWFS(t, map[string][]*filer_pb.Entry{
		"/dir": {
			{Name: "alpha"},
			{Name: "beta"},
		},
	})

	dirPath := util.FullPath("/dir")
	dirInode := wfs.inodeToPath.Lookup(dirPath, time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(dirPath)
	if !wfs.inodeToPath.MarkDirectoryReadThrough(dirPath, time.Now()) {
		t.Fatal("failed to switch directory to direct-read mode")
	}

	handle, _ := wfs.AcquireDirectoryHandle()
	smallOut := fuse.NewDirEntryList(make([]byte, 80), 0)
	if status := wfs.ReadDir(make(chan struct{}), &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirInode},
		Fh:       uint64(handle),
		Offset:   0,
	}, smallOut); status != fuse.OK {
		t.Fatalf("ReadDir partial pass status = %v, want OK", status)
	}
	if len(wfs.refreshingDirs) != 1 {
		t.Fatalf("active direct refresh count = %d, want 1 before release", len(wfs.refreshingDirs))
	}

	wfs.ReleaseDir(&fuse.ReleaseIn{Fh: uint64(handle)})

	if len(wfs.refreshingDirs) != 0 {
		t.Fatalf("active direct refresh count = %d, want 0 after release", len(wfs.refreshingDirs))
	}
	if !wfs.inodeToPath.ShouldReadDirectoryDirect(dirPath) {
		t.Fatal("directory left direct-read mode after aborting an incomplete refresh")
	}
	if wfs.inodeToPath.IsChildrenCached(dirPath) {
		t.Fatal("directory became cached after aborting an incomplete refresh")
	}
}
