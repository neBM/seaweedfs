package mount

import (
	"context"
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type fakeVolumeLocationClient struct {
	refreshCalls int
}

func (f *fakeVolumeLocationClient) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(context.Context, string) ([]string, error) {
		return nil, nil
	}
}

func (f *fakeVolumeLocationClient) RefreshVolumeLocations() {
	f.refreshCalls++
}

func TestRefreshVolumeLocationsDelegatesToClient(t *testing.T) {
	client := &fakeVolumeLocationClient{}
	root := util.FullPath("/")
	inodeToPath := NewInodeToPath(root, 0)
	inodeToPath.MarkChildrenCached(root)

	wfs := &WFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		filerClient:   client,
		inodeToPath:   inodeToPath,
	}

	if _, err := wfs.RefreshVolumeLocations(context.Background(), &mount_pb.RefreshVolumeLocationsRequest{}); err != nil {
		t.Fatalf("RefreshVolumeLocations returned error: %v", err)
	}
	if client.refreshCalls != 1 {
		t.Fatalf("RefreshVolumeLocations calls = %d, want 1", client.refreshCalls)
	}
	if !inodeToPath.IsChildrenCached(root) {
		t.Fatal("RefreshVolumeLocations unexpectedly invalidated inode-to-path cache")
	}
}

func TestRefreshVolumeLocationsNoopWithoutDirectClient(t *testing.T) {
	root := util.FullPath("/")
	inodeToPath := NewInodeToPath(root, 0)
	inodeToPath.MarkChildrenCached(root)

	wfs := &WFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		inodeToPath:   inodeToPath,
	}

	if _, err := wfs.RefreshVolumeLocations(context.Background(), &mount_pb.RefreshVolumeLocationsRequest{}); err != nil {
		t.Fatalf("RefreshVolumeLocations returned error: %v", err)
	}
	if !inodeToPath.IsChildrenCached(root) {
		t.Fatal("RefreshVolumeLocations unexpectedly invalidated inode-to-path cache")
	}
}

func TestPrepareHotRestartReportsLiveStateWithoutBlocking(t *testing.T) {
	wfs := newHotRestartLiveTestWFS(t)
	_ = wfs.fhMap.AcquireFileHandle(wfs, 101, newHotRestartStateEntry(101))

	resp, err := wfs.PrepareHotRestart(context.Background(), &mount_pb.PrepareHotRestartRequest{})
	if err != nil {
		t.Fatalf("PrepareHotRestart returned error: %v", err)
	}
	if resp.Accepted {
		t.Fatal("PrepareHotRestart unexpectedly accepted a non-quiescent worker")
	}
	if resp.Status.GetOpenFileHandles() != 1 {
		t.Fatalf("OpenFileHandles = %d, want 1", resp.Status.GetOpenFileHandles())
	}
	if resp.Status.GetBlockingNewHandles() {
		t.Fatal("PrepareHotRestart unexpectedly blocked new handles for a non-quiescent worker")
	}
}

func TestPrepareHotRestartBlocksOpenDirUntilCancelled(t *testing.T) {
	wfs := newHotRestartLiveTestWFS(t)

	resp, err := wfs.PrepareHotRestart(context.Background(), &mount_pb.PrepareHotRestartRequest{})
	if err != nil {
		t.Fatalf("PrepareHotRestart returned error: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("PrepareHotRestart rejected a quiescent worker: %+v", resp.Status)
	}

	status := wfs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &fuse.OpenOut{})
	if status != fuse.Status(syscall.EBUSY) {
		t.Fatalf("OpenDir status while blocked = %v, want %v", status, fuse.Status(syscall.EBUSY))
	}

	statusResp, err := wfs.HotRestartStatus(context.Background(), &mount_pb.HotRestartStatusRequest{})
	if err != nil {
		t.Fatalf("HotRestartStatus returned error: %v", err)
	}
	if !statusResp.GetBlockingNewHandles() || !statusResp.GetQuiescent() {
		t.Fatalf("HotRestartStatus = %+v, want blocked quiescent worker", statusResp)
	}

	if _, err := wfs.CancelHotRestart(context.Background(), &mount_pb.CancelHotRestartRequest{}); err != nil {
		t.Fatalf("CancelHotRestart returned error: %v", err)
	}

	out := &fuse.OpenOut{}
	status = wfs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, out)
	if status != fuse.OK {
		t.Fatalf("OpenDir status after cancel = %v, want %v", status, fuse.OK)
	}
	wfs.ReleaseDir(&fuse.ReleaseIn{Fh: out.Fh})
}
