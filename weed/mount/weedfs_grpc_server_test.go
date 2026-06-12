package mount

import (
	"context"
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
