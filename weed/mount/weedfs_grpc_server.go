package mount

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
)

func (wfs *WFS) Configure(ctx context.Context, request *mount_pb.ConfigureRequest) (*mount_pb.ConfigureResponse, error) {
	if wfs.option.Collection == "" {
		return nil, fmt.Errorf("mount quota only works when mounted to a new folder with a collection")
	}
	glog.V(0).Infof("quota changed from %d to %d", wfs.option.Quota, request.CollectionCapacity)
	wfs.option.Quota = request.GetCollectionCapacity()
	return &mount_pb.ConfigureResponse{}, nil
}

func (wfs *WFS) RefreshVolumeLocations(ctx context.Context, request *mount_pb.RefreshVolumeLocationsRequest) (*mount_pb.RefreshVolumeLocationsResponse, error) {
	if wfs.filerClient == nil {
		return &mount_pb.RefreshVolumeLocationsResponse{}, nil
	}
	mountDir := ""
	if wfs.option != nil {
		mountDir = wfs.option.MountDirectory
	}
	glog.V(0).Infof("refreshing cached volume locations for mount %s", mountDir)
	wfs.filerClient.RefreshVolumeLocations()
	return &mount_pb.RefreshVolumeLocationsResponse{}, nil
}

func (wfs *WFS) HotRestartStatus(ctx context.Context, request *mount_pb.HotRestartStatusRequest) (*mount_pb.HotRestartStatusResponse, error) {
	return hotRestartStatusToProto(wfs.HotRestartStatusSnapshot()), nil
}

func (wfs *WFS) PrepareHotRestart(ctx context.Context, request *mount_pb.PrepareHotRestartRequest) (*mount_pb.PrepareHotRestartResponse, error) {
	status := wfs.PrepareHotRestartGate()
	return &mount_pb.PrepareHotRestartResponse{
		Accepted: status.Quiescent() && status.BlockingNewHandles,
		Status:   hotRestartStatusToProto(status),
	}, nil
}

func (wfs *WFS) CancelHotRestart(ctx context.Context, request *mount_pb.CancelHotRestartRequest) (*mount_pb.CancelHotRestartResponse, error) {
	wfs.CancelHotRestartGate()
	return &mount_pb.CancelHotRestartResponse{}, nil
}

func hotRestartStatusToProto(status HotRestartStatusSnapshot) *mount_pb.HotRestartStatusResponse {
	return &mount_pb.HotRestartStatusResponse{
		OpenFileHandles:      uint64(status.OpenFileHandles),
		OpenDirectoryHandles: uint64(status.OpenDirectoryHandles),
		PendingAsyncFlushes:  uint64(status.PendingAsyncFlushes),
		Quiescent:            status.Quiescent(),
		BlockingNewHandles:   status.BlockingNewHandles,
	}
}
