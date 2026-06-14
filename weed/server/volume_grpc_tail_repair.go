package weed_server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) RepairVolumeTail(ctx context.Context, req *volume_server_pb.RepairVolumeTailRequest) (*volume_server_pb.RepairVolumeTailResponse, error) {
	resp := &volume_server_pb.RepairVolumeTailResponse{}

	vid := needle.VolumeId(req.GetVolumeId())
	v := vs.store.GetVolume(vid)
	if v == nil {
		return resp, fmt.Errorf("volume %d not found", req.GetVolumeId())
	}

	plan, err := storage.PlanVolumeTailRepair(v.Id, v.Version(), v.FileName(".dat"), v.FileName(".idx"))
	if err != nil {
		return resp, err
	}
	resp.Details = append(resp.Details, plan.Describe()...)

	if !req.GetApply() {
		return resp, nil
	}

	if len(plan.Actions) == 0 {
		files, errs := v.Scrub()
		if len(errs) != 0 {
			return resp, fmt.Errorf("post-validation scrub for volume %d failed after %d files: %w", vid, files, errors.Join(errs...))
		}
		resp.Details = append(resp.Details, fmt.Sprintf("validated healthy volume %d across %d files", vid, files))
		if req.GetMarkWritable() && v.IsReadOnly() {
			if err := vs.makeVolumeWritable(ctx, v); err != nil {
				return resp, fmt.Errorf("mark volume %d writable: %w", vid, err)
			}
			resp.MarkedWritable = true
			resp.Details = append(resp.Details, fmt.Sprintf("volume %d is now writable", vid))
		}
		return resp, nil
	}

	if !v.IsReadOnly() {
		return resp, fmt.Errorf("volume %d must already be read-only before applying a tail repair", vid)
	}

	backupDir := req.GetBackupDir()
	if backupDir == "" {
		backupDir = filepath.Join(filepath.Dir(plan.DataPath), ".tail-repair-backups", fmt.Sprintf("%d-%s", vid, time.Now().UTC().Format("20060102T150405Z")))
	}

	if err := vs.store.UnmountVolume(vid); err != nil {
		return resp, fmt.Errorf("unmount volume %d: %w", vid, err)
	}

	if err := storage.ApplyVolumeTailRepair(plan, backupDir); err != nil {
		if mountErr := vs.store.MountVolume(vid); mountErr != nil {
			return resp, fmt.Errorf("apply tail repair to volume %d: %v; remount after failure: %w", vid, err, mountErr)
		}
		return resp, fmt.Errorf("apply tail repair to volume %d: %w", vid, err)
	}

	if err := vs.store.MountVolume(vid); err != nil {
		return resp, fmt.Errorf("mount repaired volume %d: %w", vid, err)
	}

	reloaded := vs.store.GetVolume(vid)
	if reloaded == nil {
		return resp, fmt.Errorf("repaired volume %d did not remount", vid)
	}

	files, errs := reloaded.Scrub()
	if len(errs) != 0 {
		return resp, fmt.Errorf("post-repair scrub for volume %d failed after %d files: %w", vid, files, errors.Join(errs...))
	}

	resp.Applied = true
	for _, backupPath := range storage.VolumeTailRepairBackupPaths(plan) {
		resp.BackupFiles = append(resp.BackupFiles, filepath.Join(backupDir, filepath.Base(backupPath)))
	}
	resp.Details = append(resp.Details, fmt.Sprintf("reloaded repaired volume %d and scrubbed %d files cleanly", vid, files))

	if req.GetMarkWritable() {
		if err := vs.makeVolumeWritable(ctx, reloaded); err != nil {
			return resp, fmt.Errorf("mark repaired volume %d writable: %w", vid, err)
		}
		resp.MarkedWritable = true
		resp.Details = append(resp.Details, fmt.Sprintf("volume %d is now writable", vid))
	}

	return resp, nil
}
