package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type VolumeTailRepairAction struct {
	Path       string
	FromSize   int64
	TruncateTo int64
	Reason     string
}

type VolumeTailRepairPlan struct {
	VolumeId         needle.VolumeId
	Version          needle.Version
	DataPath         string
	IndexPath        string
	DataSize         int64
	IndexSize        int64
	HealthyDataSize  int64
	HealthyIndexSize int64
	HealthyEntries   int64
	TotalEntries     int64
	Actions          []VolumeTailRepairAction
}

func (plan *VolumeTailRepairPlan) Describe() []string {
	if plan == nil {
		return nil
	}

	details := []string{
		fmt.Sprintf(
			"volume %d healthy prefix %d/%d entries; data %d -> %d bytes; index %d -> %d bytes",
			plan.VolumeId,
			plan.HealthyEntries,
			plan.TotalEntries,
			plan.DataSize,
			plan.HealthyDataSize,
			plan.IndexSize,
			plan.HealthyIndexSize,
		),
	}
	if len(plan.Actions) == 0 {
		return append(details, "no tail repair actions required")
	}
	for _, action := range plan.Actions {
		details = append(details, fmt.Sprintf("truncate %s from %d to %d: %s", action.Path, action.FromSize, action.TruncateTo, action.Reason))
	}
	return details
}

func PlanVolumeTailRepair(volumeId needle.VolumeId, version needle.Version, dataPath string, indexPath string) (*VolumeTailRepairPlan, error) {
	dataFile, err := os.OpenFile(dataPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open data file %s: %w", dataPath, err)
	}
	defer dataFile.Close()

	indexFile, err := os.OpenFile(indexPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open index file %s: %w", indexPath, err)
	}
	defer indexFile.Close()

	dataStat, err := dataFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat data file %s: %w", dataPath, err)
	}
	indexStat, err := indexFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat index file %s: %w", indexPath, err)
	}

	plan := &VolumeTailRepairPlan{
		VolumeId:  volumeId,
		Version:   version,
		DataPath:  dataPath,
		IndexPath: indexPath,
		DataSize:  dataStat.Size(),
		IndexSize: indexStat.Size(),
	}

	fullIndexSize := plan.IndexSize - plan.IndexSize%types.NeedleMapEntrySize
	partialIndexTail := plan.IndexSize - fullIndexSize
	if fullIndexSize == 0 {
		if partialIndexTail > 0 {
			return nil, fmt.Errorf("index file %s only has a partial tail and no healthy entries", indexPath)
		}
		return nil, fmt.Errorf("index file %s is empty", indexPath)
	}

	dataBackend := backend.NewDiskFile(dataFile)
	indexReader := io.NewSectionReader(indexFile, 0, fullIndexSize)

	var (
		firstBadEntry   int64 = -1
		validAfterBad   bool
		healthyDataSize = int64(super_block.SuperBlockSize)
		entryCount      int64
	)

	err = idx.WalkIndexFile(indexReader, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		entryCount++

		entryTail := offset.ToActualOffset() + indexedEntryActualSize(size, version)
		verifyErr := verifyIndexedEntry(dataBackend, version, key, offset.ToActualOffset(), size)
		if verifyErr == nil {
			if firstBadEntry >= 0 {
				validAfterBad = true
				return nil
			}
			healthyDataSize = entryTail
			return nil
		}

		if firstBadEntry < 0 {
			firstBadEntry = entryCount - 1
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk index file %s: %w", indexPath, err)
	}

	if validAfterBad {
		return nil, fmt.Errorf("volume %d has valid entries after the first corrupt entry; corruption is not confined to the tail", volumeId)
	}

	plan.TotalEntries = entryCount
	plan.HealthyEntries = entryCount
	plan.HealthyIndexSize = fullIndexSize
	if firstBadEntry >= 0 {
		plan.HealthyEntries = firstBadEntry
		plan.HealthyIndexSize = firstBadEntry * types.NeedleMapEntrySize
		if plan.HealthyIndexSize == 0 {
			return nil, fmt.Errorf("volume %d has no healthy index prefix to repair", volumeId)
		}
	}
	plan.HealthyDataSize = healthyDataSize

	if plan.DataSize < plan.HealthyDataSize {
		return nil, fmt.Errorf("data file %s is smaller than the healthy prefix requires: have %d want %d", dataPath, plan.DataSize, plan.HealthyDataSize)
	}

	if plan.HealthyIndexSize < plan.IndexSize {
		plan.Actions = append(plan.Actions, VolumeTailRepairAction{
			Path:       indexPath,
			FromSize:   plan.IndexSize,
			TruncateTo: plan.HealthyIndexSize,
			Reason:     fmt.Sprintf("remove corrupt index tail after %d healthy entries", plan.HealthyEntries),
		})
	}

	if plan.DataSize > plan.HealthyDataSize {
		plan.Actions = append(plan.Actions, VolumeTailRepairAction{
			Path:       dataPath,
			FromSize:   plan.DataSize,
			TruncateTo: plan.HealthyDataSize,
			Reason:     "remove extra data tail beyond the last healthy indexed needle",
		})
	}

	return plan, nil
}

func ApplyVolumeTailRepair(plan *VolumeTailRepairPlan, backupDir string) error {
	if plan == nil {
		return fmt.Errorf("repair plan is required")
	}
	if len(plan.Actions) == 0 {
		return nil
	}
	if backupDir == "" {
		return fmt.Errorf("backup directory is required")
	}
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return fmt.Errorf("create backup directory %s: %w", backupDir, err)
	}

	for _, sourcePath := range VolumeTailRepairBackupPaths(plan) {
		if err := copyVolumeRepairBackup(sourcePath, filepath.Join(backupDir, filepath.Base(sourcePath))); err != nil {
			return err
		}
	}

	for _, action := range plan.Actions {
		if err := truncateVolumeRepairFile(action); err != nil {
			return err
		}
	}

	return nil
}

func VolumeTailRepairBackupPaths(plan *VolumeTailRepairPlan) []string {
	if plan == nil {
		return nil
	}

	backupPaths := []string{plan.DataPath, plan.IndexPath}
	volumeInfoPath := volumeInfoPathFromDataPath(plan.DataPath)
	if _, err := os.Stat(volumeInfoPath); err == nil {
		backupPaths = append(backupPaths, volumeInfoPath)
	}
	slices.Sort(backupPaths)
	return slices.Compact(backupPaths)
}

func indexedEntryActualSize(size types.Size, version needle.Version) int64 {
	if size.IsDeleted() {
		return needle.GetActualSize(0, version)
	}
	return needle.GetActualSize(size, version)
}

func verifyIndexedEntry(dataFile backend.BackendStorageFile, version needle.Version, key types.NeedleId, offset int64, size types.Size) error {
	n := new(needle.Needle)
	readSize := size
	if size.IsDeleted() {
		readSize = 0
	}
	if err := n.ReadData(dataFile, offset, readSize, version); err != nil {
		return err
	}
	if n.Id != key {
		return fmt.Errorf("index key %v does not match needle id %v", key, n.Id)
	}
	return nil
}

func volumeInfoPathFromDataPath(dataPath string) string {
	return dataPath[:len(dataPath)-len(filepath.Ext(dataPath))] + ".vif"
}

func copyVolumeRepairBackup(sourcePath string, backupPath string) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open backup source %s: %w", sourcePath, err)
	}
	defer sourceFile.Close()

	sourceInfo, err := sourceFile.Stat()
	if err != nil {
		return fmt.Errorf("stat backup source %s: %w", sourcePath, err)
	}

	backupFile, err := os.OpenFile(backupPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, sourceInfo.Mode().Perm())
	if err != nil {
		return fmt.Errorf("create backup file %s: %w", backupPath, err)
	}
	defer backupFile.Close()

	if _, err := io.Copy(backupFile, sourceFile); err != nil {
		return fmt.Errorf("copy %s to %s: %w", sourcePath, backupPath, err)
	}
	if err := backupFile.Sync(); err != nil {
		return fmt.Errorf("sync backup file %s: %w", backupPath, err)
	}

	return nil
}

func truncateVolumeRepairFile(action VolumeTailRepairAction) error {
	file, err := os.OpenFile(action.Path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open repair target %s: %w", action.Path, err)
	}
	defer file.Close()

	if err := file.Truncate(action.TruncateTo); err != nil {
		return fmt.Errorf("truncate %s to %d: %w", action.Path, action.TruncateTo, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync %s after truncate: %w", action.Path, err)
	}

	return nil
}
