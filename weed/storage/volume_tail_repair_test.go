package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestPlanVolumeTailRepairTruncatesExtraDataTail(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	for i := 1; i <= 8; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	v.Close()

	dataPath := filepath.Join(dir, "1.dat")
	indexPath := filepath.Join(dir, "1.idx")
	healthyDataSize := fileSize(t, dataPath)
	healthyIndexSize := fileSize(t, indexPath)

	appendBytes(t, dataPath, make([]byte, 64))

	plan, err := PlanVolumeTailRepair(needle.VolumeId(1), needle.GetCurrentVersion(), dataPath, indexPath)
	if err != nil {
		t.Fatalf("PlanVolumeTailRepair: %v", err)
	}

	if got, want := len(plan.Actions), 1; got != want {
		t.Fatalf("expected %d repair action, got %d", want, got)
	}
	action := plan.Actions[0]
	if got, want := action.Path, dataPath; got != want {
		t.Fatalf("expected data-path repair, got %q", got)
	}
	if got, want := action.TruncateTo, healthyDataSize; got != want {
		t.Fatalf("expected truncate-to %d, got %d", want, got)
	}
	if got, want := plan.HealthyIndexSize, healthyIndexSize; got != want {
		t.Fatalf("expected healthy index size %d, got %d", want, got)
	}

	backupDir := filepath.Join(dir, "backup")
	if err := ApplyVolumeTailRepair(plan, backupDir); err != nil {
		t.Fatalf("ApplyVolumeTailRepair: %v", err)
	}

	if got, want := fileSize(t, dataPath), healthyDataSize; got != want {
		t.Fatalf("expected repaired data size %d, got %d", want, got)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "1.dat")); err != nil {
		t.Fatalf("expected backup file: %v", err)
	}

	reloaded, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload repaired volume: %v", err)
	}
	defer reloaded.Close()
}

func TestPlanVolumeTailRepairTruncatesCorruptIndexTail(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	for i := 1; i <= 8; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	v.Close()

	dataPath := filepath.Join(dir, "1.dat")
	indexPath := filepath.Join(dir, "1.idx")
	healthyDataSize := fileSize(t, dataPath)
	healthyIndexSize := fileSize(t, indexPath)

	appendIndexEntry(t, indexPath, types.Uint64ToNeedleId(9001), types.ToOffset(healthyDataSize), types.Size(128))
	appendIndexEntry(t, indexPath, types.Uint64ToNeedleId(9002), types.ToOffset(healthyDataSize+needle.GetActualSize(types.Size(128), needle.GetCurrentVersion())), types.Size(128))
	appendIndexEntry(t, indexPath, types.Uint64ToNeedleId(9003), types.ToOffset(healthyDataSize+2*needle.GetActualSize(types.Size(128), needle.GetCurrentVersion())), types.Size(128))

	plan, err := PlanVolumeTailRepair(needle.VolumeId(1), needle.GetCurrentVersion(), dataPath, indexPath)
	if err != nil {
		t.Fatalf("PlanVolumeTailRepair: %v", err)
	}

	if got, want := len(plan.Actions), 1; got != want {
		t.Fatalf("expected %d repair action, got %d", want, got)
	}
	action := plan.Actions[0]
	if got, want := action.Path, indexPath; got != want {
		t.Fatalf("expected index-path repair, got %q", got)
	}
	if got, want := action.TruncateTo, healthyIndexSize; got != want {
		t.Fatalf("expected truncate-to %d, got %d", want, got)
	}
	if got, want := plan.HealthyDataSize, healthyDataSize; got != want {
		t.Fatalf("expected healthy data size %d, got %d", want, got)
	}

	backupDir := filepath.Join(dir, "backup")
	if err := ApplyVolumeTailRepair(plan, backupDir); err != nil {
		t.Fatalf("ApplyVolumeTailRepair: %v", err)
	}

	if got, want := fileSize(t, indexPath), healthyIndexSize; got != want {
		t.Fatalf("expected repaired index size %d, got %d", want, got)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "1.idx")); err != nil {
		t.Fatalf("expected backup file: %v", err)
	}

	reloaded, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload repaired volume: %v", err)
	}
	defer reloaded.Close()
}

func TestPlanVolumeTailRepairRefusesValidEntriesAfterCorruptTailStart(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	for i := 1; i <= 8; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	v.Close()

	dataPath := filepath.Join(dir, "1.dat")
	indexPath := filepath.Join(dir, "1.idx")
	healthyDataSize := fileSize(t, dataPath)

	originalIndexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read index file: %v", err)
	}
	lastEntryOffset := len(originalIndexBytes) - types.NeedleMapEntrySize
	lastEntry := append([]byte(nil), originalIndexBytes[lastEntryOffset:]...)

	appendIndexEntry(t, indexPath, types.Uint64ToNeedleId(9001), types.ToOffset(healthyDataSize), types.Size(128))
	appendBytes(t, indexPath, lastEntry)

	if _, err := PlanVolumeTailRepair(needle.VolumeId(1), needle.GetCurrentVersion(), dataPath, indexPath); err == nil {
		t.Fatalf("expected repair planning to refuse non-tail corruption")
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func appendIndexEntry(t *testing.T, path string, key types.NeedleId, offset types.Offset, size types.Size) {
	t.Helper()
	appendBytes(t, path, needle_map.ToBytes(key, offset, size))
}

func appendBytes(t *testing.T, path string, data []byte) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open %s for append: %v", path, err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
}
