package filer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestCheckChunkReferencesMarksMetadataAndLeaseProtections(t *testing.T) {
	f := &Filer{
		ChunkLeases: NewChunkLeaseManager(),
	}
	f.LeaseChunks([]string{"2,def"})

	statuses, err := f.CheckChunkReferences(context.Background(), []string{"1,abc", "2,def", "3,none"}, true)
	if err != nil {
		t.Fatalf("CheckChunkReferences: %v", err)
	}
	if statuses["1,abc"].Referenced || statuses["1,abc"].Leased {
		t.Fatalf("persistent metadata references should be protected by revisions, not scanned by the chunk guard: %+v", statuses["1,abc"])
	}
	if !statuses["2,def"].Leased {
		t.Fatalf("2,def should be protected by lease")
	}
	if statuses["3,none"].Referenced || statuses["3,none"].Leased {
		t.Fatalf("3,none should be unprotected: %+v", statuses["3,none"])
	}
}

func TestChunkLeaseManagerExpiresLeases(t *testing.T) {
	leases := NewChunkLeaseManager()
	now := time.Unix(100, 0)
	leases.nowFunc = func() time.Time { return now }
	leases.Lease([]string{"1,abc"}, time.Second)

	if !leases.Snapshot([]string{"1,abc"})["1,abc"] {
		t.Fatalf("lease should be active before expiry")
	}

	now = now.Add(2 * time.Second)
	if leases.Snapshot([]string{"1,abc"})["1,abc"] {
		t.Fatalf("lease should expire")
	}
}

func TestCheckChunkReferencesForVolumeReportsMissingLeasedKey(t *testing.T) {
	fileId := needle.NewFileId(needle.VolumeId(7), 123, 456).String()
	f := &Filer{
		ChunkLeases: NewChunkLeaseManager(),
	}
	f.LeaseChunks([]string{fileId})

	_, missing, err := f.CheckChunkReferencesForVolume(context.Background(), nil, false, 7, []uint64{999})
	if err != nil {
		t.Fatalf("CheckChunkReferencesForVolume: %v", err)
	}
	if got, want := strings.Join(missing, ","), fileId; got != want {
		t.Fatalf("missing = %v, want %s", missing, want)
	}

	_, missing, err = f.CheckChunkReferencesForVolume(context.Background(), nil, false, 7, []uint64{123})
	if err != nil {
		t.Fatalf("CheckChunkReferencesForVolume with present key: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing = %v, want none", missing)
	}
}
