package weed_server

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestChunkReferenceGuardBlocksReferencedFileId(t *testing.T) {
	fileId := needle.NewFileId(needle.VolumeId(7), 123, 456).String()
	guard := newChunkReferenceGuardForTest(func(ctx context.Context, req *filer_pb.CheckChunkReferencesRequest) (*filer_pb.CheckChunkReferencesResponse, error) {
		if got := req.FileIds; len(got) != 1 || got[0] != fileId {
			t.Fatalf("file ids = %v, want %s", got, fileId)
		}
		return &filer_pb.CheckChunkReferencesResponse{
			ChunkStatus: map[string]*filer_pb.ChunkReferenceStatus{
				req.FileIds[0]: {
					Referenced: true,
					Paths:      []string{"/buckets/mail/dovecot-uidlist"},
				},
			},
		}, nil
	})

	err := guard.CheckFileIds(context.Background(), []string{fileId})
	if err == nil {
		t.Fatal("expected referenced file id to be blocked")
	}
	if !strings.Contains(err.Error(), "referenced") || !strings.Contains(err.Error(), "/buckets/mail/dovecot-uidlist") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkReferenceGuardBlocksLeasedFileId(t *testing.T) {
	fileId := needle.NewFileId(needle.VolumeId(7), 123, 456).String()
	guard := newChunkReferenceGuardForTest(func(ctx context.Context, req *filer_pb.CheckChunkReferencesRequest) (*filer_pb.CheckChunkReferencesResponse, error) {
		return &filer_pb.CheckChunkReferencesResponse{
			ChunkStatus: map[string]*filer_pb.ChunkReferenceStatus{
				req.FileIds[0]: {Leased: true},
			},
		}, nil
	})

	err := guard.CheckFileIds(context.Background(), []string{fileId})
	if err == nil {
		t.Fatal("expected leased file id to be blocked")
	}
	if !strings.Contains(err.Error(), "leased") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkReferenceGuardBlocksVacuumMissingFileId(t *testing.T) {
	indexFileName := writeCompactIndex(t, []uint64{1001, 1002})
	guard := newChunkReferenceGuardForTest(func(ctx context.Context, req *filer_pb.CheckChunkReferencesRequest) (*filer_pb.CheckChunkReferencesResponse, error) {
		if req.VolumeId != 7 {
			t.Fatalf("volume id = %d, want 7", req.VolumeId)
		}
		if got, want := req.PresentFileKeys, []uint64{1001, 1002}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("present keys = %v, want %v", got, want)
		}
		return &filer_pb.CheckChunkReferencesResponse{
			MissingFileIds: []string{"7,000003eb00000001"},
		}, nil
	})

	err := guard.CheckCompactIndex(context.Background(), needle.VolumeId(7), indexFileName)
	if err == nil {
		t.Fatal("expected compacted index with missing referenced file id to be blocked")
	}
	if !strings.Contains(err.Error(), "missing protected file ids") || !strings.Contains(err.Error(), "7,000003eb00000001") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkReferenceGuardAllowsVacuumWhenFilerFindsNoMissingReferences(t *testing.T) {
	indexFileName := writeCompactIndex(t, []uint64{1001, 1002})
	guard := newChunkReferenceGuardForTest(func(ctx context.Context, req *filer_pb.CheckChunkReferencesRequest) (*filer_pb.CheckChunkReferencesResponse, error) {
		return &filer_pb.CheckChunkReferencesResponse{}, nil
	})

	if err := guard.CheckCompactIndex(context.Background(), needle.VolumeId(7), indexFileName); err != nil {
		t.Fatalf("expected compacted index to pass: %v", err)
	}
}

func writeCompactIndex(t *testing.T, keys []uint64) string {
	t.Helper()

	indexFileName := t.TempDir() + "/volume.cpx"
	var data []byte
	for _, key := range keys {
		entry := needle_map.ToBytes(types.Uint64ToNeedleId(key), types.ToOffset(int64(key)), types.Size(512))
		data = append(data, entry...)
	}
	if err := os.WriteFile(indexFileName, data, 0o600); err != nil {
		t.Fatalf("write index entries: %v", err)
	}
	return indexFileName
}
