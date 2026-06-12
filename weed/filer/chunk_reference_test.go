package filer

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCheckChunkReferencesMarksMetadataAndLeaseProtections(t *testing.T) {
	store := newChunkReferenceTestStore()
	store.entries["/dir"] = &Entry{FullPath: "/dir", Attr: Attr{Mode: os.ModeDir}}
	store.entries["/dir/file.txt"] = &Entry{
		FullPath: "/dir/file.txt",
		Chunks: []*filer_pb.FileChunk{
			{FileId: "1,abc"},
		},
	}
	f := &Filer{
		Store:       NewFilerStoreWrapper(store),
		ChunkLeases: NewChunkLeaseManager(),
	}
	f.LeaseChunks([]string{"2,def"})

	statuses, err := f.CheckChunkReferences(context.Background(), []string{"1,abc", "2,def", "3,none"}, true)
	if err != nil {
		t.Fatalf("CheckChunkReferences: %v", err)
	}
	if !statuses["1,abc"].Referenced {
		t.Fatalf("1,abc should be referenced by metadata")
	}
	if got, want := statuses["1,abc"].Paths, []string{"/dir/file.txt"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("paths = %v, want %v", got, want)
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

func TestCheckChunkReferencesForVolumeReportsMissingReferencedKey(t *testing.T) {
	fileId := needle.NewFileId(needle.VolumeId(7), 123, 456).String()
	store := newChunkReferenceTestStore()
	store.entries["/file.txt"] = &Entry{
		FullPath: "/file.txt",
		Chunks:   []*filer_pb.FileChunk{{FileId: fileId}},
	}
	f := &Filer{
		Store:       NewFilerStoreWrapper(store),
		ChunkLeases: NewChunkLeaseManager(),
	}

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

type chunkReferenceTestStore struct {
	entries map[string]*Entry
}

func newChunkReferenceTestStore() *chunkReferenceTestStore {
	return &chunkReferenceTestStore{entries: make(map[string]*Entry)}
}

func (s *chunkReferenceTestStore) GetName() string                             { return "chunk_reference_test" }
func (s *chunkReferenceTestStore) Initialize(util.Configuration, string) error { return nil }
func (s *chunkReferenceTestStore) Shutdown()                                   {}
func (s *chunkReferenceTestStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *chunkReferenceTestStore) CommitTransaction(context.Context) error   { return nil }
func (s *chunkReferenceTestStore) RollbackTransaction(context.Context) error { return nil }
func (s *chunkReferenceTestStore) InsertEntry(_ context.Context, entry *Entry) error {
	s.entries[string(entry.FullPath)] = entry
	return nil
}
func (s *chunkReferenceTestStore) UpdateEntry(_ context.Context, entry *Entry) error {
	s.entries[string(entry.FullPath)] = entry
	return nil
}
func (s *chunkReferenceTestStore) FindEntry(_ context.Context, p util.FullPath) (*Entry, error) {
	if entry, ok := s.entries[string(p)]; ok {
		return entry, nil
	}
	return nil, filer_pb.ErrNotFound
}
func (s *chunkReferenceTestStore) DeleteEntry(_ context.Context, p util.FullPath) error {
	delete(s.entries, string(p))
	return nil
}
func (s *chunkReferenceTestStore) DeleteFolderChildren(_ context.Context, p util.FullPath) error {
	prefix := childPrefix(p)
	for path := range s.entries {
		if strings.HasPrefix(path, prefix) {
			delete(s.entries, path)
		}
	}
	return nil
}
func (s *chunkReferenceTestStore) ListDirectoryEntries(_ context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (string, error) {
	names := s.childNames(dirPath, startFileName, includeStartFile, "")
	lastFileName := ""
	for i, name := range names {
		if int64(i) >= limit {
			break
		}
		entry := s.entries[childPath(dirPath, name)]
		cont, err := eachEntryFunc(entry)
		if err != nil {
			return lastFileName, err
		}
		lastFileName = name
		if !cont {
			break
		}
	}
	return lastFileName, nil
}
func (s *chunkReferenceTestStore) ListDirectoryPrefixedEntries(_ context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (string, error) {
	names := s.childNames(dirPath, startFileName, includeStartFile, prefix)
	lastFileName := ""
	for i, name := range names {
		if int64(i) >= limit {
			break
		}
		entry := s.entries[childPath(dirPath, name)]
		cont, err := eachEntryFunc(entry)
		if err != nil {
			return lastFileName, err
		}
		lastFileName = name
		if !cont {
			break
		}
	}
	return lastFileName, nil
}
func (s *chunkReferenceTestStore) KvPut(context.Context, []byte, []byte) error { return nil }
func (s *chunkReferenceTestStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, ErrKvNotFound
}
func (s *chunkReferenceTestStore) KvDelete(context.Context, []byte) error { return nil }

func (s *chunkReferenceTestStore) childNames(dirPath util.FullPath, startFileName string, includeStartFile bool, namePrefix string) []string {
	prefix := childPrefix(dirPath)
	var names []string
	for path := range s.entries {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		rest := path[len(prefix):]
		if strings.Contains(rest, "/") || rest == "" {
			continue
		}
		if namePrefix != "" && !strings.HasPrefix(rest, namePrefix) {
			continue
		}
		if rest > startFileName || (includeStartFile && rest == startFileName) {
			names = append(names, rest)
		}
	}
	sort.Strings(names)
	return names
}

func childPrefix(dirPath util.FullPath) string {
	if dirPath == "/" {
		return "/"
	}
	return string(dirPath) + "/"
}

func childPath(dirPath util.FullPath, name string) string {
	if dirPath == "/" {
		return "/" + name
	}
	return string(dirPath) + "/" + name
}
