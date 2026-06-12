package filer

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const PendingChunkLeaseTTL = 24 * time.Hour

type ChunkLeaseManager struct {
	mu      sync.Mutex
	leases  map[string]time.Time
	nowFunc func() time.Time
}

func NewChunkLeaseManager() *ChunkLeaseManager {
	return &ChunkLeaseManager{
		leases:  make(map[string]time.Time),
		nowFunc: time.Now,
	}
}

func (m *ChunkLeaseManager) Lease(fileIds []string, ttl time.Duration) {
	if m == nil || len(fileIds) == 0 {
		return
	}
	expiresAt := m.nowFunc().Add(ttl)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, fileId := range fileIds {
		if fileId == "" {
			continue
		}
		m.leases[fileId] = expiresAt
	}
}

func (m *ChunkLeaseManager) Release(fileIds []string) {
	if m == nil || len(fileIds) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, fileId := range fileIds {
		delete(m.leases, fileId)
	}
}

func (m *ChunkLeaseManager) Snapshot(fileIds []string) map[string]bool {
	leased := make(map[string]bool, len(fileIds))
	if m == nil || len(fileIds) == 0 {
		return leased
	}
	needed := make(map[string]struct{}, len(fileIds))
	for _, fileId := range fileIds {
		needed[fileId] = struct{}{}
	}
	now := m.nowFunc()
	m.mu.Lock()
	defer m.mu.Unlock()
	for fileId, expiresAt := range m.leases {
		if !expiresAt.After(now) {
			delete(m.leases, fileId)
			continue
		}
		if _, ok := needed[fileId]; ok {
			leased[fileId] = true
		}
	}
	return leased
}

func (f *Filer) LeaseChunks(fileIds []string) {
	f.ChunkLeases.Lease(fileIds, PendingChunkLeaseTTL)
}

func (f *Filer) ReleaseChunkLeases(ctx context.Context, chunks []*filer_pb.FileChunk) {
	if f == nil || f.ChunkLeases == nil || len(chunks) == 0 {
		return
	}
	fileIds := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		fileIds = append(fileIds, chunk.GetFileIdString())
		if !chunk.IsChunkManifest {
			continue
		}
		dataChunks, err := ResolveOneChunkManifest(ctx, f.MasterClient.LookupFileId, chunk)
		if err != nil {
			continue
		}
		for _, dataChunk := range dataChunks {
			fileIds = append(fileIds, dataChunk.GetFileIdString())
		}
	}
	f.ChunkLeases.Release(fileIds)
}

func (f *Filer) CheckChunkReferences(ctx context.Context, fileIds []string, includePaths bool) (map[string]*filer_pb.ChunkReferenceStatus, error) {
	statuses := make(map[string]*filer_pb.ChunkReferenceStatus, len(fileIds))
	needed := make(map[string]struct{}, len(fileIds))
	for _, fileId := range fileIds {
		if fileId == "" {
			continue
		}
		statuses[fileId] = &filer_pb.ChunkReferenceStatus{}
		needed[fileId] = struct{}{}
	}
	if len(needed) == 0 {
		return statuses, nil
	}

	if f.ChunkLeases != nil {
		for fileId, leased := range f.ChunkLeases.Snapshot(fileIds) {
			if leased {
				statuses[fileId].Leased = true
			}
		}
	}

	if err := f.walkChunkReferences(ctx, "/", needed, includePaths, statuses); err != nil {
		return nil, err
	}
	return statuses, nil
}

func (f *Filer) walkChunkReferences(ctx context.Context, dir util.FullPath, needed map[string]struct{}, includePaths bool, statuses map[string]*filer_pb.ChunkReferenceStatus) error {
	lastFileName := ""
	for {
		var sawEntry bool
		nextLastFileName, err := f.StreamListDirectoryEntries(ctx, dir, lastFileName, false, PaginationSize, "", "", "", func(entry *Entry) (bool, error) {
			sawEntry = true
			if err := f.markEntryChunkReferences(ctx, entry, needed, includePaths, statuses); err != nil {
				return false, err
			}
			if entry.IsDirectory() {
				if err := f.walkChunkReferences(ctx, entry.FullPath, needed, includePaths, statuses); err != nil {
					return false, err
				}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if !sawEntry {
			return nil
		}
		lastFileName = nextLastFileName
	}
}

func (f *Filer) markEntryChunkReferences(ctx context.Context, entry *Entry, needed map[string]struct{}, includePaths bool, statuses map[string]*filer_pb.ChunkReferenceStatus) error {
	for _, chunk := range entry.GetChunks() {
		if err := f.markChunkReference(ctx, chunk, entry.FullPath, needed, includePaths, statuses); err != nil {
			return err
		}
	}
	return nil
}

func (f *Filer) markChunkReference(ctx context.Context, chunk *filer_pb.FileChunk, fullPath util.FullPath, needed map[string]struct{}, includePaths bool, statuses map[string]*filer_pb.ChunkReferenceStatus) error {
	fileId := chunk.GetFileIdString()
	if _, ok := needed[fileId]; ok {
		status := statuses[fileId]
		status.Referenced = true
		if includePaths && len(status.Paths) < 10 {
			status.Paths = append(status.Paths, string(fullPath))
		}
	}
	if !chunk.IsChunkManifest {
		return nil
	}
	dataChunks, err := ResolveOneChunkManifest(ctx, f.MasterClient.LookupFileId, chunk)
	if err != nil {
		return err
	}
	for _, dataChunk := range dataChunks {
		dataFileId := dataChunk.GetFileIdString()
		if _, ok := needed[dataFileId]; !ok {
			continue
		}
		status := statuses[dataFileId]
		status.Referenced = true
		if includePaths && len(status.Paths) < 10 {
			status.Paths = append(status.Paths, string(fullPath))
		}
	}
	return nil
}
