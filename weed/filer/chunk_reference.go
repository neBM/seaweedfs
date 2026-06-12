package filer

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
	statuses, _, err := f.CheckChunkReferencesForVolume(ctx, fileIds, includePaths, 0, nil)
	return statuses, err
}

func (f *Filer) CheckChunkReferencesForVolume(ctx context.Context, fileIds []string, includePaths bool, volumeId uint32, presentFileKeys []uint64) (map[string]*filer_pb.ChunkReferenceStatus, []string, error) {
	statuses := make(map[string]*filer_pb.ChunkReferenceStatus, len(fileIds))
	needed := make(map[string]struct{}, len(fileIds))
	for _, fileId := range fileIds {
		if fileId == "" {
			continue
		}
		statuses[fileId] = &filer_pb.ChunkReferenceStatus{}
		needed[fileId] = struct{}{}
	}
	presentKeys := make(map[uint64]struct{}, len(presentFileKeys))
	for _, key := range presentFileKeys {
		presentKeys[key] = struct{}{}
	}
	if len(needed) == 0 && volumeId == 0 {
		return statuses, nil, nil
	}

	if f.ChunkLeases != nil {
		for fileId, leased := range f.ChunkLeases.Snapshot(fileIds) {
			if leased {
				statuses[fileId].Leased = true
			}
		}
	}

	missing := make(map[string]struct{})
	if volumeId > 0 && f.ChunkLeases != nil {
		for _, fileId := range f.ChunkLeases.ActiveFileIdsForVolume(volumeId) {
			if !isFileKeyPresent(fileId, presentKeys) {
				missing[fileId] = struct{}{}
			}
		}
	}

	if err := f.walkChunkReferences(ctx, "/", needed, includePaths, statuses, volumeId, presentKeys, missing); err != nil {
		return nil, nil, err
	}
	return statuses, sortedMissingFileIds(missing), nil
}

func (f *Filer) walkChunkReferences(ctx context.Context, dir util.FullPath, needed map[string]struct{}, includePaths bool, statuses map[string]*filer_pb.ChunkReferenceStatus, volumeId uint32, presentKeys map[uint64]struct{}, missing map[string]struct{}) error {
	lastFileName := ""
	for {
		var sawEntry bool
		nextLastFileName, err := f.StreamListDirectoryEntries(ctx, dir, lastFileName, false, PaginationSize, "", "", "", func(entry *Entry) (bool, error) {
			sawEntry = true
			if err := f.markEntryChunkReferences(ctx, entry, needed, includePaths, statuses, volumeId, presentKeys, missing); err != nil {
				return false, err
			}
			if entry.IsDirectory() {
				if err := f.walkChunkReferences(ctx, entry.FullPath, needed, includePaths, statuses, volumeId, presentKeys, missing); err != nil {
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

func (f *Filer) markEntryChunkReferences(ctx context.Context, entry *Entry, needed map[string]struct{}, includePaths bool, statuses map[string]*filer_pb.ChunkReferenceStatus, volumeId uint32, presentKeys map[uint64]struct{}, missing map[string]struct{}) error {
	for _, chunk := range entry.GetChunks() {
		if err := f.markChunkReference(ctx, chunk, entry.FullPath, needed, includePaths, statuses, volumeId, presentKeys, missing); err != nil {
			return err
		}
	}
	return nil
}

func (f *Filer) markChunkReference(ctx context.Context, chunk *filer_pb.FileChunk, fullPath util.FullPath, needed map[string]struct{}, includePaths bool, statuses map[string]*filer_pb.ChunkReferenceStatus, volumeId uint32, presentKeys map[uint64]struct{}, missing map[string]struct{}) error {
	fileId := chunk.GetFileIdString()
	f.markOneChunkReference(fileId, fullPath, needed, includePaths, statuses, volumeId, presentKeys, missing)
	if !chunk.IsChunkManifest {
		return nil
	}
	dataChunks, err := ResolveOneChunkManifest(ctx, f.MasterClient.LookupFileId, chunk)
	if err != nil {
		return err
	}
	for _, dataChunk := range dataChunks {
		f.markOneChunkReference(dataChunk.GetFileIdString(), fullPath, needed, includePaths, statuses, volumeId, presentKeys, missing)
	}
	return nil
}

func (f *Filer) markOneChunkReference(fileId string, fullPath util.FullPath, needed map[string]struct{}, includePaths bool, statuses map[string]*filer_pb.ChunkReferenceStatus, volumeId uint32, presentKeys map[uint64]struct{}, missing map[string]struct{}) {
	if _, ok := needed[fileId]; ok {
		status := statuses[fileId]
		status.Referenced = true
		if includePaths && len(status.Paths) < 10 {
			status.Paths = append(status.Paths, string(fullPath))
		}
	}
	if volumeId > 0 && !isFileKeyPresent(fileId, presentKeys) && isFileIdInVolume(fileId, volumeId) {
		missing[fileId] = struct{}{}
	}
}

func (m *ChunkLeaseManager) ActiveFileIdsForVolume(volumeId uint32) []string {
	if m == nil {
		return nil
	}
	now := m.nowFunc()
	m.mu.Lock()
	defer m.mu.Unlock()
	var fileIds []string
	for fileId, expiresAt := range m.leases {
		if !expiresAt.After(now) {
			delete(m.leases, fileId)
			continue
		}
		if isFileIdInVolume(fileId, volumeId) {
			fileIds = append(fileIds, fileId)
		}
	}
	return fileIds
}

func isFileKeyPresent(fileId string, presentKeys map[uint64]struct{}) bool {
	parsed, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return false
	}
	_, ok := presentKeys[uint64(parsed.Key)]
	return ok
}

func isFileIdInVolume(fileId string, volumeId uint32) bool {
	parsed, err := needle.ParseFileIdFromString(fileId)
	return err == nil && uint32(parsed.VolumeId) == volumeId
}

func sortedMissingFileIds(missing map[string]struct{}) []string {
	fileIds := make([]string, 0, len(missing))
	for fileId := range missing {
		fileIds = append(fileIds, fileId)
	}
	sort.Strings(fileIds)
	return fileIds
}
