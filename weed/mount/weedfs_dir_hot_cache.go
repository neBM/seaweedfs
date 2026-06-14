package mount

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type listDirectoryEntriesFunc func(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) error

type hotDirectoryListingCache struct {
	sync.RWMutex
	entries map[util.FullPath][]*filer.Entry
}

func (wfs *WFS) listCachedDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) error {
	if wfs.listDirectoryEntriesFn != nil {
		return wfs.listDirectoryEntriesFn(ctx, dirPath, startFileName, includeStartFile, limit, eachEntryFunc)
	}
	return wfs.metaCache.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, eachEntryFunc)
}

func cloneDirectoryEntries(entries []*filer.Entry) []*filer.Entry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]*filer.Entry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, entry.ShallowClone())
	}
	return cloned
}

func (wfs *WFS) rememberHotDirectoryListing(dirPath util.FullPath, entries []*filer.Entry) {
	if len(entries) == 0 {
		return
	}
	cloned := cloneDirectoryEntries(entries)
	wfs.dirListingCache.Lock()
	if wfs.dirListingCache.entries == nil {
		wfs.dirListingCache.entries = make(map[util.FullPath][]*filer.Entry)
	}
	wfs.dirListingCache.entries[dirPath] = cloned
	wfs.dirListingCache.Unlock()
}

func (wfs *WFS) seedDirectoryHandleFromHotListing(dh *DirectoryHandle, dirPath util.FullPath) bool {
	if !wfs.inodeToPath.IsChildrenCached(dirPath) || wfs.inodeToPath.ShouldReadDirectoryDirect(dirPath) {
		return false
	}
	wfs.dirListingCache.RLock()
	entries, found := wfs.dirListingCache.entries[dirPath]
	wfs.dirListingCache.RUnlock()
	if !found || len(entries) == 0 {
		return false
	}
	dh.entryStream = append(dh.entryStream[:0], entries...)
	dh.isFinished = true
	return true
}

func (wfs *WFS) clearHotDirectoryListing(dirPath util.FullPath) {
	wfs.dirListingCache.Lock()
	delete(wfs.dirListingCache.entries, dirPath)
	wfs.dirListingCache.Unlock()
}

func (wfs *WFS) clearAllHotDirectoryListings() {
	wfs.dirListingCache.Lock()
	clear(wfs.dirListingCache.entries)
	wfs.dirListingCache.Unlock()
}

func (wfs *WFS) invalidateDirectoryCacheWithReason(dirPath util.FullPath, reason string) {
	wfs.clearHotDirectoryListing(dirPath)
	wfs.inodeToPath.InvalidateChildrenCacheWithReason(dirPath, reason)
}
