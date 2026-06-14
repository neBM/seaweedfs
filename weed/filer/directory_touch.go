package filer

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TouchedDirectoryUpdate captures a parent-directory metadata change so callers
// can notify subscribers after the underlying mutation is durable.
type TouchedDirectoryUpdate struct {
	OldEntry *Entry
	NewEntry *Entry
}

// TouchDirectories updates directory mtime/ctime for each unique path and
// returns the old/new entry pairs for later notification. Root is updated
// in-memory only because it is synthetic and not persisted in the filer store.
func (f *Filer) TouchDirectories(ctx context.Context, dirs []util.FullPath) ([]TouchedDirectoryUpdate, error) {
	now := time.Now()
	seen := make(map[string]struct{}, len(dirs))
	updates := make([]TouchedDirectoryUpdate, 0, len(dirs))

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		key := string(dir)
		if _, found := seen[key]; found {
			continue
		}
		seen[key] = struct{}{}

		if key == "/" {
			Root.Attr.Mtime = now
			Root.Attr.Ctime = now
			continue
		}

		oldEntry, err := f.FindEntry(ctx, dir)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				continue
			}
			return nil, fmt.Errorf("touch directory %s: %w", dir, err)
		}
		if oldEntry == nil {
			continue
		}
		if !oldEntry.IsDirectory() {
			return nil, fmt.Errorf("touch directory %s: not a directory", dir)
		}

		newEntry := oldEntry.ShallowClone()
		newEntry.Attr.Mtime = now
		newEntry.Attr.Ctime = now
		if err := f.UpdateEntryWithExpectedRevision(ctx, oldEntry, newEntry, nil); err != nil {
			return nil, fmt.Errorf("touch directory %s: %w", dir, err)
		}

		updates = append(updates, TouchedDirectoryUpdate{
			OldEntry: oldEntry.ShallowClone(),
			NewEntry: newEntry.ShallowClone(),
		})
	}

	return updates, nil
}
