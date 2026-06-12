package storage

import (
	"os"

	idx2 "github.com/seaweedfs/seaweedfs/weed/storage/idx"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func CollectLiveFileKeysFromIndex(indexFileName string) ([]uint64, error) {
	indexFile, err := os.Open(indexFileName)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	var keys []uint64
	if err := idx2.WalkIndexFile(indexFile, 0, func(key NeedleId, offset Offset, size Size) error {
		if offset.IsZero() || size.IsDeleted() || !size.IsValid() {
			return nil
		}
		keys = append(keys, uint64(key))
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}
