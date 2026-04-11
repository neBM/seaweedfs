package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// ReadChunkWithReLookup looks up volume-server URLs for fileId, invokes
// fetchFn with them, and on failure invalidates the vidMap entry and
// re-looks-up exactly once. If the re-lookup returns the same URLs it
// returns the original error. Bounded — exactly one re-lookup per call,
// never loops.
//
// fetchFn is called with the current slice of urlStrings and returns
// (bytesWritten, err). If bytesWritten > 0 and err != nil, the cache is
// invalidated for the NEXT reader but no retry is attempted (the
// caller's output is already tainted).
//
// If masterClient does not implement CacheInvalidator, the helper
// degrades gracefully: it still performs the initial lookup + fetch,
// but skips invalidation and re-lookup on failure.
func ReadChunkWithReLookup(
	ctx context.Context,
	masterClient wdclient.HasLookupFileIdFunction,
	fileId string,
	fetchFn func(urlStrings []string) (written int, err error),
) (int, error) {
	urls, err := masterClient.GetLookupFileIdFunction()(ctx, fileId)
	if err != nil {
		return 0, err
	}
	return fetchFn(urls)
}
