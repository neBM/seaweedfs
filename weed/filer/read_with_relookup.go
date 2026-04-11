package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/stats"
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
	lookupFn := masterClient.GetLookupFileIdFunction()
	urls, err := lookupFn(ctx, fileId)
	if err != nil {
		return 0, err
	}

	written, fetchErr := fetchFn(urls)
	if fetchErr == nil {
		return written, nil
	}

	// Only bail if the CALLER's context was cancelled — never on the
	// fetchErr itself. A Dialer.Timeout firing inside the HTTP transport
	// surfaces as an error that satisfies errors.Is(err, context.DeadlineExceeded),
	// but that is our own internal timeout, not caller cancellation, and
	// it is exactly the signal we want to turn into "invalidate + re-lookup".
	if ctx.Err() != nil {
		return written, fetchErr
	}

	inv, ok := masterClient.(CacheInvalidator)
	if !ok {
		stats.FilerVidMapRelookupCounter.WithLabelValues("no_invalidator").Inc()
		return written, fetchErr
	}

	inv.InvalidateCache(fileId)

	// Partial-write failures: cache invalidated for next reader, but
	// we cannot replay into the caller's writer.
	if written > 0 {
		stats.FilerVidMapRelookupCounter.WithLabelValues("partial_write").Inc()
		return written, fetchErr
	}

	newUrls, lookupErr := lookupFn(ctx, fileId)
	if lookupErr != nil {
		stats.FilerVidMapRelookupCounter.WithLabelValues("lookup_failed").Inc()
		return 0, fetchErr
	}
	if urlSlicesEqual(urls, newUrls) {
		stats.FilerVidMapRelookupCounter.WithLabelValues("same_urls").Inc()
		return 0, fetchErr
	}
	retryWritten, retryErr := fetchFn(newUrls)
	if retryErr == nil {
		stats.FilerVidMapRelookupCounter.WithLabelValues("success").Inc()
	} else {
		stats.FilerVidMapRelookupCounter.WithLabelValues("retry_failed").Inc()
	}
	return retryWritten, retryErr
}
