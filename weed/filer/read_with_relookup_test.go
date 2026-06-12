package filer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// stubMasterClient implements HasLookupFileIdFunction and CacheInvalidator
// for unit tests. Each lookup call returns the value at head of queued
// responses, or the last one forever if the queue is exhausted.
type stubMasterClient struct {
	// lookups is a queue of (urls, err) to return on successive lookups.
	lookups []stubLookupResult
	// lookupCalls counts how many times the lookup function was invoked.
	lookupCalls int32
	// invalidateCalls counts InvalidateCache invocations.
	invalidateCalls int32
	// invalidatedIDs records the fileIds passed to InvalidateCache.
	invalidatedIDs []string
}

type stubLookupResult struct {
	urls []string
	err  error
}

func (s *stubMasterClient) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(ctx context.Context, fileId string) ([]string, error) {
		n := atomic.AddInt32(&s.lookupCalls, 1)
		idx := int(n) - 1
		if idx >= len(s.lookups) {
			idx = len(s.lookups) - 1
		}
		if idx < 0 {
			return nil, errors.New("stub: no lookups configured")
		}
		r := s.lookups[idx]
		return r.urls, r.err
	}
}

func (s *stubMasterClient) InvalidateCache(fileId string) {
	atomic.AddInt32(&s.invalidateCalls, 1)
	s.invalidatedIDs = append(s.invalidatedIDs, fileId)
}

// Compile-time assertions.
var _ wdclient.HasLookupFileIdFunction = (*stubMasterClient)(nil)
var _ CacheInvalidator = (*stubMasterClient)(nil)

func TestReadChunkWithReLookup_HappyPath(t *testing.T) {
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://alive:8080"}},
		},
	}
	var fetchCalls int
	written, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc",
		func(urls []string) (int, error) {
			fetchCalls++
			if len(urls) != 1 || urls[0] != "http://alive:8080" {
				t.Fatalf("unexpected urls: %v", urls)
			}
			return 42, nil
		})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if written != 42 {
		t.Errorf("written = %d, want 42", written)
	}
	if fetchCalls != 1 {
		t.Errorf("fetchCalls = %d, want 1", fetchCalls)
	}
	if atomic.LoadInt32(&mc.invalidateCalls) != 0 {
		t.Errorf("invalidateCalls = %d, want 0", mc.invalidateCalls)
	}
}

func TestReadChunkWithReLookup_StaleThenFresh(t *testing.T) {
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://stale:8080"}},
			{urls: []string{"http://fresh:8080"}},
		},
	}
	var fetchCalls int
	written, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc",
		func(urls []string) (int, error) {
			fetchCalls++
			if urls[0] == "http://stale:8080" {
				return 0, errors.New("connect: connection timed out")
			}
			return 100, nil
		})
	if err != nil {
		t.Fatalf("expected success after re-lookup, got %v", err)
	}
	if written != 100 {
		t.Errorf("written = %d, want 100", written)
	}
	if fetchCalls != 2 {
		t.Errorf("fetchCalls = %d, want 2", fetchCalls)
	}
	if atomic.LoadInt32(&mc.invalidateCalls) != 1 {
		t.Errorf("invalidateCalls = %d, want 1", mc.invalidateCalls)
	}
	if len(mc.invalidatedIDs) != 1 || mc.invalidatedIDs[0] != "1,abc" {
		t.Errorf("invalidatedIDs = %v, want [1,abc]", mc.invalidatedIDs)
	}
}

func TestReadChunkWithReLookup_StaleThenSame(t *testing.T) {
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://stale:8080"}},
			{urls: []string{"http://stale:8080"}}, // master hasn't propagated yet
		},
	}
	fetchErr := errors.New("connect: connection timed out")
	written, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc",
		func(urls []string) (int, error) {
			return 0, fetchErr
		})
	if !errors.Is(err, fetchErr) {
		t.Errorf("expected original fetch error, got %v", err)
	}
	if written != 0 {
		t.Errorf("written = %d, want 0", written)
	}
	if atomic.LoadInt32(&mc.invalidateCalls) != 1 {
		t.Errorf("invalidateCalls = %d, want 1", mc.invalidateCalls)
	}
	if atomic.LoadInt32(&mc.lookupCalls) != 2 {
		t.Errorf("lookupCalls = %d, want 2", mc.lookupCalls)
	}
}

func TestReadChunkWithReLookup_StaleThenLookupErr(t *testing.T) {
	lookupErr := errors.New("master unreachable")
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://stale:8080"}},
			{err: lookupErr},
		},
	}
	fetchErr := errors.New("connect: connection timed out")
	_, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc",
		func(urls []string) (int, error) {
			return 0, fetchErr
		})
	if !errors.Is(err, fetchErr) {
		t.Errorf("expected original fetch error (not lookup error), got %v", err)
	}
	if atomic.LoadInt32(&mc.invalidateCalls) != 1 {
		t.Errorf("invalidateCalls = %d, want 1 (invalidate still happens)", mc.invalidateCalls)
	}
}

// nonInvalidatingMasterClient implements HasLookupFileIdFunction but
// deliberately does NOT implement CacheInvalidator.
type nonInvalidatingMasterClient struct {
	urls []string
}

func (m *nonInvalidatingMasterClient) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(ctx context.Context, fileId string) ([]string, error) {
		return m.urls, nil
	}
}

func TestReadChunkWithReLookup_NonInvalidator(t *testing.T) {
	mc := &nonInvalidatingMasterClient{urls: []string{"http://any:8080"}}
	fetchErr := errors.New("connect: connection timed out")
	var fetchCalls int
	_, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc",
		func(urls []string) (int, error) {
			fetchCalls++
			return 0, fetchErr
		})
	if !errors.Is(err, fetchErr) {
		t.Errorf("expected fetch error, got %v", err)
	}
	if fetchCalls != 1 {
		t.Errorf("fetchCalls = %d, want 1 (no retry without invalidator)", fetchCalls)
	}
}

func TestReadChunkWithReLookup_PartialWrite(t *testing.T) {
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://stale:8080"}},
			{urls: []string{"http://fresh:8080"}}, // should NOT be consulted
		},
	}
	fetchErr := errors.New("read failed mid-chunk")
	var fetchCalls int
	written, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc",
		func(urls []string) (int, error) {
			fetchCalls++
			return 512, fetchErr // partial write
		})
	if !errors.Is(err, fetchErr) {
		t.Errorf("expected original fetch error, got %v", err)
	}
	if written != 512 {
		t.Errorf("written = %d, want 512", written)
	}
	if fetchCalls != 1 {
		t.Errorf("fetchCalls = %d, want 1 (no retry after partial write)", fetchCalls)
	}
	if atomic.LoadInt32(&mc.invalidateCalls) != 1 {
		t.Errorf("invalidateCalls = %d, want 1 (still invalidate for next reader)", mc.invalidateCalls)
	}
	if atomic.LoadInt32(&mc.lookupCalls) != 1 {
		t.Errorf("lookupCalls = %d, want 1 (no re-lookup after partial write)", mc.lookupCalls)
	}
}

// dialTimeoutErr wraps context.DeadlineExceeded the same way net.Dialer
// with a Timeout set does: errors.Is(err, context.DeadlineExceeded) is
// true, but the caller's context is not cancelled. This is exactly the
// error surfaced by the weed http client after the dial-timeout follow-up
// shipped in v0.1.11 — without the ctx.Err() check in the helper, this
// error was (incorrectly) treated as caller cancellation and skipped
// invalidation, making the whole vidMap-relookup machinery a no-op.
type dialTimeoutErr struct{}

func (dialTimeoutErr) Error() string { return "dial tcp 10.0.0.1:8080: i/o timeout" }
func (dialTimeoutErr) Timeout() bool { return true }
func (dialTimeoutErr) Is(target error) bool {
	return target == context.DeadlineExceeded
}

// TestReadChunkWithReLookup_StaleThenFreshOnDialTimeout exercises the
// dial-timeout regression: a fetch error that satisfies
// errors.Is(err, context.DeadlineExceeded) must NOT bail early when the
// caller's context is still alive. The helper has to invalidate + re-lookup
// and recover against the fresh URL.
func TestReadChunkWithReLookup_StaleThenFreshOnDialTimeout(t *testing.T) {
	if !errors.Is(dialTimeoutErr{}, context.DeadlineExceeded) {
		t.Fatalf("test precondition broken: dialTimeoutErr{} must satisfy errors.Is(context.DeadlineExceeded)")
	}
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://stale:8080"}},
			{urls: []string{"http://fresh:8080"}},
		},
	}
	var fetchCalls int
	written, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc",
		func(urls []string) (int, error) {
			fetchCalls++
			if urls[0] == "http://stale:8080" {
				return 0, dialTimeoutErr{}
			}
			return 100, nil
		})
	if err != nil {
		t.Fatalf("expected success after re-lookup, got %v", err)
	}
	if written != 100 {
		t.Errorf("written = %d, want 100", written)
	}
	if fetchCalls != 2 {
		t.Errorf("fetchCalls = %d, want 2", fetchCalls)
	}
	if atomic.LoadInt32(&mc.invalidateCalls) != 1 {
		t.Errorf("invalidateCalls = %d, want 1", mc.invalidateCalls)
	}
}

func TestReadChunkWithReLookup_ContextCanceled(t *testing.T) {
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://alive:8080"}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled when fetchFn runs
	_, err := ReadChunkWithReLookup(ctx, mc, "1,abc",
		func(urls []string) (int, error) {
			return 0, ctx.Err()
		})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if atomic.LoadInt32(&mc.invalidateCalls) != 0 {
		t.Errorf("invalidateCalls = %d, want 0 (ctx cancellation must not trigger invalidation)", mc.invalidateCalls)
	}
}

func TestReadChunkWithReLookup_ConcurrentInvalidation(t *testing.T) {
	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{"http://stale:8080"}},
			{urls: []string{"http://fresh:8080"}},
		},
	}
	var wg sync.WaitGroup
	const goroutines = 16
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = ReadChunkWithReLookup(context.Background(), mc, "1,abc",
				func(urls []string) (int, error) {
					if len(urls) > 0 && urls[0] == "http://stale:8080" {
						return 0, errors.New("stale")
					}
					return 1, nil
				})
		}()
	}
	wg.Wait()
	// Invariant: at least one invalidate happened. Over-counting is
	// acceptable (documented in spec).
	if atomic.LoadInt32(&mc.invalidateCalls) < 1 {
		t.Errorf("invalidateCalls = %d, want >= 1", mc.invalidateCalls)
	}
}

func TestFetchChunkRangeReLookupDoesNotRetryStaleUrlsBeforeInvalidation(t *testing.T) {
	oldRetryWaitTime := util.RetryWaitTime
	util.RetryWaitTime = 2 * time.Second
	defer func() {
		util.RetryWaitTime = oldRetryWaitTime
	}()

	var staleHits int32
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&staleHits, 1)
		http.Error(w, "old volume server", http.StatusInternalServerError)
	}))
	defer stale.Close()

	fresh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("fresh"))
	}))
	defer fresh.Close()

	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{stale.URL}},
			{urls: []string{fresh.URL}},
		},
	}

	buffer := make([]byte, len("fresh"))
	n, err := fetchChunkRange(context.Background(), buffer, nil, mc, "1,abc", nil, false, 0)
	if err != nil {
		t.Fatalf("expected success after stale-url invalidation and relookup, got %v", err)
	}
	if n != len("fresh") || string(buffer) != "fresh" {
		t.Fatalf("fetched %d bytes %q, want %d bytes %q", n, string(buffer), len("fresh"), "fresh")
	}
	if got := atomic.LoadInt32(&staleHits); got != 1 {
		t.Fatalf("stale URL hits = %d, want 1 before invalidation/relookup", got)
	}
	if got := atomic.LoadInt32(&mc.invalidateCalls); got != 1 {
		t.Fatalf("invalidateCalls = %d, want 1", got)
	}
}

func TestFullChunkReLookupDoesNotRetryStaleUrlsBeforeInvalidation(t *testing.T) {
	oldRetryWaitTime := util.RetryWaitTime
	util.RetryWaitTime = 2 * time.Second
	defer func() {
		util.RetryWaitTime = oldRetryWaitTime
	}()

	var staleHits int32
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&staleHits, 1)
		http.Error(w, "old volume server", http.StatusInternalServerError)
	}))
	defer stale.Close()

	fresh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("fresh"))
	}))
	defer fresh.Close()

	mc := &stubMasterClient{
		lookups: []stubLookupResult{
			{urls: []string{stale.URL}},
			{urls: []string{fresh.URL}},
		},
	}

	buffer := make([]byte, len("fresh"))
	n, err := ReadChunkWithReLookup(context.Background(), mc, "1,abc", func(urls []string) (int, error) {
		return util_http.FetchChunkDataOnce(context.Background(), buffer, urls, nil, false, true, 0, "1,abc")
	})
	if err != nil {
		t.Fatalf("expected full-chunk success after stale-url invalidation and relookup, got %v", err)
	}
	if n != len("fresh") || string(buffer) != "fresh" {
		t.Fatalf("fetched %d bytes %q, want %d bytes %q", n, string(buffer), len("fresh"), "fresh")
	}
	if got := atomic.LoadInt32(&staleHits); got != 1 {
		t.Fatalf("stale URL hits = %d, want 1 before invalidation/relookup", got)
	}
	if got := atomic.LoadInt32(&mc.invalidateCalls); got != 1 {
		t.Fatalf("invalidateCalls = %d, want 1", got)
	}
}
