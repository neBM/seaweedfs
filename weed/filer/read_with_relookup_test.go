package filer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

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
