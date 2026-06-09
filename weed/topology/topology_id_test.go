package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
)

func TestSetTopologyIdFromReplayIgnoresConflictingId(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	if initialized := topo.SetTopologyIdFromReplay("cluster-a", "test"); !initialized {
		t.Fatal("expected first replayed topology id to initialize topology")
	}
	if got := topo.GetTopologyId(); got != "cluster-a" {
		t.Fatalf("expected initial topology id to be cluster-a, got %q", got)
	}

	if initialized := topo.SetTopologyIdFromReplay("cluster-b", "test"); initialized {
		t.Fatal("conflicting replayed topology id must not reinitialize topology")
	}
	if got := topo.GetTopologyId(); got != "cluster-a" {
		t.Fatalf("expected conflicting topology id to be ignored, got %q", got)
	}
}
