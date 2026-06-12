package command

import (
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func TestXYZ(t *testing.T) {
	glog.V(0).Infoln("Last-Modified", time.Unix(int64(1373273596), 0).UTC().Format(http.TimeFormat))
}

func TestParseFilerGrpcAddressesTreatsBarePortAsGrpc(t *testing.T) {
	addresses := parseFilerGrpcAddresses("seaweedfs-filer:18888")
	if len(addresses) != 1 {
		t.Fatalf("len(addresses) = %d, want 1", len(addresses))
	}
	if got, want := addresses[0].ToGrpcAddress(), "seaweedfs-filer:18888"; got != want {
		t.Fatalf("ToGrpcAddress() = %q, want %q", got, want)
	}
}

func TestParseFilerGrpcAddressesPreservesExplicitServerAddress(t *testing.T) {
	addresses := parseFilerGrpcAddresses("seaweedfs-filer:8888.18888")
	if len(addresses) != 1 {
		t.Fatalf("len(addresses) = %d, want 1", len(addresses))
	}
	if got, want := addresses[0].ToGrpcAddress(), "seaweedfs-filer:18888"; got != want {
		t.Fatalf("ToGrpcAddress() = %q, want %q", got, want)
	}
}
