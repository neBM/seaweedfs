package mount

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/postgres2"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type postgresStoreConfig struct {
	strings map[string]string
	ints    map[string]int
	bools   map[string]bool
}

func (c postgresStoreConfig) GetString(key string) string              { return c.strings[key] }
func (c postgresStoreConfig) GetBool(key string) bool                  { return c.bools[key] }
func (c postgresStoreConfig) GetInt(key string) int                    { return c.ints[key] }
func (c postgresStoreConfig) GetStringSlice(key string) []string       { return nil }
func (c postgresStoreConfig) SetDefault(key string, value interface{}) {}

func TestRegistryStyleCachedMetadataFlushUsesUnknownRevisionWithPostgres2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed mount regression in short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available in PATH")
	}

	store := newPostgres2RevisionStore(t)
	wfs, testServer := newCreateTestWFSWithStore(t, store)

	const (
		hashstateDir  = "/docker/registry/v2/repositories/library/busybox/_uploads/2d6fd9c4-3d31-4b11-b7b8-7fbcb5a4f31f/hashstates/sha256"
		hashstateName = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	)

	dirPath := util.FullPath(hashstateDir)
	fullPath := dirPath.Child(hashstateName)
	dirInode := wfs.inodeToPath.Lookup(dirPath, 123, true, false, 0, true)

	remoteEntry := &filer_pb.Entry{
		Name: hashstateName,
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			Uid:      0,
			Gid:      0,
			Crtime:   123,
			Mtime:    123,
			Ctime:    123,
		},
	}
	revision := int64(7)
	remoteEntry.EntryRevision = &revision

	requireNoError(t, testServer.store.InsertEntry(context.Background(), filer.FromPbEntry(hashstateDir, remoteEntry)))
	requireNoError(t, wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(hashstateDir, remoteEntry)))
	wfs.inodeToPath.MarkChildrenCached(dirPath)

	cachedEntry, status := wfs.maybeLoadEntry(fullPath)
	if status != fuse.OK {
		t.Fatalf("maybeLoadEntry status = %v, want OK", status)
	}
	if cachedEntry.GetEntryRevision() != 0 || cachedEntry.EntryRevision != nil {
		t.Fatalf("cached entry revision = %v, want nil after meta-cache round-trip", cachedEntry.GetEntryRevision())
	}

	out := &fuse.CreateOut{}
	status = wfs.Create(make(chan struct{}), &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: dirInode,
			Caller: fuse.Caller{
				Owner: fuse.Owner{
					Uid: 0,
					Gid: 0,
				},
			},
		},
		Flags: syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC,
		Mode:  0o644,
	}, hashstateName, out)
	if status != fuse.OK {
		t.Fatalf("Create status = %v, want OK", status)
	}

	fileHandle := wfs.GetHandle(FileHandleId(out.Fh))
	if fileHandle == nil {
		t.Fatal("Create did not return an open file handle")
	}
	fileHandle.dirtyMetadata = true

	if flushStatus := wfs.Flush(make(chan struct{}), &fuse.FlushIn{
		InHeader: fuse.InHeader{
			NodeId: out.NodeId,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: 0, Gid: 0}},
		},
		Fh: out.Fh,
	}); flushStatus != fuse.OK {
		t.Fatalf("Flush status = %v, want OK", flushStatus)
	}

	snapshot := testServer.snapshot()
	if snapshot.expected != nil {
		t.Fatalf("UpdateEntry expected revision = %d, want nil for cached registry entry", *snapshot.expected)
	}
	if snapshot.createExpected != nil {
		t.Fatalf("CreateEntry expected revision = %d, want nil for cached registry entry", *snapshot.createExpected)
	}
}

func newPostgres2RevisionStore(t *testing.T) filer.FilerStore {
	t.Helper()

	containerName := fmt.Sprintf("seaweedfs-mount-test-pg-%d-%d", os.Getpid(), time.Now().UnixNano())
	args := []string{
		"run", "-d", "--rm",
		"--name", containerName,
		"-e", "POSTGRES_USER=seaweedfs",
		"-e", "POSTGRES_PASSWORD=seaweedfs",
		"-e", "POSTGRES_DB=seaweedfs",
		"-p", "127.0.0.1::5432",
		"postgres:16-alpine",
	}
	if output, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("start postgres container: %v\n%s", err, string(output))
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	})

	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); {
		if err := exec.Command("docker", "exec", containerName, "pg_isready", "-U", "seaweedfs", "-d", "seaweedfs").Run(); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
			t.Fatalf("postgres container not ready within timeout\n%s", string(logs))
		}
	}

	portOutput, err := exec.Command(
		"docker", "inspect", "-f",
		`{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}`,
		containerName,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("inspect postgres port: %v\n%s", err, string(portOutput))
	}

	config := postgresStoreConfig{
		strings: map[string]string{
			"createTable": `
  CREATE TABLE IF NOT EXISTS "%s" (
    dirhash   BIGINT,
    name      VARCHAR(65535),
    directory VARCHAR(65535),
    meta      bytea,
    PRIMARY KEY (dirhash, name)
  );
`,
			"upsertQuery": `
  INSERT INTO "%[1]s" (dirhash, name, directory, meta)
    VALUES($1, $2, $3, $4)
    ON CONFLICT (dirhash, name) DO UPDATE SET
      directory=EXCLUDED.directory,
      meta=EXCLUDED.meta
`,
			"username": "seaweedfs",
			"password": "seaweedfs",
			"hostname": "127.0.0.1",
			"database": "seaweedfs",
			"schema":   "",
			"sslmode":  "disable",
		},
		ints: map[string]int{
			"port":                            mustAtoi(t, stringTrim(portOutput)),
			"connection_max_idle":             5,
			"connection_max_open":             10,
			"connection_max_lifetime_seconds": 60,
		},
		bools: map[string]bool{
			"enableUpsert":         true,
			"pgbouncer_compatible": false,
		},
	}

	store := &postgres2.PostgresStore2{}
	if err := store.Initialize(config, ""); err != nil {
		logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
		t.Fatalf("initialize postgres2 store: %v\n%s", err, string(logs))
	}
	return store
}

func mustAtoi(t *testing.T, value string) int {
	t.Helper()

	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		t.Fatalf("parse integer %q: %v", value, err)
	}
	return parsed
}

func stringTrim(data []byte) string {
	for len(data) > 0 && (data[len(data)-1] == '\n' || data[len(data)-1] == '\r' || data[len(data)-1] == ' ') {
		data = data[:len(data)-1]
	}
	for len(data) > 0 && data[0] == ' ' {
		data = data[1:]
	}
	return string(data)
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
