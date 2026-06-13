package fuse_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// FuseTestFramework provides utilities for FUSE integration testing.
// It starts explicit master, volume, filer, and mount processes so the harness
// is independent of weed mini startup behavior.
type FuseTestFramework struct {
	t                     *testing.T
	tempDir               string
	mountPoint            string
	dataDir               string
	logDir                string
	masterProcess         *os.Process
	volumeProcess         *os.Process
	filerProcess          *os.Process
	mountProcess          *os.Process
	filerAddr             string
	masterPort            int
	masterGrpcPort        int
	volumePort            int
	volumeGrpcPort        int
	filerPort             int
	filerGrpcPort         int
	weedBinary            string
	weedBinaryErr         error
	filerConfigPath       string
	postgresContainerName string
	postgresHostPort      int
	isSetup               bool
}

var (
	weedBinaryOnce sync.Once
	weedBinaryPath string
	weedBinaryErr  error
)

type FilerStoreKind string

const (
	FilerStoreLevelDB2  FilerStoreKind = "leveldb2"
	FilerStorePostgres2 FilerStoreKind = "postgres2"
)

type PostgresBackendConfig struct {
	Image    string
	Username string
	Password string
	Database string
}

type FilerStoreConfig struct {
	Kind     FilerStoreKind
	Postgres *PostgresBackendConfig
}

// TestConfig holds configuration for FUSE tests
type TestConfig struct {
	Collection    string
	Replication   string
	ChunkSizeMB   int
	CacheSizeMB   int
	NumVolumes    int
	EnableDebug   bool
	DirAutoCreate bool
	FilerStore    FilerStoreConfig
	MountOptions  []string
	SkipCleanup   bool // for debugging failed tests
}

func DefaultPostgres2FilerStoreConfig() FilerStoreConfig {
	return FilerStoreConfig{
		Kind: FilerStorePostgres2,
		Postgres: &PostgresBackendConfig{
			Image:    "postgres:16-alpine",
			Username: "seaweedfs",
			Password: "seaweedfs",
			Database: "seaweedfs",
		},
	}
}

// DefaultTestConfig returns a default configuration for FUSE tests
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		Collection:    "",
		Replication:   "000",
		ChunkSizeMB:   4,
		CacheSizeMB:   100,
		NumVolumes:    3,
		EnableDebug:   false,
		DirAutoCreate: true,
		FilerStore: FilerStoreConfig{
			Kind: FilerStoreLevelDB2,
		},
		MountOptions: []string{},
		SkipCleanup:  false,
	}
}

// NewFuseTestFramework creates a new FUSE testing framework.
func NewFuseTestFramework(t *testing.T, config *TestConfig) *FuseTestFramework {
	if config == nil {
		config = DefaultTestConfig()
	}

	tempDir, err := os.MkdirTemp("", "seaweedfs_fuse_test_")
	require.NoError(t, err)

	ports := allocatePorts(t, 6)
	weedBinary, weedBinaryErr := findWeedBinary()

	return &FuseTestFramework{
		t:              t,
		tempDir:        tempDir,
		mountPoint:     filepath.Join(tempDir, "mount"),
		dataDir:        filepath.Join(tempDir, "data"),
		logDir:         filepath.Join(tempDir, "logs"),
		masterPort:     ports[0],
		masterGrpcPort: ports[1],
		volumePort:     ports[2],
		volumeGrpcPort: ports[3],
		filerPort:      ports[4],
		filerGrpcPort:  ports[5],
		filerAddr:      fmt.Sprintf("127.0.0.1:%d", ports[4]),
		weedBinary:     weedBinary,
		weedBinaryErr:  weedBinaryErr,
		isSetup:        false,
	}
}

func allocatePorts(t *testing.T, n int) []int {
	t.Helper()

	listeners := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()

	for i := 0; i < n; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}

	return ports
}

// Setup starts explicit master, volume, filer, and mount processes.
func (f *FuseTestFramework) Setup(config *TestConfig) error {
	if f.isSetup {
		return fmt.Errorf("framework already setup")
	}
	if f.weedBinaryErr != nil {
		return fmt.Errorf("failed to locate weed binary: %w", f.weedBinaryErr)
	}

	dirs := []string{f.mountPoint, f.logDir, f.dataDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	if err := f.prepareFilerStore(config); err != nil {
		return fmt.Errorf("prepare filer store: %w", err)
	}

	if err := f.startMaster(config); err != nil {
		return fmt.Errorf("failed to start weed master: %v", err)
	}
	if err := f.waitForService(f.masterHTTPAddr(), 30*time.Second); err != nil {
		f.dumpLog("master")
		return fmt.Errorf("weed master not ready: %v", err)
	}

	if err := f.startVolume(config); err != nil {
		return fmt.Errorf("failed to start weed volume: %v", err)
	}
	if err := f.waitForService(f.volumeHTTPAddr(), 30*time.Second); err != nil {
		f.dumpLog("volume")
		return fmt.Errorf("weed volume not ready: %v", err)
	}

	if err := f.startFiler(config); err != nil {
		return fmt.Errorf("failed to start weed filer: %v", err)
	}
	if err := f.waitForService(f.filerAddr, 30*time.Second); err != nil {
		f.dumpLog("filer")
		return fmt.Errorf("weed filer not ready: %v", err)
	}

	// Mount FUSE filesystem
	if err := f.mountFuse(config); err != nil {
		return fmt.Errorf("failed to mount FUSE: %v", err)
	}

	// Wait for mount to be ready
	if err := f.waitForMount(30 * time.Second); err != nil {
		f.dumpLog("mount")
		return fmt.Errorf("FUSE mount not ready: %v", err)
	}

	f.isSetup = true
	return nil
}

// Remount restarts only the FUSE mount process while keeping the backing
// SeaweedFS services alive. This forces the next open to rebuild state from
// filer-backed metadata instead of any previous in-process handles.
func (f *FuseTestFramework) Remount(config *TestConfig) error {
	if !f.isSetup {
		return fmt.Errorf("framework not setup")
	}

	if f.mountProcess != nil {
		if err := f.unmountFuse(); err != nil {
			return fmt.Errorf("unmount existing FUSE mount: %w", err)
		}
	}
	if err := f.mountFuse(config); err != nil {
		return fmt.Errorf("start replacement FUSE mount: %w", err)
	}
	if err := f.waitForMount(30 * time.Second); err != nil {
		f.dumpLog("mount")
		return fmt.Errorf("replacement FUSE mount not ready: %w", err)
	}
	return nil
}

// Cleanup stops all processes and removes temporary files.
// If the test failed, it dumps logs automatically.
func (f *FuseTestFramework) Cleanup() {
	if f.t.Failed() {
		f.DumpLogs()
		f.dumpPostgresLog()
	}

	if f.mountProcess != nil {
		f.unmountFuse()
	}

	// Stop processes in reverse order
	for _, proc := range []*os.Process{f.mountProcess, f.filerProcess, f.volumeProcess, f.masterProcess} {
		if proc != nil {
			proc.Signal(syscall.SIGTERM)
			proc.Wait()
		}
	}

	if f.postgresContainerName != "" {
		f.stopPostgresContainer()
	}

	f.copyLogsForCI()

	if !DefaultTestConfig().SkipCleanup {
		os.RemoveAll(f.tempDir)
	}
}

// DumpLogs prints the tail of all SeaweedFS process logs to test output.
func (f *FuseTestFramework) DumpLogs() {
	for _, name := range []string{"master", "volume", "filer", "mount"} {
		f.dumpLog(name)
	}
}

// GetMountPoint returns the FUSE mount point path
func (f *FuseTestFramework) GetMountPoint() string {
	return f.mountPoint
}

// GetFilerAddr returns the filer address
func (f *FuseTestFramework) GetFilerAddr() string {
	return f.filerAddr
}

func (f *FuseTestFramework) masterAddress() string {
	return fmt.Sprintf("127.0.0.1:%d.%d", f.masterPort, f.masterGrpcPort)
}

func (f *FuseTestFramework) masterHTTPAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", f.masterPort)
}

func (f *FuseTestFramework) volumeHTTPAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", f.volumePort)
}

func (f *FuseTestFramework) filerMountAddr() string {
	return fmt.Sprintf("127.0.0.1:%d.%d", f.filerPort, f.filerGrpcPort)
}

// startProcess is a helper that starts a weed sub-command with output captured
// to a log file in f.logDir.
func (f *FuseTestFramework) startProcess(name string, args []string) (*os.Process, error) {
	logFile, err := os.Create(filepath.Join(f.logDir, name+".log"))
	if err != nil {
		return nil, fmt.Errorf("create log file: %v", err)
	}
	cmd := exec.Command(f.weedBinary, args...)
	cmd.Dir = f.tempDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	// Close the file handle — the child process inherited it.
	logFile.Close()
	return cmd.Process, nil
}

// ReadProcessLog returns the captured log for a named SeaweedFS subprocess.
func (f *FuseTestFramework) ReadProcessLog(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(f.logDir, name+".log"))
}

func (f *FuseTestFramework) dumpPostgresLog() {
	if f.postgresContainerName == "" {
		return
	}
	output, err := exec.Command("docker", "logs", f.postgresContainerName).CombinedOutput()
	if err != nil {
		f.t.Logf("[postgres log] (not available: %v)", err)
		return
	}
	const maxTail = 16 * 1024
	if len(output) > maxTail {
		output = output[len(output)-maxTail:]
	}
	f.t.Logf("[postgres log tail (%d bytes)]\n%s", len(output), string(output))
}

// dumpLog prints the last lines of a process log file to the test output
// for debugging when a service fails to start or a test fails.
func (f *FuseTestFramework) dumpLog(name string) {
	data, err := os.ReadFile(filepath.Join(f.logDir, name+".log"))
	if err != nil {
		f.t.Logf("[%s log] (not available: %v)", name, err)
		return
	}
	// Show last 16KB on failure for meaningful context.
	const maxTail = 16 * 1024
	if len(data) > maxTail {
		data = data[len(data)-maxTail:]
	}
	f.t.Logf("[%s log tail (%d bytes)]\n%s", name, len(data), string(data))
}

// copyLogsForCI copies SeaweedFS process logs to /tmp/seaweedfs-fuse-logs/
// so the CI workflow can upload them as artifacts.
func (f *FuseTestFramework) copyLogsForCI() {
	ciLogDir := "/tmp/seaweedfs-fuse-logs"
	os.MkdirAll(ciLogDir, 0755)
	for _, name := range []string{"master", "volume", "filer", "mount"} {
		src := filepath.Join(f.logDir, name+".log")
		data, err := os.ReadFile(src)
		if err != nil {
			continue
		}
		os.WriteFile(filepath.Join(ciLogDir, name+".log"), data, 0644)
	}
	if f.postgresContainerName != "" {
		if data, err := exec.Command("docker", "logs", f.postgresContainerName).CombinedOutput(); err == nil {
			os.WriteFile(filepath.Join(ciLogDir, "postgres.log"), data, 0644)
		}
	}
}

func (f *FuseTestFramework) startMaster(config *TestConfig) error {
	args := []string{
		"master",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port=" + strconv.Itoa(f.masterPort),
		"-port.grpc=" + strconv.Itoa(f.masterGrpcPort),
		"-mdir=" + filepath.Join(f.dataDir, "master"),
	}
	if config.EnableDebug {
		args = append([]string{"-v=4"}, args...)
	}

	proc, err := f.startProcess("master", args)
	if err != nil {
		return err
	}
	f.masterProcess = proc
	return nil
}

func (f *FuseTestFramework) startVolume(config *TestConfig) error {
	volumeDir := filepath.Join(f.dataDir, "volume")
	if err := os.MkdirAll(volumeDir, 0o755); err != nil {
		return fmt.Errorf("create volume dir: %w", err)
	}

	args := []string{
		"volume",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port=" + strconv.Itoa(f.volumePort),
		"-port.grpc=" + strconv.Itoa(f.volumeGrpcPort),
		"-master=" + f.masterAddress(),
		"-dir=" + volumeDir,
		"-max=10",
	}
	if config.EnableDebug {
		args = append([]string{"-v=4"}, args...)
	}

	proc, err := f.startProcess("volume", args)
	if err != nil {
		return err
	}
	f.volumeProcess = proc
	return nil
}

func (f *FuseTestFramework) startFiler(config *TestConfig) error {
	filerDir := filepath.Join(f.dataDir, "filer")
	if err := os.MkdirAll(filerDir, 0o755); err != nil {
		return fmt.Errorf("create filer dir: %w", err)
	}

	args := []string{
		"filer",
		"-ip=127.0.0.1",
		"-ip.bind=127.0.0.1",
		"-port=" + strconv.Itoa(f.filerPort),
		"-port.grpc=" + strconv.Itoa(f.filerGrpcPort),
		"-master=" + f.masterAddress(),
		"-defaultStoreDir=" + filerDir,
	}
	if config.EnableDebug {
		args = append([]string{"-v=4"}, args...)
	}

	proc, err := f.startProcess("filer", args)
	if err != nil {
		return err
	}
	f.filerProcess = proc
	return nil
}

func (f *FuseTestFramework) prepareFilerStore(config *TestConfig) error {
	if config == nil {
		config = DefaultTestConfig()
	}
	switch config.FilerStore.Kind {
	case "", FilerStoreLevelDB2:
		return f.removeFilerConfig()
	case FilerStorePostgres2:
		pg := config.FilerStore.Postgres
		if pg == nil {
			return fmt.Errorf("postgres2 filer store requires postgres config")
		}
		if _, err := exec.LookPath("docker"); err != nil {
			f.t.Skip("docker not available in PATH, skipping postgres-backed FUSE regression")
		}
		if err := f.startPostgresContainer(pg); err != nil {
			return err
		}
		if err := f.writePostgresFilerConfig(pg); err != nil {
			f.stopPostgresContainer()
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported filer store kind %q", config.FilerStore.Kind)
	}
}

func (f *FuseTestFramework) removeFilerConfig() error {
	if f.filerConfigPath == "" {
		f.filerConfigPath = filepath.Join(f.tempDir, "filer.toml")
	}
	if err := os.Remove(f.filerConfigPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove filer config: %w", err)
	}
	return nil
}

func (f *FuseTestFramework) writePostgresFilerConfig(config *PostgresBackendConfig) error {
	f.filerConfigPath = filepath.Join(f.tempDir, "filer.toml")
	content := fmt.Sprintf(`[postgres2]
enabled = true
createTable = """
  CREATE TABLE IF NOT EXISTS "%%s" (
    dirhash   BIGINT,
    name      VARCHAR(65535),
    directory VARCHAR(65535),
    meta      bytea,
    PRIMARY KEY (dirhash, name)
  );
"""
hostname = "127.0.0.1"
port = %d
username = "%s"
password = "%s"
database = "%s"
schema = ""
sslmode = "disable"
connection_max_idle = 5
connection_max_open = 10
connection_max_lifetime_seconds = 60
pgbouncer_compatible = false
enableUpsert = true
upsertQuery = """
  INSERT INTO "%%[1]s" (dirhash, name, directory, meta)
    VALUES($1, $2, $3, $4)
    ON CONFLICT (dirhash, name) DO UPDATE SET
      directory=EXCLUDED.directory,
      meta=EXCLUDED.meta
"""
`, f.postgresHostPort, config.Username, config.Password, config.Database)
	if err := os.WriteFile(f.filerConfigPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write postgres filer config: %w", err)
	}
	return nil
}

func (f *FuseTestFramework) startPostgresContainer(config *PostgresBackendConfig) error {
	name := fmt.Sprintf("seaweedfs-fuse-pg-%d-%d", os.Getpid(), time.Now().UnixNano())
	args := []string{
		"run", "-d", "--rm",
		"--name", name,
		"-e", "POSTGRES_USER=" + config.Username,
		"-e", "POSTGRES_PASSWORD=" + config.Password,
		"-e", "POSTGRES_DB=" + config.Database,
		"-p", "127.0.0.1::5432",
		config.Image,
	}
	output, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("start postgres container: %w\n%s", err, string(output))
	}
	f.postgresContainerName = name
	port, err := f.lookupDockerMappedPort(name, "5432/tcp")
	if err != nil {
		f.stopPostgresContainer()
		return err
	}
	f.postgresHostPort = port
	if err := f.waitForPostgresReady(config); err != nil {
		f.stopPostgresContainer()
		return err
	}
	return nil
}

func (f *FuseTestFramework) stopPostgresContainer() {
	if f.postgresContainerName == "" {
		return
	}
	_ = exec.Command("docker", "rm", "-f", f.postgresContainerName).Run()
	f.postgresContainerName = ""
	f.postgresHostPort = 0
}

func (f *FuseTestFramework) lookupDockerMappedPort(containerName, containerPort string) (int, error) {
	output, err := exec.Command(
		"docker", "inspect", "-f",
		fmt.Sprintf(`{{(index (index .NetworkSettings.Ports %q) 0).HostPort}}`, containerPort),
		containerName,
	).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("inspect docker port mapping: %w\n%s", err, string(output))
	}
	port, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return 0, fmt.Errorf("parse docker port mapping %q: %w", strings.TrimSpace(string(output)), err)
	}
	return port, nil
}

func (f *FuseTestFramework) waitForPostgresReady(config *PostgresBackendConfig) error {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command(
			"docker", "exec", f.postgresContainerName,
			"pg_isready",
			"-U", config.Username,
			"-d", config.Database,
		)
		if err := cmd.Run(); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("postgres container %s not ready within timeout", f.postgresContainerName)
}

// mountFuse mounts the SeaweedFS FUSE filesystem
func (f *FuseTestFramework) mountFuse(config *TestConfig) error {
	args := []string{
		"mount",
		"-filer=" + f.filerMountAddr(),
		"-dir=" + f.mountPoint,
		"-filer.path=/",
		"-allowOthers=false",
	}
	if config.DirAutoCreate {
		args = append(args, "-dirAutoCreate")
	}

	if config.Collection != "" {
		args = append(args, "-collection="+config.Collection)
	}
	if config.Replication != "" {
		args = append(args, "-replication="+config.Replication)
	}
	if config.ChunkSizeMB > 0 {
		args = append(args, fmt.Sprintf("-chunkSizeLimitMB=%d", config.ChunkSizeMB))
	}
	if config.CacheSizeMB > 0 {
		args = append(args, fmt.Sprintf("-cacheCapacityMB=%d", config.CacheSizeMB))
	}
	if config.EnableDebug {
		args = append([]string{"-v=4"}, args...)
	}

	args = append(args, config.MountOptions...)

	proc, err := f.startProcess("mount", args)
	if err != nil {
		return err
	}
	f.mountProcess = proc
	return nil
}

// unmountFuse unmounts the FUSE filesystem
func (f *FuseTestFramework) unmountFuse() error {
	if f.mountProcess != nil {
		f.mountProcess.Signal(syscall.SIGTERM)
		f.mountProcess.Wait()
		f.mountProcess = nil
	}

	// Also try system unmount as backup
	exec.Command("fusermount3", "-u", f.mountPoint).Run()
	exec.Command("fusermount", "-u", f.mountPoint).Run()
	return nil
}

// waitForService waits for a service to be available
func (f *FuseTestFramework) waitForService(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("service at %s not ready within timeout", addr)
}

// waitForMount waits for the FUSE mount to be ready
func (f *FuseTestFramework) waitForMount(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(f.mountPoint); err == nil {
			if _, err := os.ReadDir(f.mountPoint); err == nil {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount point not ready within timeout")
}

// findWeedBinary locates the weed binary, building one under /tmp when needed
// so the integration suite can run from a clean checkout.
func findWeedBinary() (string, error) {
	if fromEnv := os.Getenv("WEED_BINARY"); fromEnv != "" {
		if isExecutableFile(fromEnv) {
			return fromEnv, nil
		}
		return "", fmt.Errorf("WEED_BINARY is set but not executable: %s", fromEnv)
	}

	if p, err := exec.LookPath("weed"); err == nil {
		return p, nil
	}

	candidates := []string{
		"../../weed/weed",
		"./weed",
		"../weed",
	}
	for _, candidate := range candidates {
		if isExecutableFile(candidate) {
			abs, _ := filepath.Abs(candidate)
			return abs, nil
		}
	}

	weedBinaryOnce.Do(func() {
		repoRoot := ""
		if _, file, _, ok := runtime.Caller(0); ok {
			repoRoot = filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
		}
		if repoRoot == "" {
			weedBinaryErr = errors.New("unable to detect SeaweedFS repository root")
			return
		}

		binDir := filepath.Join(os.TempDir(), "seaweedfs_fuse_it_bin")
		if err := os.MkdirAll(binDir, 0o755); err != nil {
			weedBinaryErr = fmt.Errorf("create binary directory %s: %w", binDir, err)
			return
		}
		binPath := filepath.Join(binDir, "weed")

		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", binPath, ".")
		cmd.Dir = filepath.Join(repoRoot, "weed")
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			weedBinaryErr = fmt.Errorf("build weed binary: %w\n%s", err, out.String())
			return
		}
		if !isExecutableFile(binPath) {
			weedBinaryErr = fmt.Errorf("built weed binary is not executable: %s", binPath)
			return
		}
		weedBinaryPath = binPath
	})

	if weedBinaryErr != nil {
		return "", weedBinaryErr
	}
	return weedBinaryPath, nil
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Mode().Perm()&0o111 != 0
}

// Helper functions for test assertions

// AssertFileExists checks if a file exists in the mount point
func (f *FuseTestFramework) AssertFileExists(relativePath string) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	_, err := os.Stat(fullPath)
	require.NoError(f.t, err, "file should exist: %s", relativePath)
}

// AssertFileNotExists checks if a file does not exist in the mount point
func (f *FuseTestFramework) AssertFileNotExists(relativePath string) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	_, err := os.Stat(fullPath)
	require.True(f.t, os.IsNotExist(err), "file should not exist: %s", relativePath)
}

// AssertFileContent checks if a file has expected content
func (f *FuseTestFramework) AssertFileContent(relativePath string, expectedContent []byte) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	actualContent, err := os.ReadFile(fullPath)
	require.NoError(f.t, err, "failed to read file: %s", relativePath)
	require.Equal(f.t, expectedContent, actualContent, "file content mismatch: %s", relativePath)
}

// AssertFileMode checks if a file has expected permissions
func (f *FuseTestFramework) AssertFileMode(relativePath string, expectedMode fs.FileMode) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	info, err := os.Stat(fullPath)
	require.NoError(f.t, err, "failed to stat file: %s", relativePath)
	require.Equal(f.t, expectedMode, info.Mode(), "file mode mismatch: %s", relativePath)
}

// CreateTestFile creates a test file with specified content
func (f *FuseTestFramework) CreateTestFile(relativePath string, content []byte) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	dir := filepath.Dir(fullPath)
	require.NoError(f.t, os.MkdirAll(dir, 0755), "failed to create directory: %s", dir)
	require.NoError(f.t, os.WriteFile(fullPath, content, 0644), "failed to create file: %s", relativePath)
}

// CreateTestDir creates a test directory
func (f *FuseTestFramework) CreateTestDir(relativePath string) {
	fullPath := filepath.Join(f.mountPoint, relativePath)
	require.NoError(f.t, os.MkdirAll(fullPath, 0755), "failed to create directory: %s", relativePath)
}
