//go:build linux

package mount

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	hotRestartWFSEnabled  = "SEAWEEDFS_WFS_HOTRESTART_WORKER"
	hotRestartWFSMountFD  = "SEAWEEDFS_WFS_HOTRESTART_MOUNT_FD"
	hotRestartWFSAdoptFD  = "SEAWEEDFS_WFS_HOTRESTART_ADOPT_LIVE_FD"
	hotRestartWFSStatOnce = "SEAWEEDFS_WFS_HOTRESTART_STAT_ONCE"
	hotRestartWFSStatPath = "SEAWEEDFS_WFS_HOTRESTART_STAT_PATH"

	hotRestartWFSReadyLine    = "READY"
	hotRestartWFSFileName     = "hello.txt"
	hotRestartWFSFileContents = ""
	hotRestartWFSFileInode    = 1001
)

func TestHotRestartWFSWorkerHelper(t *testing.T) {
	if os.Getenv(hotRestartWFSEnabled) == "" {
		t.Skip("subprocess helper")
	}

	mountFD := os.Getenv(hotRestartWFSMountFD)
	if mountFD == "" {
		fmt.Fprintln(os.Stderr, "missing worker configuration")
		os.Exit(1)
	}

	wfs := newHotRestartLiveTestWFS(t)
	server, err := fuse.NewServer(
		wfs,
		fmt.Sprintf("/dev/fd/%s", mountFD),
		&fuse.MountOptions{
			SingleThreaded: true,
			AdoptLiveFD:    os.Getenv(hotRestartWFSAdoptFD) == "1",
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewServer: %v\n", err)
		os.Exit(1)
	}

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		fmt.Fprintf(os.Stderr, "WaitMount: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(hotRestartWFSReadyLine)
	_ = os.Stdout.Sync()
	_, _ = io.ReadAll(os.Stdin)
	os.Exit(0)
}

func TestHotRestartWFSStatOnceHelper(t *testing.T) {
	if os.Getenv(hotRestartWFSStatOnce) == "" {
		t.Skip("subprocess helper")
	}

	info, err := os.Stat(os.Getenv(hotRestartWFSStatPath))
	if err != nil {
		fmt.Printf("ERR:%v\n", err)
	} else {
		fmt.Printf("OK:%d:%t\n", info.Size(), info.Mode().IsRegular())
	}
	_ = os.Stdout.Sync()
	os.Exit(0)
}

func TestWFSWorkerReplacementOnLiveFuseFD(t *testing.T) {
	requireHotRestartFusePrereqs(t)

	session := newHotRestartLiveSession(t)
	defer session.Cleanup(t)

	worker1 := session.StartWorker(t, false)
	if result, timedOut := session.StatOnce(3 * time.Second); timedOut {
		worker1.Shutdown(t)
		t.Fatal("initial WFS stat timed out before replacement")
	} else if result != "OK:0:true" {
		worker1.Shutdown(t)
		t.Fatalf("unexpected initial WFS stat result: %q", result)
	}
	worker1.Shutdown(t)

	worker2 := session.StartWorkerNoWait(t, true)
	defer worker2.Shutdown(t)

	result := worker2.WaitForReady(3 * time.Second)
	if !result.ready {
		worker2.Terminate(t)
		t.Fatalf("WFS worker did not become ready after live-fd adoption: %s", worker2.Describe(result))
	}

	readResult, timedOut := session.StatOnce(3 * time.Second)
	if timedOut {
		if err := abortHotRestartFUSEConnection(session.connectionID); err != nil {
			t.Fatalf("abort fuse connection after timed out WFS stat: %v", err)
		}
		t.Fatal("replacement WFS stat timed out after live-fd adoption")
	}
	if readResult != "OK:0:true" {
		t.Fatalf("unexpected replacement WFS stat result: %q", readResult)
	}
}

type hotRestartLiveSession struct {
	mountPoint   string
	fuseConnFile *os.File
	connectionID uint64
}

func newHotRestartLiveSession(t *testing.T) *hotRestartLiveSession {
	t.Helper()

	rootDir := t.TempDir()
	mountPoint := filepath.Join(rootDir, "mount")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatalf("mkdir mount point: %v", err)
	}

	fd, err := callHotRestartFusermount(mountPoint)
	if err != nil {
		t.Fatalf("mount via fusermount: %v", err)
	}

	return &hotRestartLiveSession{
		mountPoint:   mountPoint,
		fuseConnFile: os.NewFile(uintptr(fd), "fuse-connection"),
	}
}

func (s *hotRestartLiveSession) Cleanup(t *testing.T) {
	t.Helper()

	if s.fuseConnFile == nil {
		return
	}
	if err := unmountHotRestartWithFusermount(s.mountPoint); err != nil {
		if s.connectionID != 0 {
			_ = abortHotRestartFUSEConnection(s.connectionID)
		}
		if lazyErr := lazyUnmountHotRestartWithFusermount(s.mountPoint); lazyErr != nil {
			t.Fatalf("unmount %s: %v; lazy unmount fallback: %v", s.mountPoint, err, lazyErr)
		}
	}
	if err := s.fuseConnFile.Close(); err != nil {
		t.Fatalf("close fuse connection: %v", err)
	}
	s.fuseConnFile = nil
}

func (s *hotRestartLiveSession) StartWorker(t *testing.T, adoptLiveFD bool) *hotRestartLiveWorker {
	t.Helper()

	worker := s.StartWorkerNoWait(t, adoptLiveFD)
	result := worker.WaitForReady(3 * time.Second)
	if !result.ready {
		worker.Terminate(t)
		t.Fatalf("worker did not become ready: %s", worker.Describe(result))
	}
	if s.connectionID == 0 {
		connectionID, err := lookupHotRestartFUSEConnectionID(s.mountPoint)
		if err != nil {
			worker.Terminate(t)
			t.Fatalf("lookup fuse connection id: %v", err)
		}
		s.connectionID = connectionID
	}
	return worker
}

func (s *hotRestartLiveSession) StartWorkerNoWait(t *testing.T, adoptLiveFD bool) *hotRestartLiveWorker {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestHotRestartWFSWorkerHelper$")
	cmd.Env = append(os.Environ(),
		hotRestartWFSEnabled+"=1",
		hotRestartWFSMountFD+"=3",
	)
	if adoptLiveFD {
		cmd.Env = append(cmd.Env, hotRestartWFSAdoptFD+"=1")
	}
	cmd.ExtraFiles = []*os.File{s.fuseConnFile}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}

	worker := &hotRestartLiveWorker{
		cmd:     cmd,
		stdin:   stdin,
		readyCh: make(chan struct{}),
		exitCh:  make(chan error, 1),
		stderr:  &stderr,
	}
	go worker.watchStdout(stdoutPipe)
	go func() {
		worker.exitCh <- cmd.Wait()
		close(worker.exitCh)
	}()

	return worker
}

func (s *hotRestartLiveSession) StatOnce(timeout time.Duration) (string, bool) {
	client := startHotRestartWFSStatOnce(filepath.Join(s.mountPoint, hotRestartWFSFileName))
	defer client.Close()
	return client.WaitForResult(timeout)
}

type hotRestartLiveWorker struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	readyCh   chan struct{}
	exitCh    chan error
	stderr    *bytes.Buffer
	readyOnce sync.Once
}

type hotRestartLiveWorkerResult struct {
	ready    bool
	exitErr  error
	timedOut bool
}

type hotRestartWFSReadOnceClient struct {
	cmd    *exec.Cmd
	lines  chan string
	exitCh chan error
	stderr *bytes.Buffer
}

func (w *hotRestartLiveWorker) watchStdout(stdout io.ReadCloser) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == hotRestartWFSReadyLine {
			w.readyOnce.Do(func() { close(w.readyCh) })
		}
	}
}

func (w *hotRestartLiveWorker) WaitForReady(timeout time.Duration) hotRestartLiveWorkerResult {
	select {
	case <-w.readyCh:
		return hotRestartLiveWorkerResult{ready: true}
	case err := <-w.exitCh:
		return hotRestartLiveWorkerResult{exitErr: err}
	case <-time.After(timeout):
		return hotRestartLiveWorkerResult{timedOut: true}
	}
}

func (w *hotRestartLiveWorker) Shutdown(t *testing.T) {
	t.Helper()

	if w.stdin != nil {
		_ = w.stdin.Close()
		w.stdin = nil
	}
	select {
	case err := <-w.exitCh:
		if err != nil {
			t.Fatalf("worker exit: %s", w.Describe(hotRestartLiveWorkerResult{exitErr: err}))
		}
	case <-time.After(3 * time.Second):
		w.Terminate(t)
		t.Fatal("worker did not exit after stdin close")
	}
}

func (w *hotRestartLiveWorker) Terminate(t *testing.T) {
	t.Helper()

	if w.cmd == nil || w.cmd.Process == nil {
		return
	}
	if w.stdin != nil {
		_ = w.stdin.Close()
		w.stdin = nil
	}

	select {
	case <-w.exitCh:
		return
	default:
	}

	_ = w.cmd.Process.Kill()
	select {
	case <-w.exitCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker kill")
	}
}

func (w *hotRestartLiveWorker) Describe(result hotRestartLiveWorkerResult) string {
	stderr := strings.TrimSpace(w.stderr.String())
	switch {
	case result.ready:
		return "ready"
	case result.timedOut:
		if stderr == "" {
			return "timed out without stderr"
		}
		return fmt.Sprintf("timed out; stderr=%q", stderr)
	case result.exitErr != nil:
		if stderr == "" {
			return result.exitErr.Error()
		}
		return fmt.Sprintf("%v; stderr=%q", result.exitErr, stderr)
	default:
		return "unknown state"
	}
}

func startHotRestartWFSStatOnce(path string) *hotRestartWFSReadOnceClient {
	cmd := exec.Command(os.Args[0], "-test.run=^TestHotRestartWFSStatOnceHelper$")
	cmd.Env = append(os.Environ(),
		hotRestartWFSStatOnce+"=1",
		hotRestartWFSStatPath+"="+path,
	)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(fmt.Sprintf("read-once stdout pipe: %v", err))
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		panic(fmt.Sprintf("start read-once client: %v", err))
	}

	client := &hotRestartWFSReadOnceClient{
		cmd:    cmd,
		lines:  make(chan string, 2),
		exitCh: make(chan error, 1),
		stderr: &stderr,
	}
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			client.lines <- strings.TrimSpace(scanner.Text())
		}
	}()
	go func() {
		client.exitCh <- cmd.Wait()
		close(client.exitCh)
		close(client.lines)
	}()
	return client
}

func (c *hotRestartWFSReadOnceClient) WaitForResult(timeout time.Duration) (string, bool) {
	select {
	case line, ok := <-c.lines:
		if !ok {
			return "", false
		}
		return line, false
	case <-time.After(timeout):
		return "", true
	case <-c.exitCh:
		return "", false
	}
}

func (c *hotRestartWFSReadOnceClient) Close() {
	select {
	case <-c.exitCh:
		return
	default:
	}

	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	select {
	case <-c.exitCh:
	case <-time.After(3 * time.Second):
		panic(fmt.Sprintf("timed out waiting for read-once client exit: stderr=%q", strings.TrimSpace(c.stderr.String())))
	}
}

func newHotRestartLiveTestWFS(t *testing.T) *WFS {
	t.Helper()

	root := util.FullPath("/")
	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}

	option := &Option{
		ChunkSizeLimit:     1024,
		ConcurrentReaders:  1,
		VolumeServerAccess: "filerProxy",
		FilerAddresses: []pb.ServerAddress{
			"127.0.0.1:8888",
		},
		FilerMountRootPath:     "/",
		MountUid:               99,
		MountGid:               100,
		MountMode:              0o755,
		MountMtime:             time.Unix(1710000000, 0),
		MountCtime:             time.Unix(1710000000, 0),
		UidGidMapper:           uidGidMapper,
		uniqueCacheDirForWrite: t.TempDir(),
	}

	wfs := &WFS{
		option:            option,
		signature:         1,
		inodeToPath:       NewInodeToPath(root, 0),
		fhMap:             NewFileHandleToInode(),
		dhMap:             NewDirectoryHandleToInode(),
		fhLockTable:       util.NewLockTable[FileHandleId](),
		pendingAsyncFlush: make(map[uint64]chan struct{}),
	}
	wfs.copyBufferPool.New = func() any {
		return make([]byte, 1024)
	}
	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		root,
		false,
		func(path util.FullPath) {
			wfs.inodeToPath.MarkChildrenCached(path)
		},
		func(path util.FullPath) bool {
			return wfs.inodeToPath.IsChildrenCached(path)
		},
		func(util.FullPath, *filer_pb.Entry) {},
		nil,
	)
	wfs.inodeToPath.MarkChildrenCached(root)
	t.Cleanup(func() {
		wfs.metaCache.Shutdown()
	})

	entry := &filer.Entry{
		FullPath: util.NewFullPath("/", hotRestartWFSFileName),
		Attr: filer.Attr{
			Mtime:    time.Unix(1710000000, 0),
			Crtime:   time.Unix(1710000000, 0),
			Ctime:    time.Unix(1710000000, 0),
			Mode:     0o644,
			Uid:      99,
			Gid:      100,
			FileSize: uint64(len(hotRestartWFSFileContents)),
			Inode:    hotRestartWFSFileInode,
		},
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), entry); err != nil {
		t.Fatalf("insert cached entry: %v", err)
	}
	return wfs
}

func requireHotRestartFusePrereqs(t *testing.T) {
	t.Helper()

	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("/dev/fuse unavailable: %v", err)
	}
	if _, err := hotRestartFusermountBinary(); err != nil {
		t.Skipf("fusermount unavailable: %v", err)
	}
}

func callHotRestartFusermount(mountPoint string) (int, error) {
	local, remote, err := hotRestartUnixSocketpair()
	if err != nil {
		return 0, err
	}
	defer local.Close()
	defer remote.Close()

	bin, err := hotRestartFusermountBinary()
	if err != nil {
		return 0, err
	}

	cmd := exec.Command(bin, mountPoint)
	cmd.Env = append(os.Environ(), "_FUSE_COMMFD=3")
	cmd.ExtraFiles = []*os.File{remote}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("fusermount failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	fd, err := hotRestartGetConnection(local)
	if err != nil {
		return 0, err
	}
	syscall.CloseOnExec(fd)
	return fd, nil
}

func unmountHotRestartWithFusermount(mountPoint string) error {
	return runHotRestartFusermountUnmount(mountPoint, "-u")
}

func lazyUnmountHotRestartWithFusermount(mountPoint string) error {
	return runHotRestartFusermountUnmount(mountPoint, "-uz")
}

func runHotRestartFusermountUnmount(mountPoint, flag string) error {
	bin, err := hotRestartFusermountBinary()
	if err != nil {
		return err
	}

	cmd := exec.Command(bin, flag, mountPoint)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("fusermount %s failed: %w: %s", flag, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func hotRestartFusermountBinary() (string, error) {
	if path, err := exec.LookPath("fusermount3"); err == nil {
		return path, nil
	}
	if path, err := exec.LookPath("fusermount"); err == nil {
		return path, nil
	}
	return "", fmt.Errorf("fusermount3 or fusermount not found")
}

func hotRestartUnixSocketpair() (local, remote *os.File, err error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET, 0)
	if err != nil {
		return nil, nil, os.NewSyscallError("socketpair", err)
	}
	return os.NewFile(uintptr(fds[0]), "socketpair-local"), os.NewFile(uintptr(fds[1]), "socketpair-remote"), nil
}

func hotRestartGetConnection(local *os.File) (int, error) {
	conn, err := net.FileConn(local)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, fmt.Errorf("expected UnixConn, got %T", conn)
	}

	var data [4]byte
	control := make([]byte, 4*256)
	_, oobn, _, _, err := unixConn.ReadMsgUnix(data[:], control)
	if err != nil {
		return 0, err
	}

	msgs, err := syscall.ParseSocketControlMessage(control[:oobn])
	if err != nil {
		return 0, err
	}
	if len(msgs) != 1 {
		return 0, fmt.Errorf("expected 1 socket control message, got %d", len(msgs))
	}

	fds, err := syscall.ParseUnixRights(&msgs[0])
	if err != nil {
		return 0, err
	}
	if len(fds) != 1 {
		return 0, fmt.Errorf("expected 1 inherited fd, got %d", len(fds))
	}
	if fds[0] < 0 {
		return 0, fmt.Errorf("received negative fuse fd %d", fds[0])
	}
	return fds[0], nil
}

func lookupHotRestartFUSEConnectionID(path string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Dev), nil
}

func abortHotRestartFUSEConnection(connectionID uint64) error {
	if connectionID == 0 {
		return fmt.Errorf("missing fuse connection id")
	}
	return os.WriteFile(fmt.Sprintf("/sys/fs/fuse/connections/%d/abort", connectionID), []byte("1"), 0o644)
}
