package hotrestart_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/go-fuse/v2/fuse/nodefs"
	"github.com/seaweedfs/go-fuse/v2/fuse/pathfs"
)

const (
	workerEnvEnabled    = "SEAWEEDFS_HOTRESTART_WORKER"
	workerEnvBackingDir = "SEAWEEDFS_HOTRESTART_BACKING_DIR"
	workerEnvMountFD    = "SEAWEEDFS_HOTRESTART_MOUNT_FD"
	workerReadyLine     = "READY"
	mountedFileName     = "hello.txt"
)

func TestHotRestartWorker(t *testing.T) {
	if os.Getenv(workerEnvEnabled) == "" {
		t.Skip("subprocess helper")
	}

	backingDir := os.Getenv(workerEnvBackingDir)
	mountFD := os.Getenv(workerEnvMountFD)
	if backingDir == "" || mountFD == "" {
		fmt.Fprintln(os.Stderr, "missing worker configuration")
		os.Exit(1)
	}

	pfs := pathfs.NewLoopbackFileSystem(backingDir)
	pfs = pathfs.NewLockingFileSystem(pfs)

	pathNodeFS := pathfs.NewPathNodeFs(pfs, &pathfs.PathNodeFsOptions{
		ClientInodes: true,
	})
	connector := nodefs.NewFileSystemConnector(pathNodeFS.Root(), &nodefs.Options{
		EntryTimeout:    100 * time.Millisecond,
		AttrTimeout:     100 * time.Millisecond,
		NegativeTimeout: 0,
	})

	server, err := fuse.NewServer(
		connector.RawFS(),
		fmt.Sprintf("/dev/fd/%s", mountFD),
		&fuse.MountOptions{SingleThreaded: true},
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

	fmt.Println(workerReadyLine)
	_ = os.Stdout.Sync()
	_, _ = io.ReadAll(os.Stdin)
	os.Exit(0)
}

func TestSupervisorCanMountWorkerOnInheritedFuseFD(t *testing.T) {
	session := newHotRestartSession(t)
	defer session.Cleanup(t)

	worker := session.StartWorker(t)
	defer worker.Shutdown(t)

	if got := session.ReadMountedFile(t); got != session.fileContents {
		t.Fatalf("unexpected mounted file contents: got %q want %q", got, session.fileContents)
	}
}

func TestWorkerReplacementOnLiveFuseFDCurrentLimitation(t *testing.T) {
	session := newHotRestartSession(t)
	defer session.Cleanup(t)

	worker1 := session.StartWorker(t)
	if got := session.ReadMountedFile(t); got != session.fileContents {
		t.Fatalf("unexpected mounted file contents before replacement: got %q want %q", got, session.fileContents)
	}
	worker1.Shutdown(t)

	worker2 := session.StartWorkerNoWait(t)
	defer worker2.Terminate(t)

	result := worker2.WaitForReady(1500 * time.Millisecond)
	switch {
	case result.ready:
		got := session.ReadMountedFile(t)
		t.Fatalf("replacement worker unexpectedly became ready on a live initialized FUSE fd; mounted read returned %q", got)
	case result.exitErr != nil:
		t.Logf("replacement worker exited before ready, matching current limitation: %v", result.exitErr)
	case result.timedOut:
		t.Log("replacement worker never became ready on the inherited live FUSE fd, matching current limitation")
	default:
		t.Fatalf("unexpected replacement worker state: %+v", result)
	}
}

type hotRestartSession struct {
	mountPoint   string
	backingDir   string
	fuseConnFile *os.File
	fileContents string
}

func newHotRestartSession(t *testing.T) *hotRestartSession {
	t.Helper()

	requireLinuxFuse(t)

	rootDir := t.TempDir()
	backingDir := filepath.Join(rootDir, "backing")
	mountPoint := filepath.Join(rootDir, "mount")

	if err := os.MkdirAll(backingDir, 0o755); err != nil {
		t.Fatalf("mkdir backing dir: %v", err)
	}
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatalf("mkdir mount point: %v", err)
	}

	fileContents := "hot restart baseline\n"
	if err := os.WriteFile(filepath.Join(backingDir, mountedFileName), []byte(fileContents), 0o644); err != nil {
		t.Fatalf("write backing file: %v", err)
	}

	fd, err := mountViaFusermount(mountPoint)
	if err != nil {
		t.Fatalf("mount via fusermount: %v", err)
	}

	return &hotRestartSession{
		mountPoint:   mountPoint,
		backingDir:   backingDir,
		fuseConnFile: os.NewFile(uintptr(fd), "fuse-connection"),
		fileContents: fileContents,
	}
}

func (s *hotRestartSession) Cleanup(t *testing.T) {
	t.Helper()

	if s.fuseConnFile != nil {
		if err := unmountWithFusermount(s.mountPoint); err != nil {
			t.Fatalf("unmount %s: %v", s.mountPoint, err)
		}
		if err := s.fuseConnFile.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("close fuse connection: %v", err)
		}
		s.fuseConnFile = nil
	}
}

func (s *hotRestartSession) StartWorker(t *testing.T) *hotRestartWorker {
	t.Helper()

	worker := s.StartWorkerNoWait(t)
	result := worker.WaitForReady(3 * time.Second)
	if !result.ready {
		worker.Terminate(t)
		t.Fatalf("worker did not become ready: %s", worker.Describe(result))
	}
	return worker
}

func (s *hotRestartSession) StartWorkerNoWait(t *testing.T) *hotRestartWorker {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestHotRestartWorker$")
	cmd.Env = append(os.Environ(),
		workerEnvEnabled+"=1",
		workerEnvBackingDir+"="+s.backingDir,
		workerEnvMountFD+"=3",
	)
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

	worker := &hotRestartWorker{
		cmd:     cmd,
		stdin:   stdin,
		readyCh: make(chan struct{}),
		exitCh:  make(chan error, 1),
	}

	go worker.watchStdout(stdoutPipe)
	go func() {
		worker.exitCh <- cmd.Wait()
		close(worker.exitCh)
	}()

	worker.stderrBuf = &stderr
	return worker
}

func (s *hotRestartSession) ReadMountedFile(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(s.mountPoint, mountedFileName))
	if err != nil {
		t.Fatalf("read mounted file: %v", err)
	}
	return string(data)
}

type hotRestartWorker struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	readyCh   chan struct{}
	exitCh    chan error
	stderrBuf *bytes.Buffer
	readyOnce sync.Once
}

type workerWaitResult struct {
	ready    bool
	exitErr  error
	timedOut bool
}

func (w *hotRestartWorker) watchStdout(stdout io.ReadCloser) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == workerReadyLine {
			w.readyOnce.Do(func() { close(w.readyCh) })
		}
	}
}

func (w *hotRestartWorker) WaitForReady(timeout time.Duration) workerWaitResult {
	select {
	case <-w.readyCh:
		return workerWaitResult{ready: true}
	case err := <-w.exitCh:
		return workerWaitResult{exitErr: err}
	case <-time.After(timeout):
		return workerWaitResult{timedOut: true}
	}
}

func (w *hotRestartWorker) Shutdown(t *testing.T) {
	t.Helper()
	if w.stdin != nil {
		_ = w.stdin.Close()
		w.stdin = nil
	}

	select {
	case err := <-w.exitCh:
		if err != nil {
			t.Fatalf("worker exit: %s", w.Describe(workerWaitResult{exitErr: err}))
		}
	case <-time.After(3 * time.Second):
		w.Terminate(t)
		t.Fatalf("worker did not exit after stdin close")
	}
}

func (w *hotRestartWorker) Terminate(t *testing.T) {
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
		t.Fatalf("timed out waiting for worker kill")
	}
}

func (w *hotRestartWorker) Describe(result workerWaitResult) string {
	stderr := strings.TrimSpace(w.stderrBuf.String())
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

func requireLinuxFuse(t *testing.T) {
	t.Helper()

	if runtime.GOOS != "linux" {
		t.Skip("requires Linux")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("/dev/fuse unavailable: %v", err)
	}
	if _, err := fusermountBinary(); err != nil {
		t.Skipf("fusermount unavailable: %v", err)
	}
}

func mountViaFusermount(mountPoint string) (int, error) {
	local, remote, err := unixSocketpair()
	if err != nil {
		return 0, err
	}
	defer local.Close()
	defer remote.Close()

	bin, err := fusermountBinary()
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

	fd, err := getConnection(local)
	if err != nil {
		return 0, err
	}
	syscall.CloseOnExec(fd)
	return fd, nil
}

func unmountWithFusermount(mountPoint string) error {
	bin, err := fusermountBinary()
	if err != nil {
		return err
	}

	cmd := exec.Command(bin, "-u", mountPoint)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("fusermount -u failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func fusermountBinary() (string, error) {
	if path, err := exec.LookPath("fusermount3"); err == nil {
		return path, nil
	}
	if path, err := exec.LookPath("fusermount"); err == nil {
		return path, nil
	}
	return "", fmt.Errorf("fusermount3 or fusermount not found")
}

func unixSocketpair() (local, remote *os.File, err error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET, 0)
	if err != nil {
		return nil, nil, os.NewSyscallError("socketpair", err)
	}
	return os.NewFile(uintptr(fds[0]), "socketpair-local"), os.NewFile(uintptr(fds[1]), "socketpair-remote"), nil
}

func getConnection(local *os.File) (int, error) {
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
