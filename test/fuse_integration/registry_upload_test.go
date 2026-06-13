package fuse_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type registryUploadPaths struct {
	hashstateDir  string
	hashstatePath string
	uploadData    string
	finalBlobPath string
}

// TestRegistryStyleUploadCachedMetadataFlush exercises the registry-like
// open/write/sync/close and finalize-rename sequence against a real FUSE mount.
// The deterministic stale-revision red proof still lives in weed/mount unit
// tests because the local leveldb2 filer store used by this harness does not
// enforce entry-revision compare-and-swap.
func TestRegistryStyleUploadCachedMetadataFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live FUSE registry upload regression in short mode")
	}

	config := DefaultTestConfig()
	config.DirAutoCreate = false
	config.MountOptions = append(config.MountOptions, "-umask=000")

	framework := NewFuseTestFramework(t, config)
	defer framework.Cleanup()

	require.NoError(t, framework.Setup(config))

	paths := newRegistryUploadPaths()
	initialHashstate := registryHashstateBytes("0001")
	firstRewrite := registryHashstateBytes("0002")
	secondRewrite := registryHashstateBytes("0003")
	initialUploadData := bytes.Repeat([]byte("u"), 128)
	finalUploadData := bytes.Repeat([]byte("v"), 128)

	hashstateMountPath := filepath.Join(framework.GetMountPoint(), paths.hashstatePath)
	createFileAndSync(t, hashstateMountPath, initialHashstate)
	waitForFileContent(t, hashstateMountPath, initialHashstate, 30*time.Second)

	uploadDataMountPath := filepath.Join(framework.GetMountPoint(), paths.uploadData)
	createFileAndSync(t, uploadDataMountPath, initialUploadData)
	waitForFileContent(t, uploadDataMountPath, initialUploadData, 30*time.Second)

	require.NoError(t, framework.Remount(config))
	hashstateMountPath = filepath.Join(framework.GetMountPoint(), paths.hashstatePath)
	uploadDataMountPath = filepath.Join(framework.GetMountPoint(), paths.uploadData)

	warmRegistryDirCache(t, framework, paths.hashstateDir, filepath.Base(paths.hashstatePath))
	rewriteExistingFileAndSync(t, hashstateMountPath, firstRewrite)
	waitForFileContent(t, hashstateMountPath, firstRewrite, 30*time.Second)

	require.NoError(t, framework.Remount(config))
	hashstateMountPath = filepath.Join(framework.GetMountPoint(), paths.hashstatePath)
	uploadDataMountPath = filepath.Join(framework.GetMountPoint(), paths.uploadData)

	warmRegistryDirCache(t, framework, paths.hashstateDir, filepath.Base(paths.hashstatePath))
	rewriteExistingFileAndSync(t, hashstateMountPath, secondRewrite)
	waitForFileContent(t, hashstateMountPath, secondRewrite, 30*time.Second)

	rewriteExistingFileAndSync(t, uploadDataMountPath, finalUploadData)
	finalBlobMountPath := filepath.Join(framework.GetMountPoint(), paths.finalBlobPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(finalBlobMountPath), 0o755))
	require.NoError(t, os.Rename(uploadDataMountPath, finalBlobMountPath))
	waitForFileContent(t, finalBlobMountPath, finalUploadData, 30*time.Second)
	_, statErr := os.Stat(uploadDataMountPath)
	assert.True(t, os.IsNotExist(statErr), "upload temp file should be gone after finalize rename")

	assertMountLogDoesNotContain(t, framework, "metadata revision mismatch")
	assertMountLogDoesNotContain(t, framework, "entry revision changed")
}

func newRegistryUploadPaths() registryUploadPaths {
	const (
		repository = "docker/registry/v2/repositories/library/busybox"
		uploadID   = "2d6fd9c4-3d31-4b11-b7b8-7fbcb5a4f31f"
		digest     = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	)

	uploadRoot := filepath.Join(repository, "_uploads", uploadID)
	return registryUploadPaths{
		hashstateDir:  filepath.Join(uploadRoot, "hashstates", "sha256"),
		hashstatePath: filepath.Join(uploadRoot, "hashstates", "sha256", digest),
		uploadData:    filepath.Join(uploadRoot, "data"),
		finalBlobPath: filepath.Join("docker/registry/v2/blobs/sha256", digest[:2], digest, "data"),
	}
}

func registryHashstateBytes(marker string) []byte {
	return []byte(fmt.Sprintf("offset=%s digest=sha256:%s\n", marker, strings.Repeat("a", 64)))
}

func warmRegistryDirCache(t *testing.T, framework *FuseTestFramework, relativeDir, expectedBase string) {
	t.Helper()

	dirPath := filepath.Join(framework.GetMountPoint(), relativeDir)
	entries, err := os.ReadDir(dirPath)
	require.NoError(t, err, "read registry cache warm directory %s", relativeDir)
	require.True(t, dirEntriesContain(entries, expectedBase), "expected %s to appear in %s", expectedBase, relativeDir)

	_, err = os.Stat(filepath.Join(dirPath, expectedBase))
	require.NoError(t, err, "stat registry cache warm file %s/%s", relativeDir, expectedBase)
}

func dirEntriesContain(entries []os.DirEntry, name string) bool {
	for _, entry := range entries {
		if entry.Name() == name {
			return true
		}
	}
	return false
}

func rewriteExistingFileAndSync(t *testing.T, path string, content []byte) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	require.NoError(t, err, "open existing registry upload file %s", path)

	if _, err := file.Write(content); err != nil {
		file.Close()
		require.NoError(t, err, "write registry upload file %s", path)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	require.NoError(t, syncErr, "sync registry upload file %s", path)
	require.NoError(t, closeErr, "close registry upload file %s", path)
}

func createFileAndSync(t *testing.T, path string, content []byte) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755), "create registry upload directory for %s", path)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	require.NoError(t, err, "create registry upload file %s", path)

	if _, err := file.Write(content); err != nil {
		file.Close()
		require.NoError(t, err, "write initial registry upload file %s", path)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	require.NoError(t, syncErr, "sync initial registry upload file %s", path)
	require.NoError(t, closeErr, "close initial registry upload file %s", path)
}

func assertMountLogDoesNotContain(t *testing.T, framework *FuseTestFramework, snippet string) {
	t.Helper()

	data, err := framework.ReadProcessLog("mount")
	require.NoError(t, err, "read mount log")
	assert.NotContains(t, string(data), snippet, "mount log should not contain %q", snippet)
}
