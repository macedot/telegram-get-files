package downloader

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/macedot/telegram-get-files/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeHash(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	err := os.WriteFile(testFile, []byte("hello world"), 0644)
	require.NoError(t, err)

	hash, err := ComputeHash(testFile)

	assert.NoError(t, err)
	assert.Equal(t, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", hash)
}

func TestComputeHash_FileNotFound(t *testing.T) {
	hash, err := ComputeHash("/nonexistent/file.txt")

	assert.Error(t, err)
	assert.Empty(t, hash)
}

func TestFileExists(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "exists.txt")

	err := os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	assert.True(t, FileExists(testFile))
	assert.False(t, FileExists(filepath.Join(tmpDir, "nonexistent.txt")))
}

func TestEnsureDir(t *testing.T) {
	tmpDir := t.TempDir()
	nestedPath := filepath.Join(tmpDir, "a", "b", "c", "file.txt")

	err := EnsureDir(nestedPath)

	assert.NoError(t, err)
	assert.DirExists(t, filepath.Dir(nestedPath))
}

func TestEnsureDir_Existing(t *testing.T) {
	tmpDir := t.TempDir()

	err := EnsureDir(tmpDir)

	assert.NoError(t, err)
}

func TestPool_NewPool(t *testing.T) {
	database, err := db.New(":memory:")
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()
	pool := NewPool(5, database, ctx)

	assert.NotNil(t, pool)
	assert.Equal(t, 5, pool.workers)
}

func TestPool_StartStop(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()
	pool := NewPool(2, database, ctx)
	pool.Start()

	assert.NotNil(t, pool.ctx)

	pool.Stop()
}
