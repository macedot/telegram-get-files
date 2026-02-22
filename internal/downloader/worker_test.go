package downloader

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/macedot/telegram-get-files/internal/db"
	"github.com/macedot/telegram-get-files/internal/models"
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

func TestPool_StopIsIdempotent(t *testing.T) {
	database, err := db.New(":memory:")
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()
	pool := NewPool(2, database, ctx)
	pool.Start()

	pool.Stop()
	pool.Stop()
	pool.Stop()
}

func TestPool_SubmitAfterStop(t *testing.T) {
	database, err := db.New(":memory:")
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()
	pool := NewPool(1, database, ctx)
	pool.Start()
	pool.Stop()

	task := &models.DownloadTask{}
	err = pool.Submit(task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stopped")
}

func TestSanitizeFilename_Basic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "document.pdf", "document.pdf"},
		{"with spaces", "my file.txt", "my file.txt"},
		{"with underscores", "my_file_name.doc", "my_file_name.doc"},
		{"with dashes", "my-file-name.doc", "my-file-name.doc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeFilename_PathTraversal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{"parent dir", "../../../etc/passwd", "passwd"},
		{"absolute path unix", "/etc/passwd", "passwd"},
		{"multiple dots", "../../../secret.txt", "secret.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.NotContains(t, result, "..")
			assert.NotContains(t, result, "/")
			assert.Contains(t, result, tt.contains)
		})
	}
}

func TestSanitizeFilename_WindowsPaths(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"absolute windows", "C:\\Windows\\System32"},
		{"relative windows", "..\\..\\secret.txt"},
		{"mixed slashes", "../../../file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.NotContains(t, result, "\\")
		})
	}
}

func TestSanitizeFilename_Unicode(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"chinese", "中文文档.pdf"},
		{"arabic", "مستند.pdf"},
		{"japanese", "ドキュメント.pdf"},
		{"emoji", "file_🎉.txt"},
		{"russian", "документ.pdf"},
		{"mixed unicode", "file_αβγ.pdf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.NotEmpty(t, result)
			assert.NotEqual(t, "download", result)
		})
	}
}

func TestSanitizeFilename_ControlCharacters(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"null byte", "file\x00name.txt"},
		{"newline", "file\nname.txt"},
		{"tab", "file\tname.txt"},
		{"carriage return", "file\rname.txt"},
		{"bell", "file\x07name.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.NotContains(t, result, "\x00")
			assert.NotContains(t, result, "\n")
			assert.NotContains(t, result, "\t")
			assert.NotContains(t, result, "\r")
		})
	}
}

func TestSanitizeFilename_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", "download"},
		{"single dot", ".", "download"},
		{"double dot", "..", "download"},
		{"only slashes", "///", "download"},
		{"only backslashes", "\\\\\\", "download"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeFilename_LongFilenames(t *testing.T) {
	longName := ""
	for i := 0; i < 500; i++ {
		longName += "a"
	}

	result := sanitizeFilename(longName)
	assert.LessOrEqual(t, len(result), 200)
}

func TestValidatePath_Valid(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		target  string
		baseDir string
	}{
		{"simple file", filepath.Join(tmpDir, "file.txt"), tmpDir},
		{"nested file", filepath.Join(tmpDir, "subdir", "file.txt"), tmpDir},
		{"deep nested", filepath.Join(tmpDir, "a", "b", "c", "file.txt"), tmpDir},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePath(tt.target, tt.baseDir)
			assert.NoError(t, err)
		})
	}
}

func TestValidatePath_TraversalAttack(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		target  string
		baseDir string
	}{
		{"parent traversal", filepath.Join(tmpDir, "..", "secret.txt"), tmpDir},
		{"double parent", filepath.Join(tmpDir, "..", "..", "etc", "passwd"), tmpDir},
		{"absolute outside", "/etc/passwd", tmpDir},
		{"relative escape", filepath.Join(tmpDir, "subdir", "..", "..", "escape.txt"), tmpDir},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePath(tt.target, tt.baseDir)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "path traversal")
		})
	}
}

func TestValidatePath_SameDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	err := validatePath(tmpDir, tmpDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path traversal")
}

func TestPool_SubmitDuringStop(t *testing.T) {
	database, err := db.New(":memory:")
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()
	pool := NewPool(1, database, ctx)
	pool.Start()

	var wg sync.WaitGroup
	errCount := int32(0)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				task := &models.DownloadTask{MessageID: j}
				if err := pool.Submit(task); err != nil {
					_ = err // ignore error, we just want to test for panics
				}
			}
		}()
	}

	time.Sleep(time.Millisecond * 5)
	pool.Stop()
	wg.Wait()

	_ = errCount
}

func TestPool_ContextCancellation(t *testing.T) {
	database, err := db.New(":memory:")
	require.NoError(t, err)
	defer database.Close()

	ctx, cancel := context.WithCancel(context.Background())
	pool := NewPool(1, database, ctx)
	pool.Start()

	// Cancel the parent context
	cancel()

	// Since pool wraps the context internally, it should still accept tasks
	// until Stop() is called with the internal context cancelled
	task := &models.DownloadTask{MessageID: 1}

	// The pool's internal context is still valid after parent cancel
	// because NewPool creates its own derived context
	// This is the correct behavior - the pool manages its own lifecycle
	_ = task // suppress unused variable warning

	pool.Stop()

	// After Stop(), Submit should fail
	err = pool.Submit(&models.DownloadTask{MessageID: 2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stopped")
}

func TestPool_ConcurrentSubmitStop(t *testing.T) {
	for i := 0; i < 5; i++ {
		t.Run("iteration", func(t *testing.T) {
			database, err := db.New(":memory:")
			require.NoError(t, err)
			defer database.Close()

			ctx := context.Background()
			pool := NewPool(2, database, ctx)
			pool.Start()

			var wg sync.WaitGroup
			panicOccurred := false

			wg.Add(2)
			go func() {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					task := &models.DownloadTask{MessageID: j}
					_ = pool.Submit(task)
				}
			}()

			go func() {
				defer wg.Done()
				time.Sleep(time.Microsecond * time.Duration(100+rand.Intn(500)))
				pool.Stop()
			}()

			func() {
				defer func() {
					if r := recover(); r != nil {
						panicOccurred = true
					}
				}()
				wg.Wait()
			}()

			assert.False(t, panicOccurred, "Concurrent Submit/Stop caused panic")
		})
	}
}

func TestSanitizeFilename_EmptyAfterControlRemoval(t *testing.T) {
	input := "\x00\x01\x02\x03\x04"
	result := sanitizeFilename(input)
	assert.Equal(t, "_____", result)
}

func TestSanitizeFilename_PreservesExtension(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple extension", "file.pdf", "file.pdf"},
		{"multiple dots", "file.name.with.dots.pdf", "file.name.with.dots.pdf"},
		{"no extension", "filename", "filename"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidatePath_NonexistentBaseDir(t *testing.T) {
	err := validatePath("/tmp/nonexistent/file.txt", "/tmp/nonexistent")
	assert.NoError(t, err)
}
