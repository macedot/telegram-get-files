package downloader

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
