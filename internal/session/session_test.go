package session

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFileStore(t *testing.T) {
	store := NewFileStore("session.json")
	assert.NotNil(t, store)
	assert.Equal(t, "session.json", store.path)
}

func TestFileStore_Load_NotExists(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileStore(filepath.Join(tmpDir, "nonexistent"))

	data, err := store.Load()
	assert.NoError(t, err)
	assert.Nil(t, data)
}

func TestFileStore_Load_Error(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileStore(tmpDir) // directory, not file

	_, err := store.Load()
	assert.Error(t, err)
}

func TestFileStore_Save(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFileStore(filepath.Join(tmpDir, "session.json"))

	err := store.Save([]byte("test data"))
	assert.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(tmpDir, "session.json"))
	assert.NoError(t, err)
	assert.Equal(t, "test data", string(data))
}

func TestFileStore_Save_Error(t *testing.T) {
	store := NewFileStore("/nonexistent/path/session.json")

	err := store.Save([]byte("test"))
	assert.Error(t, err)
}

func TestSaveLoadSession(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "session.json")

	data := &SessionData{
		DCID:       1,
		AuthKey:    []byte("test_auth_key"),
		ServerSalt: []byte("test_salt"),
	}

	err := SaveSession(path, data)
	require.NoError(t, err)

	loaded, err := LoadSession(path)
	assert.NoError(t, err)
	assert.Equal(t, 1, loaded.DCID)
	assert.Equal(t, []byte("test_auth_key"), loaded.AuthKey)
	assert.Equal(t, []byte("test_salt"), loaded.ServerSalt)
}

func TestLoadSession_NotExists(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nonexistent")

	loaded, err := LoadSession(path)
	assert.NoError(t, err)
	assert.Nil(t, loaded)
}

func TestLoadSession_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "session.json")

	err := os.WriteFile(path, []byte("invalid json"), 0644)
	require.NoError(t, err)

	loaded, err := LoadSession(path)
	assert.Error(t, err)
	assert.Nil(t, loaded)
}

func TestIsSessionExists(t *testing.T) {
	tmpDir := t.TempDir()
	existsPath := filepath.Join(tmpDir, "exists")
	notExistsPath := filepath.Join(tmpDir, "notexists")

	err := os.WriteFile(existsPath, []byte("test"), 0644)
	require.NoError(t, err)

	assert.True(t, IsSessionExists(existsPath))
	assert.False(t, IsSessionExists(notExistsPath))
}
