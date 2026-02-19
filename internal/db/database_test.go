package db

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/macedot/telegram-get-files/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *DB {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	database, err := New(dbPath)
	require.NoError(t, err)
	err = database.Init()
	require.NoError(t, err)
	return database
}

func TestInsertFile(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	file := &models.FileInfo{
		ChannelID:    123,
		ChannelTitle: "Test Channel",
		MessageID:    456,
		FileID:       "file_123",
		FileSize:     1024,
		OriginalName: "test.pdf",
		PrefixedName: "test.pdf",
		SentAt:       time.Now(),
	}

	err := database.InsertFile(file)
	assert.NoError(t, err)
}

func TestGetPendingFiles(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert files - one pending, one completed
	pendingFile := &models.FileInfo{
		ChannelID: 123,
		MessageID: 1,
		FileID:    "file_1",
		SentAt:    time.Now(),
	}
	err := database.InsertFile(pendingFile)
	require.NoError(t, err)

	completedFile := &models.FileInfo{
		ChannelID: 123,
		MessageID: 2,
		FileID:    "file_2",
		SentAt:    time.Now(),
	}
	err = database.InsertFile(completedFile)
	require.NoError(t, err)
	err = database.UpdateCompleted(123, 2, "file_2.pdf", "/path/file_2.pdf", "hash")
	require.NoError(t, err)

	// Get pending
	pending, err := database.GetPendingFiles()
	assert.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, 1, pending[0].MessageID)
}

func TestGetOrCreateFile_New(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	file := &models.FileInfo{
		ChannelID: 123,
		MessageID: 456,
		FileID:    "file_123",
		SentAt:    time.Now(),
	}

	// Create new file
	created, isNew, err := database.GetOrCreateFile(file)
	assert.NoError(t, err)
	assert.True(t, isNew)
	assert.NotNil(t, created)
}

func TestGetOrCreateFile_Existing(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	file := &models.FileInfo{
		ChannelID: 123,
		MessageID: 456,
		FileID:    "file_123",
		SentAt:    time.Now(),
	}

	// Create new file
	_, isNew, err := database.GetOrCreateFile(file)
	assert.NoError(t, err)
	assert.True(t, isNew)

	// Get existing file
	_, isNew, err = database.GetOrCreateFile(file)
	assert.NoError(t, err)
	assert.False(t, isNew)
}

func TestGetOrCreateOrUpdateFile_NewFile(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	file := &models.FileInfo{
		ChannelID:    123,
		MessageID:    456,
		FileID:       "file_123",
		FileSize:     1024,
		OriginalName: "test.pdf",
		SentAt:       time.Now(),
	}

	created, isNew, wasUpdated, err := database.GetOrCreateOrUpdateFile(file)
	assert.NoError(t, err)
	assert.True(t, isNew)
	assert.False(t, wasUpdated)
	assert.NotNil(t, created)
}

func TestGetOrCreateOrUpdateFile_UpdateExisting(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert original file
	original := &models.FileInfo{
		ChannelID:    123,
		MessageID:    456,
		FileID:       "old_file_id",
		FileSize:     100,
		OriginalName: "old_name.pdf",
		SentAt:       time.Now(),
	}
	err := database.InsertFile(original)
	require.NoError(t, err)

	// Update with new data
	updated := &models.FileInfo{
		ChannelID:    123,
		MessageID:    456,
		FileID:       "new_file_id",
		FileSize:     200,
		OriginalName: "new_name.pdf",
		SentAt:       time.Now(),
	}

	result, isNew, wasUpdated, err := database.GetOrCreateOrUpdateFile(updated)
	assert.NoError(t, err)
	assert.False(t, isNew)
	assert.True(t, wasUpdated)
	assert.Equal(t, "new_file_id", result.FileID)
	assert.Equal(t, int64(200), result.FileSize)
}

func TestGetOrCreateOrUpdateFile_Unchanged(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert file
	original := &models.FileInfo{
		ChannelID:    123,
		MessageID:    456,
		FileID:       "file_123",
		FileSize:     1024,
		OriginalName: "test.pdf",
		SentAt:       time.Now(),
	}
	err := database.InsertFile(original)
	require.NoError(t, err)

	// Try to update with same data
	result, isNew, wasUpdated, err := database.GetOrCreateOrUpdateFile(original)
	assert.NoError(t, err)
	assert.False(t, isNew)
	assert.False(t, wasUpdated)
	assert.Equal(t, "file_123", result.FileID)
}

func TestUpdateStarted(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert file
	file := &models.FileInfo{
		ChannelID: 123,
		MessageID: 456,
		FileID:    "file_123",
		SentAt:    time.Now(),
	}
	err := database.InsertFile(file)
	require.NoError(t, err)

	// Update started
	err = database.UpdateStarted(123, 456)
	assert.NoError(t, err)
}

func TestUpdateCompleted(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert file
	file := &models.FileInfo{
		ChannelID: 123,
		MessageID: 456,
		FileID:    "file_123",
		SentAt:    time.Now(),
	}
	err := database.InsertFile(file)
	require.NoError(t, err)

	// Update completed
	err = database.UpdateCompleted(123, 456, "downloaded.pdf", "/path/downloaded.pdf", "abc123")
	assert.NoError(t, err)

	// Verify
	pending, err := database.GetPendingFiles()
	assert.NoError(t, err)
	assert.Len(t, pending, 0)
}

func TestUpdateFile(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert file
	file := &models.FileInfo{
		ChannelID:    123,
		MessageID:    456,
		FileID:       "old_id",
		FileSize:     100,
		OriginalName: "old.pdf",
		SentAt:       time.Now(),
	}
	err := database.InsertFile(file)
	require.NoError(t, err)

	// Update file
	file.FileID = "new_id"
	file.FileSize = 200
	file.OriginalName = "new.pdf"
	err = database.UpdateFile(file)
	assert.NoError(t, err)
}

func TestGetPendingFiles_Empty(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	pending, err := database.GetPendingFiles()
	assert.NoError(t, err)
	assert.Len(t, pending, 0)
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	database, err := New(dbPath)
	require.NoError(t, err)
	err = database.Init()
	require.NoError(t, err)

	err = database.Close()
	assert.NoError(t, err)
}

func TestMultipleFilesSameChannel(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert multiple files in same channel
	for i := 0; i < 5; i++ {
		file := &models.FileInfo{
			ChannelID: 123,
			MessageID: 100 + i,
			FileID:    fmt.Sprintf("file_%d", i),
			SentAt:    time.Now(),
		}
		err := database.InsertFile(file)
		assert.NoError(t, err)
	}

	pending, err := database.GetPendingFiles()
	assert.NoError(t, err)
	assert.Len(t, pending, 5)
}

func TestUniqueConstraint(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Insert file twice with same channel/message
	file := &models.FileInfo{
		ChannelID: 123,
		MessageID: 456,
		FileID:    "file_123",
		SentAt:    time.Now(),
	}
	err := database.InsertFile(file)
	assert.NoError(t, err)

	// Try to insert again - should fail
	err = database.InsertFile(file)
	assert.Error(t, err)
}
