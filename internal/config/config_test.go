package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, "session.json", cfg.SessionFile)
	assert.Equal(t, "telegram_files.db", cfg.DatabasePath)
	assert.Equal(t, "./downloaded_files", cfg.DownloadPath)
	assert.Equal(t, 5, cfg.Workers)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 30, cfg.ScanPollInterval)
	assert.Equal(t, 30, cfg.DownloadPollInterval)
}

func TestLoadFromFile_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	content := `{
		"api_id": 123456,
		"api_hash": "test_hash",
		"session_file": "my_session.json",
		"database_path": "my_db.db",
		"download_path": "/downloads",
		"workers": 10,
		"log_level": "debug",
		"scan_poll_interval": 60,
		"download_poll_interval": 45
	}`

	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadFromFile(configPath)

	assert.NoError(t, err)
	assert.Equal(t, 123456, cfg.APIID)
	assert.Equal(t, "test_hash", cfg.APIHash)
	assert.Equal(t, "my_session.json", cfg.SessionFile)
	assert.Equal(t, "my_db.db", cfg.DatabasePath)
	assert.Equal(t, "/downloads", cfg.DownloadPath)
	assert.Equal(t, 10, cfg.Workers)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, 60, cfg.ScanPollInterval)
	assert.Equal(t, 45, cfg.DownloadPollInterval)
}

func TestLoadFromFile_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	err := os.WriteFile(configPath, []byte("invalid json"), 0644)
	require.NoError(t, err)

	_, err = LoadFromFile(configPath)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
}

func TestLoadFromFile_FileNotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/config.json")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such file")
}

func TestValidate_MissingAPIID(t *testing.T) {
	cfg := &Config{
		APIID:   0,
		APIHash: "test_hash",
	}

	err := cfg.Validate()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "api_id")
}

func TestValidate_MissingAPIHash(t *testing.T) {
	cfg := &Config{
		APIID:   123,
		APIHash: "",
	}

	err := cfg.Validate()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "api_hash")
}

func TestValidate_ZeroWorkers(t *testing.T) {
	cfg := &Config{
		APIID:   123,
		APIHash: "hash",
		Workers: 0,
	}

	err := cfg.Validate()

	assert.NoError(t, err)
	assert.Equal(t, 1, cfg.Workers) // Should be set to 1
}

func TestValidate_NegativeWorkers(t *testing.T) {
	cfg := &Config{
		APIID:   123,
		APIHash: "hash",
		Workers: -5,
	}

	err := cfg.Validate()

	assert.NoError(t, err)
	assert.Equal(t, 1, cfg.Workers) // Should be set to 1
}

func TestValidate_ZeroScanPollInterval(t *testing.T) {
	cfg := &Config{
		APIID:                123,
		APIHash:              "hash",
		ScanPollInterval:     0,
		DownloadPollInterval: 30,
	}

	err := cfg.Validate()

	assert.NoError(t, err)
	assert.Equal(t, 30, cfg.ScanPollInterval) // Should default to 30
}

func TestValidate_ZeroDownloadPollInterval(t *testing.T) {
	cfg := &Config{
		APIID:                123,
		APIHash:              "hash",
		ScanPollInterval:     30,
		DownloadPollInterval: 0,
	}

	err := cfg.Validate()

	assert.NoError(t, err)
	assert.Equal(t, 30, cfg.DownloadPollInterval) // Should default to 30
}

func TestValidate_Valid(t *testing.T) {
	cfg := &Config{
		APIID:                123,
		APIHash:              "hash",
		SessionFile:          "session.json",
		DatabasePath:         "db.sqlite",
		DownloadPath:         "./downloads",
		Workers:              5,
		LogLevel:             "info",
		ScanPollInterval:     30,
		DownloadPollInterval: 30,
	}

	err := cfg.Validate()

	assert.NoError(t, err)
}

func TestSaveExample(t *testing.T) {
	tmpDir := t.TempDir()
	examplePath := filepath.Join(tmpDir, "example.json")

	err := SaveExample(examplePath)

	assert.NoError(t, err)

	// Verify the file can be loaded
	cfg, err := LoadFromFile(examplePath)
	assert.NoError(t, err)
	assert.Equal(t, 123456, cfg.APIID)
	assert.Equal(t, "your_api_hash_here", cfg.APIHash)
}

func TestLoadFromFile_UsesDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	// Only set required fields
	content := `{
		"api_id": 123,
		"api_hash": "hash"
	}`

	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadFromFile(configPath)

	assert.NoError(t, err)
	// Verify defaults are applied
	assert.Equal(t, "session.json", cfg.SessionFile)
	assert.Equal(t, "telegram_files.db", cfg.DatabasePath)
	assert.Equal(t, "./downloaded_files", cfg.DownloadPath)
	assert.Equal(t, 5, cfg.Workers)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 30, cfg.ScanPollInterval)
	assert.Equal(t, 30, cfg.DownloadPollInterval)
}
