// Package config handles loading and validation of application configuration.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config holds all application configuration settings.
type Config struct {
	APIID                int    `json:"api_id"`
	APIHash              string `json:"api_hash"`
	SessionFile          string `json:"session_file"`
	DatabasePath         string `json:"database_path"`
	DownloadPath         string `json:"download_path"`
	Workers              int    `json:"workers"`
	LogLevel             string `json:"log_level"`
	ScanPollInterval     int    `json:"scan_poll_interval"`
	DownloadPollInterval int    `json:"download_poll_interval"`
	DownloadTimeout      int    `json:"download_timeout"`      // seconds, default 600
	ScanBatchSize        int    `json:"scan_batch_size"`       // default 100
	WatchPollLimit       int    `json:"watch_poll_limit"`      // default 10
	RetryDelay           int    `json:"retry_delay"`           // seconds, default 3
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		SessionFile:          "session.json",
		DatabasePath:         "telegram_files.db",
		DownloadPath:         "./downloaded_files",
		Workers:              5,
		LogLevel:             "info",
		ScanPollInterval:     30,
		DownloadPollInterval: 30,
		DownloadTimeout:      600,
		ScanBatchSize:        100,
		WatchPollLimit:       10,
		RetryDelay:           3,
	}
}

// LoadFromFile reads configuration from a JSON file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// Validate checks that all required fields are set.
func (c *Config) Validate() error {
	if c.APIID == 0 {
		return fmt.Errorf("api_id is required")
	}
	if c.APIHash == "" {
		return fmt.Errorf("api_hash is required")
	}
	if c.Workers < 1 {
		c.Workers = 1
	}
	if c.ScanPollInterval < 1 {
		c.ScanPollInterval = 30
	}
	if c.DownloadPollInterval < 1 {
		c.DownloadPollInterval = 30
	}
	if c.DownloadTimeout < 1 {
		c.DownloadTimeout = 600
	}
	if c.ScanBatchSize < 1 {
		c.ScanBatchSize = 100
	}
	if c.WatchPollLimit < 1 {
		c.WatchPollLimit = 10
	}
	if c.RetryDelay < 1 {
		c.RetryDelay = 3
	}
	return nil
}

// SaveExample creates an example configuration file.
func SaveExample(path string) error {
	example := &Config{
		APIID:                123456,
		APIHash:              "your_api_hash_here",
		SessionFile:          "session.json",
		DatabasePath:         "telegram_files.db",
		DownloadPath:         "./downloaded_files",
		Workers:              5,
		LogLevel:             "info",
		ScanPollInterval:     30,
		DownloadPollInterval: 30,
		DownloadTimeout:      600,
		ScanBatchSize:        100,
		WatchPollLimit:       10,
		RetryDelay:           3,
	}

	data, err := json.MarshalIndent(example, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal example config: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("failed to write example config: %w", err)
	}

	return nil
}
