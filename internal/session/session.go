package session

import (
	"encoding/json"
	"fmt"
	"os"
)

// Store defines the interface for session storage.
type Store interface {
	Load() ([]byte, error)
	Save(data []byte) error
}

// FileStore implements Store using a file.
type FileStore struct {
	path string
}

// NewFileStore creates a new file-based session store.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Load reads session data from file.
func (f *FileStore) Load() ([]byte, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read session file: %w", err)
	}
	return data, nil
}

// Save writes session data to file.
func (f *FileStore) Save(data []byte) error {
	if err := os.WriteFile(f.path, data, 0600); err != nil {
		return fmt.Errorf("failed to write session file: %w", err)
	}
	return nil
}

// SessionData represents the stored session information.
type SessionData struct {
	DCID       int    `json:"dc_id"`
	AuthKey    []byte `json:"auth_key"`
	ServerSalt []byte `json:"server_salt"`
}

// SaveSession saves session data to a file.
func SaveSession(path string, data *SessionData) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}
	
	if err := os.WriteFile(path, jsonData, 0600); err != nil {
		return fmt.Errorf("failed to write session file: %w", err)
	}
	
	return nil
}

// LoadSession loads session data from a file.
func LoadSession(path string) (*SessionData, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read session file: %w", err)
	}
	
	var session SessionData
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session: %w", err)
	}
	
	return &session, nil
}

// IsSessionExists checks if a session file exists.
func IsSessionExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
