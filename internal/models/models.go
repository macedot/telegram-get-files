// Package models defines the core data structures used throughout the application.
package models

import "time"

// Channel represents a Telegram channel or group.
type Channel struct {
	ID    int64
	Title string
	Type  string // "Channel", "Supergroup", "Basic Group", etc.
}

// MediaMessage represents a message containing media (file, photo, video, etc.).
type MediaMessage struct {
	MessageID    int
	ChannelID    int64
	SenderID     *int64 // nullable
	FileName     string
	FileSize     int64
	Date         time.Time
	FileID       string
	ChannelTitle string
}

// DownloadTask represents a work item in the download queue.
type DownloadTask struct {
	MessageID    int
	ChannelID    int64
	ChannelTitle string
	FileName     string
	FileSize     int64
	OriginalName string
	FileID       string
	FilePath     string
}

// FileInfo represents a file record in the database.
// Matches the file_status table schema.
type FileInfo struct {
	ID             int64
	CreatedAt      time.Time
	ChannelID      int64
	ChannelTitle   string
	MessageID      int
	SenderID       *int64
	SenderUsername *string
	OriginalName   string
	PrefixedName   string
	FileID         string
	FileSize       int64
	SentAt         time.Time
	StartedAt      *time.Time
	DownloadedAt   *time.Time
	FilePath       *string
	DataHash       *string
}

// FileStatus represents the download status of a file.
type FileStatus string

const (
	StatusPending   FileStatus = "pending"
	StatusStarted   FileStatus = "started"
	StatusCompleted FileStatus = "completed"
	StatusFailed    FileStatus = "failed"
)
