package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/macedot/telegram-get-files/internal/models"
	_ "modernc.org/sqlite"
)

// DB wraps the database connection.
type DB struct {
	conn *sql.DB
}

// New creates a new database connection.
func New(path string) (*DB, error) {
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	conn.Exec("PRAGMA busy_timeout = 5000")
	conn.Exec("PRAGMA journal_mode = WAL")

	return &DB{conn: conn}, nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.conn.Close()
}

// Init creates the database tables if they don't exist.
func (d *DB) Init() error {
	query := `
	CREATE TABLE IF NOT EXISTS file_status (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at TEXT NOT NULL,
		channel_id INTEGER NOT NULL,
		channel_title TEXT,
		message_id INTEGER NOT NULL,
		sender_id INTEGER,
		sender_username TEXT,
		original_name TEXT,
		prefixed_name TEXT,
		file_id TEXT,
		file_size INTEGER,
		sent_at TEXT NOT NULL,
		started_at TEXT,
		downloaded_at TEXT,
		file_path TEXT,
		data_hash TEXT,
		UNIQUE(channel_id, message_id)
	);
	`

	_, err := d.conn.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create indexes
	_, err = d.conn.Exec("CREATE INDEX IF NOT EXISTS idx_pending ON file_status(downloaded_at)")
	if err != nil {
		return fmt.Errorf("failed to create pending index: %w", err)
	}

	_, err = d.conn.Exec("CREATE INDEX IF NOT EXISTS idx_channel ON file_status(channel_id)")
	if err != nil {
		return fmt.Errorf("failed to create channel index: %w", err)
	}

	return nil
}

// InsertFile adds a new file record to the database.
func (d *DB) InsertFile(file *models.FileInfo) error {
	query := `
	INSERT INTO file_status (
		created_at, channel_id, channel_title, message_id,
		sender_id, sender_username, original_name, prefixed_name,
		file_id, file_size, sent_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := d.conn.Exec(query,
		time.Now().UTC().Format(time.RFC3339),
		file.ChannelID,
		file.ChannelTitle,
		file.MessageID,
		file.SenderID,
		file.SenderUsername,
		file.OriginalName,
		file.PrefixedName,
		file.FileID,
		file.FileSize,
		file.SentAt.UTC().Format(time.RFC3339),
	)

	if err != nil {
		return fmt.Errorf("failed to insert file: %w", err)
	}

	return nil
}

// GetPendingFiles returns all files that haven't been downloaded yet.
// Includes files with started_at set but no downloaded_at (stuck/incomplete downloads).
func (d *DB) GetPendingFiles() ([]*models.FileInfo, error) {
	query := `
	SELECT id, created_at, channel_id, channel_title, message_id,
		sender_id, sender_username, original_name, prefixed_name,
		file_id, file_size, sent_at, started_at, downloaded_at, file_path, data_hash
	FROM file_status
	WHERE downloaded_at IS NULL
	ORDER BY created_at ASC
	`

	rows, err := d.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending files: %w", err)
	}
	defer rows.Close()

	var files []*models.FileInfo
	for rows.Next() {
		var id int64
		var createdAt, sentAt, startedAt, downloadedAt, channelTitle, senderUsername, originalName, prefixedName, fileID, filePath, dataHash sql.NullString
		var senderID sql.NullInt64
		var fileSize sql.NullInt64
		var rowChannelID int64
		var rowMessageID int

		err := rows.Scan(
			&id, &createdAt, &rowChannelID, &channelTitle, &rowMessageID,
			&senderID, &senderUsername, &originalName, &prefixedName, &fileID, &fileSize, &sentAt,
			&startedAt, &downloadedAt, &filePath, &dataHash,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file row: %w", err)
		}

		file := &models.FileInfo{
			ID:        id,
			ChannelID: rowChannelID,
			MessageID: rowMessageID,
		}
		if channelTitle.Valid {
			file.ChannelTitle = channelTitle.String
		}
		if originalName.Valid {
			file.OriginalName = originalName.String
		}
		if prefixedName.Valid {
			file.PrefixedName = prefixedName.String
		}
		if fileID.Valid {
			file.FileID = fileID.String
		}
		if fileSize.Valid {
			file.FileSize = fileSize.Int64
		}
		if createdAt.Valid {
			if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
				file.CreatedAt = t
			}
		}
		if sentAt.Valid {
			if t, err := time.Parse(time.RFC3339, sentAt.String); err == nil {
				file.SentAt = t
			}
		}

		files = append(files, file)
	}

	return files, rows.Err()
}

// GetByChannelMessage checks if a file already exists for a channel/message pair.
func (d *DB) GetByChannelMessage(channelID int64, messageID int) (*models.FileInfo, error) {
	query := `
	SELECT id, created_at, channel_id, channel_title, message_id,
		sender_id, sender_username, original_name, prefixed_name,
		file_id, file_size, sent_at, started_at, downloaded_at, file_path, data_hash
	FROM file_status
	WHERE channel_id = ? AND message_id = ?
	`

	var id int64
	var createdAt, sentAt, startedAt, downloadedAt, channelTitle, senderUsername, originalName, prefixedName, fileID, filePath, dataHash sql.NullString
	var senderID sql.NullInt64
	var fileSize sql.NullInt64
	var rowChannelID int64
	var rowMessageID int

	err := d.conn.QueryRow(query, channelID, messageID).Scan(
		&id, &createdAt, &rowChannelID, &channelTitle, &rowMessageID,
		&senderID, &senderUsername, &originalName, &prefixedName, &fileID, &fileSize, &sentAt,
		&startedAt, &downloadedAt, &filePath, &dataHash,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query file: %w", err)
	}

	file := &models.FileInfo{
		ID:        id,
		ChannelID: rowChannelID,
		MessageID: rowMessageID,
	}
	if senderID.Valid {
		file.SenderID = &senderID.Int64
	}
	if channelTitle.Valid {
		file.ChannelTitle = channelTitle.String
	}
	if originalName.Valid {
		file.OriginalName = originalName.String
	}
	if prefixedName.Valid {
		file.PrefixedName = prefixedName.String
	}
	if fileID.Valid {
		file.FileID = fileID.String
	}
	if fileSize.Valid {
		file.FileSize = fileSize.Int64
	}
	if createdAt.Valid {
		if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
			file.CreatedAt = t
		}
	}
	if sentAt.Valid {
		if t, err := time.Parse(time.RFC3339, sentAt.String); err == nil {
			file.SentAt = t
		}
	}

	return file, nil
}

// GetOrCreateFile checks if a file exists and returns it, or creates a new one.
func (d *DB) GetOrCreateFile(file *models.FileInfo) (*models.FileInfo, bool, error) {
	existing, err := d.GetByChannelMessage(file.ChannelID, file.MessageID)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		return existing, false, nil
	}

	if err := d.InsertFile(file); err != nil {
		return nil, false, err
	}

	return file, true, nil
}

// GetOrCreateOrUpdateFile checks if a file exists, creates new, or updates if different.
// Returns (file, created, updated) where:
// - created: true if new file was inserted
// - updated: true if existing file metadata changed
func (d *DB) GetOrCreateOrUpdateFile(file *models.FileInfo) (*models.FileInfo, bool, bool, error) {
	existing, err := d.GetByChannelMessage(file.ChannelID, file.MessageID)
	if err != nil {
		return nil, false, false, err
	}

	if existing == nil {
		if err := d.InsertFile(file); err != nil {
			return nil, false, false, err
		}
		return file, true, false, nil
	}

	updated := false
	if existing.FileID != file.FileID {
		existing.FileID = file.FileID
		updated = true
	}
	if existing.FileSize != file.FileSize {
		existing.FileSize = file.FileSize
		updated = true
	}
	if existing.OriginalName != file.OriginalName {
		existing.OriginalName = file.OriginalName
		updated = true
	}
	if existing.PrefixedName != file.PrefixedName {
		existing.PrefixedName = file.PrefixedName
		updated = true
	}

	if updated {
		if err := d.UpdateFile(existing); err != nil {
			return nil, false, false, err
		}
	}

	return existing, false, updated, nil
}

// UpdateFile updates an existing file record.
func (d *DB) UpdateFile(file *models.FileInfo) error {
	query := `
	UPDATE file_status
	SET original_name = ?, prefixed_name = ?, file_id = ?, file_size = ?
	WHERE channel_id = ? AND message_id = ?
	`

	_, err := d.conn.Exec(query,
		file.OriginalName, file.PrefixedName, file.FileID, file.FileSize,
		file.ChannelID, file.MessageID,
	)

	if err != nil {
		return fmt.Errorf("failed to update file: %w", err)
	}

	return nil
}

// UpdateStarted marks a file as started downloading.
func (d *DB) UpdateStarted(channelID int64, messageID int) error {
	query := `
	UPDATE file_status
	SET started_at = ?
	WHERE channel_id = ? AND message_id = ?
	`

	_, err := d.conn.Exec(query,
		time.Now().UTC().Format(time.RFC3339),
		channelID,
		messageID,
	)

	if err != nil {
		return fmt.Errorf("failed to update started: %w", err)
	}

	return nil
}

// UpdateCompleted marks a file as downloaded with its hash and path.
func (d *DB) UpdateCompleted(channelID int64, messageID int, prefixedName, filePath, dataHash string) error {
	query := `
	UPDATE file_status
	SET prefixed_name = ?, file_path = ?, data_hash = ?, downloaded_at = ?
	WHERE channel_id = ? AND message_id = ?
	`

	_, err := d.conn.Exec(query,
		prefixedName,
		filePath,
		dataHash,
		time.Now().UTC().Format(time.RFC3339),
		channelID,
		messageID,
	)

	if err != nil {
		return fmt.Errorf("failed to update completed: %w", err)
	}

	return nil
}

// UpdateFailed marks a file as failed.
func (d *DB) UpdateFailed(channelID int64, messageID int) error {
	query := `
	UPDATE file_status
	SET started_at = NULL
	WHERE channel_id = ? AND message_id = ? AND downloaded_at IS NULL
	`

	_, err := d.conn.Exec(query,
		channelID,
		messageID,
	)

	if err != nil {
		return fmt.Errorf("failed to update failed: %w", err)
	}

	return nil
}

// ResetStatus resets all file statuses to pending (clears started_at and downloaded_at).
func (d *DB) ResetStatus() error {
	query := `
	UPDATE file_status
	SET started_at = NULL, downloaded_at = NULL, file_path = NULL, data_hash = NULL
	WHERE downloaded_at IS NOT NULL OR started_at IS NOT NULL
	`

	_, err := d.conn.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to reset status: %w", err)
	}

	return nil
}
