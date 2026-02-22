package downloader

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/macedot/telegram-get-files/internal/db"
	"github.com/macedot/telegram-get-files/internal/logger"
	"github.com/macedot/telegram-get-files/internal/models"
)

// Pool manages a pool of download workers.
type Pool struct {
	workers         int
	db              *db.DB
	api             *tg.Client
	downloadPath    string
	downloadTimeout int // seconds
	queue           chan *models.DownloadTask
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	stopped         bool
}

// NewPool creates a new download worker pool.
func NewPool(workers int, database *db.DB, ctx context.Context) *Pool {
	ctx, cancel := context.WithCancel(ctx)
	return &Pool{
		workers:         workers,
		db:              database,
		downloadTimeout: 600, // default 10 minutes
		queue:           make(chan *models.DownloadTask, 100),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// WithClient sets the Telegram API client for the pool.
func (p *Pool) WithClient(api *tg.Client) *Pool {
	p.api = api
	return p
}

// WithDownloadPath sets the download path for the pool.
func (p *Pool) WithDownloadPath(path string) *Pool {
	p.downloadPath = path
	return p
}

// WithDownloadTimeout sets the download timeout in seconds.
func (p *Pool) WithDownloadTimeout(timeout int) *Pool {
	if timeout > 0 {
		p.downloadTimeout = timeout
	}
	return p
}

// Start begins all worker goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// Stop signals all workers to stop and waits for them to finish.
// Safe to call multiple times.
func (p *Pool) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()

	close(p.queue)
	p.wg.Wait()
	p.cancel()
}

// Submit adds a task to the download queue.
// Returns error if pool is stopped or context cancelled.
func (p *Pool) Submit(task *models.DownloadTask) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return fmt.Errorf("pool is stopped")
	}

	select {
	case p.queue <- task:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// worker is the main loop for each download worker.
func (p *Pool) worker(id int) {
	log := logger.GetLogger()
	defer func() {
		if r := recover(); r != nil {
			log.Error().Interface("panic", r).Int("worker", id).Msg("Worker panicked, recovering")
		}
		p.wg.Done()
	}()

	for {
		select {
		case task, ok := <-p.queue:
			if !ok {
				return
			}
			if err := p.downloadTask(task); err != nil {
				log.Error().Err(err).Int("worker", id).Int("message_id", task.MessageID).Msg("Download failed")
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pool) downloadTask(task *models.DownloadTask) error {
	log := logger.GetLogger()
	log.Info().Int("message_id", task.MessageID).Str("file", task.FileName).Msg("Starting download")

	if p.api == nil {
		return fmt.Errorf("telegram client not set")
	}

	if p.downloadPath == "" {
		return fmt.Errorf("download path not set")
	}

	if task.FilePath != "" && FileExists(task.FilePath) {
		log.Info().Str("file", task.FilePath).Msg("File already exists in database path, marking as complete")
		if p.db != nil {
			if err := p.db.UpdateCompleted(task.ChannelID, task.MessageID, task.FileName, task.FilePath, ""); err != nil {
				log.Error().Err(err).Int("message_id", task.MessageID).Msg("Failed to update database")
			}
		}
		return nil
	}

	doc, err := p.fetchFreshDocument(task)
	if err != nil {
		log.Error().Err(err).Int("message_id", task.MessageID).Msg("Failed to fetch fresh document")
		if p.db != nil {
			if err := p.db.UpdateFailed(task.ChannelID, task.MessageID); err != nil {
				log.Error().Err(err).Int("message_id", task.MessageID).Msg("Failed to mark download as failed in database")
			}
		}
		return fmt.Errorf("failed to fetch fresh file reference: %w", err)
	}

	fileID := doc.ID
	location := &tg.InputDocumentFileLocation{
		ID:            fileID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}

	cleanName := sanitizeFilename(task.FileName)
	fileName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), cleanName)
	filePath := filepath.Join(p.downloadPath, fileName)

	if err := validatePath(filePath, p.downloadPath); err != nil {
		log.Error().Err(err).Str("original", task.FileName).Msg("Path validation failed")
		return fmt.Errorf("invalid file path: %w", err)
	}

	if err := EnsureDir(filePath); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if FileExists(filePath) {
		log.Info().Str("file", fileName).Msg("File already exists, skipping")
		return nil
	}

	tempPath := filePath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Compute hash during download using MultiWriter to avoid double I/O
	hasher := sha256.New()
	multiWriter := io.MultiWriter(file, hasher)

	log.Debug().Int64("file_id", doc.ID).Int64("access_hash", doc.AccessHash).Msg("Starting file download")

	// Use pool context as parent for proper cancellation on shutdown
	downloadCtx, cancel := context.WithTimeout(p.ctx, time.Duration(p.downloadTimeout)*time.Second)
	defer cancel()
	_, err = downloader.NewDownloader().Download(p.api, location).Stream(downloadCtx, multiWriter)

	if err != nil {
		os.Remove(tempPath)
		if p.db != nil {
			if err := p.db.UpdateFailed(task.ChannelID, task.MessageID); err != nil {
				log.Error().Err(err).Int("message_id", task.MessageID).Msg("Failed to mark download as failed in database")
			}
		}
		return fmt.Errorf("download failed: %w", err)
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename file: %w", err)
	}

	// Hash was computed during download
	hash := fmt.Sprintf("%x", hasher.Sum(nil))

	if p.db != nil {
		if err := p.db.UpdateCompleted(task.ChannelID, task.MessageID, fileName, filePath, hash); err != nil {
			log.Error().Err(err).Int("message_id", task.MessageID).Msg("Failed to update database")
		}
	}

	log.Info().Int("message_id", task.MessageID).Str("file", fileName).Msg("Download complete")
	return nil
}

// ComputeHash computes SHA256 hash of a file.
func ComputeHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to compute hash: %w", err)
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// FileExists checks if a file already exists at the given path.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// EnsureDir ensures the directory for the given path exists.
func EnsureDir(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0755)
}

// sanitizeFilename removes path components and dangerous filesystem characters
// while preserving unicode characters for international filename support.
func sanitizeFilename(name string) string {
	// Replace backslashes with forward slashes for consistent handling
	name = strings.ReplaceAll(name, "\\", "/")

	// Remove any path components (handles / and .. sequences)
	name = filepath.Base(name)

	// Replace control characters with underscores
	result := strings.Builder{}
	for _, r := range name {
		if unicode.IsControl(r) {
			result.WriteRune('_')
		} else {
			result.WriteRune(r)
		}
	}
	name = result.String()

	// Limit length to prevent filesystem issues
	if len(name) > 200 {
		for i := 200; i > 0; i-- {
			if utf8.RuneStart(name[i]) {
				name = name[:i]
				break
			}
		}
	}

	// Prevent empty, dot-only, or slash-only filenames
	if name == "" || name == "." || name == ".." || name == "/" {
		name = "download"
	}

	return name
}

// validatePath ensures the target path is within the allowed download directory.
func validatePath(targetPath, downloadDir string) error {
	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		return fmt.Errorf("failed to resolve target path: %w", err)
	}

	absDownload, err := filepath.Abs(downloadDir)
	if err != nil {
		return fmt.Errorf("failed to resolve download path: %w", err)
	}

	if !strings.HasPrefix(absTarget, absDownload+string(filepath.Separator)) {
		return fmt.Errorf("path traversal attempt detected")
	}

	return nil
}

// fetchFreshDocument fetches the current document from Telegram to get fresh file reference.
func (p *Pool) fetchFreshDocument(task *models.DownloadTask) (*tg.Document, error) {
	// Use pool context for proper cancellation on shutdown
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()

	msgs, err := p.api.MessagesGetMessages(ctx, []tg.InputMessageClass{&tg.InputMessageID{ID: task.MessageID}})
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	messages, ok := msgs.(*tg.MessagesMessages)
	if !ok {
		return nil, fmt.Errorf("unexpected messages type: %T", msgs)
	}

	if len(messages.Messages) == 0 {
		return nil, fmt.Errorf("message not found: %d", task.MessageID)
	}

	msg, ok := messages.Messages[0].(*tg.Message)
	if !ok {
		return nil, fmt.Errorf("unexpected message type: %T", messages.Messages[0])
	}

	if msg.Media == nil {
		return nil, fmt.Errorf("message has no media")
	}

	doc, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return nil, fmt.Errorf("media is not a document")
	}

	document, ok := doc.Document.(*tg.Document)
	if !ok {
		return nil, fmt.Errorf("document is not a valid document type")
	}

	return document, nil
}
