package downloader

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/macedot/telegram-get-files/internal/db"
	"github.com/macedot/telegram-get-files/internal/logger"
	"github.com/macedot/telegram-get-files/internal/models"
)

// Pool manages a pool of download workers.
type Pool struct {
	workers      int
	db           *db.DB
	api          *tg.Client
	downloadPath string
	queue        chan *models.DownloadTask
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewPool creates a new download worker pool.
func NewPool(workers int, database *db.DB, ctx context.Context) *Pool {
	ctx, cancel := context.WithCancel(ctx)
	return &Pool{
		workers: workers,
		db:      database,
		queue:   make(chan *models.DownloadTask, 100),
		ctx:     ctx,
		cancel:  cancel,
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

// Start begins all worker goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// Stop signals all workers to stop and waits for them to finish.
func (p *Pool) Stop() {
	p.cancel()
	close(p.queue)
	p.wg.Wait()
}

// Submit adds a task to the download queue.
func (p *Pool) Submit(task *models.DownloadTask) {
	select {
	case p.queue <- task:
	case <-p.ctx.Done():
	}
}

// worker is the main loop for each download worker.
func (p *Pool) worker(id int) {
	defer p.wg.Done()
	log := logger.GetLogger()

	for {
		select {
		case task, ok := <-p.queue:
			if !ok {
				return
			}
			if err := p.downloadTask(task); err != nil {
				log.Error().Err(err).Int("worker", id).Int("message_id", task.MessageID).Msg("Download failed")
				if p.db != nil {
					if err := p.db.UpdateFailed(task.ChannelID, task.MessageID); err != nil {
						log.Error().Err(err).Int("message_id", task.MessageID).Msg("Failed to mark download as failed in database")
					}
				}
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

	fileName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), task.FileName)
	filePath := filepath.Join(p.downloadPath, fileName)

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

	log.Debug().Int64("file_id", doc.ID).Int64("access_hash", doc.AccessHash).Msg("Starting file download")

	downloadCtx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	_, err = downloader.NewDownloader().Download(p.api, location).Stream(downloadCtx, file)

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

	hash, err := ComputeHash(filePath)
	if err != nil {
		log.Error().Err(err).Str("file", fileName).Msg("Failed to compute hash")
		hash = ""
	}

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

// fetchFreshDocument fetches the current document from Telegram to get fresh file reference.
func (p *Pool) fetchFreshDocument(task *models.DownloadTask) (*tg.Document, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
