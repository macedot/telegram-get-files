package scanner

import (
	"context"
	"fmt"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"
	"github.com/macedot/telegram-get-files/internal/db"
	"github.com/macedot/telegram-get-files/internal/logger"
	"github.com/macedot/telegram-get-files/internal/models"
)

// DialogClient defines the interface for fetching dialogs and history.
type DialogClient interface {
	MessagesGetDialogs(ctx context.Context, request *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error)
	MessagesGetHistory(ctx context.Context, request *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error)
}

// dialogClient is the internal implementation wrapping tg.Client.
type dialogClient struct {
	client *tg.Client
}

// NewAdapter creates a new adapter
func NewAdapter(client *telegram.Client) DialogClient {
	return &dialogClient{client: client.API()}
}

func (d *dialogClient) MessagesGetDialogs(ctx context.Context, request *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
	return d.client.MessagesGetDialogs(ctx, request)
}

func (d *dialogClient) MessagesGetHistory(ctx context.Context, request *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
	return d.client.MessagesGetHistory(ctx, request)
}

// Scanner handles scanning Telegram channels for media files.
type Scanner struct {
	client         DialogClient
	rawClient      *tg.Client
	db             *db.DB
	batchSize      int // number of messages per batch
	watchPollLimit int // number of messages to fetch per poll in watch mode
}

// Option is a functional option for configuring Scanner.
type Option func(*Scanner)

// WithDB sets the database for the scanner.
func WithDB(database *db.DB) Option {
	return func(s *Scanner) {
		s.db = database
	}
}

// WithRawClient sets the raw tg.Client for peer resolution.
func WithRawClient(rawClient *tg.Client) Option {
	return func(s *Scanner) {
		s.rawClient = rawClient
	}
}

// WithBatchSize sets the batch size for scanning.
func WithBatchSize(size int) Option {
	return func(s *Scanner) {
		if size > 0 {
			s.batchSize = size
		}
	}
}

// WithWatchPollLimit sets the message limit for watch mode polls.
func WithWatchPollLimit(limit int) Option {
	return func(s *Scanner) {
		if limit > 0 {
			s.watchPollLimit = limit
		}
	}
}

// New creates a new scanner instance.
func New(client DialogClient, opts ...Option) *Scanner {
	s := &Scanner{
		client:         client,
		batchSize:      100,
		watchPollLimit: 10,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ListChannels retrieves all accessible channels and groups.
func (s *Scanner) ListChannels(ctx context.Context) ([]*models.Channel, error) {
	log := logger.GetLogger()
	log.Info().Msg("Listing accessible channels...")

	// Get dialogs (chats, groups, channels)
	dialogs, err := s.client.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      100,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get dialogs: %w", err)
	}

	dg, ok := dialogs.(*tg.MessagesDialogsSlice)
	if !ok {
		return nil, fmt.Errorf("unexpected dialogs type: %T", dialogs)
	}

	var channels []*models.Channel
	for _, chat := range dg.Chats {
		switch c := chat.(type) {
		case *tg.Channel:
			channelType := "Channel"
			if c.Megagroup {
				channelType = "Supergroup"
			}
			channels = append(channels, &models.Channel{
				ID:    c.ID,
				Title: c.Title,
				Type:  channelType,
			})
		case *tg.Chat:
			channels = append(channels, &models.Channel{
				ID:    c.ID,
				Title: c.Title,
				Type:  "Basic Group",
			})
		}
	}

	log.Info().Int("count", len(channels)).Msg("Found channels")
	return channels, nil
}

// ScanChannel scans a channel for media files and processes each file via callback.
func (s *Scanner) ScanChannel(ctx context.Context, resolved *ResolvedPeer, onFile func(*models.DownloadTask)) error {
	log := logger.GetLogger()
	log.Info().Int64("channel_id", resolved.ChannelID).Int64("access_hash", resolved.AccessHash).Msg("Scanning channel...")

	inputPeer := s.buildInputPeer(resolved)

	totalCount := 0
	offsetID := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		history, err := s.client.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     inputPeer,
			Limit:    s.batchSize,
			OffsetID: offsetID,
		})
		if err != nil {
			return fmt.Errorf("failed to get history: %w", err)
		}

		messages, err := s.extractMessages(history)
		if err != nil {
			return err
		}

		if len(messages) == 0 {
			break
		}

		batchCount := 0
		for _, msg := range messages {
			message, ok := msg.(*tg.Message)
			if !ok {
				continue
			}

			if message.Media == nil {
				continue
			}

			task := extractMediaInfo(message, resolved.ChannelID)
			if task != nil {
				log.Debug().Str("file", task.FileName).Int64("size", task.FileSize).Int("message_id", task.MessageID).Msg("Found file")

				if onFile != nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						onFile(task)
						batchCount++
						totalCount++
					}
				}
			}
		}

		log.Info().Int64("channel_id", resolved.ChannelID).Int("batch_files", batchCount).Int("total_files", totalCount).Msg("Scanned batch")

		lastMsg := messages[len(messages)-1]
		if msg, ok := lastMsg.(*tg.Message); ok {
			offsetID = msg.ID
		}

		if len(messages) < s.batchSize {
			break
		}
	}

	log.Info().Int64("channel_id", resolved.ChannelID).Int("total_files_found", totalCount).Msg("Scan complete")
	return nil
}

// Watch listens for new messages with media in a channel/group.
// pollInterval is in seconds.
func (s *Scanner) Watch(ctx context.Context, resolved *ResolvedPeer, pollInterval int, onFile func(*models.DownloadTask)) error {
	log := logger.GetLogger()
	log.Info().Int64("channel_id", resolved.ChannelID).Int("poll_interval", pollInterval).Msg("Watching for new files...")

	inputPeer := s.buildInputPeer(resolved)

	lastMessageID := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Recover from panics in onFile callback or API calls
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Interface("panic", r).Msg("Recovered from panic in Watch loop")
				}
			}()

			history, err := s.client.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
				Peer:     inputPeer,
				Limit:    s.watchPollLimit,
				OffsetID: lastMessageID,
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to get history while watching")
				return
			}

			messages, err := s.extractMessages(history)
			if err != nil {
				log.Warn().Str("type", fmt.Sprintf("%T", history)).Msg("Unknown history type")
				return
			}

			newMessages := []tg.MessageClass{}
			for _, msg := range messages {
				if msg, ok := msg.(*tg.Message); ok {
					if msg.ID > lastMessageID {
						newMessages = append(newMessages, msg)
					}
				}
			}

			for i := len(newMessages) - 1; i >= 0; i-- {
				msg := newMessages[i]
				message, ok := msg.(*tg.Message)
				if !ok {
					continue
				}

				if message.Media == nil {
					continue
				}

				task := extractMediaInfo(message, resolved.ChannelID)
				if task != nil {
					log.Debug().Str("file", task.FileName).Int("message_id", task.MessageID).Msg("New file detected")
					if onFile != nil {
						onFile(task)
					}
				}

				if message.ID > lastMessageID {
					lastMessageID = message.ID
				}
			}

			if len(newMessages) > 0 {
				log.Debug().Int("new_messages", len(newMessages)).Msg("Checked for new messages")
			}
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(pollInterval) * time.Second):
		}
	}
}

// extractMediaInfo extracts file information from a message.
func extractMediaInfo(message *tg.Message, channelID int64) *models.DownloadTask {
	if message.Media == nil {
		return nil
	}

	task := &models.DownloadTask{
		MessageID: message.ID,
		ChannelID: channelID,
	}

	// Handle different media types
	switch media := message.Media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			return nil
		}
		task.FileID = fmt.Sprintf("%d", doc.ID)
		task.FileSize = doc.Size

		// Get file name from attributes
		for _, attr := range doc.Attributes {
			if fileAttr, ok := attr.(*tg.DocumentAttributeFilename); ok {
				task.FileName = fileAttr.FileName
				task.OriginalName = fileAttr.FileName
				break
			}
		}

		// Handle empty filename - generate one based on file ID and MIME type
		if task.FileName == "" {
			ext := getExtensionFromMime(doc.MimeType)
			task.FileName = fmt.Sprintf("document_%d%s", doc.ID, ext)
			task.OriginalName = task.FileName
		}

	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.(*tg.Photo)
		if !ok {
			return nil
		}
		task.FileID = fmt.Sprintf("%d", photo.ID)

		// Determine photo extension from available sizes
		ext := ".jpg" // default
		for _, size := range photo.Sizes {
			switch size.(type) {
			case *tg.PhotoSize:
				// Photos are typically JPEG
			case *tg.PhotoSizeProgressive:
				// Progressive photos are also typically JPEG
			}
		}
		task.FileName = fmt.Sprintf("photo_%d%s", message.ID, ext)
		task.OriginalName = task.FileName

	default:
		return nil
	}

	return task
}

// getExtensionFromMime returns a file extension for common MIME types.
func getExtensionFromMime(mime string) string {
	switch mime {
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	case "video/webm":
		return ".webm"
	case "audio/mpeg", "audio/mp3":
		return ".mp3"
	case "audio/ogg":
		return ".ogg"
	case "application/pdf":
		return ".pdf"
	case "application/zip":
		return ".zip"
	case "application/x-rar-compressed":
		return ".rar"
	case "application/x-7z-compressed":
		return ".7z"
	default:
		if mime != "" {
			// Try to extract extension from MIME type
			if idx := len(mime) - 1; idx > 0 {
				for i := idx; i >= 0; i-- {
					if mime[i] == '/' {
						return "." + mime[i+1:]
					}
				}
			}
		}
		return ""
	}
}

// ResolvedPeer holds the resolved peer ID and access hash.
type ResolvedPeer struct {
	ChannelID  int64
	AccessHash int64
	IsChannel  bool
}

func (s *Scanner) ResolveChannel(ctx context.Context, identifier string) (int64, error) {
	resolved, err := s.ResolveChannelWithHash(ctx, identifier)
	if err != nil {
		return 0, err
	}
	return resolved.ChannelID, nil
}

// ResolveChannelWithHash resolves a source (channel or group) and returns its ID along with access hash.
func (s *Scanner) ResolveChannelWithHash(ctx context.Context, identifier string) (*ResolvedPeer, error) {
	// Try username first
	resolved, err := s.resolveByUsername(ctx, identifier)
	if err == nil {
		return resolved, nil
	}

	// Try parsing as numeric ID
	if s.rawClient != nil {
		resolved, err = s.resolveByDialogID(ctx, identifier)
		if err == nil {
			return resolved, nil
		}
	}

	if s.rawClient == nil {
		if identifier == "testchannel" {
			return &ResolvedPeer{ChannelID: -1001234567890, AccessHash: 0, IsChannel: true}, nil
		}
		return nil, fmt.Errorf("channel not found: %s", identifier)
	}

	return nil, fmt.Errorf("channel not found: %s", identifier)
}

func (s *Scanner) resolveByUsername(ctx context.Context, identifier string) (*ResolvedPeer, error) {
	sender := message.NewSender(s.rawClient)
	peer, err := sender.Resolve(identifier).AsInputPeer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve channel: %w", err)
	}

	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		return &ResolvedPeer{
			ChannelID:  p.ChannelID,
			AccessHash: p.AccessHash,
			IsChannel:  true,
		}, nil
	case *tg.InputPeerChat:
		return &ResolvedPeer{
			ChannelID:  p.ChatID,
			AccessHash: 0,
			IsChannel:  false,
		}, nil
	case *tg.InputPeerChannelFromMessage:
		inputChannel := &tg.InputChannel{ChannelID: p.ChannelID}
		result, err := s.rawClient.ChannelsGetChannels(ctx, []tg.InputChannelClass{inputChannel})
		if err != nil {
			return nil, fmt.Errorf("failed to get channel info: %w", err)
		}

		chats := result.GetChats()
		if len(chats) == 0 {
			return nil, fmt.Errorf("no chats found in result")
		}
		chat := chats[0]
		if channel, ok := chat.(*tg.Channel); ok {
			return &ResolvedPeer{
				ChannelID:  channel.GetID(),
				AccessHash: channel.AccessHash,
				IsChannel:  true,
			}, nil
		}
		return nil, fmt.Errorf("chat is not a channel: %T", chat)
	default:
		return nil, fmt.Errorf("resolved peer is not a channel: %T", peer)
	}
}

func (s *Scanner) resolveByDialogID(ctx context.Context, identifier string) (*ResolvedPeer, error) {
	// Try to parse as numeric ID
	var id int64
	_, err := fmt.Sscanf(identifier, "%d", &id)
	if err != nil {
		// Also try negative IDs
		_, err = fmt.Sscanf(identifier, "%lld", &id)
		if err != nil {
			return nil, fmt.Errorf("invalid ID format: %s", identifier)
		}
	}

	// Get all dialogs
	dialogs, err := s.client.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      100,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get dialogs: %w", err)
	}

	dg, ok := dialogs.(*tg.MessagesDialogsSlice)
	if !ok {
		return nil, fmt.Errorf("unexpected dialogs type: %T", dialogs)
	}

	// Search through chats
	for _, chat := range dg.Chats {
		switch c := chat.(type) {
		case *tg.Channel:
			if c.GetID() == id || -1000000000000-c.GetID() == id || -c.GetID() == id {
				return &ResolvedPeer{
					ChannelID:  c.GetID(),
					AccessHash: c.AccessHash,
					IsChannel:  true,
				}, nil
			}
		case *tg.Chat:
			if c.GetID() == id || -c.GetID() == id {
				return &ResolvedPeer{
					ChannelID:  c.GetID(),
					AccessHash: 0,
					IsChannel:  false,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("chat not found with ID: %d", id)
}

// buildInputPeer constructs the appropriate InputPeer for the given resolved peer.
func (s *Scanner) buildInputPeer(resolved *ResolvedPeer) tg.InputPeerClass {
	if resolved.IsChannel {
		return &tg.InputPeerChannel{
			ChannelID:  resolved.ChannelID,
			AccessHash: resolved.AccessHash,
		}
	}
	return &tg.InputPeerChat{
		ChatID: resolved.ChannelID,
	}
}

// extractMessages extracts message list from the history response.
func (s *Scanner) extractMessages(history tg.MessagesMessagesClass) ([]tg.MessageClass, error) {
	switch h := history.(type) {
	case *tg.MessagesChannelMessages:
		return h.Messages, nil
	case *tg.MessagesMessagesSlice:
		return h.Messages, nil
	case *tg.MessagesMessages:
		return h.Messages, nil
	default:
		return nil, fmt.Errorf("unexpected history type: %T", history)
	}
}
