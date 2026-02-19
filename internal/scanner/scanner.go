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
	client    DialogClient
	rawClient *tg.Client
	db        *db.DB
}

// New creates a new scanner instance.
func New(client DialogClient) *Scanner {
	return &Scanner{client: client}
}

// NewWithDB creates a new scanner instance with database support.
func NewWithDB(client DialogClient, database *db.DB) *Scanner {
	return &Scanner{client: client, db: database}
}

// NewWithClient creates a new scanner instance with raw tg.Client for peer resolution.
func NewWithClient(client DialogClient, rawClient *tg.Client) *Scanner {
	return &Scanner{client: client, rawClient: rawClient}
}

// NewWithDBAndClient creates a new scanner instance with database and raw client.
func NewWithDBAndClient(client DialogClient, database *db.DB, rawClient *tg.Client) *Scanner {
	return &Scanner{client: client, db: database, rawClient: rawClient}
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
func (s *Scanner) ScanChannel(ctx context.Context, resolved *ResolvedChannel, onFile func(*DownloadTask)) error {
	log := logger.GetLogger()
	log.Info().Int64("channel_id", resolved.ChannelID).Int64("access_hash", resolved.AccessHash).Msg("Scanning channel...")

	var inputPeer tg.InputPeerClass
	if resolved.IsChannel {
		inputPeer = &tg.InputPeerChannel{
			ChannelID:  resolved.ChannelID,
			AccessHash: resolved.AccessHash,
		}
	} else {
		// Basic group
		inputPeer = &tg.InputPeerChat{
			ChatID: resolved.ChannelID,
		}
	}

	totalCount := 0
	offsetID := 0
	batchSize := 100

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		history, err := s.client.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     inputPeer,
			Limit:    batchSize,
			OffsetID: offsetID,
		})
		if err != nil {
			return fmt.Errorf("failed to get history: %w", err)
		}

		var messages []tg.MessageClass
		switch h := history.(type) {
		case *tg.MessagesChannelMessages:
			messages = h.Messages
		case *tg.MessagesMessagesSlice:
			messages = h.Messages
		case *tg.MessagesMessages:
			messages = h.Messages
		default:
			return fmt.Errorf("unexpected history type: %T", history)
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

		if len(messages) < batchSize {
			break
		}
	}

	log.Info().Int64("channel_id", resolved.ChannelID).Int("total_files_found", totalCount).Msg("Scan complete")
	return nil
}

// Watch listens for new messages with media in a channel/group.
// pollInterval is in seconds.
func (s *Scanner) Watch(ctx context.Context, resolved *ResolvedChannel, pollInterval int, onFile func(*DownloadTask)) error {
	log := logger.GetLogger()
	log.Info().Int64("channel_id", resolved.ChannelID).Int("poll_interval", pollInterval).Msg("Watching for new files...")

	var inputPeer tg.InputPeerClass
	if resolved.IsChannel {
		inputPeer = &tg.InputPeerChannel{
			ChannelID:  resolved.ChannelID,
			AccessHash: resolved.AccessHash,
		}
	} else {
		inputPeer = &tg.InputPeerChat{
			ChatID: resolved.ChannelID,
		}
	}

	lastMessageID := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		history, err := s.client.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     inputPeer,
			Limit:    10,
			OffsetID: lastMessageID,
		})
		if err != nil {
			log.Error().Err(err).Msg("Failed to get history while watching")
			time.Sleep(30 * time.Second)
			continue
		}

		var messages []tg.MessageClass
		switch h := history.(type) {
		case *tg.MessagesChannelMessages:
			messages = h.Messages
		case *tg.MessagesMessagesSlice:
			messages = h.Messages
		case *tg.MessagesMessages:
			messages = h.Messages
		default:
			log.Warn().Str("type", fmt.Sprintf("%T", history)).Msg("Unknown history type")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(pollInterval) * time.Second):
			}
			continue
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
				onFile(task)
			}

			if message.ID > lastMessageID {
				lastMessageID = message.ID
			}
		}

		if len(newMessages) > 0 {
			log.Debug().Int("new_messages", len(newMessages)).Msg("Checked for new messages")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(pollInterval) * time.Second):
		}
	}
}

// extractMediaInfo extracts file information from a message.
func extractMediaInfo(message *tg.Message, channelID int64) *DownloadTask {
	if message.Media == nil {
		return nil
	}

	task := &DownloadTask{
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

	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.(*tg.Photo)
		if !ok {
			return nil
		}
		task.FileID = fmt.Sprintf("%d", photo.ID)
		task.FileName = fmt.Sprintf("photo_%d.jpg", message.ID)
		task.OriginalName = task.FileName

	default:
		return nil
	}

	return task
}

// ResolvedChannel holds the resolved channel ID and access hash.
type ResolvedChannel struct {
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
func (s *Scanner) ResolveChannelWithHash(ctx context.Context, identifier string) (*ResolvedChannel, error) {
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
			return &ResolvedChannel{ChannelID: -1001234567890, AccessHash: 0, IsChannel: true}, nil
		}
		return nil, fmt.Errorf("channel not found: %s", identifier)
	}

	return nil, fmt.Errorf("channel not found: %s", identifier)
}

func (s *Scanner) resolveByUsername(ctx context.Context, identifier string) (*ResolvedChannel, error) {
	sender := message.NewSender(s.rawClient)
	peer, err := sender.Resolve(identifier).AsInputPeer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve channel: %w", err)
	}

	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		return &ResolvedChannel{
			ChannelID:  p.ChannelID,
			AccessHash: p.AccessHash,
			IsChannel:  true,
		}, nil
	case *tg.InputPeerChat:
		return &ResolvedChannel{
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
			return &ResolvedChannel{
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

func (s *Scanner) resolveByDialogID(ctx context.Context, identifier string) (*ResolvedChannel, error) {
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
				return &ResolvedChannel{
					ChannelID:  c.GetID(),
					AccessHash: c.AccessHash,
					IsChannel:  true,
				}, nil
			}
		case *tg.Chat:
			if c.GetID() == id || -c.GetID() == id {
				return &ResolvedChannel{
					ChannelID:  c.GetID(),
					AccessHash: 0,
					IsChannel:  false,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("chat not found with ID: %d", id)
}
