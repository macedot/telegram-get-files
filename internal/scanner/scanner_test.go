package scanner

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/macedot/telegram-get-files/internal/models"
	"github.com/stretchr/testify/assert"
)

// MockDialogClient implements DialogClient for testing
type MockDialogClient struct {
	DialogsToReturn tg.MessagesDialogsClass
	HistoryToReturn tg.MessagesMessagesClass
	Err             error
}

func (m MockDialogClient) MessagesGetDialogs(ctx context.Context, request *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return m.DialogsToReturn, nil
}

func (m MockDialogClient) MessagesGetHistory(ctx context.Context, request *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return m.HistoryToReturn, nil
}

func TestScanner_ListChannels(t *testing.T) {
	dialogs := &tg.MessagesDialogsSlice{
		Chats: []tg.ChatClass{
			&tg.Channel{
				ID:        123,
				Title:     "Test Channel",
				Megagroup: false,
			},
			&tg.Chat{
				ID:    456,
				Title: "Test Group",
			},
		},
	}

	client := MockDialogClient{
		DialogsToReturn: dialogs,
	}

	scanner := New(client)
	channels, err := scanner.ListChannels(context.Background())

	assert.NoError(t, err)
	assert.Len(t, channels, 2)
	assert.Equal(t, "Test Channel", channels[0].Title)
	assert.Equal(t, "Channel", channels[0].Type)
	assert.Equal(t, "Test Group", channels[1].Title)
	assert.Equal(t, "Basic Group", channels[1].Type)
}

func TestScanner_ListChannels_Megagroup(t *testing.T) {
	dialogs := &tg.MessagesDialogsSlice{
		Chats: []tg.ChatClass{
			&tg.Channel{
				ID:        123,
				Title:     "Super Group",
				Megagroup: true,
			},
		},
	}

	client := MockDialogClient{
		DialogsToReturn: dialogs,
	}

	scanner := New(client)
	channels, err := scanner.ListChannels(context.Background())

	assert.NoError(t, err)
	assert.Len(t, channels, 1)
	assert.Equal(t, "Supergroup", channels[0].Type)
}

func TestScanner_ListChannels_Error(t *testing.T) {
	client := MockDialogClient{
		Err: assert.AnError,
	}

	scanner := New(client)
	channels, err := scanner.ListChannels(context.Background())

	assert.Error(t, err)
	assert.Nil(t, channels)
}

func TestScanner_ScanChannel(t *testing.T) {
	messages := &tg.MessagesChannelMessages{
		Messages: []tg.MessageClass{
			&tg.Message{
				ID: 100,
				Media: &tg.MessageMediaDocument{
					Document: &tg.Document{
						ID:   999,
						Size: 1024,
						Attributes: []tg.DocumentAttributeClass{
							&tg.DocumentAttributeFilename{
								FileName: "test.pdf",
							},
						},
					},
				},
			},
		},
	}

	client := MockDialogClient{
		HistoryToReturn: messages,
	}

	scanner := New(client)

	var foundTasks []*models.DownloadTask
	resolved := &ResolvedPeer{ChannelID: 123, AccessHash: 0, IsChannel: true}
	err := scanner.ScanChannel(context.Background(), resolved, func(task *models.DownloadTask) {
		foundTasks = append(foundTasks, task)
	})

	assert.NoError(t, err)
	assert.Len(t, foundTasks, 1)
	assert.Equal(t, 100, foundTasks[0].MessageID)
	assert.Equal(t, int64(123), foundTasks[0].ChannelID)
	assert.Equal(t, "test.pdf", foundTasks[0].FileName)
	assert.Equal(t, int64(1024), foundTasks[0].FileSize)
}

func TestScanner_ScanChannel_Photo(t *testing.T) {
	messages := &tg.MessagesChannelMessages{
		Messages: []tg.MessageClass{
			&tg.Message{
				ID: 200,
				Media: &tg.MessageMediaPhoto{
					Photo: &tg.Photo{
						ID: 888,
					},
				},
			},
		},
	}

	client := MockDialogClient{
		HistoryToReturn: messages,
	}

	scanner := New(client)

	var foundTasks []*models.DownloadTask
	resolved := &ResolvedPeer{ChannelID: 123, AccessHash: 0, IsChannel: true}
	err := scanner.ScanChannel(context.Background(), resolved, func(task *models.DownloadTask) {
		foundTasks = append(foundTasks, task)
	})

	assert.NoError(t, err)
	assert.Len(t, foundTasks, 1)
	assert.Equal(t, 200, foundTasks[0].MessageID)
	assert.Contains(t, foundTasks[0].FileName, "photo_")
}

func TestScanner_ScanChannel_EmptyHistory(t *testing.T) {
	messages := &tg.MessagesChannelMessages{
		Messages: []tg.MessageClass{},
	}

	client := MockDialogClient{
		HistoryToReturn: messages,
	}

	scanner := New(client)

	var foundTasks []*models.DownloadTask
	resolved := &ResolvedPeer{ChannelID: 123, AccessHash: 0, IsChannel: true}
	err := scanner.ScanChannel(context.Background(), resolved, func(task *models.DownloadTask) {
		foundTasks = append(foundTasks, task)
	})

	assert.NoError(t, err)
	assert.Len(t, foundTasks, 0)
}

func TestScanner_ScanChannel_NonMediaMessage(t *testing.T) {
	messages := &tg.MessagesChannelMessages{
		Messages: []tg.MessageClass{
			&tg.Message{
				ID:      300,
				Media:   nil,
				Message: "This is a text message",
			},
		},
	}

	client := MockDialogClient{
		HistoryToReturn: messages,
	}

	scanner := New(client)

	var foundTasks []*models.DownloadTask
	resolved := &ResolvedPeer{ChannelID: 123, AccessHash: 0, IsChannel: true}
	err := scanner.ScanChannel(context.Background(), resolved, func(task *models.DownloadTask) {
		foundTasks = append(foundTasks, task)
	})

	assert.NoError(t, err)
	assert.Len(t, foundTasks, 0)
}

func TestScanner_ScanChannel_IgnoresNonDocumentPhoto(t *testing.T) {
	messages := &tg.MessagesChannelMessages{
		Messages: []tg.MessageClass{
			&tg.Message{
				ID: 400,
				Media: &tg.MessageMediaVenue{
					Geo: &tg.GeoPoint{},
				},
			},
		},
	}

	client := MockDialogClient{
		HistoryToReturn: messages,
	}

	scanner := New(client)

	var foundTasks []*models.DownloadTask
	resolved := &ResolvedPeer{ChannelID: 123, AccessHash: 0, IsChannel: true}
	err := scanner.ScanChannel(context.Background(), resolved, func(task *models.DownloadTask) {
		foundTasks = append(foundTasks, task)
	})

	assert.NoError(t, err)
	assert.Len(t, foundTasks, 0)
}

func TestScanner_ScanChannel_ContextCancellation(t *testing.T) {
	messages := &tg.MessagesChannelMessages{
		Messages: []tg.MessageClass{
			&tg.Message{
				ID: 500,
				Media: &tg.MessageMediaDocument{
					Document: &tg.Document{
						ID:   777,
						Size: 512,
					},
				},
			},
		},
	}

	client := MockDialogClient{
		HistoryToReturn: messages,
		Err:             context.Canceled,
	}

	scanner := New(client)

	resolved := &ResolvedPeer{ChannelID: 123, AccessHash: 0, IsChannel: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := scanner.ScanChannel(ctx, resolved, func(task *models.DownloadTask) {})

	assert.Error(t, err)
}

func TestResolvedPeer_IsChannel(t *testing.T) {
	// Test channel type
	resolved := &ResolvedPeer{ChannelID: 123, AccessHash: 456, IsChannel: true}
	assert.True(t, resolved.IsChannel)
	assert.Equal(t, int64(123), resolved.ChannelID)
	assert.Equal(t, int64(456), resolved.AccessHash)

	// Test group type
	resolvedGroup := &ResolvedPeer{ChannelID: 789, AccessHash: 0, IsChannel: false}
	assert.False(t, resolvedGroup.IsChannel)
	assert.Equal(t, int64(789), resolvedGroup.ChannelID)
}
