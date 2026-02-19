package telegram

import (
	"context"
	"testing"

	"github.com/macedot/telegram-get-files/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	cfg := &config.Config{
		APIID:       123456,
		APIHash:     "test_hash",
		SessionFile: "session.json",
	}

	client := NewClient(cfg)

	require.NotNil(t, client)
	assert.Equal(t, 123456, client.apiID)
	assert.Equal(t, "test_hash", client.apiHash)
	assert.Equal(t, "session.json", client.sessionPath)
}

func TestNewClient_DefaultConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.APIID = 123
	cfg.APIHash = "hash"

	client := NewClient(cfg)

	require.NotNil(t, client)
	assert.Equal(t, 123, client.apiID)
	assert.Equal(t, "hash", client.apiHash)
}

func TestClient_IsAuthorized_NotExists(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.SessionFile = "/nonexistent/session.json"

	client := NewClient(cfg)

	assert.False(t, client.IsAuthorized())
}

func TestClient_Close(t *testing.T) {
	client := &Client{}

	err := client.Close()
	assert.NoError(t, err)
}

func TestClient_GetMe_NotStarted(t *testing.T) {
	client := &Client{}

	_, err := client.GetMe(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client not started")
}

func TestClient_Raw_Nil(t *testing.T) {
	client := &Client{}

	assert.Nil(t, client.Raw())
}
