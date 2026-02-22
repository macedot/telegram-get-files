package telegram

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"golang.org/x/term"

	"github.com/macedot/telegram-get-files/internal/config"
	"github.com/macedot/telegram-get-files/internal/logger"
)

// Client wraps the gotd Telegram client.
type Client struct {
	client      *telegram.Client
	apiID       int
	apiHash     string
	sessionPath string
}

// NewClient creates a new Telegram client.
func NewClient(cfg *config.Config) *Client {
	return &Client{
		apiID:       cfg.APIID,
		apiHash:     cfg.APIHash,
		sessionPath: cfg.SessionFile,
	}
}

// Start initializes and connects the Telegram client, then executes the callback.
func (c *Client) Start(ctx context.Context, callback func(ctx context.Context) error) error {
	log := logger.GetLogger()
	log.Info().Msg("Starting Telegram client")
	log.Debug().Int("api_id", c.apiID).Str("session", c.sessionPath).Msg("Client configuration")

	sessionStorage := &session.FileStorage{
		Path: c.sessionPath,
	}

	log.Debug().Msg("Creating Telegram client...")
	client := telegram.NewClient(c.apiID, c.apiHash, telegram.Options{
		SessionStorage: sessionStorage,
	})

	c.client = client

	log.Info().Msg("Connecting to Telegram servers (this may take a moment)...")

	err := client.Run(ctx, func(ctx context.Context) error {
		log.Info().Msg("Connected! Running authentication...")
		if err := c.authenticate(ctx); err != nil {
			log.Error().Err(err).Msg("Authentication failed")
			return fmt.Errorf("authentication failed: %w", err)
		}

		log.Info().Msg("Authentication successful")
		if callback != nil {
			return callback(ctx)
		}

		return nil
	})
	if err != nil {
		log.Error().Err(err).Msg("Client error")
	}
	return err
}

// authenticate handles the authentication flow using terminal prompts.
func (c *Client) authenticate(ctx context.Context) error {
	flow := auth.NewFlow(
		&terminalAuthenticator{},
		auth.SendCodeOptions{},
	)

	return c.client.Auth().IfNecessary(ctx, flow)
}

// GetMe returns the current user information.
func (c *Client) GetMe(ctx context.Context) (*tg.User, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not started")
	}
	return c.client.Self(ctx)
}

// Close is a no-op because the gotd client manages its own lifecycle via the Run() method.
// The connection is automatically closed when the context passed to Start() is cancelled
// or when the callback function returns. This method exists for interface compatibility.
func (c *Client) Close() error {
	return nil
}

// Raw returns the raw MTProto client for advanced operations.
func (c *Client) Raw() *telegram.Client {
	return c.client
}

// IsAuthorized checks if the session is already authorized.
func (c *Client) IsAuthorized() bool {
	_, err := os.Stat(c.sessionPath)
	return !os.IsNotExist(err)
}

// terminalAuthenticator implements auth.UserAuthenticator for terminal-based authentication.
type terminalAuthenticator struct{}

// Phone implements auth.UserAuthenticator.
func (t *terminalAuthenticator) Phone(ctx context.Context) (string, error) {
	fmt.Print("Enter phone number (with country code, e.g., +1234567890): ")
	reader := bufio.NewReader(os.Stdin)
	phone, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(phone), nil
}

// Password implements auth.UserAuthenticator for 2FA.
func (t *terminalAuthenticator) Password(ctx context.Context) (string, error) {
	fmt.Print("Enter 2FA password (if enabled): ")
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return "", err
	}
	return string(password), nil
}

// Code implements auth.UserAuthenticator for the verification code.
func (t *terminalAuthenticator) Code(ctx context.Context, sentCode *tg.AuthSentCode) (string, error) {
	fmt.Printf("Enter verification code: ")
	reader := bufio.NewReader(os.Stdin)
	code, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(code), nil
}

// SignUp implements auth.UserAuthenticator for new user registration.
func (t *terminalAuthenticator) SignUp(ctx context.Context) (auth.UserInfo, error) {
	fmt.Print("First name: ")
	reader := bufio.NewReader(os.Stdin)
	firstName, err := reader.ReadString('\n')
	if err != nil {
		return auth.UserInfo{}, err
	}

	fmt.Print("Last name: ")
	lastName, err := reader.ReadString('\n')
	if err != nil {
		return auth.UserInfo{}, err
	}

	return auth.UserInfo{
		FirstName: strings.TrimSpace(firstName),
		LastName:  strings.TrimSpace(lastName),
	}, nil
}

// AcceptTermsOfService implements auth.UserAuthenticator.
func (t *terminalAuthenticator) AcceptTermsOfService(ctx context.Context, tos tg.HelpTermsOfService) error {
	fmt.Println("Terms of Service:", tos.Text)
	fmt.Print("Accept? (y/n): ")
	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(strings.ToLower(response)) != "y" {
		return fmt.Errorf("terms of service not accepted")
	}
	return nil
}
