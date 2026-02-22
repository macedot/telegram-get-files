package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/macedot/telegram-get-files/internal/config"
	"github.com/macedot/telegram-get-files/internal/db"
	"github.com/macedot/telegram-get-files/internal/downloader"
	"github.com/macedot/telegram-get-files/internal/logger"
	"github.com/macedot/telegram-get-files/internal/models"
	"github.com/macedot/telegram-get-files/internal/scanner"
	"github.com/macedot/telegram-get-files/internal/telegram"
	"github.com/rs/zerolog"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "scan":
		runScan(os.Args[2:])
	case "download":
		runDownload(os.Args[2:])
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Telegram File Downloader")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  telegram-get-files <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  scan       Scan channels/groups and queue files for download")
	fmt.Println("  download   Download queued files")
	fmt.Println("  help       Show this help message")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  telegram-get-files scan -config config.json -source mychannel")
	fmt.Println("  telegram-get-files scan -config config.json -source -1001234567890 -watch")
	fmt.Println("  telegram-get-files download -config config.json -workers 5")
	fmt.Println()
	fmt.Println("Scan command options:")
	fmt.Println("  -config=path    Path to configuration file (default: config.json)")
	fmt.Println("  -source=id      Channel/group ID or username to scan")
	fmt.Println("  -list           List available channels and groups")
	fmt.Println("  -watch          Continuously watch for new files")
	fmt.Println("  -force          Reset file status before scanning")
	fmt.Println()
	fmt.Println("Download command options:")
	fmt.Println("  -config=path    Path to configuration file (default: config.json)")
	fmt.Println("  -workers=n      Number of concurrent downloads (default: 5)")
	fmt.Println("  -watch          Continuously watch for new pending files")
	fmt.Println("  -force          Reset file status before downloading")
}

func runScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "Path to configuration file")
	source := fs.String("source", "", "Channel/group ID or username to scan")
	listOnly := fs.Bool("list", false, "List available channels and groups only")
	watch := fs.Bool("watch", false, "Continuously watch for new files")
	force := fs.Bool("force", false, "Reset file status before scanning")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	cfg, err := config.LoadFromFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	logger.Init(cfg.LogLevel)
	log := logger.GetLogger()

	// Initialize database
	database, err := db.New(cfg.DatabasePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	if err := database.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing database: %v\n", err)
		os.Exit(1)
	}

	if *force {
		log.Info().Msg("Resetting file status...")
		if err := database.ResetStatus(); err != nil {
			fmt.Fprintf(os.Stderr, "Error resetting status: %v\n", err)
			os.Exit(1)
		}
		log.Info().Msg("File status reset complete")
	}

	// Initialize Telegram client
	tgClient := telegram.NewClient(cfg)

	// Execute scan logic inside the client's Run callback
	scanCallback := func(ctx context.Context) error {
		if *listOnly {
			// List channels functionality
			log.Info().Msg("Listing accessible channels/groups...")

			// Create scanner and list channels
			s := scanner.New(scanner.NewAdapter(tgClient.Raw()))
			channels, err := s.ListChannels(ctx)
			if err != nil {
				return fmt.Errorf("error listing channels: %w", err)
			}

			if len(channels) == 0 {
				fmt.Println("No channels found.")
				return nil
			}

			fmt.Printf("Found %d accessible channels/groups:\n\n", len(channels))
			for i, ch := range channels {
				fmt.Printf("%d. %s (ID: %d, Type: %s)\n", i+1, ch.Title, ch.ID, ch.Type)
			}
			return nil
		}

		if *source == "" {
			return fmt.Errorf("-source is required (or use -list to list channels)")
		}

		log.Info().Str("source", *source).Msg("Starting scan")

		// Parse source (channel/group ID or username)
		s := scanner.New(
			scanner.NewAdapter(tgClient.Raw()),
			scanner.WithDB(database),
			scanner.WithRawClient(tgClient.Raw().API()),
		)

		// Resolve the source (handles usernames, peer IDs like -100XXX, and numeric IDs)
		resolvedChannel, err := s.ResolveChannelWithHash(ctx, *source)
		if err != nil {
			return fmt.Errorf("error resolving source: %w", err)
		}

		log.Info().Int64("source_id", resolvedChannel.ChannelID).Int64("access_hash", resolvedChannel.AccessHash).Msg("Source resolved")

		// Scan stats
		var found, added, updated int

		// Process each file as it's found during scan
		processFile := func(task *models.DownloadTask) {
			found++
			log.Debug().Str("file", task.FileName).Int64("size", task.FileSize).Int("message_id", task.MessageID).Msg("Found file")

			if database != nil {
				fileInfo := &models.FileInfo{
					ChannelID:    task.ChannelID,
					MessageID:    task.MessageID,
					OriginalName: task.OriginalName,
					PrefixedName: task.FileName,
					FileID:       task.FileID,
					FileSize:     task.FileSize,
					SentAt:       time.Now(),
				}
				_, isNew, wasUpdated, err := database.GetOrCreateOrUpdateFile(fileInfo)
				if err != nil {
					log.Error().Err(err).Int("message_id", task.MessageID).Msg("Failed to save file to database")
				} else {
					if isNew {
						added++
					} else if wasUpdated {
						updated++
					}
				}
			}
		}

		// Initial full scan
		if err := s.ScanChannel(ctx, resolvedChannel, processFile); err != nil {
			return fmt.Errorf("error scanning source: %w", err)
		}

		log.Info().Int("found", found).Int("added", added).Int("updated", updated).Msg("Source scan complete")
		fmt.Printf("Scan complete: found %d files, added %d new, updated %d\n", found, added, updated)

		// Watch mode
		if *watch {
			log.Info().Int("poll_interval", cfg.ScanPollInterval).Msg("Watching for new files...")
			watchCallback := func(task *models.DownloadTask) {
				log.Debug().Str("file", task.FileName).Int("message_id", task.MessageID).Msg("New file detected in watch mode")
				processFile(task)
			}
			if err := s.Watch(ctx, resolvedChannel, cfg.ScanPollInterval, watchCallback); err != nil {
				return fmt.Errorf("error watching source: %w", err)
			}
		}
		return nil
	}

	// Create context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	if err := tgClient.Start(ctx, scanCallback); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runDownload(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "Path to configuration file")
	workers := fs.Int("workers", 0, "Number of concurrent downloads (0 = use config value)")
	watch := fs.Bool("watch", false, "Continuously watch for new pending files")
	force := fs.Bool("force", false, "Reset file status before downloading")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	cfg, err := config.LoadFromFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	if *workers > 0 {
		cfg.Workers = *workers
	}

	logger.Init(cfg.LogLevel)
	log := logger.GetLogger()

	database, err := db.New(cfg.DatabasePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	if err := database.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing database: %v\n", err)
		os.Exit(1)
	}

	if *force {
		log.Info().Msg("Resetting file status...")
		if err := database.ResetStatus(); err != nil {
			fmt.Fprintf(os.Stderr, "Error resetting status: %v\n", err)
			os.Exit(1)
		}
		log.Info().Msg("File status reset complete")
	}

	tgClient := telegram.NewClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	downloadCallback := func(ctx context.Context) error {
		return downloadFromDatabase(ctx, database, tgClient, cfg, log, *watch)
	}

	if err := tgClient.Start(ctx, downloadCallback); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func downloadFromDatabase(ctx context.Context, database *db.DB, tgClient *telegram.Client, cfg *config.Config, log zerolog.Logger, watch bool) error {
	batchNum := 0

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Context cancelled, stopping download")
			return nil
		default:
		}

		files, err := database.GetPendingFiles()
		if err != nil {
			return fmt.Errorf("failed to get pending files: %w", err)
		}

		if len(files) == 0 {
			if !watch {
				fmt.Println("No pending files to download.")
				return nil
			}
			log.Debug().Msg("No pending files, waiting...")
			time.Sleep(time.Duration(cfg.DownloadPollInterval) * time.Second)
			continue
		}

		batchNum++
		log.Info().Int("batch", batchNum).Int("count", len(files)).Msg("Starting downloads")

		api := tgClient.Raw().API()
		poolCtx, poolCancel := context.WithCancel(context.Background())
		pool := downloader.NewPool(cfg.Workers, database, poolCtx).
			WithClient(api).
			WithDownloadPath(cfg.DownloadPath)

		pool.Start()

		for _, file := range files {
			var filePath string
			if file.FilePath != nil {
				filePath = *file.FilePath
			}
			task := &models.DownloadTask{
				MessageID:    file.MessageID,
				ChannelID:    file.ChannelID,
				ChannelTitle: file.ChannelTitle,
				FileName:     file.PrefixedName,
				FileSize:     file.FileSize,
				OriginalName: file.OriginalName,
				FileID:       file.FileID,
				FilePath:     filePath,
			}
			if err := pool.Submit(task); err != nil {
				log.Warn().Err(err).Int("message_id", task.MessageID).Msg("Failed to submit task, pool shutting down")
			}
		}

		pool.Stop()
		poolCancel()

		log.Info().Msg("Downloads complete")

		if !watch {
			remaining, err := database.GetPendingFiles()
			if err != nil {
				log.Error().Err(err).Msg("Failed to check remaining files")
				return nil
			}
			if len(remaining) == 0 {
				fmt.Println("All pending files downloaded.")
				return nil
			}
			log.Info().Int("remaining", len(remaining)).Int("batch", batchNum).Msg("More files pending, waiting before continuing...")
			time.Sleep(3 * time.Second)
		} else {
			log.Info().Int("poll_interval", cfg.DownloadPollInterval).Msg("Waiting for new files...")
			time.Sleep(time.Duration(cfg.DownloadPollInterval) * time.Second)
		}
	}
}
