# Telegram File Downloader (Go)

A Go-based tool for downloading files from Telegram channels and groups. This is a migration from the original Python implementation with improved concurrency and structure.

## Features

- **Independent Scan and Download**: Scan and download can run as separate processes
- **Scan Command**: List channels and scan all messages for media files
- **Download Command**: Download queued files with concurrent workers  
- **Watch Mode**: Continuously watch for new files (both scan and download)
- **SQLite Database**: Track file status and metadata persistently
- **Session Persistence**: Reuse Telegram sessions across runs
- **Concurrent Downloads**: Configurable worker pool for parallel downloads
- **File Verification**: SHA256 hash verification for integrity
- **Terminal Authentication**: Interactive login with phone/code/2FA support
- **Graceful Shutdown**: Clean termination on Ctrl+C

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd telegram-get-files

# Build the binary
go build -o telegram-get-files .

# Or install directly
go install
```

## Configuration

1. Copy the example configuration:
```bash
cp config.example.json config.json
```

2. Edit `config.json` with your Telegram API credentials:
```json
{
  "api_id": 123456,
  "api_hash": "your_api_hash_here",
  "session_file": "session.json",
  "database_path": "telegram_files.db",
  "download_path": "./downloaded_files",
  "workers": 5,
  "log_level": "info",
  "scan_poll_interval": 30,
  "download_poll_interval": 30
}
```

3. Get your API credentials from [my.telegram.org](https://my.telegram.org)

## Usage

### List Available Channels

```bash
./telegram-get-files scan -config config.json -list
```

### Scan a Channel/Group

Scans all messages in a channel/group and saves found files to the database:

```bash
./telegram-get-files scan -config config.json -source mychannel
./telegram-get-files scan -config config.json -source -1001234567890
./telegram-get-files scan -config config.json -source 806693599
```

The scanner reports statistics:
```
Scan complete: found 100 files, added 95 new, updated 0
```

### Watch Mode (Scan)

Continuously watch for new files in a channel/group:

```bash
./telegram-get-files scan -config config.json -source mychannel -watch
```

Uses `scan_poll_interval` from config (default: 30 seconds).

### Download Queued Files

Downloads all pending files from the database:

```bash
./telegram-get-files download -config config.json
```

### Download with Custom Worker Count

```bash
./telegram-get-files download -config config.json -workers 10
```

### Watch Mode (Download)

Continuously watch for new pending files and download them:

```bash
./telegram-get-files download -config config.json -workers 5 -watch
```

Uses `download_poll_interval` from config (default: 30 seconds).

### Interrupt Downloads

Press Ctrl+C to gracefully stop downloads in progress.

## First Run Authentication

On first run, the application will prompt for:
1. Phone number (with country code, e.g., +1234567890)
2. Verification code sent to your Telegram app
3. 2FA password (if enabled)

The session is saved to `session.json` for subsequent runs.

## Workflow

### Independent Operation

Scan and download are now independent processes:

1. **Terminal 1** - Scan and watch for new files:
```bash
./telegram-get-files scan -source mychannel -watch
```

2. **Terminal 2** - Download pending files:
```bash
./telegram-get-files download -workers 5 -watch
```

### One-Time Scan and Download

1. Scan a channel/group:
```bash
./telegram-get-files scan -source 806693599
# Output: Scan complete: found 1000 files, added 950 new, updated 0
```

2. Download pending files:
```bash
./telegram-get-files download -workers 5
```

The scanner automatically:
- Fetches all messages using pagination
- Saves files to SQLite database
- Reports: found X files, added Y new, updated Z
- Avoids duplicates on rescan
- Only updates if metadata changed

## Project Structure

```
.
├── main.go                          # Entry point and CLI
├── config.example.json              # Example configuration
├── config.json                    # Your configuration (not in git)
├── internal/
│   ├── config/                   # Configuration handling
│   │   └── config.go
│   ├── db/                      # SQLite database operations
│   │   └── database.go
│   ├── logger/                  # Structured logging
│   │   └── logger.go
│   ├── models/                  # Data structures
│   │   └── models.go
│   ├── telegram/                # Telegram client wrapper
│   │   └── client.go
│   ├── scanner/                 # Channel scanning logic
│   │   └── scanner.go
│   └── downloader/              # Download worker pool
│       └── worker.go
├── go.mod                       # Go module definition
└── go.sum                      # Dependency checksums
```

## Architecture

The application follows a concurrent architecture with goroutines:

1. **Scanner**: Scans Telegram channels/groups for media files using pagination
2. **Database**: SQLite for persistence of file metadata and download status
3. **Worker Pool**: Multiple goroutines that download files concurrently
4. **Auth**: Terminal-based interactive authentication using gotd's auth flow
5. **Session**: File-based session storage for Telegram authentication

### Watch Mode

Both scan and download support watch mode:
- **Scan watch**: Polls channel for new messages with media, adds to database
- **Download watch**: Polls database for new pending files, downloads them

Poll intervals are configurable via `scan_poll_interval` and `download_poll_interval` in config.

## Development

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Run tests for a specific package
go test -v ./internal/scanner/...
```

### Code Quality

```bash
go vet ./...
gofmt -w .
```

### Adding Features

The codebase is organized into packages for easy extension:

- Add new commands in `main.go`
- Extend database operations in `internal/db/`
- Add new Telegram functionality in `internal/telegram/`

## License

MIT License
