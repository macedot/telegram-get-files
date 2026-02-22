# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go CLI for downloading files from Telegram channels/groups using MTProto (gotd/td library).

## Development Commands

```bash
go build -o telegram-get-files .     # Build
go test ./...                         # Run all tests
go test -v ./internal/scanner/...     # Run specific package tests
go vet ./...                          # Lint
```

## Architecture

**Entry point**: `main.go` - CLI with `scan` and `download` commands (both support `-watch` for continuous polling, `-force` to reset status)

| Package | Responsibility |
|---------|---------------|
| `internal/telegram/client.go` | MTProto client wrapper, handles interactive auth (phone, code, 2FA) |
| `internal/scanner/scanner.go` | Discovers media in channels, saves to DB as pending; uses `ResolvedPeer` type |
| `internal/downloader/worker.go` | Worker pool for concurrent downloads with panic recovery |
| `internal/db/database.go` | SQLite persistence (WAL mode) |
| `internal/config/config.go` | JSON config loading with defaults |
| `internal/models/models.go` | Core data structures (Channel, DownloadTask, FileInfo) |

**Data flow**: `Telegram API → Scanner → SQLite (pending) → Downloader Pool → File System + SQLite (completed)`

## Key Patterns

- **Functional options**: Scanner uses `Option` func type for dependency injection (WithDB, WithRawClient, WithBatchSize, WithWatchPollLimit)
- **Worker pool**: Downloader Pool with buffered channel queue (size 100), graceful shutdown with Submit/Stop coordination, panic recovery
- **Interface-based design**: `Database` and `DialogClient` interfaces for testability
- **Builder pattern**: `NewPool().WithClient().WithDownloadPath().WithDownloadTimeout()` fluent API

## Important Details

- File references expire; downloader fetches fresh references before download
- Hash computed during download using `io.MultiWriter` (no double I/O)
- Security: path traversal protection, filename sanitization, atomic writes (tmp file + rename)
- Session persisted in `session.json` for Telegram authentication
- Config fields: `api_id`, `api_hash` (required), `session_file`, `database_path`, `download_path`, `workers`, `log_level`, `scan_poll_interval`, `download_poll_interval`, `download_timeout`, `scan_batch_size`, `watch_poll_limit`, `retry_delay`
- Empty filenames handled with generated names based on MIME type