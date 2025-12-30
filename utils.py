# utils.py
# Shared utilities and constants for get-files-list.py and download-files.py

import os
import re
import hashlib
import logging
import sqlite3
from datetime import datetime, timezone

# Configure logging (shared across scripts)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

base_download_path = './downloaded_files'
DB_PATH = 'telegram_files.db'

def sanitize_name(name):
    """Sanitize title for use as folder name."""
    return re.sub(r'[<>:"/\\|?*]', '_', name.strip()) if name else None

def get_prefixed_filename(message):
    """Generate filename with date/time prefix."""
    sent_date = message.date.strftime('%Y-%m-%d_%H-%M-%S')
    original_name = message.file.name
    if original_name:
        base_name = ''.join(c if c not in '<>:"/\\|?*' else '_' for c in original_name)
        return f"{sent_date}_{base_name}"
    else:
        ext = message.file.ext or ''
        return f"{sent_date}_file_{message.id}{ext}"

def compute_file_hash(file_path):
    """Compute SHA-256 hash of a file."""
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        return hashlib.sha256(content).hexdigest()
    except Exception as e:
        logger.error(f"Error computing hash for {file_path}: {e}")
        return None

def init_db():
    """Initialize SQLite database and create table if not exists."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            channel_id INTEGER NOT NULL,
            channel_title TEXT,
            message_id INTEGER NOT NULL,
            sender_id INTEGER,
            sender_username TEXT,
            original_name TEXT,
            prefixed_name TEXT,
            file_id TEXT,
            file_size INTEGER,
            sent_at TEXT NOT NULL,
            started_at TEXT,
            downloaded_at TEXT,
            file_path TEXT,
            data_hash TEXT,
            UNIQUE(channel_id, message_id)
        )
    ''')
    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")

def update_started(message_id, channel_id):
    """Update started_at timestamp."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE file_status SET started_at = ? WHERE message_id = ? AND channel_id = ?
    ''', (datetime.now(timezone.utc).isoformat(), message_id, channel_id))
    conn.commit()
    conn.close()

def update_completed(message_id, channel_id, prefixed_name, file_path, data_hash):
    """Update completion fields."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE file_status
        SET prefixed_name = ?, file_path = ?, data_hash = ?, downloaded_at = ?
        WHERE message_id = ? AND channel_id = ?
    ''', (
        prefixed_name, file_path, data_hash,
        datetime.now(timezone.utc).isoformat(), message_id, channel_id
    ))
    conn.commit()
    conn.close()