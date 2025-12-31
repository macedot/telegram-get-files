# download-files.py
# Fixed version:
# - Added missing import glob for temporary file cleanup

from telethon import TelegramClient
import os
import asyncio
import sys
import queue
import logging
import sqlite3
import glob  # Added missing import
from datetime import datetime, timezone
from dotenv import load_dotenv
import argparse
from utils import init_db, get_prefixed_filename, compute_file_hash, update_started, update_completed, logger, DB_PATH, base_download_path, sanitize_name

load_dotenv()

api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    raise ValueError("Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in the .env file.")

parser = argparse.ArgumentParser(description="Telegram pending media downloader")
parser.add_argument('--parallel', type=int, default=1, metavar='N', help="Maximum concurrent downloads (default: 5)")

args = parser.parse_args()

if args.parallel < 1:
    logger.error("--parallel must be a positive integer")
    sys.exit(1)

MAX_CONCURRENT_DOWNLOADS = args.parallel
logger.info(f"Maximum concurrent downloads set to: {MAX_CONCURRENT_DOWNLOADS}")

LOCK_FILE = 'download-files.lock'

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r') as f:
                old_pid = f.read().strip()
            logger.error(f"Another instance is running (PID: {old_pid}). Exiting.")
            sys.exit(1)
        except:
            logger.warning("Stale lock file found. Overwriting.")

    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        logger.info("Lock acquired.")
    except Exception as e:
        logger.error(f"Failed to create lock file: {e}")
        sys.exit(1)

def release_lock():
    if os.path.exists(LOCK_FILE):
        try:
            os.remove(LOCK_FILE)
            logger.info("Lock released.")
        except Exception as e:
            logger.error(f"Failed to remove lock file: {e}")

client = TelegramClient('session_name', api_id, api_hash)

download_queue = queue.Queue()

def cleanup_temp_files_once():
    """Run once at startup: clean .tmp files in all folders with pending downloads."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT (SELECT channel_title FROM file_status f2 WHERE f2.channel_id = f1.channel_id LIMIT 1) as channel_title
        FROM file_status f1
        WHERE downloaded_at IS NULL
    ''')
    rows = cursor.fetchall()
    conn.close()

    cleaned_count = 0
    for (channel_title,) in rows:
        safe_title = sanitize_name(channel_title or "Unknown")
        folder_path = os.path.join(base_download_path, safe_title)
        if not os.path.exists(folder_path):
            continue

        temp_files = glob.glob(os.path.join(folder_path, '*.tmp'))
        if temp_files:
            logger.warning(f"Cleaning up {len(temp_files)} leftover temporary file(s) in {folder_path}...")
            for tf in temp_files:
                try:
                    os.remove(tf)
                    cleaned_count += 1
                except Exception as e:
                    logger.error(f"Failed to remove {tf}: {e}")

    if cleaned_count > 0:
        logger.info(f"Cleanup complete: removed {cleaned_count} temporary files.")
    else:
        logger.info("No leftover temporary files found.")

def load_all_pending():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT message_id, channel_id,
               (SELECT channel_title FROM file_status f2 WHERE f2.channel_id = f1.channel_id LIMIT 1) as channel_title
        FROM file_status f1
        WHERE downloaded_at IS NULL
    ''')
    rows = cursor.fetchall()
    conn.close()

    for message_id, channel_id, channel_title in rows:
        safe_title = sanitize_name(channel_title or "Unknown")
        folder_path = os.path.join(base_download_path, safe_title)
        download_queue.put((message_id, channel_id, folder_path))

    logger.info(f"Loaded {len(rows)} pending downloads into queue.")

async def download_worker():
    while True:
        try:
            message_id, channel_id, target_path = download_queue.get_nowait()
        except queue.Empty:
            await asyncio.sleep(1)
            continue

        message = await client.get_messages(channel_id, ids=message_id)
        if not message or not message.media:
            download_queue.task_done()
            continue

        update_started(message_id, channel_id)

        prefixed_name = get_prefixed_filename(message)
        final_path = os.path.join(target_path, prefixed_name)
        temp_path = final_path + '.tmp'

        try:
            logger.info(f'Downloading: {prefixed_name}')
            await message.download_media(file=temp_path)

            data_hash = compute_file_hash(temp_path)
            if data_hash is None:
                raise Exception("Failed to compute hash")

            os.rename(temp_path, final_path)
            update_completed(message_id, channel_id, prefixed_name, final_path, data_hash)
            logger.info(f'Successfully saved: {prefixed_name}')
        except Exception as e:
            logger.error(f'Error downloading message {message_id}: {e}')
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
        finally:
            download_queue.task_done()

async def run_workers():
    tasks = []
    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        task = asyncio.create_task(download_worker())
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass

async def main():
    acquire_lock()
    init_db()
    await client.start()
    me = await client.get_me()
    logger.info(f'Logged in as {me.first_name or me.username or "User"} (ID: {me.id})')

    cleanup_temp_files_once()

    load_all_pending()
    # if download_queue.empty():
    #     logger.info("No pending downloads found. Exiting.")
    #     release_lock()
    #     await client.disconnect()
    #     return

    logger.info("Download workers started. Press Ctrl+C to stop.")
    await run_workers()

    release_lock()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
