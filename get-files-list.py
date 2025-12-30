# get-files-list.py
# Main script: resolves target, scans history, manages DB state, queues pending downloads
# Does NOT perform actual downloads — only prepares and updates metadata

from telethon import TelegramClient
from telethon.errors import UsernameNotOccupiedError, ChannelInvalidError, ChannelPrivateError, UsernameInvalidError
import os
import asyncio
import sys
import glob
from dotenv import load_dotenv
import argparse
from utils import init_db, sanitize_name, get_prefixed_filename, compute_file_hash, mark_as_completed_if_file_exists, insert_or_handle_existing, load_pending_from_db, logger, base_download_path

load_dotenv()

api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    raise ValueError("Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in the .env file.")

parser = argparse.ArgumentParser(description="Telegram media lister and queue manager")
parser.add_argument('target_identifier', help="Channel/group identifier")
parser.add_argument('--parallel', type=int, default=5, metavar='N', help="Not used in this script (kept for compatibility)")

args = parser.parse_args()
target_identifier = args.target_identifier

client = TelegramClient('session_name', api_id, api_hash)

target_entity = None
target_path = None

async def resolve_target(identifier):
    global target_entity
    logger.info(f'Resolving identifier: {identifier}')
    try:
        target_entity = await client.get_entity(identifier)
    except Exception as e:
        logger.error(f'Error resolving "{identifier}": {str(e)}')
        await client.disconnect()
        sys.exit(1)
    title = getattr(target_entity, 'title', "Unknown")
    logger.info(f'Successfully resolved: "{title}" (ID: {target_entity.id})')

async def prepare_download_folder():
    global target_path
    title = getattr(target_entity, 'title', "Unknown")
    safe_title = sanitize_name(title)
    target_path = os.path.join(base_download_path, safe_title)
    os.makedirs(target_path, exist_ok=True)

    temp_files = glob.glob(os.path.join(target_path, '*.tmp'))
    if temp_files:
        logger.warning(f"Cleaning up {len(temp_files)} leftover temporary file(s)...")
        for tf in temp_files:
            try:
                os.remove(tf)
            except Exception as e:
                logger.error(f"Failed to remove {tf}: {e}")

async def scan_and_queue():
    load_pending_from_db()

    logger.info("Scanning historical messages...")
    processed = 0
    async for message in client.iter_messages(target_entity, reverse=True):
        if message.media:
            insert_or_handle_existing(message)
            processed += 1

    logger.info(f"Historical scan complete: {processed} media messages processed.")

async def main():
    init_db()
    await client.start()
    me = await client.get_me()
    logger.info(f'Logged in as {me.first_name or me.username or "User"} (ID: {me.id})')

    await resolve_target(target_identifier)
    await prepare_download_folder()
    await scan_and_queue()

    logger.info("Listing and queuing complete. Run download-files.py to process pending downloads.")
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
