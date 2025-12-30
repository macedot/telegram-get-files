# get-files-list.py
# Updated version:
# - Removed temporary file cleanup from this script
# - Cleanup now handled exclusively by download-files.py before processing each file

from telethon import TelegramClient
from telethon.errors import UsernameNotOccupiedError, ChannelInvalidError, ChannelPrivateError, UsernameInvalidError
import os
import asyncio
import sys
import sqlite3
from datetime import datetime, timezone
from dotenv import load_dotenv
import argparse
from utils import init_db, sanitize_name, get_prefixed_filename, compute_file_hash, logger, base_download_path, DB_PATH

load_dotenv()

api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    raise ValueError("Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in the .env file.")

parser = argparse.ArgumentParser(description="Telegram media lister and queue manager")
parser.add_argument('target_identifier', nargs='?', help="Channel/group identifier (optional for listing mode)")
args = parser.parse_args()

client = TelegramClient('session_name', api_id, api_hash)

target_entity = None
target_path = None

def mark_as_completed_if_file_exists(message):
    prefixed_name = get_prefixed_filename(message)
    expected_path = os.path.join(target_path, prefixed_name)

    if os.path.exists(expected_path):
        data_hash = compute_file_hash(expected_path)
        if data_hash is None:
            return False

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE file_status
            SET prefixed_name = ?, file_path = ?, data_hash = ?, downloaded_at = ?
            WHERE channel_id = ? AND message_id = ?
        ''', (
            prefixed_name, expected_path, data_hash,
            datetime.now(timezone.utc).isoformat(),
            message.chat_id, message.id
        ))
        updated = cursor.rowcount > 0
        conn.commit()
        conn.close()

        if updated:
            logger.info(f"Existing file verified and marked complete: {prefixed_name}")
        return updated
    return False

def insert_or_handle_existing(message):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('SELECT downloaded_at FROM file_status WHERE channel_id = ? AND message_id = ?',
                   (message.chat_id, message.id))
    row = cursor.fetchone()

    if row:
        conn.close()
        return

    sender = message.sender
    sender_username = sender.username if sender else None
    sender_id = sender.id if sender else None

    cursor.execute('''
        INSERT INTO file_status (
            created_at, channel_id, channel_title, message_id,
            sender_id, sender_username, original_name,
            file_id, file_size, sent_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        datetime.now(timezone.utc).isoformat(),
        message.chat_id,
        getattr(message.chat, 'title', None),
        message.id,
        sender_id,
        sender_username,
        message.file.name,
        message.file.id,
        message.file.size,
        message.date.isoformat()
    ))
    conn.commit()
    conn.close()

    mark_as_completed_if_file_exists(message)

async def list_all_channels_and_groups():
    items = []

    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        entity_type = "Other"
        if hasattr(entity, 'broadcast') and entity.broadcast:
            entity_type = "Channel (broadcast)"
        elif hasattr(entity, 'megagroup') and entity.megagroup:
            entity_type = "Supergroup"
        elif dialog.is_group:
            entity_type = "Basic Group"
        elif dialog.is_channel:
            entity_type = "Channel"
        else:
            continue

        items.append({
            'type': entity_type,
            'title': dialog.title,
            'id': dialog.id
        })

    type_order = {
        "Channel (broadcast)": 0,
        "Channel": 1,
        "Supergroup": 2,
        "Basic Group": 3,
        "Other": 4
    }

    items.sort(key=lambda x: (type_order.get(x['type'], 5), x['title'].lower()))

    logger.info("\nAccessible channels and groups (sorted by type, then title):")
    logger.info("=" * 80)
    current_type = None
    for item in items:
        if item['type'] != current_type:
            logger.info(f"\n{item['type']}:")
            current_type = item['type']
        logger.info(f"  • {item['title']} | ID: {item['id']}")
    logger.info("\n" + "=" * 80)
    logger.info("To process a specific entity, run:")
    logger.info("python get-files-list.py <identifier>\n")

async def resolve_target(identifier):
    global target_entity
    logger.info(f'Resolving identifier: {identifier}')
    try:
        target_entity = await client.get_entity(identifier)
    except (ChannelInvalidError, ValueError):
        try:
            int_id = int(identifier)
            target_entity = await client.get_entity(int_id)
        except Exception as inner_e:
            logger.error(f'Cannot resolve identifier "{identifier}": {inner_e}')
            await client.disconnect()
            sys.exit(1)
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

    # Cleanup removed — now handled in downloader before each download

async def scan_and_queue():
    logger.info("Scanning historical messages and verifying existing files...")
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

    if args.target_identifier is None:
        await list_all_channels_and_groups()
    else:
        await resolve_target(args.target_identifier)
        await prepare_download_folder()
        await scan_and_queue()
        logger.info("Processing complete. Run or keep running download-files.py to process any pending/re-queued files.")

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
