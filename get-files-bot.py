# get-files-bot.py
# Updated version:
# - Before historical processing, removes any leftover .tmp files in the target folder
# - Prevents incomplete downloads from previous runs blocking new attempts

from telethon import TelegramClient, events
from telethon.errors import UsernameNotOccupiedError, ChannelInvalidError, ChannelPrivateError, UsernameInvalidError
import os
import asyncio
import re
import sys
import glob
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read API credentials from environment variables
api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    raise ValueError("Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in the .env file.")

# Parse command-line arguments
if len(sys.argv) < 2 or len(sys.argv) > 5:
    print("Usage: python get-files-bot.py <target_identifier> [--parallel N]")
    print("  <target_identifier>: @username, https://t.me/username, or numeric ID")
    print("  --parallel N      : Maximum concurrent downloads (default: 5)")
    print("\nExamples:")
    print("  python get-files-bot.py @durov")
    print("  python get-files-bot.py -806693599 --parallel 10")
    sys.exit(1)

# Extract target identifier (first non-option argument)
target_identifier = sys.argv[1]

# Default concurrency
MAX_CONCURRENT_DOWNLOADS = 5

# Parse optional --parallel
i = 2
while i < len(sys.argv):
    if sys.argv[i] == '--parallel':
        if i + 1 >= len(sys.argv):
            print("Error: --parallel requires a number")
            sys.exit(1)
        try:
            MAX_CONCURRENT_DOWNLOADS = int(sys.argv[i + 1])
            if MAX_CONCURRENT_DOWNLOADS < 1:
                raise ValueError
        except ValueError:
            print("Error: --parallel must be followed by a positive integer")
            sys.exit(1)
        i += 2
    else:
        print(f"Unknown argument: {sys.argv[i]}")
        sys.exit(1)

print(f"Maximum concurrent downloads set to: {MAX_CONCURRENT_DOWNLOADS}")

# Create the client (personal user account)
client = TelegramClient('session_name', api_id, api_hash)

# Base directory for all downloads
base_download_path = './downloaded_files'
os.makedirs(base_download_path, exist_ok=True)

# Global variables for target information
target_entity = None
target_path = None

def sanitize_name(name):
    """Sanitize title for use as folder name."""
    return re.sub(r'[<>:"/\\|?*]', '_', name.strip())

def cleanup_temp_files(folder_path):
    """Remove any leftover .tmp files from previous interrupted downloads."""
    temp_files = glob.glob(os.path.join(folder_path, '*.tmp'))
    if temp_files:
        print(f"Cleaning up {len(temp_files)} leftover temporary file(s)...")
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
                print(f"Removed: {os.path.basename(temp_file)}")
            except Exception as e:
                print(f"Failed to remove {temp_file}: {e}")
    else:
        print("No temporary files found.")

async def download_media_safely(message, download_path, semaphore):
    """Download media with temporary extension, rename on success, skip if final file exists."""
    if not message.media:
        return

    file_name = message.file.name or f'file_{message.id}{message.file.ext or ""}'
    final_path = os.path.join(download_path, file_name)

    if os.path.exists(final_path):
        print(f'Skipped (already exists): {file_name}')
        return

    temp_path = final_path + '.tmp'

    async with semaphore:
        try:
            print(f'Downloading: {file_name}')
            await message.download_media(file=temp_path)
            os.rename(temp_path, final_path)
            print(f'Successfully saved: {file_name}')
        except Exception as e:
            print(f'Error downloading {file_name}: {e}')
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

async def process_messages(messages, download_path):
    """Process a list of messages with parallel downloads."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    tasks = [
        download_media_safely(msg, download_path, semaphore)
        for msg in messages if msg.media
    ]
    if tasks:
        await asyncio.gather(*tasks)

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

    print("\nAccessible channels and groups (sorted by type, then title):")
    print("=" * 80)
    current_type = None
    for item in items:
        if item['type'] != current_type:
            print(f"\n{item['type']}:")
            current_type = item['type']
        print(f"  • {item['title']} | ID: {item['id']}")
    print("\n" + "=" * 80)
    print("To monitor a specific entity, run:")
    print("python get-files-bot.py <identifier> [--parallel N]\n")

async def setup_target(target_identifier):
    global target_entity, target_path

    print(f'Resolving identifier: {target_identifier}')
    try:
        target_entity = await client.get_entity(target_identifier)
    except UsernameNotOccupiedError:
        print(f'Error: The username "{target_identifier}" does not exist.')
        await client.disconnect()
        sys.exit(1)
    except UsernameInvalidError:
        print(f'Error: Invalid identifier "{target_identifier}".')
        await client.disconnect()
        sys.exit(1)
    except (ChannelInvalidError, ValueError):
        try:
            int_id = int(target_identifier)
            target_entity = await client.get_entity(int_id)
        except:
            print(f'Error: Cannot resolve identifier "{target_identifier}".')
            await client.disconnect()
            sys.exit(1)
    except ChannelPrivateError:
        print(f'Error: Entity is private or inaccessible.')
        await client.disconnect()
        sys.exit(1)
    except Exception as e:
        print(f'Error resolving "{target_identifier}": {str(e)}')
        await client.disconnect()
        sys.exit(1)

    title = getattr(target_entity, 'title', "Unknown")
    print(f'Successfully resolved: "{title}" (ID: {target_entity.id})')

    safe_title = sanitize_name(title)
    target_path = os.path.join(base_download_path, safe_title)
    os.makedirs(target_path, exist_ok=True)

    # Clean up any leftover .tmp files before starting
    cleanup_temp_files(target_path)

    # Historical check with parallel downloads
    print(f'Checking historical messages for missing files in "{title}" (up to {MAX_CONCURRENT_DOWNLOADS} parallel downloads)...')
    messages = []
    async for message in client.iter_messages(target_entity, reverse=True):
        messages.append(message)

    # Process in batches to avoid memory issues
    batch_size = 100
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        await process_messages(batch, target_path)

    print(f'Historical check complete. Now monitoring "{title}" for new files. Press Ctrl+C to stop.')

@client.on(events.NewMessage)
async def new_file_handler(event):
    if target_entity and event.message.chat_id == target_entity.id and event.message.media:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        await download_media_safely(event.message, target_path, semaphore)
        print(f'New file downloaded from "{getattr(target_entity, 'title', 'target')}"')

async def main():
    await client.start()
    me = await client.get_me()
    print(f'Logged in as {me.first_name or me.username or "User"} (ID: {me.id})')

    if len(sys.argv) == 1:
        # No arguments → list entities
        await list_all_channels_and_groups()
        await client.disconnect()
    else:
        # Has identifier → run monitoring
        await setup_target(target_identifier)
        await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())