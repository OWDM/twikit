import asyncio
import os
import time
import csv
import json
import re
import random
import logging
import argparse
from datetime import datetime, timedelta
from configparser import ConfigParser
from twikit import Client, TooManyRequests

# =====================
# Args
# =====================
parser = argparse.ArgumentParser(description='X (Twitter) Daily Scraper with per-day slicing and safe stop')
parser.add_argument('--account', required=True, help='Account section name from config.ini')
parser.add_argument('--query', required=True, help='Base search query (we will inject since:/until:)')
parser.add_argument('--day', required=True, help='Target day in YYYY-MM-DD (inclusive). until will be day+1 (exclusive)')
parser.add_argument('--product', default='Latest', choices=['Top', 'Latest', 'Media'])
parser.add_argument('--max-tweets', type=int, default=100000, help='Safety cap (rarely hit for a single day)')
parser.add_argument('--batch-size', type=int, default=20, help='Search page size; 20 is safest')
parser.add_argument('--max-failures', type=int, default=3, help='Consecutive failure cap before giving up')
args = parser.parse_args()

# =====================
# Derived, Paths, Constants
# =====================
ACCOUNT_NAME = args.account
PRODUCT = args.product
MAX_TWEETS = args.max_tweets
BATCH_SIZE = args.batch_size
MAX_FAILURES = args.max_failures
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]

# Day window
DAY = args.day
try:
    day_dt = datetime.strptime(DAY, '%Y-%m-%d')
except ValueError:
    raise SystemExit('--day must be YYYY-MM-DD')

day_until_dt = day_dt + timedelta(days=1)
DAY_UNTIL = day_until_dt.strftime('%Y-%m-%d')

# Files are per-account per-day (so reruns don’t collide)
COOKIES_FILE = f'cookies_{ACCOUNT_NAME}.json'
CHECKPOINT_FILE = f'checkpoint_{ACCOUNT_NAME}_{DAY}.json'
TWEETS_FILE = f'tweets_{ACCOUNT_NAME}_{DAY}.csv'

# =====================
# Logging
# =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scraper_{ACCOUNT_NAME}_{DAY}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(f'TwitterScraper_{ACCOUNT_NAME}_{DAY}')

# =====================
# Query helpers
# =====================
_since_until_re = re.compile(r"\s*(since|until)\s*:\s*\d{4}-\d{2}-\d{2}", re.IGNORECASE)

def strip_since_until(q: str) -> str:
    return _since_until_re.sub('', q).strip()

BASE_QUERY = strip_since_until(args.query)
DAY_QUERY = f"{BASE_QUERY} since:{DAY} until:{DAY_UNTIL}"

# =====================
# IO helpers
# =====================

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
    return {'count': 0, 'cursor': None, 'last_success': None}


def save_checkpoint(data):
    data['day'] = DAY
    data['query'] = BASE_QUERY
    data['product'] = PRODUCT
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")


def init_existing_ids(csv_path: str):
    ids = set()
    if os.path.exists(csv_path):
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row:
                        ids.add(row[0])
        except Exception as e:
            logger.error(f"Error reading existing tweets: {e}")
    return ids


def save_tweets_batch(tweets, writer, existing_ids) -> int:
    new_count = 0
    for tw in tweets:
        if tw.id in existing_ids:
            continue
        try:
            writer.writerow([
                tw.id,
                f"@{tw.user.screen_name}",
                tw.text,
                tw.created_at,
                getattr(tw, 'retweet_count', 0),
                getattr(tw, 'favorite_count', 0),
                getattr(tw, 'reply_count', 0),
            ])
            existing_ids.add(tw.id)
            new_count += 1
        except Exception as e:
            logger.error(f"Error saving tweet {tw.id}: {e}")
    return new_count

# =====================
# Auth
# =====================
async def check_cookies_valid(client: Client) -> bool:
    try:
        await client.get_trends('trending')
        return True
    except Exception as e:
        logger.error(f"Cookie validation failed: {e}")
        return False


async def init_client() -> Client | None:
    config = ConfigParser()
    config.read('config.ini')
    if ACCOUNT_NAME not in config:
        logger.error(f"Account '{ACCOUNT_NAME}' not found in config.ini")
        return None
    try:
        username = config[ACCOUNT_NAME]['username']
        email = config[ACCOUNT_NAME]['email']
        password = config[ACCOUNT_NAME]['password']
        proxy_str = config[ACCOUNT_NAME].get('proxy', '').strip()
        proxy = None if proxy_str.lower() in ['none', 'no-proxy', ''] else proxy_str
    except KeyError as e:
        logger.error(f"Missing config for account {ACCOUNT_NAME}: {e}")
        return None

    client = Client(language='ar', proxy=proxy)

    if os.path.exists(COOKIES_FILE):
        try:
            client.load_cookies(COOKIES_FILE)
            logger.info('Loaded existing cookies')
            if await check_cookies_valid(client):
                logger.info('Cookies are valid')
                return client
            else:
                logger.warning('Cookies expired — re-login required')
        except Exception as e:
            logger.error(f"Error loading cookies: {e}")

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f'Logging in (attempt {attempt+1}/{MAX_RETRIES})...')
            await client.login(auth_info_1=username, auth_info_2=email, password=password)
            client.save_cookies(COOKIES_FILE)
            logger.info('Logged in & saved cookies')
            return client
        except Exception as e:
            logger.error(f"Login failed (attempt {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAYS[attempt]
                logger.info(f"Retrying login in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
    logger.error(f'All {MAX_RETRIES} login attempts failed')
    return None

# =====================
# Search with retries
# =====================
async def safe_search_with_retry(client: Client, query: str, count: int = BATCH_SIZE, cursor=None):
    for attempt in range(MAX_RETRIES):
        try:
            result = await client.search_tweet(query, PRODUCT, count=count, cursor=cursor)
            return result
        except TooManyRequests as e:
            reset_time = getattr(e, 'rate_limit_reset', None)
            if reset_time:
                wait_time = max(reset_time - time.time(), 0) + 5
                logger.warning(f"Rate limited. Waiting {wait_time:.0f}s until {datetime.fromtimestamp(reset_time)}")
                await asyncio.sleep(wait_time)
                continue
            else:
                wait_time = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                logger.warning(f"Rate limited (no reset time). Waiting {wait_time}s")
                await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Search error (attempt {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAYS[attempt]
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All {MAX_RETRIES} search attempts failed")
    return None

# =====================
# Main: per-day slice, safe stop
# =====================
async def run():
    logger.info(f"Starting daily scraper for account '{ACCOUNT_NAME}'")
    logger.info(f"Base query: {BASE_QUERY}")
    logger.info(f"Day window: since:{DAY} until:{DAY_UNTIL} | product={PRODUCT}")

    client = await init_client()
    if not client:
        return

    ckpt = load_checkpoint()
    collected = ckpt.get('count', 0)
    cursor = ckpt.get('cursor')
    last_success = ckpt.get('last_success')
    existing_ids = init_existing_ids(TWEETS_FILE)

    new_file = not os.path.exists(TWEETS_FILE) or os.path.getsize(TWEETS_FILE) == 0
    with open(TWEETS_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(['ID', 'Username', 'Text', 'Created At', 'Retweets', 'Likes', 'Replies'])

        failures = 0
        no_new_pages = 0
        seen_cursors = set()

        while collected < MAX_TWEETS and failures < MAX_FAILURES:
            logger.info(f"Collected {collected}/{MAX_TWEETS} — cursor={cursor}")

            result = await safe_search_with_retry(client, DAY_QUERY, BATCH_SIZE, cursor)
            if not result:
                failures += 1
                logger.warning(f"Search failed ({failures}/{MAX_FAILURES})")
                # keep cursor; try again from same spot
            else:
                tweets = list(result)
                if not tweets:
                    failures += 1
                    logger.warning(f"No tweets found ({failures}/{MAX_FAILURES}) at this page")
                else:
                    added = save_tweets_batch(tweets, writer, existing_ids)
                    collected += added
                    last_success = datetime.now().isoformat()
                    f.flush()

                    if added == 0:
                        no_new_pages += 1
                    else:
                        no_new_pages = 0

                    # pagination: prefer next_cursor; if absent, try result.next()
                    new_cursor = None
                    try:
                        if hasattr(result, 'next_cursor') and result.next_cursor:
                            new_cursor = result.next_cursor
                        else:
                            next_result = await result.next()
                            if hasattr(next_result, 'next_cursor') and next_result.next_cursor:
                                new_cursor = next_result.next_cursor
                    except Exception as e:
                        logger.error(f"Error getting next cursor: {e}")

                    if new_cursor:
                        cursor = new_cursor
                    else:
                        # No next cursor — likely end of this day slice
                        logger.info('No next cursor returned — likely end of day slice. Stopping.')
                        break

                    # loop protection
                    if cursor in seen_cursors:
                        logger.info('Cursor repeated; stopping to avoid loop.')
                        break
                    seen_cursors.add(cursor)

                    logger.info(f"Added {added} tweets. Total={collected}")

            # Save ckpt each iteration
            ckpt.update({'count': collected, 'cursor': cursor, 'last_success': last_success})
            save_checkpoint(ckpt)

            # stopping condition: multiple empty pages => done with this day
            if no_new_pages >= 3:
                logger.info('No new unique tweets in 3 consecutive pages — stopping day run.')
                break

            # Adaptive delay
            delay = random.randint(5, 15) if failures == 0 else random.randint(30, 60) * failures
            logger.info(f"Waiting {delay} seconds before next request...")
            await asyncio.sleep(delay)

        if failures >= MAX_FAILURES:
            logger.warning(f"Stopping due to {failures} consecutive failures")
        logger.info(f"Completed day {DAY}. Collected {collected} tweets.")

if __name__ == '__main__':
    asyncio.run(run())
