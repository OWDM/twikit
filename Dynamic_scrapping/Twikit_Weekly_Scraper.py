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
from typing import List, Optional
from twikit import Client, TooManyRequests

"""
X (Twitter) scraper — Weekly runner that slices your range into per-day queries.
STRICT "Latest" ONLY: no fallback to Top. If a page returns 404 on Latest, it's treated as a failure/end.

Example:
python twikit_weekly_scraper_latest_only.py \
  --account Account1 \
  --query "(@HungerStation OR هنقرستيشن OR هانقرستيشن) -كوبون -كود" \
  --start 2025-08-01 \
  --days 7
"""

# =====================
# Args
# =====================
parser = argparse.ArgumentParser(description='X (Twitter) Weekly Scraper — per-day slicing, LATEST only')
parser.add_argument('--account', required=True, help='Account section name from config.ini')
parser.add_argument('--query', required=True, help='Base search query (we inject since:/until:)')
parser.add_argument('--start', required=True, help='Start day YYYY-MM-DD (inclusive)')
parser.add_argument('--end', help='End day YYYY-MM-DD (exclusive). If omitted, uses --days')
parser.add_argument('--days', type=int, default=7, help='If --end missing, scrape this many days starting from --start')
parser.add_argument('--max-tweets', type=int, default=100000, help='Safety cap per day (rarely hit)')
parser.add_argument('--batch-size', type=int, default=20, help='Search page size; 20 is safest')
parser.add_argument('--max-failures', type=int, default=3, help='Consecutive failure cap before giving up (per day)')
parser.add_argument('--sleep-between-days', type=int, default=30, help='Cool-down seconds between day slices')
args = parser.parse_args()

# =====================
# Derived, Paths, Constants
# =====================
ACCOUNT_NAME = args.account
MAX_TWEETS = args.max_tweets
BATCH_SIZE = args.batch_size
MAX_FAILURES = args.max_failures
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]
SLEEP_BETWEEN_DAYS = args.sleep_between_days
PRODUCT = 'Latest'   # hard-enforce Latest only

# =====================
# Date window helpers
# =====================
def parse_day(s: str) -> datetime:
    try:
        return datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        raise SystemExit(f'Invalid date: {s} (expected YYYY-MM-DD)')

start_dt = parse_day(args.start)
if args.end:
    end_dt = parse_day(args.end)
else:
    end_dt = start_dt + timedelta(days=max(args.days, 1))

if end_dt <= start_dt:
    raise SystemExit('--end must be after --start (or use --days >= 1)')

# Build list of days [start, end)
DAYS: List[datetime] = []
cur = start_dt
while cur < end_dt:
    DAYS.append(cur)
    cur += timedelta(days=1)

# =====================
# Logging (single weekly log)
# =====================
LOG_FILE = f'scraper_{ACCOUNT_NAME}_{start_dt.strftime("%Y%m%d")}_{(end_dt - timedelta(days=1)).strftime("%Y%m%d")}_latest.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(f'TwitterScraper_{ACCOUNT_NAME}_WEEKLY_LATEST')

# =====================
# Query helpers
# =====================
_since_until_re = re.compile(r"\s*(since|until)\s*:\s*\d{4}-\d{2}-\d{2}", re.IGNORECASE)

def strip_since_until(q: str) -> str:
    return _since_until_re.sub('', q).strip()

BASE_QUERY = strip_since_until(args.query)

# =====================
# IO helpers (per day)
# =====================

def files_for_day(day: datetime):
    ds = day.strftime('%Y-%m-%d')
    return (
        f'cookies_{ACCOUNT_NAME}.json',
        f'checkpoint_{ACCOUNT_NAME}_{ds}.json',
        f'tweets_{ACCOUNT_NAME}_{ds}.csv',
        f'scraper_{ACCOUNT_NAME}_{ds}_latest.log'
    )


def load_checkpoint(path: str):
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading checkpoint {path}: {e}")
    return {'count': 0, 'cursor': None, 'last_success': None}


def save_checkpoint(path: str, data):
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Error saving checkpoint {path}: {e}")


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


async def init_client(cookies_file: str) -> Optional[Client]:
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

    if os.path.exists(cookies_file):
        try:
            client.load_cookies(cookies_file)
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
            client.save_cookies(cookies_file)
            logger.info('Logged in & saved cookies')
            return client
        except Exception as e:
            msg = str(e)
            if 'Your account is locked' in msg:
                logger.error('Account locked. Visit https://x.com/account/access to unlock. Aborting.')
                return None
            logger.error(f"Login failed (attempt {attempt+1}/{MAX_RETRIES}): {msg}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAYS[attempt]
                logger.info(f"Retrying login in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
    logger.error('All login attempts failed')
    return None

# =====================
# Search with retries — LATEST ONLY (no Top fallback)
# =====================
async def safe_search_with_retry(client: Client, query: str, count: int = BATCH_SIZE, cursor=None):
    for attempt in range(MAX_RETRIES):
        try:
            return await client.search_tweet(query, 'Latest', count=count, cursor=cursor)
        except TooManyRequests as e:
            reset_time = getattr(e, 'rate_limit_reset', None)
            if reset_time:
                wait_time = max(reset_time - time.time(), 0) + 5
                logger.warning(f"Rate limited. Waiting {wait_time:.0f}s until {datetime.fromtimestamp(reset_time)}")
                await asyncio.sleep(wait_time)
                continue
            else:
                wait_time = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                logger.warning(f"Rate limited (no reset). Waiting {wait_time}s")
                await asyncio.sleep(wait_time)
        except Exception as e:
            msg = str(e)
            # Treat 404 as hard failure for this attempt; no fallback to Top
            if '404' in msg:
                logger.error('Received 404 on Latest — treating as failure for this page (no fallback).')
            else:
                logger.error(f"Search error on Latest (attempt {attempt+1}/{MAX_RETRIES}): {msg}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAYS[attempt]
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error('All search attempts failed for this page (Latest only)')
    return None

# =====================
# Per-day runner
# =====================
async def run_one_day(client: Client, day: datetime):
    ds = day.strftime('%Y-%m-%d')
    du = (day + timedelta(days=1)).strftime('%Y-%m-%d')
    cookies_file, ckpt_file, csv_file, day_log = files_for_day(day)

    day_logger = logging.getLogger(f'TwitterScraper_{ACCOUNT_NAME}_{ds}_LATEST')
    fh = logging.FileHandler(day_log)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    day_logger.addHandler(fh)
    day_logger.setLevel(logging.INFO)

    base_query = BASE_QUERY
    day_query = f"{base_query} since:{ds} until:{du}"

    day_logger.info(f"Starting day {ds} | product=Latest ONLY")
    day_logger.info(f"Base query: {base_query}")
    day_logger.info(f"Day query:  {day_query}")

    ckpt = load_checkpoint(ckpt_file)
    collected = ckpt.get('count', 0)
    cursor = ckpt.get('cursor')
    last_success = ckpt.get('last_success')
    existing_ids = init_existing_ids(csv_file)

    new_file = not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(['ID', 'Username', 'Text', 'Created At', 'Retweets', 'Likes', 'Replies'])

        failures = 0
        no_new_pages = 0
        seen_cursors = set()

        while collected < MAX_TWEETS and failures < MAX_FAILURES:
            day_logger.info(f"Collected {collected}/{MAX_TWEETS} — cursor={cursor}")

            result = await safe_search_with_retry(client, day_query, BATCH_SIZE, cursor)
            if not result:
                failures += 1
                day_logger.warning(f"Search failed ({failures}/{MAX_FAILURES})")
            else:
                tweets = list(result)
                if not tweets:
                    failures += 1
                    day_logger.warning(f"No tweets found ({failures}/{MAX_FAILURES}) at this page")
                else:
                    added = save_tweets_batch(tweets, writer, existing_ids)
                    collected += added
                    last_success = datetime.now().isoformat()
                    f.flush()

                    if added == 0:
                        no_new_pages += 1
                    else:
                        no_new_pages = 0

                    # pagination
                    new_cursor = None
                    try:
                        if hasattr(result, 'next_cursor') and result.next_cursor:
                            new_cursor = result.next_cursor
                        else:
                            next_result = await result.next()
                            if hasattr(next_result, 'next_cursor') and next_result.next_cursor:
                                new_cursor = next_result.next_cursor
                    except Exception as e:
                        day_logger.error(f"Error getting next cursor: {e}")

                    if new_cursor:
                        cursor = new_cursor
                    else:
                        day_logger.info('No next cursor — end of day slice. Stopping.')
                        break

                    if cursor in seen_cursors:
                        day_logger.info('Cursor repeated; stopping to avoid loop.')
                        break
                    seen_cursors.add(cursor)

                    day_logger.info(f"Added {added} tweets. Total={collected}")

            # Save checkpoint each iteration
            ckpt.update({'count': collected, 'cursor': cursor, 'last_success': last_success,
                         'day': ds, 'query': base_query, 'product': 'Latest'})
            save_checkpoint(ckpt_file, ckpt)

            # stop if several empty pages in a row
            if no_new_pages >= 3:
                day_logger.info('No new unique tweets in 3 consecutive pages — stopping day run.')
                break

            # Adaptive delay
            delay = random.randint(5, 15) if failures == 0 else random.randint(30, 60) * failures
            day_logger.info(f"Waiting {delay} seconds before next request...")
            await asyncio.sleep(delay)

        if failures >= MAX_FAILURES:
            day_logger.warning(f"Stopping due to {failures} consecutive failures")
        day_logger.info(f"Completed day {ds}. Collected {collected} tweets.")

# =====================
# Weekly runner
# =====================
async def run():
    logger.info(f"Starting weekly scraper for account '{ACCOUNT_NAME}' (Latest only)")
    logger.info(f"Base query: {BASE_QUERY}")
    logger.info(f"Range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')} (exclusive)")
    cookies_file, _, _, _ = files_for_day(DAYS[0])

    client = await init_client(cookies_file)
    if not client:
        return

    for i, day in enumerate(DAYS):
        await run_one_day(client, day)
        if i < len(DAYS) - 1:
            logger.info(f"Cooling down {SLEEP_BETWEEN_DAYS}s before next day...")
            await asyncio.sleep(SLEEP_BETWEEN_DAYS)

    logger.info('All days completed (Latest only).')

if __name__ == '__main__':
    asyncio.run(run())
