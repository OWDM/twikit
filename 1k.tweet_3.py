"""
HungerStation Latest Tweets Scraper — query-aware checkpoint & CSV (twikit)
Fixes the issue where a previous checkpoint (e.g., count=1004) causes the run to instantly stop
when you change the query. This version:
  - Computes a stable hash from the QUERY string
  - Uses per-query checkpoint and CSV filenames
  - Always searches 'Latest' only (no Top)

Run:
  python3 x_hungerstation_latest_queryaware.py

Optional: set MAX_TWEETS / BATCH_SIZE as you like.
"""

import asyncio
import os
import time
import csv
import json
import random
import hashlib
import re
from datetime import datetime
from configparser import ConfigParser
from twikit import Client, TooManyRequests

# =====================
# User-fixed query (Latest only)
# =====================
QUERY = '(@HungerStation OR هنقرستيشن OR هانقرستيشن) -كوبون -كود until:2025-09-04 since:2025-08-26'
PRODUCT = 'Latest'
MAX_TWEETS = 1000
BATCH_SIZE = 20

# =====================
# Derive per-query file names
# =====================

def normalize_query(q: str) -> str:
    return re.sub(r"\s+", " ", q.strip().lower())

QUERY_NORM = normalize_query(QUERY)
QUERY_HASH = hashlib.md5(QUERY_NORM.encode('utf-8')).hexdigest()[:10]

COOKIES_FILE = 'cookies.json'
CHECKPOINT_FILE = f'checkpoint_{QUERY_HASH}.json'
TWEETS_FILE = f'tweets_hungerstation_latest_09-04__08-26_{QUERY_HASH}.csv'

# =====================
# Helpers
# =====================
async def check_cookies_valid(client: Client) -> bool:
    try:
        await client.get_trends('trending')
        return True
    except Exception:
        return False

async def safe_search(client: Client, query: str, count: int = BATCH_SIZE, cursor=None):
    try:
        result = await client.search_tweet(query, PRODUCT, count=count, cursor=cursor)
        return result
    except TooManyRequests as e:
        reset_time = getattr(e, 'rate_limit_reset', time.time() + 900)
        wait_time = max(reset_time - time.time(), 0) + 10
        print(f"Rate limited. Waiting {wait_time:.0f}s until {datetime.fromtimestamp(reset_time)}")
        await asyncio.sleep(wait_time)
        return await safe_search(client, query, count, cursor)
    except Exception as e:
        print(f"Search error: {e}")
        await asyncio.sleep(60)
        return None

# Per-query checkpoint structure
DEFAULT_CKPT = {
    'query_hash': QUERY_HASH,
    'count': 0,
    'cursor': None,
    'last_success_time': None,
}

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if data.get('query_hash') != QUERY_HASH:
                # Different query: start fresh
                return DEFAULT_CKPT.copy()
            return data
        except Exception:
            pass
    return DEFAULT_CKPT.copy()

def save_checkpoint(data):
    data['query_hash'] = QUERY_HASH
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def init_existing_ids(csv_path: str):
    ids = set()
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    ids.add(row[0])
    return ids


def save_tweets_batch(tweets, writer, existing_ids) -> int:
    new_count = 0
    for tw in tweets:
        if tw.id in existing_ids:
            continue
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
    return new_count

# =====================
# Auth / Login
# =====================
async def init_client() -> Client | None:
    client = Client(language='ar')

    if os.path.exists(COOKIES_FILE):
        try:
            client.load_cookies(COOKIES_FILE)
            print("✓ Loaded existing cookies")
            if await check_cookies_valid(client):
                print("✓ Cookies are valid")
                return client
            else:
                print("✗ Cookies expired — will re-login")
        except Exception as e:
            print(f"✗ Error loading cookies: {e}")

    config = ConfigParser()
    config.read('config.ini')
    try:
        username = config['X']['username']
        email = config['X']['email']
        password = config['X']['password']
    except KeyError as e:
        print(f"✗ Missing configuration: {e}")
        return None

    try:
        print("Logging in…")
        await client.login(auth_info_1=username, auth_info_2=email, password=password)
        client.save_cookies(COOKIES_FILE)
        print("✓ Logged in & saved cookies")
        return client
    except Exception as e:
        print(f"✗ Login failed: {e}")
        return None

# =====================
# Main loop (Latest only)
# =====================
async def run():
    print(f"QUERY_HASH={QUERY_HASH}")
    print(f"Checkpoint file: {CHECKPOINT_FILE}")
    print(f"Output CSV     : {TWEETS_FILE}")

    client = await init_client()
    if not client:
        print("Failed to initialize client")
        return

    ckpt = load_checkpoint()
    collected = ckpt['count']
    cursor = ckpt['cursor']

    existing_ids = init_existing_ids(TWEETS_FILE)

    new_file = not os.path.exists(TWEETS_FILE) or os.path.getsize(TWEETS_FILE) == 0
    with open(TWEETS_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(['ID', 'Username', 'Text', 'Created At', 'Retweets', 'Likes', 'Replies'])

        failures = 0
        while collected < MAX_TWEETS and failures < 20:
            print(f"Collected {collected}/{MAX_TWEETS} — product={PRODUCT} — cursor={cursor}")

            result = await safe_search(client, QUERY, BATCH_SIZE, cursor)
            if not result:
                failures += 1
                print(f"No result returned. failures={failures}")
                cursor = None
            else:
                tweets = list(result)
                if not tweets:
                    failures += 1
                    print(f"Empty page. failures={failures}")
                    cursor = None
                else:
                    added = save_tweets_batch(tweets, writer, existing_ids)
                    collected += added
                    failures = 0
                    ckpt['last_success_time'] = datetime.now().isoformat()

                    # pagination forward
                    try:
                        nxt = await result.next()
                        cursor = getattr(nxt, 'next_cursor', None)
                    except Exception as e:
                        print(f"Pagination error: {e}")
                        cursor = None

                    print(f"Added {added} new. Total={collected}")

            # checkpoint & backoff
            ckpt.update({'count': collected, 'cursor': cursor})
            save_checkpoint(ckpt)

            delay = random.randint(30, 90) if failures == 0 else random.randint(120, 300)
            print(f"Waiting {delay}s before next request…")
            await asyncio.sleep(delay)

        if failures >= 20:
            print("Stopped: too many consecutive failures (20)")
        print(f"Done. Collected {collected} tweets.")

if __name__ == '__main__':
    asyncio.run(run())
