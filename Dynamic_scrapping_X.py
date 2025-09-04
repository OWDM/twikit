import asyncio
import os
import time
import csv
import json
import random
import logging
import argparse
from datetime import datetime
from configparser import ConfigParser
from twikit import Client, TooManyRequests

# =====================
# Configuration
# =====================
parser = argparse.ArgumentParser(description='Twitter Scraper with Multiple Accounts and Proxy Support')
parser.add_argument('--account', type=str, required=True, help='Account name from config.ini')
parser.add_argument('--query', type=str, default='(@HungerStation OR هنقرستيشن OR هانقرستيشن) -كوبون -كود until:2023-12-15 since:2023-12-08', help='Search query')
parser.add_argument('--max-tweets', type=int, default=5000, help='Maximum tweets to collect')
parser.add_argument('--product', type=str, default='Latest', choices=['Top', 'Latest', 'Media'], help='Search product type')
args = parser.parse_args()

# Dynamic configuration based on account
ACCOUNT_NAME = args.account
COOKIES_FILE = f'cookies_{ACCOUNT_NAME}.json'
CHECKPOINT_FILE = f'checkpoint_{ACCOUNT_NAME}.json'
TWEETS_FILE = f'tweets_{ACCOUNT_NAME}.csv'
QUERY = args.query
PRODUCT = args.product
MAX_TWEETS = args.max_tweets
BATCH_SIZE = 20  # Reduced to Twitter's default to avoid issues
MAX_FAILURES = 10  # Increased to allow for more rate limit handling
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]

# =====================
# Logging Configuration
# =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scraper_{ACCOUNT_NAME}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(f'TwitterScraper_{ACCOUNT_NAME}')

# =====================
# Enhanced Helpers
# =====================
async def check_cookies_valid(client: Client) -> bool:
    try:
        await client.get_trends('trending')
        return True
    except Exception as e:
        logger.error(f"Cookie validation failed: {e}")
        return False

async def safe_search_with_retry(client: Client, query: str, count: int = BATCH_SIZE, cursor=None):
    """Perform a search with enhanced rate limit handling"""
    for attempt in range(MAX_RETRIES):
        try:
            result = await client.search_tweet(query, PRODUCT, count=count, cursor=cursor)
            return result
        except TooManyRequests as e:
            # Get the rate limit reset time from the exception
            reset_time = getattr(e, 'rate_limit_reset', None)
            
            if reset_time:
                # Calculate wait time based on Twitter's reset time
                wait_time = max(reset_time - time.time(), 0) + 5  # Add 5 seconds buffer
                logger.warning(f"Rate limited. Waiting {wait_time:.0f}s until {datetime.fromtimestamp(reset_time)}")
                await asyncio.sleep(wait_time)
                # Continue to next attempt without counting as a failure
                continue
            else:
                # If no reset time provided, use exponential backoff
                wait_time = RETRY_DELAYS[attempt]
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

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
    return { 'count': 0, 'cursor': None, 'last_success': None }

def save_checkpoint(data):
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
# Auth / Login with Enhanced Error Handling
# =====================
async def init_client() -> Client | None:
    config = ConfigParser()
    config.read('config.ini')
    
    # Get account-specific configuration
    if ACCOUNT_NAME not in config:
        logger.error(f"Account '{ACCOUNT_NAME}' not found in config.ini")
        return None
        
    try:
        username = config[ACCOUNT_NAME]['username']
        email = config[ACCOUNT_NAME]['email']
        password = config[ACCOUNT_NAME]['password']
        proxy_str = config[ACCOUNT_NAME].get('proxy', '').strip()
        
        # Handle proxy value conversion
        if proxy_str.lower() in ['none', 'no-proxy', '']:
            proxy = None
        else:
            proxy = proxy_str
    except KeyError as e:
        logger.error(f"Missing config for account {ACCOUNT_NAME}: {e}")
        return None

    # Initialize client with proxy if provided
    client = Client(language='ar', proxy=proxy)
    
    if os.path.exists(COOKIES_FILE):
        try:
            client.load_cookies(COOKIES_FILE)
            logger.info("Loaded existing cookies")
            if await check_cookies_valid(client):
                logger.info("Cookies are valid")
                return client
            else:
                logger.warning("Cookies expired — re-login required")
        except Exception as e:
            logger.error(f"Error loading cookies: {e}")

    # Login with retry logic
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Logging in (attempt {attempt+1}/{MAX_RETRIES})...")
            await client.login(auth_info_1=username, auth_info_2=email, password=password)
            client.save_cookies(COOKIES_FILE)
            logger.info("Logged in & saved cookies")
            return client
        except Exception as e:
            logger.error(f"Login failed (attempt {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAYS[attempt]
                logger.info(f"Retrying login in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
    
    logger.error(f"All {MAX_RETRIES} login attempts failed")
    return None

# =====================
# Enhanced Main Loop with Better Rate Limit Handling
# =====================
async def run():
    logger.info(f"Starting scraper for account '{ACCOUNT_NAME}' with query: {QUERY}")
    
    client = await init_client()
    if not client:
        return

    ckpt = load_checkpoint()
    collected = ckpt['count']
    cursor = ckpt['cursor']
    last_success = ckpt.get('last_success')
    existing_ids = init_existing_ids(TWEETS_FILE)

    new_file = not os.path.exists(TWEETS_FILE) or os.path.getsize(TWEETS_FILE) == 0
    with open(TWEETS_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(['ID', 'Username', 'Text', 'Created At', 'Retweets', 'Likes', 'Replies'])

        failures = 0
        while collected < MAX_TWEETS and failures < MAX_FAILURES:
            logger.info(f"Collected {collected}/{MAX_TWEETS} — cursor={cursor}")
            
            result = await safe_search_with_retry(client, QUERY, BATCH_SIZE, cursor)
            if not result:
                failures += 1
                logger.warning(f"Search failed ({failures}/{MAX_FAILURES})")
                cursor = None
            else:
                tweets = list(result)
                if not tweets:
                    failures += 1
                    logger.warning(f"No tweets found ({failures}/{MAX_FAILURES})")
                    cursor = None
                else:
                    added = save_tweets_batch(tweets, writer, existing_ids)
                    collected += added
                    failures = 0
                    last_success = datetime.now().isoformat()
                    
                    # Flush to ensure data is written to disk
                    f.flush()
                    
                    try:
                        # Try to get next cursor for pagination
                        if hasattr(result, 'next_cursor') and result.next_cursor:
                            cursor = result.next_cursor
                        else:
                            # Try to get next page using the next() method
                            next_result = await result.next()
                            if hasattr(next_result, 'next_cursor') and next_result.next_cursor:
                                cursor = next_result.next_cursor
                            else:
                                cursor = None
                    except Exception as e:
                        logger.error(f"Error getting next cursor: {e}")
                        cursor = None
                    
                    logger.info(f"Added {added} tweets. Total={collected}")

            ckpt.update({'count': collected, 'cursor': cursor, 'last_success': last_success})
            save_checkpoint(ckpt)

            # Adaptive delay based on success/failure and time since last success
            if failures == 0:
                delay = random.randint(5, 15)  # Shorter delay on success
            else:
                # Longer delay on failure, increasing with consecutive failures
                delay = random.randint(30, 60) * failures
                
            logger.info(f"Waiting {delay} seconds before next request...")
            await asyncio.sleep(delay)

        if failures >= MAX_FAILURES:
            logger.warning(f"Stopping due to {failures} consecutive failures")
        logger.info(f"Completed. Collected {collected} tweets.")

if __name__ == "__main__":
    asyncio.run(run())