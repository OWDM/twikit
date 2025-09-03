import asyncio
import os
import time
import csv
import json
import random
from datetime import datetime, timedelta
from configparser import ConfigParser
from twikit import Client, TooManyRequests

# Configuration
COOKIES_FILE = 'cookies.json'
CHECKPOINT_FILE = 'checkpoint.json'
TWEETS_FILE = 'tweets_large_4.csv'
QUERY = '(ثمانية OR ثمانيه)'
MAX_TWEETS = 1000
BATCH_SIZE = 20

# Define different search strategies
SEARCH_STRATEGIES = [
    {'product': 'Latest', 'count': BATCH_SIZE},
    {'product': 'Top', 'count': BATCH_SIZE},
]

async def check_cookies_valid(client):
    """Check if loaded cookies are still valid"""
    try:
        await client.get_trends('trending')
        return True
    except Exception:
        return False

async def safe_search(client, query, product='Latest', count=20, cursor=None):
    """Perform a search with rate limit handling"""
    try:
        result = await client.search_tweet(query, product, count=count, cursor=cursor)
        return result
    except TooManyRequests as e:
        reset_time = getattr(e, 'rate_limit_reset', time.time() + 900)
        wait_time = max(reset_time - time.time(), 0) + 10
        print(f"Rate limited. Waiting {wait_time:.0f} seconds until {datetime.fromtimestamp(reset_time)}")
        await asyncio.sleep(wait_time)
        return await safe_search(client, query, product, count, cursor)
    except Exception as e:
        print(f"Search error: {e}")
        await asyncio.sleep(60)
        return None

def load_checkpoint():
    """Load progress from checkpoint file"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {
        'count': 0, 
        'cursor': None, 
        'strategy_index': 0,
        'last_successful_strategy': 0,
        'consecutive_failures': 0,
        'last_success_time': None,
        'query_variations': 0
    }

def save_checkpoint(data):
    """Save progress to checkpoint file"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(data, f)

def save_tweets_batch(tweets, writer, existing_ids):
    """Save a batch of tweets to CSV, avoiding duplicates"""
    new_count = 0
    for tweet in tweets:
        if tweet.id not in existing_ids:
            writer.writerow([
                tweet.id,
                f"@{tweet.user.screen_name}",
                tweet.text,
                tweet.created_at,
                tweet.retweet_count,
                tweet.favorite_count,
                tweet.reply_count
            ])
            existing_ids.add(tweet.id)
            new_count += 1
    return new_count

async def main():
    """Main async function to handle Twitter login and operations"""
    client = Client(language='ar')
    
    if os.path.exists(COOKIES_FILE):
        try:
            client.load_cookies(COOKIES_FILE)
            print("✓ Loaded existing cookies")
            if await check_cookies_valid(client):
                print("✓ Cookies are valid")
                return client
            else:
                print("✗ Cookies are expired")
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
        print("Logging in...")
        await client.login(
            auth_info_1=username,
            auth_info_2=email, 
            password=password
        )
        client.save_cookies(COOKIES_FILE)
        print("✓ Successfully logged in and saved cookies")
        return client
    except Exception as e:
        print(f"✗ Login failed: {e}")
        return None

def generate_query_variations(base_query, variation_index):
    """Generate different variations of the query to find more results"""
    variations = [
        base_query,
        base_query.replace('(to:thmanyahCompany)', ''),
        base_query.replace('(ثمانية OR ثمانيه)', 'ثمانية'),
        base_query.replace('(ثمانية OR ثمانيه)', 'ثمانيه'),
        base_query.replace('lang:ar', ''),
    ]
    
    # Add date-based variations
    current_date = datetime.now()
    date_variations = [
        f"{base_query} since:{current_date - timedelta(days=30):%Y-%m-%d}",
        f"{base_query} since:{current_date - timedelta(days=60):%Y-%m-%d}",
        f"{base_query} since:{current_date - timedelta(days=90):%Y-%m-%d}",
    ]
    
    variations.extend(date_variations)
    
    # Return the variation based on index (cycle through them)
    return variations[variation_index % len(variations)]

async def run_all_operations():
    """Run all async operations within the same event loop"""
    client = await main()
    
    if not client:
        print("Failed to initialize client")
        return
    
    # Load checkpoint
    checkpoint = load_checkpoint()
    collected_count = checkpoint['count']
    cursor = checkpoint.get('cursor')
    strategy_index = checkpoint.get('strategy_index', 0)
    last_successful_strategy = checkpoint.get('last_successful_strategy', 0)
    consecutive_failures = checkpoint.get('consecutive_failures', 0)
    last_success_time = checkpoint.get('last_success_time')
    query_variations = checkpoint.get('query_variations', 0)
    
    # Load existing tweet IDs to avoid duplicates
    existing_ids = set()
    if os.path.exists(TWEETS_FILE):
        with open(TWEETS_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    existing_ids.add(row[0])
    
    # Open CSV file for appending
    with open(TWEETS_FILE, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write header if file is new
        if collected_count == 0:
            writer.writerow(['ID', 'Username', 'Text', 'Created At', 'Retweets', 'Likes', 'Replies'])
        
        # Main collection loop
        while collected_count < MAX_TWEETS and consecutive_failures < 20:
            # Generate a query variation
            current_query = generate_query_variations(QUERY, query_variations)
            
            print(f"Collected {collected_count}/{MAX_TWEETS} tweets. Strategy: {SEARCH_STRATEGIES[strategy_index]['product']}, Query: {current_query[:50]}...")
            
            # Get current strategy
            strategy = SEARCH_STRATEGIES[strategy_index]
            
            # Get tweets with error handling
            try:
                result = await safe_search(
                    client, 
                    current_query, 
                    strategy['product'], 
                    strategy['count'], 
                    cursor
                )
                
                if not result:
                    raise Exception("No result returned")
                
                # Convert result to list of tweets
                tweets = list(result)
                
                if not tweets:
                    raise Exception("No tweets in result")
                
                # Save tweets and update count
                new_count = save_tweets_batch(tweets, writer, existing_ids)
                
                if new_count > 0:
                    collected_count += new_count
                    consecutive_failures = 0
                    last_successful_strategy = strategy_index
                    last_success_time = datetime.now().isoformat()
                    query_variations = 0  # Reset query variations on success
                    
                    # Update cursor for pagination
                    if hasattr(result, 'next_cursor') and result.next_cursor:
                        cursor = result.next_cursor
                    else:
                        # Try to get next page
                        try:
                            next_result = await result.next()
                            if hasattr(next_result, 'next_cursor') and next_result.next_cursor:
                                cursor = next_result.next_cursor
                            else:
                                cursor = None
                                strategy_index = (strategy_index + 1) % len(SEARCH_STRATEGIES)
                        except Exception as e:
                            print(f"Error getting next page: {e}")
                            cursor = None
                            strategy_index = (strategy_index + 1) % len(SEARCH_STRATEGIES)
                    
                    print(f"Added {new_count} new tweets. Total: {collected_count}")
                else:
                    consecutive_failures += 1
                    print(f"No new tweets found. Consecutive failures: {consecutive_failures}")
                    
                    # Try a different strategy and query variation
                    strategy_index = (strategy_index + 1) % len(SEARCH_STRATEGIES)
                    query_variations = (query_variations + 1) % 8  # Cycle through 8 variations
                    cursor = None
                
            except Exception as e:
                print(f"Error with strategy {strategy['product']}: {e}")
                consecutive_failures += 1
                
                # Try a different strategy and query variation
                strategy_index = (strategy_index + 1) % len(SEARCH_STRATEGIES)
                query_variations = (query_variations + 1) % 8  # Cycle through 8 variations
                cursor = None
            
            # Update checkpoint
            save_checkpoint({
                'count': collected_count,
                'cursor': cursor,
                'strategy_index': strategy_index,
                'last_successful_strategy': last_successful_strategy,
                'consecutive_failures': consecutive_failures,
                'last_success_time': last_success_time,
                'query_variations': query_variations
            })
            
            # Avoid rate limiting - variable delay based on success
            if consecutive_failures > 0:
                delay = random.randint(120, 300)  # Longer delay on failures (2-5 minutes)
            else:
                delay = random.randint(30, 90)  # Shorter delay on success (30-90 seconds)
            
            print(f"Waiting {delay} seconds before next request...")
            await asyncio.sleep(delay)
        
        if consecutive_failures >= 20:
            print("Too many consecutive failures (20). Stopping.")
        
        print(f"Completed! Collected {collected_count} tweets")

if __name__ == "__main__":
    asyncio.run(run_all_operations())