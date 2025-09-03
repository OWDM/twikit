import asyncio
import os
import time
import csv
import json
from datetime import datetime
from configparser import ConfigParser
from twikit import Client, TooManyRequests

# Configuration
COOKIES_FILE = 'cookies.json'
CHECKPOINT_FILE = 'checkpoint.json'
TWEETS_FILE = 'tweets_large.csv'
QUERY = '(ثمانية OR ثمانيه) (to:thmanyahCompany) lang:ar'
MAX_TWEETS = 1000
BATCH_SIZE = 20  # Twitter's maximum per request

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
        # Calculate wait time - Twitter tells us when to resume
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
    return {'count': 0, 'cursor': None, 'product': 'Latest'}

def save_checkpoint(count, cursor, product):
    """Save progress to checkpoint file"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({'count': count, 'cursor': cursor, 'product': product}, f)

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
    product = checkpoint.get('product', 'Latest')
    
    # Load existing tweet IDs to avoid duplicates
    existing_ids = set()
    if os.path.exists(TWEETS_FILE):
        with open(TWEETS_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if row:  # Check if row is not empty
                    existing_ids.add(row[0])  # Assuming ID is first column
    
    # Open CSV file for appending
    with open(TWEETS_FILE, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write header if file is new
        if collected_count == 0:
            writer.writerow(['ID', 'Username', 'Text', 'Created At', 'Retweets', 'Likes', 'Replies'])
        
        # Main collection loop
        while collected_count < MAX_TWEETS:
            print(f"Collected {collected_count}/{MAX_TWEETS} tweets. Product: {product}")
            
            # Get tweets with error handling
            result = await safe_search(client, QUERY, product, BATCH_SIZE, cursor)
            
            if not result:
                print("No result returned, trying different product...")
                # Try a different product type
                products = ['Top', 'Latest', 'Media']
                current_index = products.index(product) if product in products else 0
                next_index = (current_index + 1) % len(products)
                product = products[next_index]
                cursor = None  # Reset cursor when changing product
                save_checkpoint(collected_count, cursor, product)
                continue
            
            # Convert result to list of tweets
            tweets = list(result)
            
            if not tweets:
                print("No tweets in result, trying different product...")
                products = ['Top', 'Latest', 'Media']
                current_index = products.index(product) if product in products else 0
                next_index = (current_index + 1) % len(products)
                product = products[next_index]
                cursor = None
                save_checkpoint(collected_count, cursor, product)
                continue
            
            # Save tweets and update count
            new_count = save_tweets_batch(tweets, writer, existing_ids)
            collected_count += new_count
            
            # Update cursor for pagination
            if hasattr(result, 'next_cursor') and result.next_cursor:
                cursor = result.next_cursor
            else:
                # Try to get next page using the next() method
                try:
                    next_result = await result.next()
                    if hasattr(next_result, 'next_cursor') and next_result.next_cursor:
                        cursor = next_result.next_cursor
                    else:
                        print("No more pages available for this product")
                        # Switch to different product
                        products = ['Top', 'Latest', 'Media']
                        current_index = products.index(product) if product in products else 0
                        next_index = (current_index + 1) % len(products)
                        product = products[next_index]
                        cursor = None
                except Exception as e:
                    print(f"Error getting next page: {e}")
                    cursor = None
            
            # Update checkpoint
            save_checkpoint(collected_count, cursor, product)
            
            print(f"Added {new_count} new tweets. Total: {collected_count}")
            
            # Avoid rate limiting - delay between requests
            delay = 10  # Conservative delay
            print(f"Waiting {delay} seconds before next request...")
            await asyncio.sleep(delay)
    
    print(f"Completed! Collected {collected_count} tweets")

if __name__ == "__main__":
    asyncio.run(run_all_operations())