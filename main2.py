import asyncio
import os
from twikit import Client
from configparser import ConfigParser
import csv

# Configuration
COOKIES_FILE = 'cookies.json'
MINIMUM_TWEETS = 20
QUERY = '(ثمانية OR ثمانيه) (to:thmanyahCompany) lang:ar'

async def check_cookies_valid(client):
    """Check if loaded cookies are still valid by making a simple API call"""
    try:
        # Try a simple, low-impact API call to validate cookies
        trends = await client.get_trends('trending')
        return True
    except Exception:
        return False

async def main():
    """Main async function to handle Twitter login and operations"""
    # Initialize client
    client = Client(language='en-US')
    
    # Load cookies if they exist
    if os.path.exists(COOKIES_FILE):
        try:
            client.load_cookies(COOKIES_FILE)
            print("✓ Loaded existing cookies")
            
            # Verify cookies are still valid
            if await check_cookies_valid(client):
                print("✓ Cookies are valid")
                return client
            else:
                print("✗ Cookies are expired")
        except Exception as e:
            print(f"✗ Error loading cookies: {e}")
    
    # Fresh login if no cookies or they're invalid
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

async def search_tweets(client, query, count=10):
    """Search for tweets using the authenticated client"""
    if not client:
        print("No client available for search")
        return []
    
    try:
        print(f"Searching for '{query}'...")
        tweets = await client.search_tweet(query, 'Latest', count=count)
        print(f"✓ Found {len(tweets)} tweets")
        return tweets
    except Exception as e:
        print(f"✗ Search failed: {e}")
        return []

async def run_all_operations():

    """Run all async operations within the same event loop"""
    client = await main()
    
    if client:
        # Perform search
        tweets = await search_tweets(client, QUERY, MINIMUM_TWEETS)
        
        # Display results
        for i, tweet in enumerate(tweets, 1):
            print(f"\n--- Tweet {i} ---")
            print(f"User: @{tweet.user.screen_name}")
            print(f"Text: {tweet.text[:100]}..." if len(tweet.text) > 100 else f"Text: {tweet.text}")
            print(f"Date: {tweet.created_at}")
            print(f"Likes: {tweet.favorite_count}, Retweets: {tweet.retweet_count}")
        
        # Optional: Save tweets to file
        if tweets:
            print(f"\n✓ Successfully retrieved {len(tweets)} tweets")

        # Save tweets to CSV file
    if tweets:
        with open('tweets_2.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Tweet_count', 'Username', 'Text', 'Created At', 'Retweets', 'Likes'])
            
            for i, tweet in enumerate(tweets, 1):
                writer.writerow([
                    i,
                    f"@{tweet.user.screen_name}",
                    tweet.text,
                    tweet.created_at,
                    tweet.retweet_count,
                    tweet.favorite_count
                ])
        print(f"✓ Saved {len(tweets)} tweets to tweets.csv")


if __name__ == "__main__":
    # Run everything in a single event loop
    asyncio.run(run_all_operations())