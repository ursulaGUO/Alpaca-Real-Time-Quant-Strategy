from atproto import Client, models
from atproto_client.exceptions import InvokeTimeoutError
import httpx
from datetime import datetime, timezone, timedelta
import argparse
import os
from dotenv import load_dotenv
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer 
import pandas as pd
import time

dotenv_path = os.path.expanduser('~/.secrets/.env')
load_dotenv(dotenv_path)
emailname = os.getenv('blueSky_user_name')
password = os.getenv('blueSky_password')

client = Client()
client.login(emailname, password)

DB_FILE = "data/trade_data.db"

MAX_REQUESTS = 3000  # Maximum requests allowed
TIME_WINDOW = 300  # 5 minutes (in seconds)
request_count = 0
start_time = time.time()

def wait_if_needed():
    """Enforces the rate limit by delaying requests if approaching the limit."""
    global request_count, start_time

    elapsed_time = time.time() - start_time

    if request_count >= MAX_REQUESTS:
        # Wait until 5 minutes have passed
        sleep_time = TIME_WINDOW - elapsed_time
        if sleep_time > 0:
            print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

        # Reset request counter and start time after waiting
        request_count = 0
        start_time = time.time()

    request_count += 1  # Increment request count for each API call

def initialize_db():
    """Create a new SQLite database file with a posts table."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bluesky_posts (
                keyword TEXT,
                author TEXT,
                date TEXT,
                likes INTEGER,
                shares INTEGER,
                quotes INTEGER,
                replies INTEGER,
                text TEXT,
                sentiment_score REAL DEFAULT NULL,
                PRIMARY KEY (keyword, author, date)
            )
        """)
        conn.commit()

def ensure_datetime(value):
    """Convert datetime to ISO string format (if not already)."""
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value

def search_bluesky_posts(client, keyword_list, since, until, limit=100, max_retries=5, timeout=30):
    """Search for BlueSky posts with retries on timeout errors."""
    since_str = ensure_datetime(since)
    until_str = ensure_datetime(until)

    # Join multiple keywords with OR to broaden search results
    query_string = " OR ".join(keyword_list)

    params = models.AppBskyFeedSearchPosts.Params(
        q=query_string,
        since=since_str,
        until=until_str,
        sort="latest",
        lang="en",
        limit=limit
    )

    retries = 0
    while retries < max_retries:
        try:
            response = client.app.bsky.feed.search_posts(params, timeout=timeout)

            if not hasattr(response, "posts") or not response.posts:
                return []

            return response.posts

        except (InvokeTimeoutError, httpx.ReadTimeout) as e:
            retries += 1
            wait_time = 5 * retries  # Exponential backoff: 5s, 10s, 15s...
            print(f"Timeout error: {e}. Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(wait_time)

    print("Max retries reached. Skipping this request.")
    return []


analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    """Add -1 to 1 sentiment score to a text."""
    sentiment = analyzer.polarity_scores(text)
    return sentiment["compound"]

def save_posts_to_db(posts, keyword):
    """Save bluesky_posts to an SQLite database."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            posts = sorted(posts, key=lambda post: post.record.created_at)
            for post in posts:
                try:
                    sscore = get_sentiment_score(post.record.text)
                    cursor.execute("""
                        INSERT INTO bluesky_posts (keyword, author, date, likes, shares, quotes, replies, text, sentiment_score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (keyword, 
                          post.author.handle, 
                          post.record.created_at, 
                          post.like_count,
                          post.repost_count, 
                          post.quote_count, 
                          post.reply_count, 
                          post.record.text.replace("\n", " "),
                          sscore))
                except sqlite3.IntegrityError:
                    # If post already exists (same keyword, author, and date), skip it
                    continue

            conn.commit()  # Ensure all data is saved
    except sqlite3.Error as e:
        print(f"Database error in save_posts_to_db: {e}")

def get_top_positive_posts(keyword, limit=5):
    """Get the top positive sentiment posts for a given keyword."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT author, date, text, sentiment_score FROM bluesky_posts
        WHERE keyword = ? ORDER BY sentiment_score DESC LIMIT ?
    """, (keyword, limit))
    
    results = cursor.fetchall()
    conn.close()
    return results

def find_head_or_tail_date_db(file, keyword, tail=True):
    """Find the latest or earliest date in the sqlite db."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    if tail:
        cursor.execute("""
            SELECT date FROM bluesky_posts
            WHERE keyword = ? ORDER BY date DESC LIMIT 1
        """, (keyword,))
    else:
        cursor.execute("""
            SELECT date FROM bluesky_posts
            WHERE keyword = ? ORDER BY date ASC LIMIT 1
        """, (keyword,))
    
    result = cursor.fetchone()
    conn.close()
    return None if result is None else datetime.fromisoformat(result[0].replace("Z", "+00:00"))


def parse_datetime(date_str):
    """Safely parse date strings in either '%Y-%m-%d' or ISO format."""
    if isinstance(date_str, datetime):
        # If the input is already a datetime object, return it directly
        return date_str.replace(tzinfo=timezone.utc)
    
    try:
        # Try parsing '%Y-%m-%d' format
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            # Try parsing ISO format (YYYY-MM-DDTHH:MM:SS.SSSZ)
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"Invalid date format: {date_str}") from e


import time
from datetime import datetime, timezone, timedelta

def download_bluesky_posts(keyword_dict, since, until=None, like_limit=10):
    """Download BlueSky posts for a list of keywords and filter them before saving."""
    
    if keyword_dict is None or since is None:
        return

    initialize_db()
    
    # Use the time parsing function
    default_since = parse_datetime(since)
    default_until = parse_datetime(until) if until else datetime.now(timezone.utc)  # Fixed end date for forward search
    
    time_step = timedelta(hours=1)  # Move by 1 hour if no posts are found

    for symbol, keyword_list in keyword_dict.items():
        print(f"Searching for posts related to '{symbol}' using keywords: {keyword_list}")

        # Get the latest and earliest timestamps from the database
        latest_timestamp = find_head_or_tail_date_db(DB_FILE, symbol, tail=True)
        earliest_timestamp = find_head_or_tail_date_db(DB_FILE, symbol, tail=False)

        # Set default values if no previous data exists
        latest_timestamp = parse_datetime(latest_timestamp) if latest_timestamp else default_since
        earliest_timestamp = parse_datetime(earliest_timestamp) if earliest_timestamp else default_until

        last_fetched_latest = None
        last_fetched_earliest = None

        if latest_timestamp is not None and latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

        if earliest_timestamp is not None and earliest_timestamp.tzinfo is None:
            earliest_timestamp = earliest_timestamp.replace(tzinfo=timezone.utc)

        if default_until is not None and default_until.tzinfo is None:
            default_until = default_until.replace(tzinfo=timezone.utc)

        if default_since is not None and default_since.tzinfo is None:
            default_since = default_since.replace(tzinfo=timezone.utc)

        # Forward search: Fetch newer posts
        while latest_timestamp < default_until:
            print(f"Fetching {symbol} newer posts from {latest_timestamp} to {default_until}...")

            wait_if_needed()
            posts_forward = search_bluesky_posts(client, keyword_list, latest_timestamp, default_until)

            if not posts_forward:
                print(f"No {symbol} posts found in this time chunk. Moving forward.")
                latest_timestamp += time_step  # Move forward by 1 hour
                continue

            # Filter posts
            filtered_posts = [post for post in posts_forward if post.like_count >= like_limit]

            if filtered_posts:
                save_posts_to_db(filtered_posts, symbol)
                newest_post_time = parse_datetime(filtered_posts[-1].record.created_at)  # Get last post time

                if last_fetched_latest is None:
                    last_fetched_latest = newest_post_time

                # Ensure that we're moving forward in time
                if newest_post_time <= latest_timestamp:
                    print(f"Detected same {symbol} newest post, forcing move forward.")
                    latest_timestamp += time_step
                    continue

                latest_timestamp = newest_post_time
            else:
                latest_timestamp += time_step  # Move forward when no posts meet criteria

        # Backward search: Fetch older posts
        while earliest_timestamp > default_since:
            print(f"Fetching {symbol} older posts from {default_since} to {earliest_timestamp}...")

            wait_if_needed()
            posts_backward = search_bluesky_posts(client, keyword_list, default_since, earliest_timestamp)

            if not posts_backward:
                print(f"No {symbol} posts found in this time chunk. Moving backward.")
                earliest_timestamp -= time_step  # Move backward by 1 hour
                continue

            # Filter posts
            filtered_posts = [post for post in posts_backward if post.like_count >= like_limit]

            if filtered_posts:
                save_posts_to_db(filtered_posts, symbol)
                oldest_post_time = parse_datetime(filtered_posts[0].record.created_at)  # Get first post time

                if last_fetched_earliest is None:
                    last_fetched_earliest = oldest_post_time

                # Ensure that we're moving backward in time
                if oldest_post_time >= earliest_timestamp:
                    print(f"Detected {symbol} same oldest post, forcing move backward.")
                    earliest_timestamp -= time_step
                    continue

                earliest_timestamp = oldest_post_time
            else:
                earliest_timestamp -= time_step  # Move backward when no posts meet criteria

        print(f"Finished downloading all BlueSky posts for '{symbol}'")

    print("Finished collecting all available posts.")



def filter_like(like_limit):
    """Filter posts based on like count."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT author, date, text, sentiment_score 
        FROM bluesky_posts
        WHERE likes >= ?
    """, (like_limit,))
    
    results = cursor.fetchall()
    conn.close()
    filtered_df = pd.DataFrame(results)
    filtered_df.to_csv(f"data/filtered.csv", index=False)
    print(filtered_df.shape[0])

stock_dict = {
    "AAPL": [
        "Apple",
    ],
    "MSFT": [
        "Microsoft",
    ],
    "GOOGL": [
        "Google",
    ],
    "AMZN": [
        "Amazon",
    ],
    "TSLA": [
        "Tesla",
    ],
    "NVDA": [
        "Nvidia",
    ]
}

def main():
    """Fetch historical data once, then update with new posts every 15 minutes."""
    
    # Step 1: Download all historical posts from 2025-02-17 to now
    start_date = "2025-02-17"
    end_date = datetime.now(timezone.utc).isoformat()  # Current time in UTC

    print(f"Downloading historical posts from {start_date} to {end_date}...")
    download_bluesky_posts(stock_dict, start_date, end_date)

    # Step 2: Enter a loop to fetch new posts every 15 minutes
    while True:
        now = datetime.now(timezone.utc).isoformat()
        print(f"Fetching latest posts up to {now}...")

        # Fetch new posts up to the current timestamp
        download_bluesky_posts(stock_dict, end_date, now)

        # Wait 15 minutes before checking again
        print("Sleeping 15 minutes")
        time.sleep(15 * 60)  # 15 minutes

if __name__ == "__main__":
    main()