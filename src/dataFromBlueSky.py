from atproto import Client, models
from datetime import datetime, timezone
import argparse
import os
from dotenv import load_dotenv
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer 

dotenv_path = os.path.expanduser('~/.secrets/.env')
load_dotenv(dotenv_path)
emailname = os.getenv('blueSky_user_name')
password = os.getenv('blueSky_password')

client = Client()
client.login(emailname, password)

DB_FILE = "data/trade_data.db"

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

def search_bluesky_posts(client, keyword_list, since, until, limit=100):
    """Search for BlueSky posts containing any of the keywords between since and until timestamps."""
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
    
    response = client.app.bsky.feed.search_posts(params)

    if not hasattr(response, "posts") or not response.posts:
        return []
    
    return response.posts


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



def download_bluesky_posts(keyword_dict, since):
    """Download BlueSky posts for a list of keywords."""

    initialize_db()
    default_since = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    default_until = datetime.now(timezone.utc)  # Fixed end date for forward search

    for symbol, keyword_list in keyword_dict.items():
        print(f"Searching for posts related to '{symbol}' using keywords: {keyword_list}")

        # Get the latest and earliest timestamps from the database
        latest_timestamp = find_head_or_tail_date_db(DB_FILE, symbol, tail=True)
        earliest_timestamp = find_head_or_tail_date_db(DB_FILE, symbol, tail=False)

        # Set default values if no previous data exists
        latest_timestamp = latest_timestamp or default_since
        earliest_timestamp = earliest_timestamp or default_until

        # Track last fetched timestamps to detect repeated fetches
        last_fetched_latest = None
        last_fetched_earliest = None

        # Forward search: Fetch newer posts
        while latest_timestamp < default_until:
            print(f"Fetching newer posts from {latest_timestamp} to {default_until}...")
            posts_forward = search_bluesky_posts(client, keyword_list, latest_timestamp, default_until)

            if not posts_forward:
                print("No more newer posts found.")
                break 

            # Check if the newest post is the same as last fetched
            newest_post_time = datetime.fromisoformat(posts_forward[0].record.created_at.replace("Z", "+00:00"))
            if newest_post_time == last_fetched_latest:
                print("Detected same newest post, stopping forward search.")
                break 

            save_posts_to_db(posts_forward, symbol)

            # Update latest timestamp and track last fetched post
            last_fetched_latest = newest_post_time
            latest_timestamp = newest_post_time

        # Backward search: Fetch older posts
        while earliest_timestamp > default_since:
            print(f"Fetching older posts from {default_since} to {earliest_timestamp}...")
            posts_backward = search_bluesky_posts(client, keyword_list, default_since, earliest_timestamp)

            if not posts_backward:
                print("No more older posts found.")
                break 

            # Check if the oldest post is the same as last fetched
            oldest_post_time = datetime.fromisoformat(posts_backward[-1].record.created_at.replace("Z", "+00:00"))
            if oldest_post_time == last_fetched_earliest:
                print("Detected same oldest post, stopping backward search.")
                break

            save_posts_to_db(posts_backward, symbol)

            # Update earliest timestamp and track last fetched post
            last_fetched_earliest = oldest_post_time
            earliest_timestamp = oldest_post_time

        print(f"Finished downloading all BlueSky posts for '{symbol}'")

    print("Finished collecting all available posts.")



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


new_dict = {
    "AAPL": [
        "Apple",
        "AAPL",
        "Tim Cook",
    ],
    "MSFT": [
        "MSFT",
        "Microsoft",
        "OpenAI",
    ],
    "GOOGL": [
        "Google",
        "GOOGL"
    ],
    "AMZN": [
        "Amazon",
        "AMZN"
    ],
    "TSLA": [
        "Tesla",
        "TSLA",
        "Elon Musk"
    ],
    "NVDA": [
        "Nvidia",
        "CUDA",
        "NVDA"
    ]
}
download_bluesky_posts(stock_dict, "2025-03-03")