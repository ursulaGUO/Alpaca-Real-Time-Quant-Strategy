from atproto import Client, models
from datetime import datetime, timezone
import csv
import os
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer 

client = Client()
client.login('18217786504a@gmail.com', 'Ann266266')

DB_FILE = "blusky_posts.db"

def initialize_db():
    """Create a new SQLite database file with a posts table."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
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

def search_bluesky_posts(client, keyword, since, until, limit=100):
    """Search for posts with a keyword between since and until timestamps."""
    since_str = ensure_datetime(since)
    until_str = ensure_datetime(until)

    params = models.AppBskyFeedSearchPosts.Params(
        q=keyword,
        since=since_str,
        until=until_str,
        sort="latest",
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
    """Save posts to an SQLite database."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            for post in posts:
                try:
                    sscore = get_sentiment_score(post.record.text)
                    cursor.execute("""
                        INSERT INTO posts (keyword, author, date, likes, shares, quotes, replies, text, sentiment_score)
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
        SELECT author, date, text, sentiment_score FROM posts
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
            SELECT date FROM posts
            WHERE keyword = ? ORDER BY date DESC LIMIT 1
        """, (keyword,))
    else:
        cursor.execute("""
            SELECT date FROM posts
            WHERE keyword = ? ORDER BY date ASC LIMIT 1
        """, (keyword,))
    
    result = cursor.fetchone()
    conn.close()
    return None if result is None else datetime.fromisoformat(result[0].replace("Z", "+00:00"))

def main():
    keyword = "microsoft"
    print("Innitialize database")
    initialize_db()

    # Define default search range
    default_since = datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    default_until = datetime(2025, 3, 3, 23, 59, 59, tzinfo=timezone.utc)

    # Get timestamps of the latest and earliest posts
    if 'bluesky_posts.csv' in os.listdir():
        latest_timestamp = find_head_or_tail_date_db('bluesky_posts.csv', keyword, tail=True)
        earliest_timestamp = find_head_or_tail_date_db('bluesky_posts.csv', keyword, tail=False)
    else:
        latest_timestamp = None
        earliest_timestamp = None

    # Set default values if no data exists
    if latest_timestamp is None:
        latest_timestamp = default_since
    if earliest_timestamp is None:
        earliest_timestamp = default_until

    # Store previous timestamps to detect repeated fetches
    last_fetched_latest = None
    last_fetched_earliest = None

    # Fetch newer posts (forward search)
    while latest_timestamp < default_until:
        print(f"Fetching newer posts from {latest_timestamp} to {default_until}...")
        posts_forward = search_bluesky_posts(client, keyword, latest_timestamp, default_until)

        if not posts_forward:
            print("No more newer posts found.")
            break 

        # Check if we keep fetching the same newest post
        newest_post_time = datetime.fromisoformat(posts_forward[0].record.created_at.replace("Z", "+00:00"))
        if newest_post_time == last_fetched_latest:
            print("Detected same newest post, stopping forward search.")
            break 
        
        save_posts_to_db(posts_forward, keyword)

        # Update latest timestamp and store last fetched post
        last_fetched_latest = newest_post_time
        latest_timestamp = newest_post_time

    # Fetch older posts (backward search)
    while earliest_timestamp > default_since:
        print(f"Fetching older posts from {default_since} to {earliest_timestamp}...")
        posts_backward = search_bluesky_posts(client, keyword, default_since, earliest_timestamp)

        if not posts_backward:
            print("No more older posts found.")
            break 

        # Check if we keep fetching the same oldest post
        oldest_post_time = datetime.fromisoformat(posts_backward[-1].record.created_at.replace("Z", "+00:00"))
        if oldest_post_time == last_fetched_earliest:
            print("Detected same oldest post, stopping backward search.")
            break

        save_posts_to_db(posts_backward, keyword)

        # Update earliest timestamp and store last fetched post
        last_fetched_earliest = oldest_post_time
        earliest_timestamp = oldest_post_time
    
    print(get_top_positive_posts(keyword))

    print("Finished collecting all available posts.")

if __name__ == "__main__":
    main()
