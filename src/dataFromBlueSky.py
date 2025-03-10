import sqlite3
import httpx
import time
import config
from atproto import Client, models
from atproto_client.exceptions import InvokeTimeoutError
from datetime import datetime, timezone, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize BlueSky client
client = Client()
client.login(config.BLUESKY_USERNAME, config.BLUESKY_PASSWORD)

# Rate limiting settings
MAX_REQUESTS = 3000  
TIME_WINDOW = 300  # 5 minutes
request_count = 0
start_time = time.time()

### =========================
###   DATABASE FUNCTIONS
### =========================

def initialize_db():
    """Create the bluesky_posts table if it does not exist."""
    with sqlite3.connect(config.DB_FILE) as conn:
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

def save_posts_to_db(posts, keyword):
    """Save BlueSky posts to the SQLite database."""
    try:
        with sqlite3.connect(config.DB_FILE) as conn:
            cursor = conn.cursor()
            for post in sorted(posts, key=lambda post: post.record.created_at):
                try:
                    sentiment_score = get_sentiment_score(post.record.text)
                    cursor.execute("""
                        INSERT INTO bluesky_posts (keyword, author, date, likes, shares, quotes, replies, text, sentiment_score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (keyword, post.author.handle, post.record.created_at, post.like_count, post.repost_count, 
                          post.quote_count, post.reply_count, post.record.text.replace("\n", " "), sentiment_score))
                except sqlite3.IntegrityError:
                    continue  # Skip duplicates
            conn.commit()
    except sqlite3.Error as e:
        print(f"Database error: {e}")

def get_last_scraped_timestamp(keyword):
    """Retrieve the latest timestamp for a keyword from the database."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(date) FROM bluesky_posts WHERE keyword = ?
    """, (keyword,))
    result = cursor.fetchone()[0]
    conn.close()
    return result if result else None

### =========================
###   SENTIMENT ANALYSIS
### =========================

analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    """Compute sentiment score between -1 and 1."""
    return analyzer.polarity_scores(text)["compound"]

### =========================
###   API REQUESTS & RATE LIMITING
### =========================

def wait_if_needed():
    """Enforces rate limits by delaying requests if needed."""
    global request_count, start_time
    elapsed_time = time.time() - start_time

    if request_count >= MAX_REQUESTS:
        sleep_time = TIME_WINDOW - elapsed_time
        if sleep_time > 0:
            print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
        request_count = 0
        start_time = time.time()

    request_count += 1  

def search_bluesky_posts(keyword_list, since, until, limit=100, max_retries=5, timeout=30):
    """Fetch posts from BlueSky API with retries on timeout errors."""
    since_str = since.isoformat().replace("+00:00", "Z")
    until_str = until.isoformat().replace("+00:00", "Z")

    query_string = " OR ".join(keyword_list)
    params = models.AppBskyFeedSearchPosts.Params(
        q=query_string, since=since_str, until=until_str, sort="latest", lang="en", limit=limit
    )

    retries = 0
    while retries < max_retries:
        try:
            response = client.app.bsky.feed.search_posts(params, timeout=timeout)
            return response.posts if hasattr(response, "posts") else []
        except (InvokeTimeoutError, httpx.ReadTimeout) as e:
            retries += 1
            wait_time = 5 * retries  # Exponential backoff
            print(f"Timeout error: {e}. Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(wait_time)

    print("Max retries reached. Skipping this request.")
    return []

### =========================
###   MAIN FUNCTION
### =========================

def download_bluesky_posts():
    """Download BlueSky posts for configured stock keywords."""
    initialize_db()
    now = datetime.now(timezone.utc)
    
    for symbol, keywords in config.STOCK_DICT.items():
        last_scraped = get_last_scraped_timestamp(symbol)
        start_time = datetime.strptime(config.SENTIMENT_START_DATE, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) if not last_scraped else datetime.fromisoformat(last_scraped.replace("Z", "+00:00"))
        
        print(f"Fetching BlueSky posts for {symbol} from {start_time} to {now}...")

        wait_if_needed()
        posts = search_bluesky_posts(keywords, start_time, now)

        if posts:
            save_posts_to_db(posts, symbol)
            print(f"Saved {len(posts)} posts for {symbol}.")
        else:
            print(f"No new posts found for {symbol}.")

    print("Finished fetching BlueSky posts.")

if __name__ == "__main__":
    print("Running BlueSky data collection...")
    download_bluesky_posts()
    print("BlueSky data collection complete.")
