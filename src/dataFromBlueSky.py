from atproto import Client, models
from datetime import datetime, timezone
import csv
import os

client = Client()
client.login('18217786504a@gmail.com', 'Ann266266')

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

def find_head_or_tail_date_csv(filename, keyword, tail=True):
    """Find the earliest (head) or latest (tail) post's timestamp for a given keyword in CSV."""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            parse_content = reversed(list(reader)) if tail else list(reader)

            for row in parse_content:
                if row and row[0] == keyword:
                    return datetime.fromisoformat(row[2].replace("Z", "+00:00"))  # Convert to datetime

    except FileNotFoundError:
        return None  

    return None 

def save_posts_to_csv(posts, keyword, filename='bluesky_posts.csv'):
    """Append new posts to a CSV file without overwriting existing ones."""
    file_exists = os.path.exists(filename)

    with open(filename, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write header only if file is new
        if not file_exists:
            writer.writerow(["Keyword", "Author", "Date", "Likes", "Shares", "Quotes", "Replies", "Text"])

        for post in posts:
            writer.writerow([
                keyword,
                post.author.handle,
                post.record.created_at,
                post.like_count,
                post.repost_count,
                post.quote_count,
                post.reply_count,
                post.record.text.replace("\n", " ")  # Remove newlines for CSV
            ])

def main():
    keyword = "nvidia"

    # Define default search range
    default_since = datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    default_until = datetime(2025, 3, 3, 23, 59, 59, tzinfo=timezone.utc)

    # Get timestamps of the latest and earliest posts
    if 'bluesky_posts.csv' in os.listdir():
        latest_timestamp = find_head_or_tail_date_csv('bluesky_posts.csv', keyword, tail=True)
        earliest_timestamp = find_head_or_tail_date_csv('bluesky_posts.csv', keyword, tail=False)
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
        
        save_posts_to_csv(posts_forward, keyword)

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

        save_posts_to_csv(posts_backward, keyword)

        # Update earliest timestamp and store last fetched post
        last_fetched_earliest = oldest_post_time
        earliest_timestamp = oldest_post_time

    print("Finished collecting all available posts.")

if __name__ == "__main__":
    main()
