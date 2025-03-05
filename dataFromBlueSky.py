from atproto import Client,models
from datetime import datetime, timezone

client = Client()
client.login('18217786504a@gmail.com','Ann266266')

def authenticate(username, password):
    client = Client()
    client.login(username, password)
    return client

def search_bluesky_posts(client, keyword, since, until, limit=100):
    since_str = since.isoformat().replace("+00:00", "Z")
    until_str = until.isoformat().replace("+00:00", "Z")

    params = models.AppBskyFeedSearchPosts.Params(
        q=keyword,
        since=since_str,
        until=until_str,
        sort="latest",
        limit=limit)
    response=client.app.bsky.feed.search_posts(params)
    
    if not hasattr(response, "posts") or not response.posts:
        print("Did not find post")
        return []
    return response.posts

def main():
    keyword = "nvidia"

    since = datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    until = datetime(2025, 3, 4, 23, 59, 59, tzinfo=timezone.utc)

    #client=authenticate()

    posts = search_bluesky_posts(client, keyword,since, until)

    if not posts:
        print("No posts found")
        return
    else:
        #print(f"posts {posts}")
        save_posts_to_file(posts)
        

def save_posts_to_file(posts, filename='bluesky_posts.txt'):
    """save post to a text file"""
    with open(filename, 'w', encoding='utf-8') as file:
        for post in posts:
            file.write(f"Author: {post.author.handle}\n")
            file.write(f"Date: {post.record.created_at}\n")
            file.write(f"Likes: {post.like_count}\n")
            file.write(f"Shares: {post.repost_count}\n")
            file.write(f"Quotes: {post.quote_count}\n")
            file.write(f"Replies: {post.reply_count}\n")
            file.write(f"Text: {post.record.text}\n")
    

if __name__ == "__main__":
    main()