import os
import sqlite3
import pandas as pd

DB_FILE = "blusky_posts.db"

def search_post(ticker, since, until, limit=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
                   SELECT keyword, author, date, text, likes, sentiment_score FROM posts
                   WHERE keyword = ? AND date >= ? AND date <= ? ORDER BY date DESC
                   """, (ticker, since, until))
    
    results = cursor.fetchall()
    conn.close()

    results_df = pd.DataFrame(results, columns=["keyword", "author", "date", "text", "like", "sentiment_score"])    
    return results_df

stock_df = search_post("Nvidia", "2025-03-01", "2025-03-04")
print(f"Average sentiment score {stock_df['sentiment_score'].mean()}")
print(f"Spread of sentiment score {stock_df['sentiment_score'].max()-stock_df['sentiment_score'].min()}")
print(f"Number of posts {stock_df.shape[0]}")
print(f"Date range {stock_df['date'].min()} to {stock_df['date'].max()}")
