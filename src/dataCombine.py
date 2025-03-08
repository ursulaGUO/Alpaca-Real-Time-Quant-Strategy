import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import ta
from queryFromPost import show_table
from dataFromBlueSky import download_bluesky_posts
import pytz

DB_FILE = "data/trade_data.db"

# ---------- Step 1: Compute Technical Indicators ----------
def compute_technical_indicators(since, until):
    """Load stock data, compute technical indicators, and save them back to SQLite."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    query = f"""
        CREATE TABLE stock_features AS
        WITH stock_window AS (
            SELECT
                *,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY timestamp
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS SMA_20,

                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY timestamp
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS SMA_50,

                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY timestamp
                    ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
                ) AS SMA_100,

                sqrt(
                    AVG(close * close) OVER (
                        PARTITION BY symbol 
                        ORDER BY timestamp 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) - 
                    (AVG(close) OVER (
                        PARTITION BY symbol 
                        ORDER BY timestamp 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) * AVG(close) OVER (
                        PARTITION BY symbol 
                        ORDER BY timestamp 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ))
                ) AS Volatility
            FROM stock_prices
            WHERE timestamp BETWEEN '{since}' AND '{until}'
        )
        SELECT
            *,
            SMA_20 + 2 * Volatility AS Bollinger_Upper,
            SMA_20 - 2 * Volatility AS Bollinger_Lower
        FROM stock_window;
        """
    cursor.execute("DROP TABLE IF EXISTS stock_features;") 
    cursor.execute(query)
    conn.commit()
    conn.close()

    print("Technical indicators computed and saved to `stock_features`.")

# ---------- Step 2: Merge with sentiment data ----------
def merge_sentiment_data(start_date="2025-02-17", end_date="2025-02-21"):
    """Merge new stock and sentiment data while retaining previous merged data."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Check min/max timestamp in `bluesky_posts`
    cursor.execute("SELECT MIN(date), MAX(date) FROM bluesky_posts")
    min_datetime, max_datetime = cursor.fetchone()

    # Download missing sentiment data if needed
    if min_datetime is None:
        download_bluesky_posts(stock_dict, start_date, end_date, like_limit=10)
    elif min_datetime > start_date:
        download_bluesky_posts(stock_dict, start_date, min_datetime, like_limit=10)

    if max_datetime is None:
        download_bluesky_posts(stock_dict, start_date, end_date, like_limit=10)
    elif max_datetime < end_date:
        download_bluesky_posts(stock_dict, max_datetime, end_date, like_limit=10)


    # Check the latest timestamp in `merged_data` (to avoid redundant merging)
    cursor.execute("SELECT MAX(timestamp) FROM merged_data")
    last_merged_timestamp = cursor.fetchone()[0]

    # If `merged_data` is empty, merge all available stock data
    if last_merged_timestamp is None or last_merged_timestamp < start_date:
        last_merged_timestamp = start_date




def merge_sentiment_data(start_date="2025-02-17", end_date="2025-02-21"):
    """Merge stock and sentiment data."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Convert `b.date` from UTC to CST before merging
    merge_query = f"""
        INSERT INTO merged_data
        SELECT 
            s.*, 
            COALESCE(
                SUM(b.sentiment_score * b.likes) / NULLIF(SUM(b.likes), 0), 
                0
            ) AS weighted_sentiment
        FROM stock_features s
        LEFT JOIN bluesky_posts b
            ON b.date BETWEEN datetime(s.timestamp, '-2 hours') 
                AND datetime(s.timestamp, '+2 hours')
            AND s.symbol = b.keyword
        WHERE s.timestamp BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY s.timestamp, s.symbol;
    """

    cursor.execute(merge_query)
    conn.commit()
    conn.close()
    print("Merged dataset saved to `merged_data` in SQLite for ML modeling.")

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


since = "2025-02-27"
until = "2025-03-08"
print("Computing technical indicators between {since} and {until}")
compute_technical_indicators(since, until)
merge_sentiment_data(since, until)
show_table('merged_data')


