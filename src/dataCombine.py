import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import pytz
from queryFromPost import show_table
from dataFromBlueSky import download_bluesky_posts

DB_FILE = "data/trade_data.db"

# Define CST timezone
UTC = pytz.utc
CST = pytz.timezone("America/Chicago")

def get_latest_timestamps():
    """Retrieve the latest timestamps from stock_prices and bluesky_posts."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Get the latest timestamp from `stock_prices`
    cursor.execute("SELECT MAX(timestamp) FROM stock_prices")
    latest_stock_time = cursor.fetchone()[0]

    # Get the latest timestamp from `bluesky_posts`
    cursor.execute("SELECT MAX(date) FROM bluesky_posts")
    latest_sentiment_time = cursor.fetchone()[0]

    # Get the latest timestamp from `merged_data`
    cursor.execute("SELECT MAX(timestamp) FROM merged_data")
    latest_merge_time = cursor.fetchone()[0]

    conn.close()

    return latest_stock_time, latest_sentiment_time, latest_merge_time

def compute_technical_indicators(since, until):
    """Ensure stock_features table exists, then compute technical indicators for new stock data and update the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Ensure the `stock_features` table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_features (
            symbol TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAl,
            close REAL,
            volume REAL,
            SMA_20 REAL,
            SMA_50 REAL,
            SMA_100 REAL,
            Volatility REAL,
            Bollinger_Upper REAL,
            Bollinger_Lower REAL,
            Momentum_5 REAL
        );
    """)

    #  Compute new technical indicators and insert into `stock_features`
    query = f"""
        INSERT INTO stock_features (symbol, open, timestamp, high, low, close, volume, SMA_20, SMA_50, SMA_100, Volatility, Bollinger_Upper, Bollinger_Lower, Momentum_5)
        WITH stock_window AS (
            SELECT
                symbol,
                open,
                timestamp,
                high,
                low,
                close,
                volume,

                -- Simple Moving Averages (SMA)
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

                -- Volatility (Rolling Standard Deviation)
                sqrt(
                    AVG(close * close) OVER (
                        PARTITION BY symbol 
                        ORDER BY timestamp 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) - 
                    POWER(
                        AVG(close) OVER (
                            PARTITION BY symbol 
                            ORDER BY timestamp 
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ), 2
                    )
                ) AS Volatility,

                -- Bollinger Bands
                (AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY timestamp
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                )) + (2 * sqrt(
                    AVG(close * close) OVER (
                        PARTITION BY symbol 
                        ORDER BY timestamp 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) - 
                    POWER(
                        AVG(close) OVER (
                            PARTITION BY symbol 
                            ORDER BY timestamp 
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ), 2
                    )
                )) AS Bollinger_Upper,

                (AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY timestamp
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                )) - (2 * sqrt(
                    AVG(close * close) OVER (
                        PARTITION BY symbol 
                        ORDER BY timestamp 
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) - 
                    POWER(
                        AVG(close) OVER (
                            PARTITION BY symbol 
                            ORDER BY timestamp 
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ), 2
                    )
                )) AS Bollinger_Lower,

                -- Momentum (5-period change)
                (close - LAG(close, 5) OVER (
                    PARTITION BY symbol ORDER BY timestamp
                )) AS Momentum_5

            FROM stock_prices
            WHERE timestamp BETWEEN '{since}' AND '{until}'
        )
        SELECT * FROM stock_window;
    """
    
    # Delete previous entries to avoid duplicates
    cursor.execute("DELETE FROM stock_features WHERE timestamp BETWEEN ? AND ?", (since, until))

    # Execute and commit
    cursor.execute(query)
    conn.commit()
    conn.close()

    print(f"Updated technical indicators from {since} to {until}.")


def merge_sentiment_data(start_date, end_date):
    """Merge stock and sentiment data while keeping previous merged data."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    merge_query = f"""
        INSERT INTO merged_data (
            timestamp, symbol, open, high, low, close, volume, 
            SMA_20, SMA_50, SMA_100, Volatility, 
            Bollinger_Upper, Bollinger_Lower, Momentum_5, 
            sentiment_score, likes, weighted_sentiment
        )
        SELECT 
            s.timestamp, s.symbol, s.open, s.high, s.low, s.close, s.volume, 
            s.SMA_20, s.SMA_50, s.SMA_100, s.Volatility, 
            s.Bollinger_Upper, s.Bollinger_Lower, s.Momentum_5,
            COALESCE(AVG(b.sentiment_score), 0) AS sentiment_score,  -- Ensure sentiment score is computed
            COALESCE(SUM(b.likes), 0) AS likes,  -- Ensure likes are aggregated properly
            COALESCE(SUM(b.sentiment_score * b.likes) / NULLIF(SUM(b.likes), 0), 0) AS weighted_sentiment
        FROM stock_features s
        LEFT JOIN (
            SELECT keyword, 
                   date,
                   sentiment_score, 
                   likes
            FROM bluesky_posts
        ) b
        ON b.date BETWEEN datetime(s.timestamp, '-2 hours') 
                          AND datetime(s.timestamp, '+2 hours')
        AND s.symbol = b.keyword
        WHERE s.timestamp BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY s.timestamp, s.symbol;
    """

    cursor.execute("DELETE FROM merged_data WHERE timestamp BETWEEN ? AND ?", (start_date, end_date))  # Avoid duplicates
    cursor.execute(merge_query)
    conn.commit()
    conn.close()

    print(f"Merged new stock & sentiment data from {start_date} to {end_date}.")


def update_pipeline():
    """Continuously check for new stock & sentiment data, then update merged_data."""
    while True:
        print("Checking for new stock & sentiment data...")

        latest_stock_time, latest_sentiment_time, latest_merge_time = get_latest_timestamps()

        if latest_merge_time is None or latest_merge_time == "2000-01-01 00:00:00":
            print("No previous merged data found. Processing entire stock data...")
            latest_merge_time = "2025-02-17"  # Start from earliest known stock data

        # Define new range for processing
        new_since = latest_merge_time
        new_until = latest_stock_time  # Merge up to latest stock timestamp

        if new_since and new_until and new_since < new_until:
            print(f"Processing new data from {new_since} to {new_until}...")

            compute_technical_indicators(new_since, new_until)
            merge_sentiment_data(new_since, new_until)

        else:
            print("No new stock data found. Sleeping for 15 minutes...")

        # Sleep before checking again
        print("Sleeping for 15 minutes.")
        time.sleep(15 * 60)  # 15 minutes


if __name__ == "__main__":
    update_pipeline()
