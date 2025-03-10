import sqlite3
import config
from datetime import datetime

### =========================
###   DATABASE FUNCTIONS
### =========================

def get_latest_timestamps():
    """Retrieve the latest timestamps from stock_prices, bluesky_posts, and merged_data."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(timestamp) FROM stock_prices")
    latest_stock_time = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(date) FROM bluesky_posts")
    latest_sentiment_time = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(timestamp) FROM merged_data")
    latest_merge_time = cursor.fetchone()[0]

    conn.close()
    return latest_stock_time, latest_sentiment_time, latest_merge_time

### =========================
###   DATABASE INDEXING & PREPROCESSING
### =========================

def optimize_database():
    """Create necessary indexes and preprocess bluesky_posts for faster joins."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    # Create indexes if they don’t exist
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_timestamp ON stock_features(timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bluesky_date ON bluesky_posts(date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bluesky_keyword ON bluesky_posts(keyword);")

    # Add min_time and max_time columns for fast time range filtering
    cursor.execute("PRAGMA table_info(bluesky_posts);")
    existing_columns = [row[1] for row in cursor.fetchall()]
    
    if "min_time" not in existing_columns:
        cursor.execute("ALTER TABLE bluesky_posts ADD COLUMN min_time DATETIME;")
    if "max_time" not in existing_columns:
        cursor.execute("ALTER TABLE bluesky_posts ADD COLUMN max_time DATETIME;")

    # Update min_time and max_time for all records
    cursor.execute("UPDATE bluesky_posts SET min_time = DATETIME(date, '-12 hours'), max_time = DATETIME(date, '+12 hours');")

    conn.commit()
    conn.close()
    print("Database optimized with indexes and precomputed time ranges.")

### =========================
###   FEATURE ENGINEERING
### =========================
def compute_technical_indicators(start_time, end_time):
    """Compute technical indicators and update the stock_features table only for new data."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_features (
            symbol TEXT,
            timestamp TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
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

    query = f"""
        INSERT OR REPLACE INTO stock_features (
            symbol, timestamp, open, high, low, close, volume,
            SMA_20, SMA_50, SMA_100, Volatility, Bollinger_Upper, Bollinger_Lower, Momentum_5
        )
        WITH stock_window AS (
            SELECT
                s.symbol, s.timestamp, s.open, s.high, s.low, s.close, s.volume,

                -- Simple Moving Averages (SMA)
                AVG(s.close) OVER (
                    PARTITION BY s.symbol ORDER BY s.timestamp
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS SMA_20,

                AVG(s.close) OVER (
                    PARTITION BY s.symbol ORDER BY s.timestamp
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS SMA_50,

                AVG(s.close) OVER (
                    PARTITION BY s.symbol ORDER BY s.timestamp
                    ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
                ) AS SMA_100,

                -- Volatility (Rolling Standard Deviation)
                sqrt(
                    AVG(s.close * s.close) OVER (
                        PARTITION BY s.symbol ORDER BY s.timestamp
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) -
                    POWER(
                        AVG(s.close) OVER (
                            PARTITION BY s.symbol ORDER BY s.timestamp
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ), 2
                    )
                ) AS Volatility,

                -- Bollinger Bands
                (AVG(s.close) OVER (
                    PARTITION BY s.symbol ORDER BY s.timestamp
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                )) + (2 * sqrt(
                    AVG(s.close * s.close) OVER (
                        PARTITION BY s.symbol ORDER BY s.timestamp
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) -
                    POWER(
                        AVG(s.close) OVER (
                            PARTITION BY s.symbol ORDER BY s.timestamp
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ), 2
                    )
                )) AS Bollinger_Upper,

                (AVG(s.close) OVER (
                    PARTITION BY s.symbol ORDER BY s.timestamp
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                )) - (2 * sqrt(
                    AVG(s.close * s.close) OVER (
                        PARTITION BY s.symbol ORDER BY s.timestamp
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) -
                    POWER(
                        AVG(s.close) OVER (
                            PARTITION BY s.symbol ORDER BY s.timestamp
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ), 2
                    )
                )) AS Bollinger_Lower,

                -- Momentum (5-period change)
                (s.close - (SELECT close from stock_prices s2 WHERE s2.symbol = s.symbol AND s2.timestamp < s.timestamp AND s2.timestamp > DATETIME(s.timestamp, '-5 minutes') ORDER BY s2.timestamp DESC LIMIT 1)) AS Momentum_5

            FROM stock_prices s
            WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'
        )
        SELECT * FROM stock_window;
    """

    #cursor.execute("DELETE FROM stock_features WHERE timestamp BETWEEN ? AND ?", (start_time, end_time)) # Not needed
    cursor.execute(query)
    conn.commit()
    conn.close()

    print(f"Updated technical indicators from {start_time} to {end_time}.")


### =========================
###   MERGING STOCK & SENTIMENT DATA
### =========================

def merge_sentiment_data(start_time, end_time):
    """Merge only new stock data that hasn't been merged yet."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    print(f"Checking data to merge from {start_time} to {end_time}...")

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
            COALESCE(AVG(b.sentiment_score), 0) AS sentiment_score,
            COALESCE(SUM(b.likes), 0) AS likes,
            COALESCE(SUM(b.sentiment_score * b.likes) / NULLIF(SUM(b.likes), 0), 0) AS weighted_sentiment
        FROM stock_features s
        LEFT JOIN bluesky_posts b
        ON s.symbol = b.keyword
        AND b.min_time <= s.timestamp 
        AND b.max_time >= s.timestamp
        WHERE s.timestamp > (SELECT COALESCE(MAX(timestamp), '2000-01-01') FROM merged_data)
        GROUP BY s.timestamp, s.symbol;
    """

    cursor.execute(merge_query)
    conn.commit()
    conn.close()

    print(f"Merged stock & sentiment data from {start_time} to {end_time}.")

### =========================
###   MAIN EXECUTION
### =========================

if __name__ == "__main__":
    optimize_database()
    latest_stock_time, latest_sentiment_time, latest_merge_time = get_latest_timestamps()

    if latest_stock_time and latest_sentiment_time:
        start_time = latest_merge_time or config.MERGE_START_DATE
        if start_time < latest_stock_time:
            compute_technical_indicators(start_time, latest_stock_time)
            merge_sentiment_data(start_time, latest_stock_time)
