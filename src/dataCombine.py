import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import ta
from queryFromPost import show_table

DB_FILE = "data/trade_data.db"

# ---------- Step 1: Compute Technical Indicators ----------
def compute_technical_indicators(since, until):
    """Load stock data, compute technical indicators, and save them back to SQLite."""
    conn = sqlite3.connect(DB_FILE)

    # Load stock prices
    query = "SELECT * FROM stock_prices WHERE timestamp BETWEEN ? AND ?"
    df = pd.read_sql(query, conn, params=(since, until), parse_dates=["timestamp"])
    print(df.head())

    # Sort data
    df = df.sort_values(["symbol", "timestamp"])

    # Compute technical indicators
    df["SMA_20"] = df.groupby("symbol")["close"].transform(lambda x: x.rolling(window=20).mean())  # 20-day SMA
    df["EMA_20"] = df.groupby("symbol")["close"].transform(lambda x: x.ewm(span=20, adjust=False).mean())  # 20-day EMA
    df["Volatility"] = df.groupby("symbol")["close"].transform(lambda x: x.rolling(window=20).std())  # Rolling Std Dev
    df["Bollinger_Upper"] = df["SMA_20"] + 2 * df["Volatility"]
    df["Bollinger_Lower"] = df["SMA_20"] - 2 * df["Volatility"]

    # RSI (Relative Strength Index)
    df["RSI"] = df.groupby("symbol")["close"].transform(lambda x: ta.momentum.RSIIndicator(x, window=14).rsi())

    # MACD
    macd = ta.trend.MACD(df["close"])
    df["MACD"] = macd.macd()
    df["MACD_Signal"] = macd.macd_signal()

    # Save to new table
    df.to_sql("stock_features", conn, if_exists="replace", index=False)
    df.to_csv("data/stock_features.csv", index=False)
    conn.close()

    print("Technical indicators computed and saved to `stock_features`.")

# ---------- Step 2: Merge with sentiment data ----------
def merge_sentiment_data(start_date="2025-02-24", end_date="2025-02-28"):
    """Merge stock and sentiment data directly in SQLite based on nearest timestamp within ±2.5 minutes."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create or replace merged_data table
    merge_query = f"""
        CREATE TABLE IF NOT EXISTS merged_data AS
        SELECT 
            s.*, 
            COALESCE(
                SUM(b.sentiment_score * b.likes) / NULLIF(SUM(b.likes), 0), 
                0
            ) AS weighted_sentiment
        FROM stock_features s
        LEFT JOIN bluesky_posts b
            ON b.date BETWEEN datetime(s.timestamp, '-5 minutes') 
                AND datetime(s.timestamp, '+5 minutes')
            AND s.symbol = b.keyword

        GROUP BY s.timestamp, s.symbol;
    """

    cursor.execute("DROP TABLE IF EXISTS merged_data;")  # Clear previous data
    cursor.execute(merge_query)
    conn.commit()
    conn.close()
    print("Merged dataset saved to `merged_data` in SQLite for ML modeling.")



since = "2025-02-24"
until = "2025-02-28"
print("Computing technical indicators between {since} and {until}")
compute_technical_indicators(since, until)
merge_sentiment_data(since, until)
show_table('merged_data')


