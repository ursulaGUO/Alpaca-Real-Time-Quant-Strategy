import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataFromBlueSky import download_bluesky_posts
import ta
from queryFromPost import show_table

DB_FILE = "trade_data.db"

# ---------- Step 1: Compute Technical Indicators ----------
def compute_technical_indicators(since, until):
    """Load stock data, compute technical indicators, and save them back to SQLite."""
    conn = sqlite3.connect(DB_FILE)

    # Load stock prices
    query = f"""SELECT * 
            FROM stock_prices
            WHERE timestamp BETWEEN {since} AND {until}"""
    df = pd.read_sql(query, conn, parse_dates=["timestamp"])
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


since = "2025-02-24"
until = "2025-02-28"
print("Computing technical indicators between {since} and {until}")
compute_technical_indicators(since, until)
#show_table("stock_features")


