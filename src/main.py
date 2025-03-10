import time
import config
import asyncio
import sqlite3
import pandas as pd
from datetime import datetime, timezone
from dataFromAlpaca import fetch_realtime_data, fetch_historical_data
from dataFromBlueSky import download_bluesky_posts
from dataCombine import merge_sentiment_data, compute_technical_indicators
from tradeLogic import trading_loop  

DB_FILE = config.DB_FILE  # Use centralized configuration

# Dictionary to track last recorded time for each stock
last_recorded_time = {}

def get_latest_features(symbol):
    """Fetch the latest feature row from the database for the given stock."""
    with sqlite3.connect(DB_FILE) as conn:
        query = """
            SELECT * FROM merged_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """
        df = pd.read_sql(query, conn, params=(symbol,))

        if df.empty:
            return None  # No data available

        # Explicitly drop `trade_count`
        if "trade_count" in df.columns:
            df.drop(columns=["trade_count"], inplace=True)

        # Update last recorded time for the stock
        last_recorded_time[symbol] = df.iloc[0]["timestamp"]

        return df.iloc[0].values[1:]  # Exclude `symbol` column from features


def run_data_collection():
    """Step 1: Stream real-time stock market data from Alpaca and periodically fetch sentiment data from BlueSky."""
    
    print("\n[Step 1] Streaming real-time stock market data from Alpaca...")
    try:
        asyncio.run(fetch_realtime_data())  # Run Alpaca WebSocket stream
    except Exception as e:
        print(f"[ERROR] Real-time stock data streaming failed: {e}")
        raise

    # Fetch BlueSky sentiment posts every 10 minutes
    if datetime.now().minute % 10 == 0:
        print("\n[Step 2] Fetching sentiment data from BlueSky...")
        try:
            download_bluesky_posts()
            print("[Step 2] Sentiment data collection completed successfully.")
        except Exception as e:
            print(f"[ERROR] Sentiment data collection failed: {e}")
            raise

def run_data_processing():
    """Step 3: Merge stock data with sentiment data and compute technical indicators."""
    print("\n[Step 3] Merging stock & sentiment data and computing indicators...")

    start_date = config.MERGE_START_DATE
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        print(f"Computing technical indicators from {start_date} to {end_date}...")
        compute_technical_indicators(start_date, end_date)
        print("[Step 3] Technical indicators computed successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to compute technical indicators: {e}")
        raise

    try:
        print(f"Merging sentiment data from {start_date} to {end_date}...")
        merge_sentiment_data(start_date, end_date)
        print("[Step 3] Merging stock and sentiment data completed successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to merge sentiment data: {e}")
        raise

if __name__ == "__main__":
    print("\n==============================")
    print("   Starting Real-Time Trading Pipeline   ")
    print("==============================\n")

    symbols_to_trade = config.ALL_SYMBOLS
    #fetch_historical_data()

    while True:
        try:
            # Step 1: Stream Alpaca & fetch BlueSky posts periodically
            run_data_collection()
            # Step 2: Merge & process data 
            run_data_processing()
            # Step 3: Prepare feature data & execute trades
            features_dict = {}
            for symbol in symbols_to_trade:
                latest_features = get_latest_features(symbol)
                if latest_features is not None:
                    features_dict[symbol] = latest_features

            trading_loop(features_dict, interval=60)
            

            

            print("\nPipeline iteration completed! Sleeping for 1 minute before next data fetch...\n")
            time.sleep(60)  # Sleep for 1 minute before fetching new data

        except Exception as e:
            print(f"\nERROR: {e}")
            print("Retrying after 30 seconds...\n")
            time.sleep(30)  # Sleep for 30 seconds before retrying
