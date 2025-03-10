import time
import config
import asyncio
import sqlite3
import pandas as pd
from datetime import datetime, timezone
from dataFromAlpaca import fetch_historical_data, fetch_realtime_data, run_data_processing
from dataFromBlueSky import download_bluesky_posts
from dataCombine import merge_sentiment_data, compute_technical_indicators
from tradeLogic import trading_loop

DB_FILE = config.DB_FILE  # Use centralized configuration

# Dictionary to track last recorded time for each stock
last_recorded_time = {}
last_sentiment_fetch = 0

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

async def start_websocket():
    """Runs Alpaca WebSocket handler asynchronously and ensures automatic reconnection."""
    while True:
        try:
            print("[INFO] Starting WebSocket streaming...")
            await fetch_realtime_data()
        except Exception as e:
            print(f"[ERROR] WebSocket disconnected: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)  # Wait before reconnecting

async def periodic_data_processing():
    """Periodically runs data processing."""
    while True:
        print("\n[INFO] Running scheduled data processing...")
        try:
            # Fetch BlueSky data
            print("\n[Step 4] Fetching BlueSky sentiment data...")
            await download_bluesky_posts()

            run_data_processing()
        except Exception as e:
            print(f"[ERROR] Periodic data processing failed: {e}")
        await asyncio.sleep(900)  # Wait 15 minutes (900 seconds)

async def main():
    """Main async function to run historical fetch, WebSocket streaming, and periodic processing."""
    print("\n==============================")
    print("   Starting Real-Time Trading Pipeline   ")
    print("==============================\n")

    # Step 1: Fetch historical stock data before real-time streaming
    fetch_historical_data()

    # Step 2: Start real-time stock data streaming
    websocket_task = asyncio.create_task(start_websocket())

    # Step 3: Start periodic data processing as a separate task
    processing_task = asyncio.create_task(periodic_data_processing())

    symbols_to_trade = config.ALL_SYMBOLS
    previous_features = {}
    start_flag = 1

    while True:
        try:
            # Step 3: done in dataFromeAlpaca

            # Step 5: Prepare feature data & execute trades
            features_dict = {}

            for symbol in symbols_to_trade:
                latest_features = get_latest_features(symbol)

                print(f"[DEBUG] Latest features for {symbol}: {latest_features}")

                if latest_features is not None:
                    if symbol in previous_features and (previous_features[symbol] == latest_features).all():
                        print(f"No change in features for {symbol}")
                    else:
                        print(f"Features updated for {symbol}")
                        features_dict[symbol] = latest_features
                        previous_features[symbol] = latest_features  # Persist the update

                        # Refrain from trading in the frist iteration
                        if start_flag == 1:
                            start_flag = 0
                        else:
                            trading_loop(features_dict)

            print("\nPipeline iteration completed! Sleeping for 1 minute before next data fetch...\n")
            await asyncio.sleep(60)  # Async-friendly sleep

        except Exception as e:
            print(f"\nERROR: {e}")
            print("Retrying after 30 seconds...\n")
            await asyncio.sleep(30)  # Async-friendly retry

if __name__ == "__main__":
    asyncio.run(main())  # Ensures async execution