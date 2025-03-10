import time
import config
import asyncio
from datetime import datetime, timezone
from dataFromAlpaca import fetch_realtime_data
from dataFromBlueSky import download_bluesky_posts
from dataCombine import merge_sentiment_data, compute_technical_indicators

DB_FILE = config.DB_FILE  # Use centralized configuration

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
    print("   Starting Real-Time Data Pipeline   ")
    print("==============================\n")

    while True:
        try:
            # Step 1: Stream Alpaca & fetch BlueSky posts periodically
            run_data_collection() 
             # Step 2: Merge & process data 
            run_data_processing() 

            print("\nPipeline iteration completed! Sleeping for 1 minute before next data fetch...\n")
            time.sleep(60)  # Sleep for 1 minute before fetching new data

        except Exception as e:
            print(f"\nERROR: {e}")
            print("Retrying after 30 seconds...\n")
            time.sleep(30)  # Sleep for 30 seconds before retrying
