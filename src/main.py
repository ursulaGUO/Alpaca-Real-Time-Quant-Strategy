import time
import config
from datetime import datetime, timezone
from dataFromAlpaca import fetch_historical_data
from dataFromBlueSky import download_bluesky_posts
from dataCombine import merge_sentiment_data, compute_technical_indicators

DB_FILE = config.DB_FILE  # Use centralized configuration

def run_data_collection():
    """Step 1: Download stock data from Alpaca and sentiment data from BlueSky."""
    print("\n[Step 1] Fetching stock market data from Alpaca...")
    try:
        fetch_historical_data()
        print("[Step 1] Stock market data collection completed successfully.")
    except Exception as e:
        print(f"[ERROR] Stock data collection failed: {e}")
        raise

    print("\n[Step 2] Fetching sentiment data from BlueSky...")
    try:
        download_bluesky_posts()
        print("[Step 2] Sentiment data collection completed successfully.")
    except Exception as e:
        print(f"[ERROR] Sentiment data collection failed: {e}")
        raise

def run_data_processing():
    """Step 2: Merge stock data with sentiment data and compute indicators."""
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
    while True:
        try:
            print("\n==============================")
            print("   Starting Full Data Pipeline   ")
            print("==============================\n")

            run_data_collection()  # Step 1: Download stock & sentiment data
            run_data_processing()  # Step 2: Merge & process data

            print("\nPipeline completed successfully! Sleeping for 1 hour before the next run...\n")
            time.sleep(3600)  # Sleep for 1 hour before running again

        except Exception as e:
            print(f"\nERROR: {e}")
            print("Retrying after 1 minute...\n")
            time.sleep(60)  # Sleep for 1 minutes before retrying
