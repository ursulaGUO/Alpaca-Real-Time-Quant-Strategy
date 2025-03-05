import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
import os
import time
import pandas as pd

# Load secrets
dotenv_path = os.path.expanduser("~/.secrets/.env")
load_dotenv(dotenv_path)
api_key = os.getenv("alpaca_api_key")
api_secret = os.getenv("alpaca_api_secret")
base_url = os.getenv("alpaca_base_url")
end_point = os.getenv("alpaca_end_point")
print("Successfully loaded Alpaca secrets.")



# Initialize Alpaca API
api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')

symbol_list = ['AAPL', 
               'MSFT', 
               'GOOGL', 
               'AMZN', 
               'TSLA',
               'NVDA',]
more_list = ['F',
               'LCID',
               'PLTR',
               'INTC',
               'SMCI',
               'NU',
               'BBD',
               'LYG',
               'BTG',
               'PSLV',
               'MARA',
               'AAL',
               'IQ',
               'BAC',
               'SOFI',
               'ABEV',
               'RGTI',
               'BABA',
               'WBD',
               'RIG',
               'T',
               'MRNA',
               ]


# Get historical market data
timeframe = '5Min'
start_date = '2023-03-01' # Bluesky launched in February 2023
end_date = "2025-03-01"

def load_existing_historical(filename):
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        return pd.DataFrame()
    



def fetch_historical_data(symbols, timeframe, start, end, folder_destination):
    
    request_count = 0
    rate_limit = 2  # Alpaca API rate limit
    for symbol in symbols:
        filename = f"{folder_destination}/{symbol}_historical.csv"
        ticker_history = load_existing_historical(filename)
        # If symbol already has data, continue from the last recorded timestamp
        if not ticker_history.empty and symbol in ticker_history["symbol"].values:
            last_timestamp = ticker_history[ticker_history["symbol"] == symbol]["timestamp"].max()
            start = last_timestamp.strftime("%Y-%m-%dT%H:%M:%S")  # Resume from last recorded time

        print(f"Fetching 5Min historical data for {symbol} from {start} to {end}...")

        try:
            # Fetch data
            bars = api.get_bars(symbol, timeframe, start=start, end=end).df
            bars["symbol"] = symbol  # Add symbol column

            if bars.empty:
                print(f"No new data for {symbol}, skipping...")
                continue

            # Append new data to existing
            ticker_history = pd.concat([ticker_history, bars])

            # Save to CSV after each fetch
            ticker_history.to_csv(filename)
            print(f"Saved {symbol} data to {filename}")

            # Handle request count to prevent hitting the rate limit
            request_count += 1
            if request_count >= rate_limit:
                print("Rate limit reached, sleeping for 60 seconds...")
                time.sleep(60)
                request_count = 0  # Reset request count

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    return ticker_history

df = fetch_historical_data(symbol_list, timeframe, start_date, end_date,"./historical_data")
print("Finished fetching historical data.")
