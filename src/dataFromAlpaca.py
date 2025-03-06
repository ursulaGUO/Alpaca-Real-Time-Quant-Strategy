import os
import time
import sqlite3
import pandas as pd
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load secrets
dotenv_path = os.path.expanduser("~/.secrets/.env")
load_dotenv(dotenv_path)
api_key = os.getenv("alpaca_api_key")
api_secret = os.getenv("alpaca_api_secret")
base_url = os.getenv("alpaca_base_url")

print("Successfully loaded Alpaca secrets.")

# Initialize Alpaca API
api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')

# Define stock symbols
symbol_list = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA']
more_list = ['F', 'LCID', 'PLTR', 'INTC', 'SMCI', 'NU', 'BBD', 'LYG', 
             'BTG', 'PSLV', 'MARA', 'AAL', 'IQ', 'BAC', 'SOFI', 'ABEV', 
             'RGTI', 'BABA', 'WBD', 'RIG', 'T', 'MRNA']
all_symbols = symbol_list + more_list

# Define parameters
timeframe = '5Min'
start_date = '2023-03-01'
end_date = "2025-03-01"
db_file = "data/trade_data.db"
rate_limit = 2  # Alpaca API rate limit

# Create SQLite connection
def create_connection(db_file):
    return sqlite3.connect(db_file)

# Create table in SQLite
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            timestamp TEXT,
            symbol TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            trade_count INTEGER DEFAULT NULL,  -- Ensure trade_count can be NULL
            PRIMARY KEY (timestamp, symbol)
        )
    """)
    conn.commit()

# Check last timestamp for a stock
def get_last_timestamp(conn, symbol):
    query = "SELECT MAX(timestamp) FROM stock_prices WHERE symbol = ?"
    cursor = conn.execute(query, (symbol,))
    last_timestamp = cursor.fetchone()[0]

    if last_timestamp:
        # Convert to ISO format for Alpaca API
        last_timestamp = datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).isoformat()
    
    return last_timestamp  # Returns None if no data exists

# Save data to SQLite using "INSERT OR REPLACE" to prevent duplicates
def save_to_db(conn, df):
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO stock_prices (timestamp, symbol, open, high, low, close, volume, trade_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (row["timestamp"].to_pydatetime().strftime("%Y-%m-%d %H:%M:%S"), 
              row["symbol"], 
              row["open"], 
              row["high"], 
              row["low"], 
              row["close"], 
              row["volume"], 
              row.get("trade_count", None)))

    conn.commit()

# Fetch historical stock data and store it in SQLite
def fetch_historical_data(symbols, timeframe, start, end, db_file):
    conn = create_connection(db_file)
    create_table(conn)
    request_count = 0

    for symbol in symbols:
        last_timestamp = get_last_timestamp(conn, symbol)
        start_time = last_timestamp if last_timestamp else start  # Resume from last fetched data

        print(f"Fetching 5Min historical data for {symbol} from {start_time} to {end}...")

        try:
            # Fetch stock price data
            bars = api.get_bars(symbol, timeframe, start=start_time, end=end).df
            bars["symbol"] = symbol  
            bars.reset_index(inplace=True)

            if bars.empty:
                print(f"No new data for {symbol}, skipping...")
                continue

            # Drop trade_count if not present in response
            if "trade_count" not in bars.columns:
                bars["trade_count"] = None

            # Save new data to SQLite
            save_to_db(conn, bars)
            print(f"Saved {symbol} data to {db_file}")

            # Handle API rate limit
            request_count += 1
            if request_count >= rate_limit:
                print("Rate limit reached, sleeping for 60 seconds...")
                time.sleep(60)
                request_count = 0  # Reset request count

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    conn.close()
    print("Data stored successfully!")

# Fetch and store data in SQLite
fetch_historical_data(symbol_list, timeframe, start_date, end_date, db_file)
