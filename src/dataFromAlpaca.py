import os
import time
import sqlite3
import pandas as pd
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import pandas_market_calendars as mcal
import pytz

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
all_symbols = symbol_list


def get_market_close_time():
    """Returns the market close time in UTC if the market is open today."""
    nyse = mcal.get_calendar('NYSE')  # Define market calendar
    eastern = pytz.timezone('US/Eastern')
    
    today = datetime.now(eastern).date()  # Get today's date in Eastern Time
    schedule = nyse.schedule(start_date=today, end_date=today)  # Fetch market schedule

    if not schedule.empty:
        market_close_et = schedule.at[today.strftime('%Y-%m-%d'), 'market_close']
        market_close_utc = market_close_et.tz_convert(pytz.utc)  # Convert to UTC
        return market_close_utc
    else:
        return None  # Market is closed today (weekend/holiday)

def get_market_open_time():
    """Returns the market open time in UTC if the market is open today."""
    nyse = mcal.get_calendar('NYSE')  # Define market calendar
    eastern = pytz.timezone('US/Eastern')
    
    today = datetime.now(eastern).date()  # Get today's date in Eastern Time
    schedule = nyse.schedule(start_date=today, end_date=today)  # Fetch market schedule

    if not schedule.empty:
        market_open_et = schedule.at[today.strftime('%Y-%m-%d'), 'market_open']
        market_open_utc = market_open_et.tz_convert(pytz.utc)  # Convert to UTC
        return market_open_utc
    else:
        return None  # Market is closed today (weekend/holiday)

# Define parameters
timeframe = '15Min'
db_file = "data/trade_data.db"
fetch_interval = 900  # Fetch new data every 15 minutes
historical_chunk_days = 5  # Fetch historical data in 5-day chunks

# Start date for fetching historical data
custom_start_date = "2023-03-01T00:00:00Z"

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
            trade_count INTEGER DEFAULT NULL,
            PRIMARY KEY (timestamp, symbol)
        )
    """)
    conn.commit()

# Check last timestamp for a stock
def get_last_timestamp(conn, symbol):
    """Fetch the latest available timestamp for a stock from the database."""
    query = "SELECT MAX(timestamp) FROM stock_prices WHERE symbol = ?"
    cursor = conn.execute(query, (symbol,))
    last_timestamp = cursor.fetchone()[0]

    if last_timestamp:
        # Convert stored timestamp to UTC-aware datetime
        last_timestamp = datetime.fromisoformat(last_timestamp).replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return last_timestamp  # Returns None if no data exists

# Save data to SQLite using "INSERT OR REPLACE" to prevent duplicates
def save_to_db(conn, df):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO stock_prices (timestamp, symbol, open, high, low, close, volume, trade_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (row["timestamp"].to_pydatetime().replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), 
              row["symbol"], 
              row["open"], 
              row["high"], 
              row["low"], 
              row["close"], 
              row["volume"], 
              row.get("trade_count", None)))
    conn.commit()

# Fetch historical stock data until the present
def fetch_historical_data(symbols, timeframe, db_file, start_date):
    """Fetch historical data in chunks and update SQLite database until up to date."""
    conn = create_connection(db_file)
    create_table(conn)

    now = datetime.now(timezone.utc)  # Ensure UTC-aware datetime
    cutoff_time = now - timedelta(minutes=15)  # 15-minute delay cutoff

    # Get today's market close time in UTC
    market_close_time = get_market_close_time()

    for symbol in symbols:
        last_timestamp = get_last_timestamp(conn, symbol)

        # Ensure last_timestamp_dt is always UTC-aware
        if last_timestamp is None:
            last_timestamp_dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            last_timestamp_dt = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)

        while last_timestamp_dt < cutoff_time:  # Stop when within 15 minutes of now
            # If today is a trading day, make sure we don't fetch past market close
            if market_close_time and last_timestamp_dt >= market_close_time:
                print(f"Market has closed for today. Stopping historical fetch for {symbol}.")
                break  # Stop fetching if market is closed

            # Ensure we only fetch data within market hours
            if last_timestamp_dt.weekday() >= 5:  # Skip weekends
                print(f"Skipping {symbol} on {last_timestamp_dt.date()} (Weekend).")
                break

            until_time = (last_timestamp_dt + timedelta(days=historical_chunk_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

            print(f"Fetching historical data for {symbol} from {last_timestamp_dt} to {until_time}...")

            try:
                bars = api.get_bars(symbol, timeframe, start=last_timestamp_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), end=until_time, feed='iex').df
                bars["symbol"] = symbol
                bars.reset_index(inplace=True)

                if bars.empty:
                    print(f"No new historical data for {symbol}, skipping to next symbol...")
                    break  # Exit the loop for this symbol and move to the next one

                # Ensure timestamp is parsed as datetime
                bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)

                # Save data to database
                save_to_db(conn, bars)
                print(f"Saved {symbol} data to {db_file}")

                # Update last_timestamp_dt safely and add 1 second to move forward
                last_timestamp_dt = bars["timestamp"].max() + timedelta(seconds=1)
                print(f"Updated last_timestamp_dt to {last_timestamp_dt}")

                # If last_timestamp_dt is past the cutoff time, stop fetching
                if last_timestamp_dt >= cutoff_time:
                    print(f"Reached 15-minute delay limit. Stopping historical fetch for {symbol}.")
                    break

            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
                break  # Stop fetching if API error occurs

    conn.close()
    print("Historical data fully fetched.")

# Fetch new streaming data every 5 minutes
def fetch_realtime_data(symbols, timeframe, db_file):
    """Continuously fetch new data every 5 minutes."""
    while True:
        conn = create_connection(db_file)
        now = datetime.now(timezone.utc) - timedelta(minutes=16)  # Ensure 15-minute delay

        for symbol in symbols:
            last_timestamp = get_last_timestamp(conn, symbol)

            # If historical data exists, resume from last timestamp
            if last_timestamp:
                start_time = last_timestamp
            else:
                start_time = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

            print(f"Fetching delayed IEX data for {symbol} from {start_time}...")

            try:
                # Omit 'end' to always get the latest allowed data
                bars = api.get_bars(symbol, timeframe, start=start_time, feed="iex").df
                bars["symbol"] = symbol
                bars.reset_index(inplace=True)

                if bars.empty:
                    print(f"No new data for {symbol}, skipping...")
                    continue
                else:
                    save_to_db(conn, bars)
                    print(f"Saved {symbol} delayed IEX data to {db_file}")

            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")

        conn.close()
        print(f"Waiting {fetch_interval / 60} minutes before next fetch...")
        time.sleep(fetch_interval)


def wake_up_to_market():
    while True:
        market_close_time = get_market_close_time()
        market_open_time = get_market_open_time()
        now = datetime.now(timezone.utc)

        if market_open_time and market_close_time:
            if market_open_time <= now < market_close_time:
                return  # Market is open, exit the loop
        time.sleep(60 * 60) 
        print(f"Checked market status again at: {now}Current time (UTC)")


# Run the full process
def main():
    """Fetch historical data first, then loop to fetch real-time data during market hours."""
    
    # Fetch historical data once
    fetch_historical_data(all_symbols, timeframe, db_file, custom_start_date)
    
    while True:
        market_open_time = get_market_open_time()
        market_close_time = get_market_close_time()
        now = datetime.now(timezone.utc)

        if market_open_time and market_close_time:
            if market_open_time <= now < market_close_time:
                print(f"Market is open. Current time (UTC): {now}")

                # Fetch real-time data, which handles sleeping 5 min
                fetch_realtime_data(all_symbols, timeframe, db_file)

            else:
                print(f"Market is closed. Waiting for market to reopen... Current time (UTC): {now}")

                # Wait until the next market open time
                wake_up_to_market()



if __name__ == "__main__":
    main()
