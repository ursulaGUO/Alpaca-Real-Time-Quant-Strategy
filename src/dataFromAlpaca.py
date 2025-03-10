import sqlite3
import pandas as pd
import alpaca_trade_api as tradeapi
from datetime import datetime, timezone, timedelta
import pandas_market_calendars as mcal
import pytz
import json
import websockets
import asyncio
import config  # Import central config file

# Initialize Alpaca API
api = tradeapi.REST(config.ALPACA_API_KEY, config.ALPACA_API_SECRET, config.ALPACA_BASE_URL, api_version="v2")

### =========================
###   MARKET TIME HELPERS
### =========================

def get_market_close_time():
    """Returns the market close time in UTC if the market is open today."""
    nyse = mcal.get_calendar("NYSE")
    eastern = pytz.timezone("US/Eastern")
    
    today = datetime.now(eastern).date()
    schedule = nyse.schedule(start_date=today, end_date=today)

    if not schedule.empty:
        market_close_et = schedule.at[today.strftime("%Y-%m-%d"), "market_close"]
        return market_close_et.tz_convert(pytz.utc)
    
    return None  # Market is closed today

def get_market_open_time():
    """Returns the market open time in UTC if the market is open today."""
    nyse = mcal.get_calendar("NYSE")
    eastern = pytz.timezone("US/Eastern")

    today = datetime.now(eastern).date()
    schedule = nyse.schedule(start_date=today, end_date=today)

    if not schedule.empty:
        market_open_et = schedule.at[today.strftime("%Y-%m-%d"), "market_open"]
        return market_open_et.tz_convert(pytz.utc)
    
    return None  # Market is closed today

### =========================
###   DATABASE FUNCTIONS
### =========================

def create_connection():
    """Create a database connection."""
    return sqlite3.connect(config.DB_FILE)

def create_table(conn):
    """Create the stock_prices table if it does not exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            timestamp TEXT PRIMARY KEY,
            symbol TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER
        )
    """)
    conn.commit()

def get_last_timestamp(conn, symbol):
    """Fetch the latest available timestamp for a stock from the database."""
    query = "SELECT MAX(timestamp) FROM stock_prices WHERE symbol = ?"
    cursor = conn.execute(query, (symbol,))
    last_timestamp = cursor.fetchone()[0]
    
    return last_timestamp if last_timestamp else None  # None if no data exists

def save_to_db(conn, df):
    """Save fetched stock data into SQLite database."""
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO stock_prices (timestamp, symbol, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (row["timestamp"].to_pydatetime().replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), 
              row["symbol"], 
              row["open"], 
              row["high"], 
              row["low"], 
              row["close"], 
              row["volume"]))
    conn.commit()

### =========================
###   FETCH DATA FROM ALPACA
### =========================

def fetch_historical_data():
    """Fetch historical data in chunks and update SQLite database."""
    conn = create_connection()
    create_table(conn)

    now = datetime.now(timezone.utc) - timedelta(minutes=15)  # Ensure 15-minute delay
    market_close_time = get_market_close_time()

    for symbol in config.ALL_SYMBOLS:
        last_timestamp = get_last_timestamp(conn, symbol)

        if last_timestamp:
            last_timestamp_dt = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)
        else:
            last_timestamp_dt = datetime.strptime(config.CUSTOM_START_DATE, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        while last_timestamp_dt < now:
            if market_close_time and last_timestamp_dt >= market_close_time:
                print(f"[{symbol}] Market closed. Stopping historical fetch.")
                break

            if last_timestamp_dt.weekday() >= 5:  # Skip weekends
                print(f"[{symbol}] Weekend detected. Skipping fetch.")
                break

            until_time = (last_timestamp_dt + timedelta(days=config.HISTORICAL_CHUNK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

            print(f"Fetching historical data for {symbol} from {last_timestamp_dt} to {until_time}...")

            try:
                bars = api.get_bars(symbol, config.TIMEFRAME, start=last_timestamp_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), end=until_time, feed="iex").df
                bars["symbol"] = symbol
                bars.reset_index(inplace=True)

                if bars.empty:
                    print(f"[{symbol}] No new historical data. Skipping.")
                    break

                bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
                save_to_db(conn, bars)
                last_timestamp_dt = bars["timestamp"].max() + timedelta(seconds=1)

            except Exception as e:
                print(f"[{symbol}] Error fetching data: {e}")
                break

    conn.close()
    print("Historical data fetch complete.")


ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v2/iex"  # Using IEX instead of SIP

# List of stock symbols to track
SYMBOLS = list(config.STOCK_DICT.keys())

# SQLite database file
DB_FILE = config.DB_FILE

async def save_stock_data(symbol, timestamp, open_price, high, low, close, volume):
    """Save stock data to SQLite database."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()

        # Ensure stock_data table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                symbol TEXT,
                timestamp TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, timestamp)
            )
        """)

        # Insert or update stock price data
        cursor.execute("""
            INSERT OR REPLACE INTO stock_data (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (symbol, timestamp, open_price, high, low, close, volume))

        conn.commit()
        print(f"[Alpaca-IEX] Saved {symbol} data at {timestamp}.")

async def alpaca_ws_handler():
    """Connects to Alpaca WebSocket API and listens for IEX stock data."""
    async with websockets.connect(ALPACA_WS_URL) as ws:
        # Authenticate
        auth_msg = json.dumps({
            "action": "auth",
            "key": config.ALPACA_API_KEY,
            "secret": config.ALPACA_API_SECRET
        })
        await ws.send(auth_msg)
        auth_response = await ws.recv()
        print(f"[Alpaca-IEX] Auth Response: {auth_response}")

        # Subscribe to IEX market data
        subscribe_msg = json.dumps({
            "action": "subscribe",
            "bars": SYMBOLS  # Subscribe to real-time bar updates
        })
        await ws.send(subscribe_msg)
        print(f"[Alpaca-IEX] Subscribed to: {SYMBOLS}")

        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)

                for stock in data:
                    if stock.get("T") == "bar":  # Only process bar data
                        symbol = stock["S"]
                        timestamp = datetime.utcfromtimestamp(stock["t"] / 1000).isoformat()
                        open_price = stock["o"]
                        high = stock["h"]
                        low = stock["l"]
                        close = stock["c"]
                        volume = stock["v"]

                        await save_stock_data(symbol, timestamp, open_price, high, low, close, volume)

            except Exception as e:
                print(f"[Alpaca-IEX] Error: {e}")
                await asyncio.sleep(5)  # Small delay before retrying

async def fetch_realtime_data():
    """Runs Alpaca WebSocket handler asynchronously."""
    await alpaca_ws_handler()

### =========================
###   MAIN EXECUTION
### =========================

if __name__ == "__main__":
    print("Running Alpaca Data Fetching...")
    
    fetch_historical_data()  # Step 1: Fetch historical data
    fetch_realtime_data()    # Step 2: Fetch latest real-time data
    
    print("Data fetching complete.")
