import sqlite3
import pandas as pd
import alpaca_trade_api as tradeapi
from datetime import datetime, timezone, timedelta
import pandas_market_calendars as mcal
from dataCombine import merge_sentiment_data, compute_technical_indicators
from queryFromPost import delete_post
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



# Initialize Alpaca API
api = tradeapi.REST(config.ALPACA_API_KEY, config.ALPACA_API_SECRET, config.ALPACA_BASE_URL, api_version="v2")

### =========================
###   DATABASE FUNCTIONS
### =========================

def create_connection():
    """Create a database connection."""
    return sqlite3.connect(config.DB_FILE)

def create_table():
    """Create the stock_prices table if it does not exist."""
    with sqlite3.connect(config.DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
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
        conn.commit()


# dataFromAlpaca.py
async def save_stock_data(symbol, timestamp, open_price, high, low, close, volume):
    """Save real-time stock data to SQLite database."""
    with sqlite3.connect(config.DB_FILE) as conn:
        cursor = conn.cursor()

        # Convert timestamp to UTC
        dt_object = datetime.fromisoformat(timestamp.replace('Z', '+00:00')) # Convert it to a datatime
        timestamp_utc = dt_object.strftime("%Y-%m-%d %H:%M:%S")  # Format as string

        cursor.execute("""
            INSERT OR REPLACE INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (symbol, timestamp_utc, open_price, high, low, close, volume))

        conn.commit()
        print(f"✅ [SAVED] {symbol} | {timestamp} | Open: {open_price}")

### =========================
###   HISTORICAL DATA FETCH
### =========================

def fetch_historical_data():
    """Fetch historical data in chunks and update SQLite database."""
    conn = create_connection()
    create_table()

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
                print(f"⏳ [{symbol}] Market closed. Stopping historical fetch.")
                break

            if last_timestamp_dt.weekday() >= 5:  # Skip weekends
                print(f"📅 [{symbol}] Weekend detected. Skipping fetch.")
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
    print(" Historical data fetch complete.")

### =========================
###   REAL-TIME DATA FETCH
### =========================

ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v2/iex"  # Using IEX instead of SIP

def get_latest_timestamp():
    """Get the latest timestamp from stock_prices, ensuring it does not go after 2024-10-01."""
    with sqlite3.connect(config.DB_FILE) as conn:
        query = "SELECT MAX(timestamp) FROM stock_prices"
        latest_time = pd.read_sql(query, conn).iloc[0, 0]

    fixed_start_date = "2024-10-01 00:00:00"  # Set a fixed start date

    if latest_time:
        return min(latest_time, fixed_start_date)  # Ensure it doesn't go fater 2024-10-01
    else:
        return fixed_start_date  # If no data exists, use the fixed date


# dataFromAlpaca.py
def run_data_processing():
    """Step 3: Merge stock data with sentiment data and compute technical indicators."""
    print("\n[Step 3] Merging stock & sentiment data and computing indicators...")

    # Fetch the latest timestamp from stock_features
    start_date = get_latest_feature_timestamp()  # Use the new function to get the latest time
    if not start_date:
        start_date = "2024-10-01 00:00:00" # Default to your initial start date

    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        print(f"Computing technical indicators from {start_date} to {end_date}...")
        print(f"Start date {start_date}, end date {end_date}")
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


# dataFromAlpaca.py
def get_latest_feature_timestamp():
    """Helper function to get the latest timestamp from stock_features."""
    try:
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM stock_features")
        latest_timestamp = cursor.fetchone()[0]
        conn.close()

        print(f"[DEBUG] get_latest_feature_timestamp() returned: {latest_timestamp}") # Added line

        return latest_timestamp
    except Exception as e:
        print(f"[ERROR] Could not retrieve latest timestamp from stock_features: {e}")
        return None

async def alpaca_ws_handler():
    """Connects to Alpaca WebSocket API and listens for real-time stock data, then triggers data processing."""
    async with websockets.connect(ALPACA_WS_URL) as ws:
        # Authenticate
        auth_msg = json.dumps({
            "action": "auth",
            "key": config.ALPACA_API_KEY,
            "secret": config.ALPACA_API_SECRET
        })
        await ws.send(auth_msg)
        auth_response = await ws.recv()
        print(f"🔑 [Alpaca-IEX] Authenticated: {auth_response}")

        # Subscribe to stock market data
        subscribe_msg = json.dumps({
            "action": "subscribe",
            "bars": config.ALL_SYMBOLS
        })
        await ws.send(subscribe_msg)
        subscribe_response = await ws.recv()
        print(f" [Subscribed] {subscribe_response}")

        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)

                for stock in data:
                    if stock.get("T") == "b":  # Only process bar data
                        symbol = stock["S"]
                        timestamp = stock["t"]  # ISO timestamp
                        open_price = stock["o"]
                        high = stock["h"]
                        low = stock["l"]
                        close = stock["c"]
                        volume = stock["v"]

                        print(f"\n[LIVE] {symbol} - {timestamp} | Open: {open_price}, High: {high}, Low: {low}, Close: {close}, Volume: {volume}")

                        # Save the real-time data
                        await save_stock_data(symbol, timestamp, open_price, high, low, close, volume)

                # ✅ **Trigger data processing right after new data is stored**
                print("\n[INFO] Running data processing after new real-time data...")
                run_data_processing()

            except Exception as e:
                print(f"[Alpaca-IEX] WebSocket Error: {e}")
                await asyncio.sleep(5)  # Retry after small delay


async def fetch_realtime_data():
    """Runs Alpaca WebSocket handler asynchronously."""
    await alpaca_ws_handler()
