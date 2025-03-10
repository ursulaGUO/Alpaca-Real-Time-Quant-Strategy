import time
import config
import asyncio
import sqlite3
import pandas as pd
from datetime import datetime, timezone
from dataFromAlpaca import fetch_historical_data
from dataFromBlueSky import download_bluesky_posts
from dataCombine import merge_sentiment_data, compute_technical_indicators
from tradeLogic import trading_loop  
import json
import websockets

DB_FILE = config.DB_FILE  # Use centralized configuration

# Dictionary to track last recorded time for each stock
last_recorded_time = {}
last_sentiment_fetch = 0

async def alpaca_ws_handler():
    """Connects to Alpaca WebSocket API and listens for IEX stock data."""
    ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v2/iex" 

    async with websockets.connect(ALPACA_WS_URL) as ws:
        # Authenticate
        auth_msg = json.dumps({
            "action": "auth",
            "key": config.ALPACA_API_KEY,
            "secret": config.ALPACA_API_SECRET
        })
        await ws.send(auth_msg)
        auth_response = await ws.recv()
        print(f"[DEBUG] Auth Response: {auth_response}")  # ← Confirm authentication
        
        # Subscribe to IEX market data
        subscribe_msg = json.dumps({
            "action": "subscribe",
            "bars": config.ALL_SYMBOLS  # Subscribe to real-time bar updates
        })
        await ws.send(subscribe_msg)
        subscribe_response = await ws.recv()  # ← Add this to check subscription success
        print(f"[DEBUG] Subscription Response: {subscribe_response}")  # ← Confirm subscription

        print(f"[Alpaca-IEX] Subscribed to: {config.ALL_SYMBOLS}")

        while True:
            try:
                message = await ws.recv()
                print(f"[DEBUG] Received message: {message}")
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

                        print(f"[Alpaca-IEX] {symbol} | {timestamp} | Open: {open_price}")

                        await save_stock_data(symbol, timestamp, open_price, high, low, close, volume)

            except Exception as e:
                print(f"[Alpaca-IEX] Error: {e}")
                await asyncio.sleep(5)  # Retry after short delay

async def main():
    """Main async function to run WebSocket handler and other tasks."""
    fetch_historical_data()  # Step 1: Fetch historical data synchronously
    await alpaca_ws_handler()  # Step 2: Start WebSocket streaming asynchronously

if __name__ == "__main__":
    print("\n==============================")
    print("   Starting Real-Time Trading Pipeline   ")
    print("==============================\n")

    try:
        asyncio.run(main())  # Use asyncio.run() to avoid event loop issues
    except KeyboardInterrupt:
        print("\nShutting down Alpaca WebSocket streaming...")
