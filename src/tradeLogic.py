import os
import json
import time
import pickle
import numpy as np
import alpaca_trade_api as tradeapi
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
import pandas as pd
import datetime

dotenv_path = os.path.expanduser("~/.secrets/.env")
load_dotenv(dotenv_path)

api_key = os.getenv("alpaca_api_key")
api_secret = os.getenv("alpaca_api_secret")
base_url = os.getenv("alpaca_api_endpoint")

# Initialize Alpaca API Client
api = tradeapi.REST(api_key, api_secret, base_url, api_version="v2")

# Load Trained Model & Scaler
MODEL_FILE = "model/LinearRegression.pkl"
SCALER_FILE = "model/scaler.pkl"

with open(MODEL_FILE, "rb") as f:
    model = pickle.load(f)

with open(SCALER_FILE, "rb") as f:
    scaler = pickle.load(f)

# Trading Variables
INITIAL_CASH = 100000  # Start with $100,000
cash = INITIAL_CASH
positions = {}  # {symbol: {quantity, avg_price}}
pending_orders = {}  # Track ongoing orders

def predict_next_open(features):
    """Uses the trained model to predict the next open price."""
    
    feature_df = pd.DataFrame([features], columns=scaler.feature_names_in_)
    features_scaled = scaler.transform(feature_df)
    
    return model.predict(features_scaled)[0]

def get_latest_price(symbol):
    """Fetches the latest price from Alpaca."""
    try:
        return float(api.get_latest_trade(symbol).price)
    except Exception as e:
        print(f"Error fetching price for {symbol}: {e}")
        return None

def execute_trade(symbol, open_price, predicted_next_open):
    global cash, positions, pending_orders

    # Get the current timestamp in UTC
    trade_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if symbol in pending_orders:
        return  # Prevent duplicate trades

    # Buy Condition
    if predicted_next_open >= open_price + 0.1:
        quantity = int(min(3000, cash / open_price) / open_price)
        if quantity > 0 and cash >= quantity * open_price:
            limit_price = round(open_price * 0.99, 2)

            api.submit_order(
                symbol=symbol,
                qty=quantity,
                side="buy",
                type="limit",
                time_in_force="gtc",
                limit_price=limit_price
            )

            print(f"\n[TRADE] {trade_time} | BUY {quantity} shares of {symbol}")
            print(f"        Open Price: {open_price:.2f} | Predicted Next Open: {predicted_next_open:.2f}")
            print(f"        Limit Price: {limit_price:.2f} | Cash Remaining: {cash - (quantity * open_price):.2f}")

            pending_orders[symbol] = "buy"

    # Sell Condition
    elif predicted_next_open < open_price and symbol in positions:
        quantity = positions[symbol]["quantity"]
        limit_price = round(open_price * 1.01, 2)

        api.submit_order(
            symbol=symbol,
            qty=quantity,
            side="sell",
            type="limit",
            time_in_force="gtc",
            limit_price=limit_price
        )

        print(f"\n[TRADE] {trade_time} | SELL {quantity} shares of {symbol}")
        print(f"        Open Price: {open_price:.2f} | Predicted Next Open: {predicted_next_open:.2f}")
        print(f"        Limit Price: {limit_price:.2f} | Cash After Sale: {cash + (quantity * open_price):.2f}")

        pending_orders[symbol] = "sell"

def trading_loop(features_dict, interval=60):
    """Runs the trading bot using the latest data from the database."""
    for symbol, features in features_dict.items():
        open_price = get_latest_price(symbol)
        if open_price is None:
            continue

        # Convert features to list (if it's a Pandas Series or NumPy array)
        features = list(features)

        # Remove the unwanted columns (open = index 1, symbol = index 0)
        features_filtered = [features[i] for i in range(len(features)) if i not in [0]]

        # Ensure feature size matches model expectations
        expected_features = len(scaler.feature_names_in_)
        if len(features_filtered) != expected_features:
            print(f"[ERROR] Feature count mismatch for {symbol}: Expected {expected_features}, got {len(features_filtered)}")
            continue

        predicted_next_open = predict_next_open(features_filtered)
        print(f"stock {symbol}, current at {features[1]}, predicted to be {predicted_next_open}")
        execute_trade(symbol, open_price, predicted_next_open)

    time.sleep(interval)
