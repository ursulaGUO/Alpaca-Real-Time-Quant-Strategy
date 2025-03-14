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
MODEL_FILE = "model/RandomForest.pkl"
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
SHORT_THRESHOLD = -0.17  # Threshold for considering a short position
MAX_SHARES = 10       # Maximum shares to hold (long or short)

def get_account_cash():
    """Fetch available cash balance from Alpaca."""
    try:
        account = api.get_account()
        return float(account.cash)  # Convert string to float
    except Exception as e:
        print(f"Error fetching account cash balance: {e}")
        return None

cash = get_account_cash()


def get_positions():
    """Fetch all open positions from Alpaca and return as a dictionary."""
    try:
        positions = {}
        alpaca_positions = api.list_positions()
        for position in alpaca_positions:
            symbol = position.symbol
            positions[symbol] = {
                "quantity": int(position.qty),  # Convert string to int
                "avg_price": float(position.avg_entry_price)  # Convert string to float
            }
        return positions
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return {}

positions = get_positions()

def get_pending_orders():
    """Fetch all pending orders from Alpaca."""
    try:
        orders = api.list_orders(status="open")  # Get open orders
        pending = {order.symbol: order.side for order in orders}
        return pending
    except Exception as e:
        print(f"Error fetching pending orders: {e}")
        return {}

pending_orders = get_pending_orders()


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

# Maintain local pending orders
local_pending_orders = {}

def clear_completed_orders():
    """
    Checks completed orders on Alpaca and clears them from local_pending_orders.
    """
    global local_pending_orders
    completed_symbols = []
    for symbol, side in local_pending_orders.items():
        try:
            orders = api.list_orders(status='closed', symbols=[symbol], limit=1)
            if orders:  # If there are closed orders for this symbol
                completed_symbols.append(symbol)
        except Exception as e:
            print(f"Error checking order status for {symbol}: {e}")
            continue

    # Clear completed symbols from local_pending_orders
    for symbol in completed_symbols:
        del local_pending_orders[symbol]
        print(f"[INFO] Cleared completed order for {symbol} from local pending orders.")

def update_local_positions(symbol, quantity, open_price, side):
    """Updates the local positions dictionary after a trade."""
    global positions

    if symbol not in positions:
        positions[symbol] = {"quantity": 0, "avg_price": 0}

    if side == "buy":
        # Update average price (simplified - can be made more accurate)
        total_value = (positions[symbol]["quantity"] * positions[symbol]["avg_price"]) + (quantity * open_price)
        total_quantity = positions[symbol]["quantity"] + quantity

        positions[symbol]["avg_price"] = total_value / total_quantity if total_quantity > 0 else 0
        positions[symbol]["quantity"] += quantity
    elif side == "sell":
        positions[symbol]["quantity"] -= quantity
        if positions[symbol]["quantity"] == 0:
            del positions[symbol]  # Remove from positions if sold out

def is_order_pending(symbol, side, open_price):
    """
    Checks if a similar order is already pending for the given symbol and side.
    """
    try:
        orders = api.list_orders(status='open', symbols=[symbol])
        for order in orders:
            if order.side == side and abs(float(order.limit_price) - open_price) <= 0.05:  #Adjust tolerance as needed
                print(f"[SKIP] Similar {side} order for {symbol} already pending (Order ID: {order.id}).")
                return True
        return False
    except Exception as e:
        print(f"[ERROR] Error checking pending orders: {e}")
        return False

def execute_trade(symbol, open_price, predicted_next_open, cash=None, positions=None):
    global local_pending_orders

    # Fetch latest account details
    cash = get_account_cash()
    positions = get_positions()
    alpaca_pending_orders = get_pending_orders()

    # Clear completed orders from local pending orders
    clear_completed_orders()

    # Merge API and local pending orders
    pending_orders = {**alpaca_pending_orders, **local_pending_orders}

    trade_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Prevent duplicate trades
    if symbol in pending_orders:
        print(f"[SKIP] {trade_time} | Order already pending for {symbol}, skipping new order.")
        return

    # Check for existing positions (long or short)
    current_position = 0
    if symbol in positions:
        current_position = positions[symbol]["quantity"] #If the current position exists then record it, else it will just be 0

    # Check for existing positions (long or short)
    long_position = symbol in positions and current_position > 0

    #Buy Condition
    if predicted_next_open >= open_price + 0.17:
       #New Check
        if is_order_pending(symbol, "buy", open_price):
            print(f"[TRADE] Order to buy {symbol} has already been created")
            return
            #If we hold less than 10 shares of the stock, long

        quantity_to_buy = MAX_SHARES - current_position
        if quantity_to_buy > 0: #If shares are more than the positions, then create it
           quantity = int(min(quantity_to_buy,min(3000, cash) / open_price))#If its still less than the max, then put in quantity
           if quantity > 0 and cash >= quantity * open_price:
                try:
                    api.submit_order(
                        symbol=symbol,
                        qty=quantity,
                        side="buy",
                        type="market",
                        time_in_force="gtc",
                    )
                    print(f"\n[TRADE] {trade_time} | BUY {quantity} shares of {symbol} (Long)")
                    print(f"        Open Price: {open_price:.2f} | Predicted Next Open: {predicted_next_open:.2f}")
                    print(f"        Cash Remaining: {cash - (quantity * open_price):.2f}") #Estimating

                    update_local_positions(symbol, quantity, open_price, "buy")  # Update local positions
                    local_pending_orders[symbol] = "buy"
                except Exception as e:
                    print(f"[ERROR] {trade_time} | Error submitting buy order for {symbol}: {e}")
           else:
                print(f"[TRADE] Max shares limit met or insufficient cash to buy {symbol}")
        else:
            print(f"[TRADE] Order has already been created to buy {symbol}")
            return
    elif (predicted_next_open < open_price and long_position):
        # Sell Condition (Long - Close)
        if is_order_pending(symbol, "sell", open_price):
            print(f"[TRADE] Order to sell {symbol} has already been created")
            return
        # Check that we actually own the stock to sell it
        quantity = positions[symbol]["quantity"]  # Sell entire position
        try:
            api.submit_order(
                symbol=symbol,
                qty=quantity,
                side="sell",
                type="market",
                time_in_force="gtc",
            )
            print(f"\n[TRADE] {trade_time} | SELL {quantity} shares of {symbol} (Close Long)")
            print(f"        Open Price: {open_price:.2f} | Predicted Next Open: {predicted_next_open:.2f}")
            print(f"        Cash After Sale: {cash + (quantity * open_price):.2f}")  # Estimating

            update_local_positions(symbol, quantity, open_price, "sell")  # Update local positions
            local_pending_orders[symbol] = "sell"
        except Exception as e:
            print(f"[ERROR] {trade_time} | Error submitting sell order for {symbol}: {e}")
    # Short Condition (Selling Short)
    elif predicted_next_open < open_price + SHORT_THRESHOLD:  # Use a more significant negative threshold

        if is_order_pending(symbol, "sell", open_price):
           print(f"[TRADE] Order to short {symbol} has already been created")
           return

            #If the existing short plus the total is still less than the total, we allow it

        quantity_to_short = MAX_SHARES + current_position
        if quantity_to_short > 0:
            quantity = int(min(quantity_to_short, min(500, cash) / open_price))
            if quantity > 0:
                try:
                   api.submit_order(
                       symbol=symbol,
                       qty=quantity,
                       side="sell",
                       type="market",
                       time_in_force="gtc",
                   )
                   print(f"\n[TRADE] {trade_time} | SELL {quantity} shares of {symbol} (Short)")
                   print(f"        Open Price: {open_price:.2f} | Predicted Next Open: {predicted_next_open:.2f}")
                   print(f"        Cash increased by: {(quantity * open_price):.2f}")  # Estimating

                   update_local_positions(symbol, quantity, open_price, "sell")  # Selling short is like selling
                   local_pending_orders[symbol] = "sell"
                except Exception as e:
                   print(f"[ERROR] {trade_time} | Error submitting short order for {symbol}: {e}")
            else:
                print(f"[TRADE] Max shares limit met to short {symbol}")
                return #Skip code if it has been ordered to max
        else:
            print(f"[TRADE] Has already been shorted to the max for  {symbol}")
            return #Skip code if it has been ordered to max

    # Buy Condition (Cover Short)
    elif predicted_next_open >= open_price and symbol in positions and positions[symbol]["quantity"] < 0:
        # If order has already been placed, skip
        if is_order_pending(symbol, "buy", open_price):
           print(f"[TRADE] Order to cover the short position for {symbol} has already been created")
           return
        quantity = abs(positions[symbol]["quantity"])  # Buy to cover entire short position
        try:
            api.submit_order(
                symbol=symbol,
                qty=quantity,
                side="buy",
                type="market",
                time_in_force="gtc",
            )
            print(f"\n[TRADE] {trade_time} | BUY {quantity} shares of {symbol} (Cover Short)")
            print(f"        Open Price: {open_price:.2f} | Predicted Next Open: {predicted_next_open:.2f}")
            # I think we need to make it so that we have an idea of how much cash is added, but this is only an estimate
            print(f"        Cash reduced by: {(quantity * open_price):.2f}")  # Estimating

            update_local_positions(symbol, quantity, open_price, "buy")  # Buying to cover is like buying
            local_pending_orders[symbol] = "buy"
        except Exception as e:
            print(f"[ERROR] {trade_time} | Error submitting buy order for {symbol}: {e}")

def synchronize_positions():
    """Synchronizes local positions with Alpaca account positions."""
    global positions

    try:
        alpaca_positions = get_positions()  # Fetch positions from Alpaca
        local_symbols = set(positions.keys())
        alpaca_symbols = set(alpaca_positions.keys())

        # Handle symbols present only locally
        for symbol in local_symbols - alpaca_symbols:
            print(f"[WARN] Symbol {symbol} found locally but not on Alpaca.  Removing local position.")
            del positions[symbol]

        # Update/Add symbols from Alpaca
        for symbol, alpaca_pos in alpaca_positions.items():
            if symbol in positions:
                if positions[symbol]["quantity"] != int(alpaca_pos["quantity"]):  # Need to convert the value of alpaca_pos to integer because that is what was done in the api call
                    print(
                        f"[INFO] Quantity mismatch for {symbol}. Local: {positions[symbol]['quantity']}, Alpaca: {alpaca_pos['quantity']}.  Updating local position.")
                    positions[symbol]["quantity"] = int(alpaca_pos["quantity"])
                    positions[symbol]["avg_price"] = float(alpaca_pos["avg_entry_price"])  # Important: update avg_price too
            else:
                print(f"[INFO] New position found on Alpaca: {symbol}.  Adding to local positions.")
                positions[symbol] = alpaca_pos

        print("[INFO] Local positions synchronized with Alpaca account.")

    except Exception as e:
        print(f"[ERROR] Error synchronizing positions: {e}")

def trading_loop(features_dict):
    """Runs the trading bot using the latest data from the database."""

    # Synchronize positions at the start of each loop or periodically
    synchronize_positions()

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