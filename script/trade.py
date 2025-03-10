import os
import json
import time
import pickle
import numpy as np
import alpaca_trade_api as tradeapi
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
import pandas as pd

dotenv_path = os.path.expanduser("~/.secrets/.env")
load_dotenv(dotenv_path)
api_key = os.getenv("alpaca_api_key")
api_secret = os.getenv("alpaca_api_secret")
base_url = os.getenv("alpaca_api_endpoint")

print("Successfully loaded Alpaca secrets.")

# Initialize Alpaca API Client
api = tradeapi.REST(api_key, api_secret, base_url, api_version="v2")


# Load Trained Model & Scaler
MODEL_FILE = "model/RF_model.pkl"
SCALER_FILE = "model/scaler.pkl"

with open(MODEL_FILE, "rb") as f:
    model = pickle.load(f)

with open(SCALER_FILE, "rb") as f:
    scaler = pickle.load(f)

# Trading Variables
INITIAL_CASH = 100000  # Start with $100,000
cash = INITIAL_CASH
buying_power = 200000  # Margin buying power
positions = {}  # Dictionary to track positions {symbol: (quantity, avg_price)}

# Function: Predict Next Open Price
def predict_next_open(features):
    """Uses the trained model to predict the next open price."""
    
    # Convert NumPy array to DataFrame with the same column names used during training
    feature_df = pd.DataFrame([features], columns=scaler.feature_names_in_)
    
    # Scale features
    features_scaled = scaler.transform(feature_df)
    
    return model.predict(features_scaled)[0]

# Function: Get Latest Market Prices
def get_latest_price(symbol):
    """Fetches the latest price from Alpaca."""
    try:
        return float(api.get_latest_trade(symbol).price)
    except Exception as e:
        print(f"Error fetching price for {symbol}: {e}")
        return None
pending_orders = {}

def execute_trade(symbol, open_price, predicted_next_open):
    global cash, buying_power, positions, pending_orders

    # Prevent duplicate buy orders
    if symbol in pending_orders and pending_orders[symbol] == "buy":
        print(f"Skipping {symbol}: Already placed a buy order.")
        return

    # Buy Condition
    if predicted_next_open >= open_price +0.1:
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
            print(f"Placed BUY limit order for {quantity} shares of {symbol} at {limit_price:.2f}")
            pending_orders[symbol] = "buy"  # Mark as pending

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
        print(f"Placed SELL limit order for {quantity} shares of {symbol} at {limit_price:.2f}")
        pending_orders[symbol] = "sell"  # Mark as pending

# Function: Check & Update Order Status
def update_positions():
    """Checks if limit orders were filled and updates portfolio."""
    global cash, buying_power, positions

    orders = api.list_orders(status="closed")
    for order in orders:
        if order.filled_qty and order.filled_at:
            symbol = order.symbol
            qty = int(order.filled_qty)
            price = float(order.filled_avg_price)

            if order.side == "buy":
                cash -= qty * price
                buying_power -= qty * price
                if symbol in positions:
                    existing_qty = positions[symbol]["quantity"]
                    avg_price = positions[symbol]["avg_price"]
                    new_qty = existing_qty + qty
                    new_price = ((existing_qty * avg_price) + (qty * price)) / new_qty
                    positions[symbol] = {"quantity": new_qty, "avg_price": new_price}
                else:
                    positions[symbol] = {"quantity": qty, "avg_price": price}

                print(f"BUY FILLED: {qty} shares of {symbol} at {price:.2f}")

            elif order.side == "sell":
                cash += qty * price
                del positions[symbol]

                print(f"SELL FILLED: {qty} shares of {symbol} at {price:.2f}")

# Function: Calculate Portfolio Value
def calculate_portfolio_value():
    """Calculates total portfolio value using real-time Alpaca prices."""
    portfolio_value = cash
    for symbol, position in positions.items():
        quantity = position["quantity"]
        market_price = get_latest_price(symbol)
        if market_price:
            portfolio_value += quantity * market_price
    return portfolio_value

# Main Trading Loop
def trading_loop(symbols, interval=60):
    """Runs the trading bot in a loop, polling for new data."""
    while True:
        for symbol in symbols:
            open_price = get_latest_price(symbol)
            if open_price is None:
                continue

            # Extract features for ML prediction
            feature_values = np.array([
                open_price, open_price, open_price, open_price,  # Fallback values
                0, 0, 0, 0, 0,  # Default values for missing indicators
                open_price, open_price, 0,0
            ])

            # Predict next open price
            predicted_next_open = predict_next_open(feature_values)

            # Execute trade decision
            execute_trade(symbol, open_price, predicted_next_open)

        # Wait for limit orders to be executed
        time.sleep(interval)
        update_positions()

        # Calculate and print portfolio value
        portfolio_value = calculate_portfolio_value()
        pnl = portfolio_value - INITIAL_CASH

        print("\nMarket Update:")
        print(f"Cash Balance: {cash:.2f}")
        print(f"Portfolio Value: {portfolio_value:.2f}")
        print("\nCurrent Holdings:")
        for sym, position in positions.items():
            quantity = position["quantity"]
            market_price = get_latest_price(sym)
            if market_price:
                print(f"  - {sym}: {quantity} shares @ {market_price:.2f}")

        print(f"Profit & Loss (PnL): {'+' if pnl >= 0 else '-'}{abs(pnl):.2f}")
        print("-" * 50)

# Run the Trading Bot
if __name__ == "__main__":
    symbols_to_trade = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "PG", "KO", "WMT", "JNJ", "GOLD"]
    trading_loop(symbols_to_trade, interval=60)  # Poll every 60 seconds