import socket
import pickle
import json
import numpy as np
from sklearn.preprocessing import StandardScaler

# Load trained model from RF_model.py
MODEL_FILE = "model/RF_model.pkl"
with open(MODEL_FILE, "rb") as f:
    model = pickle.load(f)

# Initial Trading Capital
INITIAL_CASH = 100000  # Starting cash
INITIAL_BUYING_POWER = 200000  # Margin for short selling
cash = INITIAL_CASH
buying_power = INITIAL_BUYING_POWER
positions = {}  # Dictionary to track positions {symbol: (quantity, avg_price)}

# Connect to the TCP server
HOST = "127.0.0.1"
PORT = 9090
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((HOST, PORT))

print(f"Connected to market data server at {HOST}:{PORT}\n")

with open("model/scaler.pkl", "rb") as f:
    scaler = pickle.load(f)

def predict_next_open(features):
    """Predicts the next open price using a fixed scaler."""
    features_scaled = scaler.transform(features.reshape(1, -1))  # Use existing scaler
    return model.predict(features_scaled)[0]

def execute_trade(symbol, open_price, predicted_next_open):
    """Executes buy and sell orders based on strategy rules."""
    global cash, buying_power, positions

    # Buy if prediction is at least 0.1 higher than current open
    if predicted_next_open >= open_price + 0.1:
        max_spend = min(max(3000, cash / open_price), buying_power)  # Limit to available buying power
        quantity = int(max_spend / open_price)  # Number of shares to buy

        if quantity > 0 and cash >= quantity * open_price:
            cash -= quantity * open_price
            buying_power -= quantity * open_price  # Reduce available capital
            if symbol in positions:
                existing_quantity = positions[symbol]["quantity"]
                avg_price = positions[symbol]["avg_price"]
                new_quantity = existing_quantity + quantity
                new_price = ((existing_quantity * avg_price) + (quantity * open_price)) / new_quantity
                positions[symbol] = {"quantity": new_quantity, "avg_price": new_price}
            else:
                positions[symbol] = {"quantity": quantity, "avg_price": open_price}

            print(f"💰 BUY {quantity} shares of {symbol} at {open_price:.2f}, Predicted: {predicted_next_open:.2f}, Cash: {cash:.2f}, Buying Power: {buying_power:.2f}")

    # Sell if prediction is lower than the current open and we hold this stock
    elif predicted_next_open < open_price and symbol in positions:
        quantity = positions[symbol]["quantity"]
        avg_price = positions[symbol]["avg_price"]

        cash += quantity * open_price  
        buying_power += quantity * open_price 
        del positions[symbol]  

        print(f" SELL {quantity} shares of {symbol} at {open_price:.2f}, Predicted: {predicted_next_open:.2f}, Cash: {cash:.2f}, Buying Power: {buying_power:.2f}")

latest_prices = {}  

def calculate_portfolio_value():
    """Calculates the total portfolio value using the latest market prices."""
    portfolio_value = cash
    for symbol, position in positions.items():
        quantity, avg_price = position["quantity"], position["avg_price"]
        market_price = latest_prices.get(symbol, avg_price)  # Use latest market price if available
        portfolio_value += quantity * market_price  # Only handle long positions

    return portfolio_value


try:
    while True:
        data = client.recv(1024).decode()
        if not data:
            break

        market_data = json.loads(data.strip())

        symbol = market_data["symbol"]
        open_price = float(market_data["open"])

        # Store the latest market price
        latest_prices[symbol] = open_price 

        # Extract features for prediction
        feature_values = np.array([
            open_price, float(market_data["high"]), float(market_data["low"]),
            float(market_data["close"]), float(market_data["volume"]), float(market_data["trade_count"]),
            float(market_data["SMA_20"]), float(market_data["SMA_50"]),
            float(market_data["SMA_100"]), float(market_data["Volatility"]),
            float(market_data["Bollinger_Upper"]), float(market_data["Bollinger_Lower"]),
            float(market_data["weighted_sentiment"])
        ])

        # Predict next open price
        predicted_next_open = predict_next_open(feature_values)

        # Execute trade if criteria met
        execute_trade(symbol, open_price, predicted_next_open)

        # Calculate PnL and Portfolio Value using latest prices
        portfolio_value = calculate_portfolio_value()
        pnl = portfolio_value - INITIAL_CASH

        # Print Summary
        print("\nMarket Update:")
        print(f" Symbol: {symbol}")
        print(f" Open Price: {open_price:.2f}")
        print(f" Predicted Next Open: {predicted_next_open:.2f}")
        print(f" Cash Balance: {cash:.2f}")
        print(f" Portfolio Value: {portfolio_value:.2f}")
        print(f"\n Current Holdings:")
        for sym, position in positions.items():
            quantity, avg_price = position["quantity"], position["avg_price"]
            market_price = latest_prices.get(sym, avg_price)
            print(f"  - {sym}: {quantity} shares @ {market_price:.2f} (Avg Buy: {avg_price:.2f})")


except KeyboardInterrupt:
    print("\nBacktest stopped by user.")

finally:
    client.close()
    print("\nFinal Portfolio Value:")
    final_value = calculate_portfolio_value()
    final_pnl = final_value - INITIAL_CASH

    for symbol, position in positions.items():
        quantity, avg_price = position["quantity"], position["avg_price"]
        market_price = latest_prices.get(symbol, avg_price)
        print(f"{symbol}: {quantity} shares @ {market_price:.2f} (Avg Buy: {avg_price:.2f})")

    print(f"\n💰 Final Cash: {cash:.2f}")
    print(f"📈 Total Portfolio Value: {final_value:.2f} (Initial: {INITIAL_CASH:.2f})")
    print(f"📉 Final Profit & Loss (PnL): {'+' if final_pnl >= 0 else '-'}{abs(final_pnl):.2f}")
    print("\n🔚 Backtest Complete.")

