import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

DB_FILE = "data/trade_data.db"

# Load processed data from SQLite
def load_data():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM merged_data", conn, parse_dates=["timestamp"])
    conn.close()
    return df

df = load_data()
print(df.head())

df = df.sort_values(["symbol", "timestamp"])
df["next_open"] = df.groupby("symbol")["open"].shift(-1)
df = df.dropna()
features = [
    "open",'high','low','close','volume',
    "SMA_20", "SMA_50", "SMA_100", "Volatility", 
    "Bollinger_Upper", "Bollinger_Lower", "volume", "weighted_sentiment"
]

df = df.reset_index(drop=True)
X = df[features]
y = df["next_open"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)


# Train a Random Forest Regressor
model = RandomForestRegressor(n_estimators=100, random_state=99,max_depth=15)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Evaluate model
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
open_prices_test = df.iloc[y_test.index]["open"].values
y_actual_direction = np.sign(y_test.values - open_prices_test)
y_pred_direction = np.sign(y_pred - open_prices_test)


print(f"Mean Absolute Error (MAE): {mae}")
print(f"Root Mean Squared Error (RMSE): {rmse}")


# Compute overall directional accuracy
direction_accuracy = np.mean(y_actual_direction == y_pred_direction)

# Compute accuracy for each direction
up_mask = y_actual_direction == 1
down_mask = y_actual_direction == -1
unchanged_mask = y_actual_direction == 0

up_accuracy = np.mean(y_pred_direction[up_mask] == 1) if up_mask.sum() > 0 else np.nan
down_accuracy = np.mean(y_pred_direction[down_mask] == -1) if down_mask.sum() > 0 else np.nan
unchanged_accuracy = np.mean(y_pred_direction[unchanged_mask] == 0) if unchanged_mask.sum() > 0 else np.nan

# Print results
print(f"Overall Directional Accuracy: {direction_accuracy * 100:.2f}%")
print(f"Upward Movement Accuracy: {up_accuracy * 100:.2f}%")
print(f"Downward Movement Accuracy: {down_accuracy * 100:.2f}%")
print(f"Unchanged Movement Accuracy: {unchanged_accuracy * 100:.2f}%")


# Create a DataFrame to store results
results_df = pd.DataFrame({
    "symbol": df.iloc[y_test.index]["symbol"].values, 
    "timestamp": df.iloc[y_test.index]["timestamp"].values, 
    "open": open_prices_test,     
    "next_open_actual": y_test.values,       
    "next_open_predicted": y_pred,             
    "actual_direction": y_actual_direction,   
    "predicted_direction": y_pred_direction,     
    "direction_correct": (y_actual_direction == y_pred_direction).astype(int),
    "sentiment": df.iloc[y_test.index]["weighted_sentiment"].values})

# Save to CSV
results_df.to_csv("model/RF_predictions.csv", index=False)
print("Predictions saved to `model/RF_predictions.csv`.")
