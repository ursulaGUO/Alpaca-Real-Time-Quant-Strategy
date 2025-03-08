import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle

DB_FILE = "data/trade_data.db"

def load_data(start_date=None, end_date=None):
    """Load data from SQLite and dynamically retrieve column names."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Get column names dynamically
    cursor.execute("PRAGMA table_info(merged_data);")
    all_columns = [col[1] for col in cursor.fetchall()]

    # Exclude non-numeric columns
    excluded_columns = {"timestamp", "symbol"}
    features = [col for col in all_columns if col not in excluded_columns]

    # Load data
    query = "SELECT * FROM merged_data"
    if start_date and end_date:
        query += f" WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'"
    
    df = pd.read_sql(query, conn, parse_dates=["timestamp"])
    df.columns = all_columns  # Explicitly set column names
    conn.close()

    return df, features

# Load data and features
df, features = load_data("2025-02-17", "2025-03-03")
print(df.head())

df = df.sort_values(["symbol", "timestamp"])
df["next_open"] = df.groupby("symbol")["open"].shift(-1)
df = df.dropna().reset_index(drop=True)

X = df[features]
y = df["next_open"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Standardize features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)
SCALER_FILE = "model/RF_scaler.pkl"
with open(SCALER_FILE, "wb") as f:
    pickle.dump(scaler, f)

print(f"Scaler saved to {SCALER_FILE}")

# Train a Random Forest Regressor
model = RandomForestRegressor(n_estimators=100, random_state=99, max_depth=15)
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
    "sentiment": df.iloc[y_test.index]["weighted_sentiment"].values
})

# Save to CSV
results_df.to_csv("model/RF_predictions.csv", index=False)
print("Predictions saved to `model/RF_predictions.csv`.")

# Save trained model
with open("model/RF_model.pkl", "wb") as f:
    pickle.dump(model, f)
print("Model saved to `model/RF_model.pkl`.")
