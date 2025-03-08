import sqlite3
import pandas as pd
import numpy as np
import pickle
import xgboost as xgb
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Lasso, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error

DB_FILE = "data/trade_data.db"
MODEL_DIR = "model"

# Load data from SQLite
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

# Load dataset
df, features = load_data("2025-02-17", "2025-03-03")
df = df.sort_values(["symbol", "timestamp"])
df["next_open"] = df.groupby("symbol")["open"].shift(-1)
df = df.dropna().reset_index(drop=True)

X = df[features]
y = df["next_open"]

# Split dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Standardize features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Save scaler
SCALER_FILE = f"{MODEL_DIR}/scaler.pkl"
with open(SCALER_FILE, "wb") as f:
    pickle.dump(scaler, f)
print(f"Scaler saved to {SCALER_FILE}")

# Function to train and evaluate models
def train_and_evaluate_model(model, model_name, X_train, X_test, y_train, y_test):
    """Train, evaluate, and save a model."""
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    # Evaluate
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    print(f"\n{model_name} Performance:")
    print(f"  - MAE: {mae:.4f}")
    print(f"  - RMSE: {rmse:.4f}")

    # Compute directional accuracy
    open_prices_test = df.iloc[y_test.index]["open"].values
    y_actual_direction = np.sign(y_test.values - open_prices_test)
    y_pred_direction = np.sign(y_pred - open_prices_test)
    direction_accuracy = np.mean(y_actual_direction == y_pred_direction)

    print(f"  - Directional Accuracy: {direction_accuracy * 100:.2f}%")

    # Save model
    model_file = f"{MODEL_DIR}/{model_name}.pkl"
    with open(model_file, "wb") as f:
        pickle.dump(model, f)
    print(f"  - Model saved to {model_file}")

    # Save predictions
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

    results_file = f"{MODEL_DIR}/{model_name}_predictions.csv"
    results_df.to_csv(results_file, index=False)
    print(f"  - Predictions saved to {results_file}")

    return model

# Train and evaluate Random Forest
rf_model = RandomForestRegressor(n_estimators=100, random_state=99, max_depth=15)
train_and_evaluate_model(rf_model, "RandomForest", X_train_scaled, X_test_scaled, y_train, y_test)

# Train and evaluate XGBoost with IFA Hyperparameter Tuning
xgb_model = xgb.XGBRegressor(objective="reg:squarederror")
param_grid = {
    "n_estimators": [50, 100, 200],
    "max_depth": [3, 6, 9],
    "learning_rate": [0.01, 0.05, 0.1]
}

grid_search = GridSearchCV(xgb_model, param_grid, cv=3, scoring="neg_mean_absolute_error", verbose=1)
grid_search.fit(X_train_scaled, y_train)
best_xgb_model = grid_search.best_estimator_
print(f"\nBest XGBoost Params: {grid_search.best_params_}")
train_and_evaluate_model(best_xgb_model, "XGBoost", X_train_scaled, X_test_scaled, y_train, y_test)

# Train and evaluate Linear Regression
lr_model = LinearRegression()
train_and_evaluate_model(lr_model, "LinearRegression", X_train_scaled, X_test_scaled, y_train, y_test)

print("\nAll models trained and saved successfully!")
