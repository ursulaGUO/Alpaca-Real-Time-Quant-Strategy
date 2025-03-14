import sqlite3
import pickle
import config
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Lasso, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error

### =========================
###   DATABASE FUNCTIONS
### =========================

def load_data():
    """Load all available data from the merged_data table."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    # Get column names dynamically
    cursor.execute("PRAGMA table_info(merged_data);")
    all_columns = [col[1] for col in cursor.fetchall()]

    # Exclude non-numeric columns
    excluded_columns = {"timestamp", "symbol"}
    features = [col for col in all_columns if col not in excluded_columns]

    # Load entire dataset
    df = pd.read_sql("SELECT * FROM merged_data", conn, parse_dates=["timestamp"])
    df.columns = all_columns  # Explicitly set column names
    conn.close()

    

    return df, features

### =========================
###   DATA PREPROCESSING
### =========================
def preprocess_data(df, features):
    """Prepare data for training."""
    print(f"Original data shape: {df.shape}")

    df = df.sort_values(["symbol", "timestamp"])
    df["next_open"] = df.groupby("symbol")["open"].shift(-1)

    print(f"Data after shift operation: {df.shape}")
    print("Checking null values in `next_open` column:")
    print(df["next_open"].isna().sum())

    df.head(200).to_csv("data/top_merged.csv")

    # Drop trade_count if it exists
    if "trade_count" in df.columns:
        df = df.drop(columns=["trade_count"])
        print("Dropped 'trade_count' column due to invalid values.")

    # Drop NaN values after removing trade_count
    df = df.dropna().reset_index(drop=True)
    print(f"Data after dropna: {df.shape}")

    # Update features list again after dropping columns
    features = [f for f in df.columns if f not in ["next_open", "timestamp", "symbol"]]
    
    print(f"Final features used: {features}")

    X = df[features]
    y = df["next_open"]

    print(f"Feature matrix shape: {X.shape}")
    print(f"Target variable shape: {y.shape}")

    if X.empty or y.empty:
        raise ValueError("X or y is empty after preprocessing. Check data pipeline.")

    # Split dataset
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=99
    )

    print(f"Train shape: {X_train.shape}, Test shape: {X_test.shape}")
    print(f"Rows used for training: {X_train.shape[0]}")
    print(f"Rows used for testing: {X_test.shape[0]}")

    # Standardize features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Save scaler
    with open("model/scaler.pkl", "wb") as f:
        pickle.dump(scaler, f)

    return X_train_scaled, X_test_scaled, y_train, y_test, df, features



### =========================
###   TRAINING FUNCTION
### =========================

def train_and_evaluate_model(model, model_name, X_train, X_test, y_train, y_test, df, features):
    """Train, evaluate, and save a model."""
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    # Evaluate model
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
    with open(f"model/{model_name}.pkl", "wb") as f:    
        pickle.dump(model, f)
    
    print(f"  - Model saved.")

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

    results_df.to_csv(f"model/{model_name}_predictions.csv", index=False)
    print(f"  - Predictions saved.")

    return model

### =========================
###   TRAIN ALL MODELS
### =========================

def train_all_models():
    """Load data, preprocess, and train multiple models on full dataset."""
    df, features = load_data()
    
    if df.empty:
        print("No data available for training.")
        return

    X_train, X_test, y_train, y_test, df,features = preprocess_data(df, features)

    # Train Random Forest
    rf_model = RandomForestRegressor(n_estimators=100, random_state=99, max_depth=15)
    train_and_evaluate_model(rf_model, "RandomForest", X_train, X_test, y_train, y_test, df,features)

    # Train XGBoost with Hyperparameter Tuning
    xgb_model = xgb.XGBRegressor(objective="reg:squarederror")
    param_grid = {
        "n_estimators": [50, 100, 200],
        "max_depth": [3, 6, 9],
        "learning_rate": [0.01, 0.05, 0.1]
    }

    grid_search = GridSearchCV(xgb_model, param_grid, cv=3, scoring="neg_mean_absolute_error", verbose=1)
    grid_search.fit(X_train, y_train)
    best_xgb_model = grid_search.best_estimator_
    print(f"\nBest XGBoost Params: {grid_search.best_params_}")
    train_and_evaluate_model(best_xgb_model, "XGBoost", X_train, X_test, y_train, y_test, df,features)

    # Train Linear Regression
    lr_model = LinearRegression()
    train_and_evaluate_model(lr_model, "LinearRegression", X_train, X_test, y_train, y_test, df,features)

    print("\nLinear Regression Model Coefficients:")
    for feature, coef in zip(features, lr_model.coef_):
        print(f"{feature}: {coef:.4f}")

    print("\nIntercept:")
    print(f"{lr_model.intercept_:.4f}")

    print("\nAll models trained and saved successfully.")

### =========================
###   MAIN EXECUTION
### =========================

if __name__ == "__main__":
    print("Starting training process...")
    train_all_models()
    print("Training process complete.")
