import sqlite3

import pandas as pd

# Connect to database
DB_FILE = "data/trade_data.db"
conn = sqlite3.connect(DB_FILE)

# Query the stock_prices table
query = "SELECT * FROM merged_data ORDER BY timestamp DESC LIMIT 10"
df = pd.read_sql(query, conn)

# Display Data
conn.close()
df.to_csv("data/latestMergedData.csv")

print("Stock Prices Table", df)
