import os
import sqlite3
import pandas as pd
import argparse

DB_FILE = "data/trade_data.db"

def search_post(table, ticker: str, since: str, until: str) -> pd.DataFrame:
    """
    Searches for posts in the database matching the given ticker and date range.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    query = f"""
        SELECT keyword, author, date, text, likes, sentiment_score 
        FROM {table} 
        WHERE keyword = ? AND date BETWEEN ? AND ?
        ORDER BY date DESC
    """
    cursor.execute(query, (ticker, since, until))
    results = cursor.fetchall()
    conn.close()
    
    return pd.DataFrame(results, columns=["keyword", "author", "date", "text", "likes", "sentiment_score"])

def find_unique_stocks(table,since: str=None, until: str=None) -> pd.DataFrame:
    """
    Searches for posts in the database matching the given ticker and date range.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if str(since) == 'None' and str(until) == 'None':
        query = f"""
        SELECT DISTINCT symbol
        FROM {table}
    """
        cursor.execute(query)
    else:
        query = f"""
            SELECT DISTINCT symbol
            FROM {table} 
            WHERE timestamp BETWEEN ? AND ?
        """
        cursor.execute(query, (since, until))
    results = cursor.fetchall()
    conn.close()
    
    return pd.DataFrame(results, columns=["symbol"])

def delete_post(table, ticker: str = None, since: str = None, until: str = None):
    """
    Deletes posts from the database based on the given criteria.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if ticker:
        if since and until:
            query = f"DELETE FROM {table} WHERE keyword = ? AND DATE(date) BETWEEN DATE(?) AND DATE(?)"
            params = (ticker, since, until)
        elif since:
            query = f"DELETE FROM {table} WHERE keyword = ? AND DATE(date) >= DATE(?)"
            params = (ticker, since)
        elif until:
            query = f"DELETE FROM {table} WHERE keyword = ? AND DATE(date) <= DATE(?)"
            params = (ticker, until)
        else:
            query = f"DELETE FROM {table} WHERE keyword = ?"
            params = (ticker,)
    else:
        if since and until:
            query = f"DELETE FROM {table} WHERE DATE(date) BETWEEN DATE(?) AND DATE(?)"
            params = (since, until)
        else:
            query = f"DELETE FROM {table}"
            params = ()

    cursor.execute(query, params)
    conn.commit()
    conn.close()
    print(f"Deleted from {table} all posts between {since} and {until}.")

def show_table(table, since=None, until=None):
    """
    Shows the contents of the table in the database and saves it as a CSV file.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Fetch column names
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [col[1] for col in cursor.fetchall()]  # Extract column names
    
    # Fetch data
    if since is None or until is None:
        cursor.execute(f"SELECT * FROM {table}")
    else:
        cursor.execute(f"SELECT * FROM {table} WHERE timestamp BETWEEN ? AND ?", (since, until))
    
    results = cursor.fetchall()
    conn.close()
    
    # Create DataFrame with column names
    df = pd.DataFrame(results, columns=columns)
    
    # Save DataFrame to CSV with column names
    if since is None or until is None:
        filename = f"data/{table}.csv"
    else:
        filename = f"data/{table}_{since}_{until}.csv"
    
    df.to_csv(filename, index=False)
    print(f"df saved to {filename}")

def parse_arguments():
    """
    Parses command-line arguments for querying or deleting posts.
    """
    parser = argparse.ArgumentParser(description="Query posts from data/trade_data.db")
    parser.add_argument("-table", type=str, default="bluesky_posts", choices = ['bluesky_posts','stock_prices','stock_features','merged_data'],help="The name of the table in the database")
    parser.add_argument("-ticker", type=str, help="The ticker of the stock")
    parser.add_argument("-since", type=str, help="The start date (YYYY-MM-DD)")
    parser.add_argument("-until", type=str, help="The end date (YYYY-MM-DD)")
    parser.add_argument("-query", type=str, choices=["search", "delete", "show_table","find_unique_stocks"], required=True, help="The query type: search or delete")
    
    args = parser.parse_args()
    return args.table, args.ticker, args.since, args.until, args.query

if __name__ == "__main__":
    table, ticker, since, until, query = parse_arguments()
    
    if query == "delete":
        delete_post(table, ticker, since, until)
        if ticker:
            print(f"Deleted posts for {ticker} from {since} to {until}.")
        else:
            print(f"Deleted table {table} all posts between {since} and {until}.")
    
    elif query == "search":
        if not ticker:
            raise ValueError("Ticker is required for a search query.")
        stock_df = search_post(table, ticker, since, until)
        
        if stock_df.empty:
            print("No posts found for the given criteria.")
        else:
            print(f"Average sentiment score: {stock_df['sentiment_score'].mean():.2f}")
            print(f"Spread of sentiment score: {stock_df['sentiment_score'].max() - stock_df['sentiment_score'].min():.2f}")
            print(f"Number of posts: {stock_df.shape[0]}")
            print(f"Date range: {stock_df['date'].min()} to {stock_df['date'].max()}")
    elif query == "show_table":
        show_table(table, since, until)
    elif query == "find_unique_stocks":
        stock_df = find_unique_stocks(table, since, until)
        
        if stock_df.empty:
            print(f"No stocks found for the given criteria in table {table}.")
        else:
            print(f"Number of unique stocks: {stock_df.shape[0]}")
            print(f"They are {stock_df['symbol'].values}")
            print(f"Date range: {since} to {until}")