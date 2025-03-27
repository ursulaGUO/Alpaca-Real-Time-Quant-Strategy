import os

# SQLite Database Path
DB_FILE = "data/trade_data.db"

# API Keys (Load from .env)
from dotenv import load_dotenv
dotenv_path = os.path.expanduser("~/.secrets/.env")
load_dotenv(dotenv_path)
ALPACA_API_KEY = os.getenv("alpaca_api_key")
ALPACA_API_SECRET = os.getenv("alpaca_api_secret")
ALPACA_BASE_URL = os.getenv("alpaca_base_url")
BLUESKY_USERNAME = os.getenv("blueSky_user_name")
BLUESKY_PASSWORD = os.getenv('blueSky_password')

# Stock Symbols
ALL_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "PG", "KO", "WMT", "JNJ", "GOLD"]

# Timeframe for Alpaca Data
TIMEFRAME = "15Min"

# Fetch from Alpaca
HISTORICAL_CHUNK_DAYS = 5

# Historical Data Start Date
CUSTOM_START_DATE = "2024-10-01T00:00:00Z"

# Sentiment Analysis Configuration
SENTIMENT_START_DATE = "2025-02-10T00:00:00Z"

# Merge Data Start Date
MERGE_START_DATE = "2025-02-10"

# BlueSky Keyword Mapping
STOCK_DICT = {
    "AAPL": ["Apple"],
    "MSFT": ["Microsoft"],
    "GOOGL": ["Google"],
    "AMZN": ["Amazon"],
    "TSLA": ["Tesla"],
    "NVDA": ["Nvidia"],
    "PG": ["Procter & Gamble"],
    "KO": ["Coca-Cola"],
    "WMT": ["Walmart"],
    "JNJ": ["Johnson & Johnson"],
    "GOLD": ["Barrick Gold"]
}
