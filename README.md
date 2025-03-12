# Alpaca-Real-Time-Quant-Strategy


# Logins
Log in secrets are saved in `~/.secrets/.env`. To modify, use `nano ~/.secrets/.env`, press CTRL + X to exit, and press Y and then Enter to save changes.  
 

# Trading Goals 
My trading goal is to maximize over short term gains, while also holding long-term positions that I think would pay off in the 5-day horizon. I would freqeuntly trade stocks with high market-correlation and volatility to profit from price changes in minutes-horizon. And I would hold on to long/short positions of stocks that I think will accumulate gains over the 5-day horizon.

# Trading Indicators
I chose 10 stocks to trade for short term price changes. These stocks are chosen because they mostly correlate with market trend, and I believe that market trend is better captured with the machine learning models that use historical data and sentiment analysis data from social media. 
* AAPL (Apple)
* MSFT (Microsoft)
* GOOGL (Google)
* AMZN (Amazon)
* NVDA (Nvidia)
* PG (Procter & Gamble)
* KO (Coca-Cola)
* WMT (Walmart)
* JNJ (Johnson & Johnson)
* GOLD (Barrick Gold)

Besides these, I also handpicked several couple stocks that I think I'm relatively confident about regarding their current price and price a week later, given the current political and economic trend. 
* HOOD (Robinhood): I'm pessimistic about the economy, so I'm shorting it.
* COIN (Coinbase): I read news that were pessimistic about coinbase future as their CEO were convicted. So I'm shorting it. 
* MSTR (MicroStrategy): I read MicroStrategy went all in buying bitcoin, and as a crypto skeptic, I shorted MSTR. 
* UVXY (ProShares Ultra VIX Short-Term Futures ETF): It profits from increases in S&P volatility, and with the news of pending economic recession, its a good ETF to hold. 
* SQQQ (ProShares Ultra Pro Short QQQ): Similar to UVXY, I longed SQQQ because it shorts against the market. 
* MCD (McDonald): In economic downturn, I believe people will consume more affordable food. So I longed the sotcks. 
* SHEL (Shell): I think Trump's firm stance on protecting the auto industry in the US and shutting down climate-friendly EV industry is a strong sign to long oil companies. 

# Code Structure
This is the file structure of the repository. 
* `data` folder stores the SQLite database `trade_data.db`, and potentially some other temporary cached data used for testing.
* `model` folder stores the result of the trained model, and the scaler.
* `src` folder contains most of the tools. 
  * `config.py` stores the parameters, configuration data, look-up information, etc
  * `main.py` is where the main loop is, it calls the other files to run ascynronously, and centralize all actions in steps.
  * `dataFromAlpaca.py` calls Alpaca Rest and Stream data to fetch historical and real-time stock data, and stores it into `stock_prices` table in `trade_data.db`.
  * `dataFromBlueSky.py` calls BlueSky social media platform to search for stock-specific keywords, and generate a weighted average of sentiment metric, and stores it into `bluesky_posts` table in `trade_data.db`.
  * `dataCombine.py` computes technical indicators and work on feature engineering based on all data in `stock_prices` table and stores as `stock_featurs`. It then merges with `bluesky_posts` to formulate the final read-to-train dataset in the `merged_data` table.    
  * `tradeLogic.py` contains all the trade logic to be executed, and also calls Alpaca API to check current positions, pending orders, and portfolio. 
* `script` folder contains scripts that are not used in the main pipeline, but necessary for debugging and testing.
  * `checkStockFeatures.py`, `checkStockPriceTable.py`,`checkMergeTable.py` all checks the latest data in the tables from the database.
  *  `backtest.py`, receives data from `src/tcp_server.py` and mocks a trading session.



```
Alpaca-Real-Time-Quant-Strategy/
├─ data/
│  ├─ trade_data.db
├─ model/
│  ├─ RandomForest.pkl
│  ├─ otherModels.pkl
│  ├─ scaler.pkl
├─ src/
│  ├─ config.py
│  ├─ main.py
│  ├─ dataFromAlpaca.py
│  ├─ dataFromBlueSky.py
│  ├─ dataCombine.py
│  ├─ queryFromPost.py
│  ├─ tcp_server.py
│  ├─ tradeLogic.py
│  ├─ train.py
├─ script/
│  ├─ checkStockFeatures.py
│  ├─ checkStockPriceTable.py
│  ├─ backtest.py
│  ├─ 
├─ README.md
├─ new_file
```

# Miscellaneous
## File storage
I've chosen to use SQLite database, after seeing how long the merging took with csv files. With SQLite queries, computation and merging was way faster. I also kept track of metadata such as the most recent date in the tables, so that I can avoid double calculation and merging.
## Timestamps and Timezones
I use timezone-sensitive format for all timestamps, such as this `start_time = datetime.strptime(config.SENTIMENT_START_DATE, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) if not last_scraped else datetime.fromisoformat(last_scraped.replace("Z", "+00:00"))`

## Automate Data Retrieval
This is accomplished by using `asyncio` to keep data retrieval functions running in the background.

## Model Results

RandomForest Performance:
  - MAE: 0.1601
  - RMSE: 0.7852
  - Directional Accuracy: 88.70%
  - Model saved.
  - Predictions saved.

XGBoost Performance:
  - MAE: 0.3462
  - RMSE: 1.1637
  - Directional Accuracy: 74.61%
  - Model saved.
  - Predictions saved.

LinearRegression Performance:
  - MAE: 0.1189
  - RMSE: 0.7428
  - Directional Accuracy: 90.82%
  - Model saved.
  - Predictions saved.

Linear Regression Model Coefficients:
 - open: -9.6054
 - high: 6.8342
 - low: 7.8010
 - close: 107.9648
 - volume: -0.0149
 - SMA_20: 0.5606
 - SMA_50: -0.6763
 - SMA_100: -0.0063
 - Volatility: 0.0318
 - Bollinger_Upper: 0.5532
 - Bollinger_Lower: 0.5679
 - Momentum_5: -0.0166
 - sentiment_score: 0.0024
 - likes: -0.0070
 - weighted_sentiment: -0.0064