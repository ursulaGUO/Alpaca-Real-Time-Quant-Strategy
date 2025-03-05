# Alpaca-Real-Time-Quant-Strategy

# Logins
Log in secrets are saved in `~/.secrets/.env`. To modify, use `nano ~/.secrets/.env`, press CTRL + X to exit, and press Y and then Enter to save changes. 

# File explanation
`dataFromBlueSky.py` extracts new posts from blue sky with given keywords, with time range being between previous search and now. So ideally, there is somewhere to store the 