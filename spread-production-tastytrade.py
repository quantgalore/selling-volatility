# -*- coding: utf-8 -*-
"""
Created in 2024

@author: Quant Galore
"""

import requests
import pandas as pd
import numpy as np
import time

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar
# from self_email import send_message

# =============================================================================
# Tastytrade Integration
# =============================================================================

base_url = 'https://api.tastyworks.com'

# Authenticate session

auth_url = 'https://api.tastyworks.com/sessions'
headers = {'Content-Type': 'application/json'}

session_data = {
    # Tastytrade email + pw
    "login": "your-tastytrade-email@email.com",
    "password": "your-tastytrade-password",
    "remember-me": True
}

authentication_response = requests.post(auth_url, headers=headers, json=session_data)
authentication_json = authentication_response.json()

session_token = authentication_json["data"]["session-token"]
authorized_header = {'Authorization': session_token}

# End of authentication

# Pull account information and verify balance

accounts = requests.get(f"{base_url}/customers/me/accounts", headers = {'Authorization': session_token}).json()
account_number = accounts["data"]["items"][0]["account"]["account-number"]

balances = requests.get(f"{base_url}/accounts/{account_number}/balances", headers = {'Authorization': session_token}).json()["data"]

option_buying_power = np.float64(balances["derivative-buying-power"])
print(f"Buying Power: ${option_buying_power}")

# =============================================================================
# Polygon Data
# =============================================================================

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

trading_dates = calendar.schedule(start_date = "2023-01-01", end_date = (datetime.today()+timedelta(days=1))).index.strftime("%Y-%m-%d").values

date = trading_dates[-1]

vix_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/I:VIX1D/range/1/day/2023-05-01/{date}?sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
vix_data.index = pd.to_datetime(vix_data.index, unit="ms", utc=True).tz_convert("America/New_York")
vix_data["1_mo_avg"] = vix_data["c"].rolling(window=30).mean()
vix_data["3_mo_avg"] = vix_data["c"].rolling(window=63).mean()
vix_data["6_mo_avg"] = vix_data["c"].rolling(window=126).mean()
vix_data['vol_regime'] = vix_data.apply(lambda row: 1 if (row['1_mo_avg'] > row['3_mo_avg']) else 0, axis=1)
vix_data["str_date"] = vix_data.index.strftime("%Y-%m-%d")

# Define the volatility regime
vol_regime = vix_data["vol_regime"].iloc[-1]

big_underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2020-01-01/{date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
big_underlying_data.index = pd.to_datetime(big_underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
big_underlying_data["1_mo_avg"] = big_underlying_data["c"].rolling(window=20).mean()
big_underlying_data["3_mo_avg"] = big_underlying_data["c"].rolling(window=60).mean()
big_underlying_data['regime'] = big_underlying_data.apply(lambda row: 1 if (row['c'] > row['1_mo_avg']) else 0, axis=1)

# Define the regime of the underlying asset
trend_regime = big_underlying_data['regime'].iloc[-1]

ticker = "I:SPX"
index_ticker = "I:VIX1D"
options_ticker = "SPX"

trading_date = datetime.now().strftime("%Y-%m-%d")

underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{trading_date}/{trading_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
underlying_data.index = pd.to_datetime(underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")

index_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{index_ticker}/range/1/minute/{trading_date}/{trading_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
index_data.index = pd.to_datetime(index_data.index, unit="ms", utc=True).tz_convert("America/New_York")

index_price = index_data[index_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]
price = underlying_data[underlying_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]

expected_move = (round((index_price / np.sqrt(252)), 2)/100)*.50

exp_date = trading_date

if trend_regime == 0:

    valid_calls = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={options_ticker}&contract_type=call&as_of={trading_date}&expiration_date={exp_date}&limit=1000&apiKey={polygon_api_key}").json()["results"])
    valid_calls["days_to_exp"] = (pd.to_datetime(valid_calls["expiration_date"]) - pd.to_datetime(trading_date)).dt.days
    valid_calls["distance_from_price"] = abs(valid_calls["strike_price"] - price)
    
    upper_price = round(price + (price * expected_move))
    otm_calls = valid_calls[valid_calls["strike_price"] >= upper_price]
    
    short_call = otm_calls.iloc[[0]]
    long_call = otm_calls.iloc[[1]]
    
    short_strike = short_call["strike_price"].iloc[0]
    long_strike = long_call["strike_price"].iloc[0]
    
    short_ticker_polygon = short_call["ticker"].iloc[0]
    long_ticker_polygon = long_call["ticker"].iloc[0]
 
elif trend_regime == 1:
    
    valid_puts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={options_ticker}&contract_type=put&as_of={trading_date}&expiration_date={exp_date}&limit=1000&apiKey={polygon_api_key}").json()["results"])
    valid_puts["days_to_exp"] = (pd.to_datetime(valid_puts["expiration_date"]) - pd.to_datetime(trading_date)).dt.days
    valid_puts["distance_from_price"] = abs(price - valid_puts["strike_price"])
    
    lower_price = round(price - (price * expected_move))
    otm_puts = valid_puts[valid_puts["strike_price"] <= lower_price].sort_values("distance_from_price", ascending = True)
    
    short_put = otm_puts.iloc[[0]]
    long_put = otm_puts.iloc[[1]]
    
    short_strike = short_put["strike_price"].iloc[0]
    long_strike = long_put["strike_price"].iloc[0]
    
    short_ticker_polygon = short_put["ticker"].iloc[0]
    long_ticker_polygon = long_put["ticker"].iloc[0]
    

# =============================================================================
# Pulling the option via Tastytrade    
# =============================================================================

option_url = f"https://api.tastyworks.com/option-chains/SPX/nested"

option_chain = pd.json_normalize(requests.get(option_url,  headers = {'Authorization': session_token}).json()["data"]["items"][0]["expirations"][0]["strikes"])
option_chain["strike_price"] = option_chain["strike-price"].astype(float)

short_option = option_chain[option_chain["strike_price"] == short_strike].copy()
long_option = option_chain[option_chain["strike_price"] == long_strike].copy()

if trend_regime == 0:

    short_ticker = short_option["call"].iloc[0]
    long_ticker = long_option["call"].iloc[0]
    
elif trend_regime == 1:
    
    short_ticker = short_option["put"].iloc[0]
    long_ticker = long_option["put"].iloc[0]
    

# =============================================================================
# Get most recent bid/ask
# =============================================================================

short_option_quote = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/quotes/{short_ticker_polygon}?&sort=timestamp&order=desc&limit=10&apiKey={polygon_api_key}").json()["results"]).set_index("sip_timestamp").sort_index().tail(1)
short_option_quote.index = pd.to_datetime(short_option_quote.index, unit = "ns", utc = True).tz_convert("America/New_York")

long_option_quote = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/quotes/{long_ticker_polygon}?&sort=timestamp&order=desc&limit=10&apiKey={polygon_api_key}").json()["results"]).set_index("sip_timestamp").sort_index().tail(1)
long_option_quote.index = pd.to_datetime(long_option_quote.index, unit = "ns", utc = True).tz_convert("America/New_York")

natural_price = round(short_option_quote["bid_price"].iloc[0] - long_option_quote["ask_price"].iloc[0], 2)
mid_price = round(((short_option_quote["bid_price"].iloc[0] + short_option_quote["ask_price"].iloc[0]) / 2) - ((long_option_quote["bid_price"].iloc[0] + long_option_quote["ask_price"].iloc[0]) / 2), 2)

optimal_price = np.int64((mid_price - .05) / .05) * .05

order_details = {
    "time-in-force": "Day",
    "order-type": "Limit",
    "price": optimal_price,
    "price-effect": "Credit",
    "legs": [{"action": "Buy to Open",
          "instrument-type": "Equity Option",
          "symbol": f"{long_ticker}",
          "quantity": 1},
        
          {"action": "Sell to Open",
          "instrument-type": "Equity Option",
          "symbol": f"{short_ticker}",
          "quantity": 1}]
    
                }

# Do an order dry-run to make sure the trade will go through (i.e., verifies balance, valid symbol, etc. )

validate_order = requests.post(f"https://api.tastyworks.com/accounts/{account_number}/orders/dry-run", json = order_details, headers = {'Authorization': session_token})
validation_text = validate_order.text

submit_order = requests.post(f"{base_url}/accounts/{account_number}/orders", json = order_details, headers = {'Authorization': session_token})
order_submission_text = submit_order.text
