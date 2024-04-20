# -*- coding: utf-8 -*-
"""
Created in 2024

@author: Quant Galore
"""

import requests
import pandas as pd
import numpy as np
import mysql.connector
import sqlalchemy
import matplotlib.pyplot as plt
import time

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar
# from self_email import send_message

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
# vix_data = vix_data.set_index("str_date")

vol_regime = vix_data["vol_regime"].iloc[-1]

big_underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2020-01-01/{date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
big_underlying_data.index = pd.to_datetime(big_underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
big_underlying_data["1_mo_avg"] = big_underlying_data["c"].rolling(window=20).mean()
big_underlying_data["3_mo_avg"] = big_underlying_data["c"].rolling(window=60).mean()
big_underlying_data['regime'] = big_underlying_data.apply(lambda row: 1 if (row['c'] > row['1_mo_avg']) else 0, axis=1)

trend_regime = big_underlying_data['regime'].iloc[-1]

ticker = "I:SPX"
index_ticker = "I:VIX1D"
options_ticker = "SPX"

trade_list = []

if pd.to_datetime(date).strftime("%A") == "Friday":
    pass
else:
    date = trading_dates[-2]

while 1:
    
    try:
    
        underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        underlying_data.index = pd.to_datetime(underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        index_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{index_ticker}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        index_data.index = pd.to_datetime(index_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        index_price = index_data[index_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]
        price = underlying_data[underlying_data.index.time >= pd.Timestamp("09:35").time()]["c"].iloc[0]
        
        expected_move = (round((index_price / np.sqrt(252)), 2)/100)*.50
        
        lower_price = round(price - (price * expected_move))
        upper_price = round(price + (price * expected_move))
        
        exp_date = date
        
        if trend_regime == 0:
            
            valid_calls = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={options_ticker}&contract_type=call&as_of={date}&expiration_date={exp_date}&limit=1000&apiKey={polygon_api_key}").json()["results"])
            valid_calls = valid_calls[valid_calls["ticker"].str.contains("SPXW")].copy()
            valid_calls["days_to_exp"] = (pd.to_datetime(valid_calls["expiration_date"]) - pd.to_datetime(date)).dt.days
            valid_calls["distance_from_price"] = abs(valid_calls["strike_price"] - price)
            
            otm_calls = valid_calls[valid_calls["strike_price"] >= upper_price]
            
            short_call = otm_calls.iloc[[0]]
            long_call = otm_calls.iloc[[1]]
            
            short_strike = short_call["strike_price"].iloc[0]
            long_strike = long_call["strike_price"].iloc[0]
            
            short_call_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{short_call['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            short_call_ohlcv.index = pd.to_datetime(short_call_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York") 
            
            long_call_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{long_call['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            long_call_ohlcv.index = pd.to_datetime(long_call_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York") 
            
            spread = pd.concat([short_call_ohlcv.add_prefix("short_call_"), long_call_ohlcv.add_prefix("long_call_")], axis = 1).dropna()
            spread = spread[spread.index.time >= pd.Timestamp("09:35").time()].copy()
            spread["spread_value"] = spread["short_call_c"] - spread["long_call_c"]
            
            underlying_data["distance_from_short_strike"] = round(((short_strike - underlying_data["c"]) / underlying_data["c"].iloc[0])*100, 2)
            
            cost = spread["spread_value"].iloc[0]
            final_value = spread["spread_value"].iloc[-1]
            
            gross_pnl = cost - final_value
            gross_pnl_percent = round((gross_pnl / cost)*100,2)
            
            print(f"Live PnL: ${round(gross_pnl*100,2)} | {gross_pnl_percent}% | {spread.index[-1].strftime('%H:%M')}")
            print(f"Short Strike: {short_strike} | % Away from strike: {underlying_data['distance_from_short_strike'].iloc[-1]}%")
            time.sleep(10)
            
        elif trend_regime == 1:
        
            valid_puts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={options_ticker}&contract_type=put&as_of={date}&expiration_date={exp_date}&limit=1000&apiKey={polygon_api_key}").json()["results"])
            valid_puts = valid_puts[valid_puts["ticker"].str.contains("SPXW")].copy()
            valid_puts["days_to_exp"] = (pd.to_datetime(valid_puts["expiration_date"]) - pd.to_datetime(date)).dt.days
            valid_puts["distance_from_price"] = abs(price - valid_puts["strike_price"])
            
            otm_puts = valid_puts[valid_puts["strike_price"] <= lower_price].sort_values("distance_from_price", ascending = True)
            
            short_put = otm_puts.iloc[[0]]
            long_put = otm_puts.iloc[[1]]
            
            short_strike = short_put["strike_price"].iloc[0]
            long_strike = long_put["strike_price"].iloc[0]
    
            short_put_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{short_put['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            short_put_ohlcv.index = pd.to_datetime(short_put_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York")   
            
            long_put_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{long_put['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            long_put_ohlcv.index = pd.to_datetime(long_put_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York")
            
            spread = pd.concat([short_put_ohlcv.add_prefix("short_put_"), long_put_ohlcv.add_prefix("long_put_")], axis = 1).dropna()
            spread = spread[spread.index.time >= pd.Timestamp("09:35").time()].copy()
            spread["spread_value"] = spread["short_put_c"] - spread["long_put_c"]
            
            underlying_data["distance_from_short_strike"] = round(((underlying_data["c"] - short_strike) / short_strike)*100, 2)
            
            cost = spread["spread_value"].iloc[0]
            final_value = spread["spread_value"].iloc[-1]
            
            gross_pnl = cost - final_value
            gross_pnl_percent = round((gross_pnl / cost)*100,2)
        
            print(f"\nLive PnL: ${round(gross_pnl*100,2)} | {gross_pnl_percent}% | {spread.index[-1].strftime('%H:%M')}")
            print(f"Short Strike: {short_strike} | % Away from strike: {underlying_data['distance_from_short_strike'].iloc[-1]}%")
            
            time.sleep(10)
        
    except Exception as data_error:
        print(data_error)
        continue