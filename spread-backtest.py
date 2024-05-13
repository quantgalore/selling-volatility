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

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

trading_dates = calendar.schedule(start_date = "2023-05-01", end_date = (datetime.today()-timedelta(days = 1))).index.strftime("%Y-%m-%d").values

ticker = "I:SPX"
index_ticker = "I:VIX1D"
options_ticker = "SPX"

trade_list = []
times = []

for date in trading_dates[1:]:
    
    try:
        
        start_time = datetime.now()
        
        prior_day = trading_dates[np.where(trading_dates==date)[0][0]-1]
        
        prior_day_underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{prior_day}/{prior_day}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        prior_day_underlying_data.index = pd.to_datetime(prior_day_underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        big_underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2020-01-01/{prior_day}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        big_underlying_data.index = pd.to_datetime(big_underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        big_underlying_data["1_mo_avg"] = big_underlying_data["c"].rolling(window=20).mean()
        big_underlying_data["3_mo_avg"] = big_underlying_data["c"].rolling(window=60).mean()
        big_underlying_data['regime'] = big_underlying_data.apply(lambda row: 1 if (row['c'] > row['1_mo_avg']) else 0, axis=1)
    
        underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        underlying_data.index = pd.to_datetime(underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        index_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{index_ticker}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        index_data.index = pd.to_datetime(index_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        etf_underlying_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
        etf_underlying_data.index = pd.to_datetime(etf_underlying_data.index, unit="ms", utc=True).tz_convert("America/New_York")
        
        underlying_data = underlying_data[underlying_data.index.time >= pd.Timestamp("09:35").time()].copy()
        index_data = index_data[index_data.index.time >= pd.Timestamp("09:35").time()].copy()
        etf_underlying_data = etf_underlying_data[etf_underlying_data.index.time >= pd.Timestamp("09:35").time()].copy()
        
        index_price = index_data["c"].iloc[0]        
        price = underlying_data["c"].iloc[0]
        
        expected_move = (round((index_price / np.sqrt(252)), 2)/100)*.50
        
        lower_price = round(price - (price * expected_move))
        upper_price = round(price + (price * expected_move))
        
        exp_date = date
        
        # Pull the data at 9:35 to represent the most up-to-date regime that would be available
        concatenated_regime_dataset = pd.concat([big_underlying_data, etf_underlying_data.head(1)], axis = 0)
        concatenated_regime_dataset["1_mo_avg"] = concatenated_regime_dataset["c"].rolling(window=20).mean()
        concatenated_regime_dataset["3_mo_avg"] = concatenated_regime_dataset["c"].rolling(window=60).mean()
        concatenated_regime_dataset['regime'] = concatenated_regime_dataset.apply(lambda row: 1 if (row['c'] > row['1_mo_avg']) else 0, axis=1)
        
        direction = concatenated_regime_dataset["regime"].iloc[-1]

        if direction == 0:
            
            valid_calls = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={options_ticker}&contract_type=call&as_of={date}&expiration_date={exp_date}&limit=1000&apiKey={polygon_api_key}").json()["results"])
            valid_calls = valid_calls[valid_calls["ticker"].str.contains("SPXW")].copy()
            valid_calls["days_to_exp"] = (pd.to_datetime(valid_calls["expiration_date"]) - pd.to_datetime(date)).dt.days
            valid_calls["distance_from_price"] = abs(valid_calls["strike_price"] - price)
            
            otm_calls = valid_calls[valid_calls["strike_price"] >= upper_price]
            
            short_call = otm_calls.iloc[[0]]
            long_call = otm_calls.iloc[[1]]
            
            short_call_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{short_call['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            short_call_ohlcv.index = pd.to_datetime(short_call_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York") 
            
            long_call_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{long_call['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            long_call_ohlcv.index = pd.to_datetime(long_call_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York") 
            
            spread = pd.concat([short_call_ohlcv.add_prefix("short_call_"), long_call_ohlcv.add_prefix("long_call_")], axis = 1).dropna()
            spread = spread[spread.index.time >= pd.Timestamp("09:35").time()].copy()
            spread["spread_value"] = spread["short_call_c"] - spread["long_call_c"]
            cost = spread["spread_value"].iloc[0]
            
            underlying_data["distance_from_short_strike"] = round(((short_call["strike_price"].iloc[0] - underlying_data["c"]) / underlying_data["c"].iloc[0])*100, 2)
            
            
        elif direction == 1:
        
            valid_puts = pd.json_normalize(requests.get(f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={options_ticker}&contract_type=put&as_of={date}&expiration_date={exp_date}&limit=1000&apiKey={polygon_api_key}").json()["results"])
            valid_puts = valid_puts[valid_puts["ticker"].str.contains("SPXW")].copy()
            valid_puts["days_to_exp"] = (pd.to_datetime(valid_puts["expiration_date"]) - pd.to_datetime(date)).dt.days
            valid_puts["distance_from_price"] = abs(price - valid_puts["strike_price"])
            
            otm_puts = valid_puts[valid_puts["strike_price"] <= lower_price].sort_values("distance_from_price", ascending = True)
            
            short_put = otm_puts.iloc[[0]]
            long_put = otm_puts.iloc[[1]]
        
            short_put_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{short_put['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            short_put_ohlcv.index = pd.to_datetime(short_put_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York")   
            
            long_put_ohlcv = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{long_put['ticker'].iloc[0]}/range/1/minute/{date}/{date}?adjusted=true&sort=asc&limit=1000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
            long_put_ohlcv.index = pd.to_datetime(long_put_ohlcv.index, unit = "ms", utc = True).tz_convert("America/New_York")
            
            spread = pd.concat([short_put_ohlcv.add_prefix("short_put_"), long_put_ohlcv.add_prefix("long_put_")], axis = 1).dropna()
            spread = spread[spread.index.time >= pd.Timestamp("09:35").time()].copy()
            spread["spread_value"] = spread["short_put_c"] - spread["long_put_c"]
            cost = spread["spread_value"].iloc[0]
            
            underlying_data["distance_from_short_strike"] = round(((underlying_data["c"] - short_put["strike_price"].iloc[0]) / short_put["strike_price"].iloc[0])*100, 2)
            
        final_value = spread["spread_value"].iloc[-1]
        gross_pnl = cost - final_value
        gross_pnl_percent = round((gross_pnl / cost)*100,2)
        
        trade_data = pd.DataFrame([{"date": date, "cost": cost, "final_price": final_value, "gross_pnl": gross_pnl, "gross_pnl_percent": gross_pnl_percent, "ticker": ticker, "direction": direction}])

        trade_list.append(trade_data)
            
        end_time = datetime.now()
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round((np.where(trading_dates==date)[0][0]/len(trading_dates))*100,2)
        iterations_remaining = len(trading_dates) - np.where(trading_dates==date)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

    except Exception as data_error:
        print(data_error)
        continue
    
all_trades = pd.concat(trade_list).drop_duplicates("date").set_index("date")
all_trades.index = pd.to_datetime(all_trades.index).tz_localize("America/New_York")

all_trades["max_loss"] = (5 - all_trades["cost"])
all_trades['gross_pnl'] = all_trades.apply(lambda row: row['max_loss']*-1 if (row['gross_pnl'] < row['max_loss']*-1) else row["gross_pnl"], axis=1)

all_trades["contracts"] = (5 / all_trades["max_loss"]).astype(int)
all_trades["max_loss"] = (all_trades["max_loss"]) * all_trades["contracts"]
all_trades["fees"] = all_trades["contracts"] * .04
all_trades["net_pnl"] = (all_trades["gross_pnl"] * all_trades["contracts"]) - all_trades["fees"]

all_trades["net_capital"] = 2000 + (all_trades["net_pnl"]*100).cumsum()

####

monthly = all_trades.resample("M").sum(numeric_only=True)

total_return = round(((all_trades["net_capital"].iloc[-1] - 2000) / 2000)*100, 2)
sd = round(all_trades["gross_pnl_percent"].std(), 2)

wins = all_trades[all_trades["net_pnl"] > 0]
losses = all_trades[all_trades["net_pnl"] < 0]

avg_win = wins["net_pnl"].mean()
avg_loss = losses["net_pnl"].mean()

win_rate = round(len(wins) / len(all_trades), 2)

expected_value = round((win_rate * avg_win) + ((1-win_rate) * avg_loss), 2)

plt.figure(dpi=200)
plt.xticks(rotation=45)
plt.suptitle("Selling 0-DTE Credit Spreads - Trend Following")
plt.plot(all_trades.index, all_trades["net_capital"])
plt.legend(["Net PnL (Incl. Fees)"])
plt.show()

print(f"EV per trade: ${expected_value*100}")
print(f"Win Rate: {win_rate*100}%")
print(f"Avg Profit: ${round(avg_win*100,2)}")
print(f"Avg Loss: ${round(avg_loss*100,2)}")
print(f"Total Profit: ${all_trades['net_pnl'].sum()*100}")
