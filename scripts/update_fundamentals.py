#!/usr/bin/env python3
"""
Fetches P/E ratio and dividend yield for AAPL, IBM, PLTR
using yfinance + curl_cffi to avoid YFRateLimitError.
Writes fundamentals.json in the repo root.
"""

import json
import os
import time
from curl_cffi import requests      # pip install curl_cffi
import yfinance as yf               # ensure yfinance>=0.2.54

# 1) Prepare a Chrome-impersonating session
session = requests.Session(impersonate="chrome124")

# 2) Tickers to fetch
TICKERS = ["AAPL", "IBM", "PLTR"]

# 3) Where to write fundamentals.json
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root  = os.path.dirname(script_dir)
out_path   = os.path.join(repo_root, "fundamentals.json")

funds = {}
for symbol in TICKERS:
    print(f"⏳ Fetching fundamentals for {symbol}…")
    # Inject the browser-impersonating session
    ticker = yf.Ticker(symbol, session=session)
    info   = ticker.info

    # Extract trailing EPS and current price
    eps = info.get("trailingEps")
    price = info.get("regularMarketPrice")
    pe_ratio = round(price/eps, 1) if eps and price else None

    # Extract dividendYield (decimal) → percent
    dy_decimal = info.get("dividendYield", 0.0)
    dividend_yield_pct = round(dy_decimal * 100, 1)

    funds[symbol] = {
        "eps":    eps,
        "pe":     pe_ratio,
        "div_y":  dividend_yield_pct
    }

    # 1s pause between calls
    time.sleep(1)

# 4) Write out the JSON
with open(out_path, "w") as f:
    json.dump(funds, f, indent=2)

print(f"✅ Wrote fundamentals to {out_path}")
