#!/usr/bin/env python3
"""
Fetches 180-day adjusted close price series for AAPL, IBM, PLTR
using yfinance with a curl_cffi session to avoid YFRateLimitError.
"""
import json, os, time
from datetime import datetime, timedelta

# 1) Install dependencies: pip install yfinance curl_cffi
from curl_cffi import requests                  # curl_cffi for browser impersonation :contentReference[oaicite:5]{index=5}
import yfinance as yf                           # yfinance ≥ 0.2.54 :contentReference[oaicite:6]{index=6}

# 2) Create a session that mimics Chrome
session = requests.Session(impersonate="chrome124")  # pick latest Chrome fingerprint :contentReference[oaicite:7]{index=7}

# 3) Prepare tickers and date window
TICKERS = ["AAPL", "IBM", "PLTR"]
end = datetime.now()
start = end - timedelta(days=180)

# 4) Repo root for writing JSON
repo_root = os.path.dirname(os.path.abspath(__file__))

for sym in TICKERS:
    print(f"⏳ Fetching 180-day data for {sym}…")
    # Inject our curl_cffi session into yf.Ticker
    ticker = yf.Ticker(sym, session=session)    # avoids rate-limit :contentReference[oaicite:8]{index=8}
    df     = ticker.history(
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        auto_adjust=True
    )

    # 5) Build point list [timestamp_ms, close]
    pts = [
        [int(time.mktime(idx.timetuple()) * 1000), round(row["Close"], 2)]
        for idx, row in df.iterrows()
    ]

    # 6) Write JSON
    out_path = os.path.join(repo_root, f"{sym}_180d.json")
    with open(out_path, "w") as f:
        json.dump({"prices": pts}, f, indent=2)
    print(f" → Wrote {sym}_180d.json ({len(pts)} points)")

    # 7) Brief pause to stay polite (not strictly required now)
    time.sleep(1)

print("✅ All price series updated.")
