#!/usr/bin/env python3
import requests, json, os, time

# 1) Where to write fundamentals.json
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
out_path  = os.path.join(repo_root, "fundamentals.json")

# 2) Symbols
SYMS = ["AAPL","IBM","PLTR"]

funds = {}
for sym in SYMS:
    print(f"⏳ Fetching fundamentals for {sym}…")
    url    = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}"
    params = {"modules":"price,summaryDetail"}
    resp   = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    result = resp.json().get("quoteSummary",{}).get("result",[])
    if not result:
        print(f"❌ No data for {sym}, skipping.")
        continue

    node   = result[0]
    price  = node["price"]
    detail = node["summaryDetail"]

    # extract trailing PE
    pe = price.get("trailingPE", None)
    # extract dividendYield.raw (decimal), turn into %
    dy = detail.get("dividendYield",{}).get("raw", 0.0) * 100

    funds[sym] = {
      "pe":    round(pe,1) if pe else None,
      "div_y": round(dy,1)
    }

    time.sleep(1)  # 1s pause

# 3) Write out
with open(out_path, "w") as f:
    json.dump(funds, f, indent=2)
print("→ Written fundamentals.json")
