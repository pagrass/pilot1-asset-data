#!/usr/bin/env bash
# scripts/update_fundamentals_fast.sh

python3 - <<'PYCODE'
import yfinance as yf, json, os, time

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
out_path  = os.path.join(repo_root, "fundamentals.json")
T = ["NVDA","TER","PAYC"]
funds = {}
for sym in T:
    print(f"⏳ Fetching fundamentals for {sym}…")
    t = yf.Ticker(sym).info
    eps = t.get("trailingEps")
    pr  = t.get("regularMarketPrice")
    pe  = round(pr/eps,1) if eps and pr else None
    dy  = round(t.get("dividendYield",0)*100,1)
    funds[sym] = {"eps": eps, "pe": pe, "div_y": dy}
    time.sleep(10)
with open(out_path, "w") as f: json.dump(funds, f, indent=2)
print("→ Written fundamentals.json")
PYCODE

