#!/usr/bin/env bash
# scripts/update_fundamentals_fast.sh

python3 - <<'PYCODE'
import yfinance as yf, json, os, time

# Save in current working directory (no subfolder)
out_path = os.path.join(os.getcwd(), "fundamentals.json")

T = ["NVDA","PAYC","TER", "TGT", "COST"]
sector_map = {
    "NVDA": "Technology",
    "PAYC": "Technology",
    "TER": "Technology",
    "TGT": "Consumer Staples",
    "COST": "Consumer Staples"
}
funds = {}
for sym in T:
    print(f"⏳ Fetching fundamentals for {sym}…")
    t = yf.Ticker(sym).info
    eps = t.get("trailingEps")
    pr  = t.get("regularMarketPrice")
    pe  = round(pr/eps,1) if eps and pr else None
    dy  = round(t.get("dividendYield",0)*100,1)
    funds[sym] = {
        "eps": eps,
        "pe": pe,
        "div_y": dy,
        "sector": sector_map[sym]
    }
    time.sleep(10)
with open(out_path, "w") as f: json.dump(funds, f, indent=2)
print("→ Written fundamentals.json to", out_path)
PYCODE