#!/usr/bin/env python3
"""
Fetch stock data for the Investment Decisions PILOT 2 — two-stock (pair-allocation) design.

Writes to decisions_pilot2/ (NEW folder) so the closed pilot-1 files under
decisions_pilot/stock/current/ stay frozen as the fielded record.

Slate (picked 2026-08-31 from a 76-ticker GICS-45 screen; 8 fully independent
pairs, stock A = higher past-return leg; charts vetted for single-day jumps):
  p1  CSCO (+59%)  vs INTU (-46%)   flagship, both fielded in pilot 1
  p2  NTAP (+66%)  vs EPAM (-35%)   striking #2
  p3  JBL  (+47%)  vs TYL  (-33%)   striking, obscure mid-caps
  p4  ADI  (+44%)  vs ACN  (-27%)   striking, large caps, clean charts
  p5  AKAM (+36%)  vs NOW  (-21%)   fielded pair #2 (continuity with pilot 1)
  p6  FFIV (+26%)  vs CHKP (-28%)   near-symmetric magnitudes (pure direction test)
  p7  ZM   (+21%)  vs ADBE (-18%)   moderate rung, familiar names
  p8  TXN  (+28%)  vs QCOM (+2%)    same-direction, intensity-only pair

NOTE: for a LAUNCH-DAY refresh re-run THIS script (prices + fundamentals) or
clone the prices-only variant with this slate; fundamentals should be pinned
close to launch.

Methodology identical to fetch_stocks.py (pilot 1):
  - P/B percentile vs WRDS GICS-45 peers (mktcap >= $10B floor), tertile labels
  - market cap / dividend yield / P/B / P/E / TTM net profit via yfinance
"""

import importlib.util
import json
import os
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("fs", os.path.join(HERE, "fetch_stocks.py"))
fs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fs)

STOCKS = ["CSCO", "INTU", "NTAP", "EPAM", "JBL", "TYL", "ADI", "ACN",
          "AKAM", "NOW", "FFIV", "CHKP", "ZM", "ADBE", "TXN", "QCOM"]
SECTOR_LABEL = {s: "Technology" for s in STOCKS}

BASE_DIR = os.path.join(fs.REPO_ROOT, "decisions_pilot2")


def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    run_dir = os.path.join(BASE_DIR, "stock", "runs", f"run_{date_str}")
    cur_dir = os.path.join(BASE_DIR, "stock", "current")
    for d in (run_dir, cur_dir):
        os.makedirs(d, exist_ok=True)

    summary = {"started_at": datetime.now().isoformat(timespec="seconds"),
               "stocks": [], "errors": []}

    print("📊 Loading WRDS sector-level P/B data (gsec=45)…")
    ticker_pbs, all_pbs = fs.load_wrds_sector_pbs(fs.WRDS_CSV, gsector="45")
    print(f"  {len(ticker_pbs)} IT/Technology stocks with P/B > 0")

    print(f"\n{'=' * 50}\nFETCHING STOCK PRICE DATA\n{'=' * 50}")
    for sym in STOCKS:
        print(f"⏳ {sym}…")
        pts = fs.fetch_price_data(sym)
        if pts:
            out = os.path.join(run_dir, sym.lower() + "_365d.json")
            fs.write_json(out, {"prices": pts})
            fs.copy_to_current(out, cur_dir)
            ret = round((pts[-1][1] - pts[0][1]) / pts[0][1] * 100, 1)
            print(f"  ✅ {len(pts)} points, ret={ret:+.1f}%")
            summary["stocks"].append({"symbol": sym, "points": len(pts), "return_pct": ret})
        else:
            print(f"  ❌ Failed: {sym}")
            summary["errors"].append({"symbol": sym, "type": "price"})
        time.sleep(fs.SLEEP_SEC)

    print(f"\n{'=' * 50}\nFETCHING FUNDAMENTALS\n{'=' * 50}")
    fundamentals = {}
    for sym in STOCKS:
        print(f"⏳ {sym}…")
        raw = fs.fetch_fundamentals_yf(sym)
        pb_yahoo = pe_yahoo = netprofit_m = netprofit_src = mc_millions = div_pct = None
        if raw is not None:
            mc_raw = raw.get("marketcap_raw")
            div_raw = raw.get("div_y_raw")
            pb_yahoo = round(raw["pb_yahoo"], 2) if raw.get("pb_yahoo") else None
            pe_yahoo = round(raw["pe_yahoo"], 1) if raw.get("pe_yahoo") else None
            netprofit_m = raw.get("netprofit_m")
            netprofit_src = raw.get("netprofit_src")
            mc_millions = round(mc_raw / 1_000_000, 2) if mc_raw else None
            if div_raw is not None:
                converted = round(div_raw * 100, 2)
                div_pct = converted if converted <= 20 else round(div_raw, 2)
        else:
            summary["errors"].append({"symbol": sym, "type": "fundamentals_yf"})

        pb_pctile = fs.pctile_of_value(pb_yahoo, ticker_pbs, exclude=sym)
        valuation = fs.pb_to_valuation_tertile(pb_pctile)
        fundamentals[sym] = {
            "marketcap": mc_millions, "pb_current": pb_yahoo,
            "pb_current_pctile": pb_pctile, "pb_yahoo": pb_yahoo,
            "pe_yahoo": pe_yahoo, "div_y": div_pct,
            "netprofit": netprofit_m, "netprofit_src": netprofit_src,
            "valuation": valuation, "sector": SECTOR_LABEL[sym],
        }
        print(f"  ✅ mcap={mc_millions}M, PB={pb_yahoo} ({pb_pctile}th {valuation}), div={div_pct}%")
        time.sleep(fs.SLEEP_SEC)

    fs.write_json(os.path.join(run_dir, "fundamentals.json"), fundamentals)
    fs.write_json(os.path.join(cur_dir, "fundamentals.json"), fundamentals)

    summary["fundamentals"] = fundamentals
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    fs.write_json(os.path.join(run_dir, "summary.json"), summary)
    fs.write_json(os.path.join(cur_dir, "summary.json"), summary)

    print(f"\n{'=' * 50}\nGIT COMMIT & PUSH\n{'=' * 50}")
    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors — skipping git commit/push.")
    elif not fs.PUSH:
        print("ℹ️  FETCH_PUSH=0 — skipping git commit/push (data written locally).")
    else:
        fs.git_commit_and_push(fs.REPO_ROOT, [os.path.join(BASE_DIR, "stock")])

    print("\n🏁 Done.")
    print(f"CDN base (after push): https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@latest/decisions_pilot2/stock/current/")


if __name__ == "__main__":
    main()
