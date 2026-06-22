#!/usr/bin/env python3
"""
Fetch stock data for Visual Similarity Pilot — Wave 2.

Stocks:
  Low-returns pair:  CRM (Salesforce), NOW (ServiceNow)
  High-returns pair: ARM (Arm), STM (STMicroelectronics)
  Anchors:           MSFT (Microsoft), CSCO (Cisco)

Fundamentals:
  - P/B percentile computed at SECTOR level (gsec=45, IT/Technology)
  - Valuation labels via tertiles: Low (<=33rd), Mid (33-67th), High (>=67th)
  - Market cap and dividend yield from yfinance
  - P/B ratio from Compustat (WRDS)

Output: visualsimilarity/stock/current/ + visualsimilarity/stock/runs/run_YYYY-MM-DD/
"""

import csv
import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime

try:
    import yfinance as yf
    from curl_cffi import requests as curl_requests
    _YF_SESSION = curl_requests.Session(impersonate="chrome")
    _YF_AVAILABLE = True
except Exception:  # yfinance/curl_cffi not installed -> P/B (WRDS) + returns still work
    yf = None
    _YF_SESSION = None
    _YF_AVAILABLE = False

# ======================== Config ========================

STOCKS = ["NVDA", "ADSK", "DDOG", "MSFT", "ZS", "LFUS"]

SECTOR_LABEL = {
    "NVDA": "Technology",
    "ADSK": "Technology",
    "DDOG": "Technology",
    "MSFT": "Technology",
    "ZS":   "Technology",
    "LFUS": "Technology",
}

WRDS_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "WRDS",
    "pb_ratio_sectors_40_45_with_industry.csv"
)

REPO_ROOT = "/Users/paulgrass/Library/Mobile Documents/com~apple~CloudDocs/Documents/Programming/Git/pilot3-asset-data"
BASE_DIR  = os.path.join(REPO_ROOT, "returndrivers5")

SLEEP_SEC   = int(os.environ.get("FETCH_SLEEP", "30"))
MAX_RETRIES = 3
RETRY_DELAY = 10
# Auto commit+push at the end (default on, preserving the original workflow).
# Set FETCH_PUSH=0 to fetch/write only (e.g. for a controlled manual commit).
PUSH = os.environ.get("FETCH_PUSH", "1") != "0"
# Percentile peer set = GICS-45 firms at/above this market cap ($M); drops very small
# firms whose P/B isn't a meaningful comparison. (Negative/zero P/B excluded separately.)
PEER_MKTCAP_FLOOR = int(os.environ.get("PEER_MKTCAP_FLOOR", "5000"))

# ======================== Helpers ========================

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def copy_to_current(src_path, current_dir):
    os.makedirs(current_dir, exist_ok=True)
    dst = os.path.join(current_dir, os.path.basename(src_path))
    with open(src_path, "rb") as s, open(dst, "wb") as d:
        d.write(s.read())


def fetch_price_data(ticker, max_retries=MAX_RETRIES):
    """Download 365-day price history via Yahoo Finance chart API."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?range=1y&interval=1d"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]
            pts = [
                [int(ts * 1000), round(float(c), 2)]
                for ts, c in zip(timestamps, closes)
                if c is not None
            ]
            if not pts:
                raise ValueError("No valid close prices.")
            return pts
        except Exception as e:
            print(f"   ⚠️  Attempt {attempt}/{max_retries} for {ticker}: {e}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
    return None


def fetch_fundamentals_yf(ticker):
    """Fetch market cap and dividend yield from yfinance."""
    if not _YF_AVAILABLE:
        return None
    try:
        info = yf.Ticker(ticker, session=_YF_SESSION).info
        return {
            "marketcap_raw": info.get("marketCap"),
            "div_y_raw":     info.get("dividendYield"),
            "pb_yahoo":      info.get("priceToBook"),
            "pe_yahoo":      info.get("trailingPE"),
        }
    except Exception as e:
        print(f"   ⚠️  yfinance failed for {ticker}: {e}")
        return None


def load_wrds_sector_pbs(csv_path, gsector="45", mktcap_floor=PEER_MKTCAP_FLOOR):
    """
    Load P/B values for a GICS sector from WRDS, used as the percentile peer set.
    Excludes non-interpretable P/B (<= 0) and very small firms (mktcap < floor, $M).
    Returns dict: {ticker: pb_value} and sorted list of all PBs.
    """
    ticker_pbs = {}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["gsector"] != gsector:
                continue
            try:
                pb = float(row["pb_ratio"])
                mktcap = float(row["mktcap"])
            except (TypeError, ValueError):
                continue
            if pb <= 0:                  # negative/zero book value -> not interpretable
                continue
            if mktcap < mktcap_floor:    # drop very small firms
                continue
            tic = row["tic"].strip().upper()
            # Keep latest entry per ticker (last row wins)
            ticker_pbs[tic] = pb
    all_pbs = sorted(ticker_pbs.values())
    return ticker_pbs, all_pbs


def compute_pb_percentile_sector(stock_ticker, ticker_pbs, all_pbs):
    """Compute sector-level PB percentile (excluding self)."""
    stock_pb = ticker_pbs.get(stock_ticker)
    if stock_pb is None:
        return None, None
    peer_vals = [p for t, p in ticker_pbs.items() if t != stock_ticker]
    if not peer_vals:
        return None, None
    below = sum(1 for p in peer_vals if p < stock_pb)
    pctile = round(100 * below / len(peer_vals))
    return round(stock_pb, 2), pctile


def pctile_of_value(pb_value, ticker_pbs, exclude=None):
    """Percentile of an arbitrary P/B value (e.g. current Yahoo) vs the WRDS sector peers."""
    if pb_value is None:
        return None
    peer_vals = [p for t, p in ticker_pbs.items() if t != exclude]
    if not peer_vals:
        return None
    below = sum(1 for p in peer_vals if p < pb_value)
    return round(100 * below / len(peer_vals))


def pb_to_valuation_tertile(pctile):
    """Valuation classification: Low (<=40th), Mid (41-59th), High (>=60th)."""
    if pctile is None:
        return None
    if pctile <= 40:
        return "Low"
    elif pctile >= 60:
        return "High"
    return "Mid"


def git_commit_and_push(repo_root, paths_to_add, branch="main"):
    cwd_before = os.getcwd()
    os.chdir(repo_root)
    try:
        rel_paths = [os.path.relpath(p, repo_root) for p in paths_to_add]
        diff = subprocess.run(
            ["git", "status", "--porcelain"] + rel_paths,
            capture_output=True, text=True
        )
        if diff.returncode != 0 or diff.stdout.strip() == "":
            print("ℹ️  No changes to commit; skipping push.")
            return
        subprocess.run(["git", "add"] + rel_paths, check=True)
        msg = f"returndrivers: update stock data {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        result = subprocess.run(["git", "commit", "-m", msg],
                                capture_output=True, text=True)
        if result.returncode != 0:
            print(result.stdout or result.stderr or "ℹ️  Nothing to commit.")
            return
        push = subprocess.run(["git", "push", "origin", branch],
                              capture_output=True, text=True)
        if push.returncode == 0:
            print("✅ Pushed to origin.")
        else:
            print(f"⚠️  Push failed: {push.stderr}")
    finally:
        os.chdir(cwd_before)


# ======================== Main ========================

def main():
    date_str = datetime.now().strftime("%Y-%m-%d")

    stock_run_dir = os.path.join(BASE_DIR, "stock", "runs", f"run_{date_str}")
    stock_cur_dir = os.path.join(BASE_DIR, "stock", "current")

    for d in [stock_run_dir, stock_cur_dir]:
        os.makedirs(d, exist_ok=True)

    summary = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "stocks": [],
        "errors": [],
    }

    # -------- 1. Load WRDS sector-level P/B data --------
    print(f"📊 Loading WRDS sector-level P/B data (gsec=45)…")
    ticker_pbs, all_pbs = load_wrds_sector_pbs(WRDS_CSV, gsector="45")
    print(f"  {len(ticker_pbs)} IT/Technology stocks with P/B > 0")

    # Show P/B percentiles for our 6 stocks
    print(f"\n  Sector-level P/B percentiles (IT/Technology):")
    for sym in STOCKS:
        pb, pctile = compute_pb_percentile_sector(sym, ticker_pbs, all_pbs)
        val = pb_to_valuation_tertile(pctile)
        print(f"  {sym:6s}: PB={pb}, percentile={pctile}th → {val}")

    # -------- 2. Fetch price data --------
    print(f"\n{'=' * 50}")
    print("FETCHING STOCK PRICE DATA")
    print("=" * 50)

    for sym in STOCKS:
        print(f"⏳ {sym}…")
        pts = fetch_price_data(sym)
        if pts:
            out_name = sym.lower() + "_365d.json"
            out_path = os.path.join(stock_run_dir, out_name)
            write_json(out_path, {"prices": pts})
            copy_to_current(out_path, stock_cur_dir)
            first_price = pts[0][1]
            last_price = pts[-1][1]
            ret = round((last_price - first_price) / first_price * 100, 1)
            print(f"  ✅ {out_name} ({len(pts)} points, ret={ret:+.1f}%)")
            summary["stocks"].append({"symbol": sym, "points": len(pts), "return_pct": ret})
        else:
            print(f"  ❌ Failed: {sym}")
            summary["errors"].append({"symbol": sym, "type": "price"})
        time.sleep(SLEEP_SEC)

    # -------- 3. Fetch fundamentals --------
    print(f"\n{'=' * 50}")
    print("FETCHING FUNDAMENTALS")
    print("=" * 50)

    fundamentals = {}

    for sym in STOCKS:
        print(f"⏳ {sym}…")

        # Market cap, dividend yield, current P/B and P/E from yfinance
        raw = fetch_fundamentals_yf(sym)
        pb_yahoo = pe_yahoo = None
        if raw is not None:
            mc_raw = raw.get("marketcap_raw")
            div_raw = raw.get("div_y_raw")
            pb_yahoo = round(raw["pb_yahoo"], 2) if raw.get("pb_yahoo") else None
            pe_yahoo = round(raw["pe_yahoo"], 1) if raw.get("pe_yahoo") else None
            mc_millions = round(mc_raw / 1_000_000, 2) if mc_raw else None
            if div_raw is not None:
                converted = round(div_raw * 100, 2)
                # yfinance occasionally returns dividendYield already in % form
                # (e.g. 0.89 meaning 0.89%). Multiplying by 100 gives 89 — implausible
                # for a tech stock. Guard: if result >20 treat raw as already a %.
                div_pct = converted if converted <= 20 else round(div_raw, 2)
            else:
                div_pct = None
        else:
            mc_millions = None
            div_pct = None
            summary["errors"].append({"symbol": sym, "type": "fundamentals_yf"})

        # pb_current = CURRENT Yahoo priceToBook; percentile = rank that current P/B
        # against the WRDS GICS-45 sector peer distribution; label via 40/60 tertiles.
        pb_pctile = pctile_of_value(pb_yahoo, ticker_pbs, exclude=sym)
        valuation = pb_to_valuation_tertile(pb_pctile)

        fundamentals[sym] = {
            "marketcap":         mc_millions,
            "pb_current":        pb_yahoo,      # current P/B from Yahoo Finance
            "pb_current_pctile": pb_pctile,     # current P/B ranked vs WRDS sector-45 peers
            "pb_yahoo":          pb_yahoo,      # (kept for continuity; equals pb_current)
            "pe_yahoo":          pe_yahoo,      # current trailing P/E from Yahoo
            "div_y":             div_pct,
            "valuation":         valuation,
            "sector":            SECTOR_LABEL[sym],
        }
        print(f"  ✅ {sym}: mcap={mc_millions}M, PB={pb_yahoo} ({pb_pctile}th pctile {valuation}), "
              f"PE={pe_yahoo}, div={div_pct}%")
        time.sleep(SLEEP_SEC)

    # Save fundamentals
    funds_run = os.path.join(stock_run_dir, "fundamentals.json")
    funds_cur = os.path.join(stock_cur_dir, "fundamentals.json")
    write_json(funds_run, fundamentals)
    write_json(funds_cur, fundamentals)
    print(f"\n✅ Saved fundamentals.json")

    # -------- 4. Save summary --------
    summary["fundamentals"] = fundamentals
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    write_json(os.path.join(stock_run_dir, "summary.json"), summary)
    write_json(os.path.join(stock_cur_dir, "summary.json"), summary)

    # -------- 5. Git commit & push --------
    print(f"\n{'=' * 50}")
    print("GIT COMMIT & PUSH")
    print("=" * 50)

    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors — skipping git commit/push.")
    elif not PUSH:
        print("ℹ️  FETCH_PUSH=0 — skipping git commit/push (data written locally).")
    else:
        git_commit_and_push(
            REPO_ROOT,
            [os.path.join(BASE_DIR, "stock")],
        )

    # -------- Done --------
    print("\n🏁 Done.")
    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors (see summary.json)")
    else:
        print("All stocks fetched successfully.")

    print(f"\nStock data: {stock_cur_dir}")
    print(f"\nCDN base URL (after push):")
    print(f"  https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@latest/returndrivers/stock/current/fundamentals.json")


if __name__ == "__main__":
    main()
