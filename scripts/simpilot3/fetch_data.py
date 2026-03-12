#!/usr/bin/env python3
"""
Fetch asset data for Similarity Pilot 2.
- Cryptos: BTC, BCH, TRX  (price data only)
- Stocks:  CME, ICE, MSTR, PYPL, HOOD, SOFI, XYZ  (price data + fundamentals)

Output structure under similarity_pilot2/:
  cryptos/current/   + cryptos/runs/run_YYYY-MM-DD/
  stocks/current/    + stocks/runs/run_YYYY-MM-DD/
"""

import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

import yfinance as yf

# ======================== Config ========================

CRYPTOS = ["BTC-USD", "ETH-USD", "TRX-USD"]
STOCKS  = ["CME", "ICE", "MSTR", "PYPL", "HOOD", "SOFI", "XYZ"]

# GICS industry (6-digit gind) for each stock — used for P/B percentile peer group
INDUSTRY_MAP = {
    "CME":  "402030",  # Capital Markets
    "ICE":  "402030",  # Capital Markets
    "HOOD": "402030",  # Capital Markets
    "PYPL": "402010",  # Diversified Financial Services (Transaction Processing)
    "XYZ":  "402010",  # Diversified Financial Services (Transaction Processing)
    "SOFI": "402020",  # Consumer Finance
    "MSTR": "451030",  # IT Consulting & Other Services
}

# Sector labels for display
SECTOR_LABEL = {
    "CME": "Financials", "ICE": "Financials", "HOOD": "Financials",
    "PYPL": "Financials", "XYZ": "Financials", "SOFI": "Financials",
    "MSTR": "Technology",
}

# Compustat P/B data (GICS sectors 40+45, mktcap > $5B, P/B > 0)
WRDS_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "WRDS",
    "pb_ratio_sectors_40_45_with_industry.csv"
)
MKTCAP_FLOOR = 5000  # $5B in Compustat millions

END   = datetime.now()
START = END - timedelta(days=365)

REPO_ROOT = "/Users/paulgrass/Library/Mobile Documents/com~apple~CloudDocs/Documents/Programming/Git/pilot3-asset-data"
BASE_DIR  = os.path.join(REPO_ROOT, "similarity_pilot2")

SLEEP_SEC    = 15
MAX_RETRIES  = 3
RETRY_DELAY  = 15

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


def fetch_price_data(symbol, start, end, max_retries=MAX_RETRIES):
    """Download 365-day price history via yfinance. Returns list of [timestamp_ms, close]."""
    for attempt in range(1, max_retries + 1):
        try:
            tkr = yf.Ticker(symbol)
            df = tkr.history(start=start.strftime("%Y-%m-%d"),
                             end=end.strftime("%Y-%m-%d"),
                             auto_adjust=True)
            if df is None or df.empty:
                raise ValueError("Empty dataframe returned.")
            pts = [
                [int(ts.timestamp() * 1000), round(float(row["Close"]), 2)]
                for ts, row in df.iterrows()
                if row.get("Close") is not None
            ]
            if not pts:
                raise ValueError("No valid close prices.")
            return pts
        except Exception as e:
            print(f"   ⚠️  Attempt {attempt}/{max_retries} for {symbol}: {e}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
    return None


def fetch_fundamentals_yf(symbol):
    """Fetch market cap, P/B, dividend yield, sector from yfinance info."""
    try:
        tkr = yf.Ticker(symbol)
        info = tkr.info
        return {
            "marketcap_raw": info.get("marketCap"),
            "pb_current":    info.get("priceToBook"),
            "div_y_raw":     info.get("dividendYield"),  # decimal (0.02 = 2%)
            "sector":        info.get("sector"),
        }
    except Exception as e:
        print(f"   ⚠️  Fundamentals fetch failed for {symbol}: {e}")
        return None


def load_wrds_pb(csv_path, mktcap_floor=MKTCAP_FLOOR):
    """Load Compustat P/B data from CSV grouped by gind. Filters P/B > 0 and mktcap >= floor."""
    industry_pbs = {}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pb = float(row["pb_ratio"])
            mktcap = float(row["mktcap"])
            if pb <= 0 or mktcap < mktcap_floor:
                continue
            gind = row["gind"]
            industry_pbs.setdefault(gind, []).append((row["tic"], pb))
    for gind in industry_pbs:
        industry_pbs[gind].sort(key=lambda x: x[1])
    return industry_pbs


def compute_pb_percentile(stock_ticker, gind, industry_pbs):
    """Compute percentile of stock within its GICS industry (excluding itself)."""
    peers = industry_pbs.get(gind, [])
    stock_pb = None
    peer_vals = []
    for tic, pb in peers:
        if tic == stock_ticker:
            stock_pb = pb
        else:
            peer_vals.append(pb)
    if stock_pb is None or not peer_vals:
        return None, None
    below = sum(1 for p in peer_vals if p < stock_pb)
    pctile = round(100 * below / len(peer_vals))
    return round(stock_pb, 2), pctile


def pb_to_valuation(pctile):
    if pctile is None:
        return None
    if pctile <= 30:
        return "Low"
    elif pctile >= 70:
        return "High"
    return "Mid"


def git_commit_and_push(repo_root, paths_to_add, branch="main"):
    """Stage specific paths, commit, and push."""
    cwd_before = os.getcwd()
    os.chdir(repo_root)
    try:
        # Check for changes
        rel_paths = [os.path.relpath(p, repo_root) for p in paths_to_add]
        diff = subprocess.run(
            ["git", "status", "--porcelain"] + rel_paths,
            capture_output=True, text=True
        )
        if diff.returncode != 0 or diff.stdout.strip() == "":
            print("ℹ️  No changes to commit; skipping push.")
            return

        subprocess.run(["git", "add"] + rel_paths, check=True)
        msg = f"similarity_pilot2: update data {datetime.now().strftime('%Y-%m-%d %H:%M')}"
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

    crypto_run_dir = os.path.join(BASE_DIR, "cryptos", "runs", f"run_{date_str}")
    crypto_cur_dir = os.path.join(BASE_DIR, "cryptos", "current")
    stock_run_dir  = os.path.join(BASE_DIR, "stocks", "runs", f"run_{date_str}")
    stock_cur_dir  = os.path.join(BASE_DIR, "stocks", "current")

    for d in [crypto_run_dir, crypto_cur_dir, stock_run_dir, stock_cur_dir]:
        os.makedirs(d, exist_ok=True)

    summary = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "window_start": START.strftime("%Y-%m-%d"),
        "window_end": END.strftime("%Y-%m-%d"),
        "cryptos": [],
        "stocks": [],
        "errors": [],
    }

    # -------- 1. Crypto price data --------
    print("Waiting 30s for Yahoo Finance rate limit to reset…")
    time.sleep(30)
    print("=" * 50)
    print("FETCHING CRYPTO PRICE DATA")
    print("=" * 50)

    for sym in CRYPTOS:
        print(f"⏳ {sym}…")
        pts = fetch_price_data(sym, START, END)
        if pts:
            out_name = sym.replace("-USD", "").lower() + "_365d.json"
            out_path = os.path.join(crypto_run_dir, out_name)
            write_json(out_path, {"prices": pts})
            copy_to_current(out_path, crypto_cur_dir)
            print(f"  ✅ {out_name} ({len(pts)} points)")
            summary["cryptos"].append({"symbol": sym, "points": len(pts)})
        else:
            print(f"  ❌ Failed: {sym}")
            summary["errors"].append({"symbol": sym, "type": "price"})
        time.sleep(SLEEP_SEC)

    # -------- 2. Stock price data --------
    print("\n" + "=" * 50)
    print("FETCHING STOCK PRICE DATA")
    print("=" * 50)

    for sym in STOCKS:
        print(f"⏳ {sym}…")
        pts = fetch_price_data(sym, START, END)
        if pts:
            out_name = sym.lower() + "_365d.json"
            out_path = os.path.join(stock_run_dir, out_name)
            write_json(out_path, {"prices": pts})
            copy_to_current(out_path, stock_cur_dir)
            print(f"  ✅ {out_name} ({len(pts)} points)")
            summary["stocks"].append({"symbol": sym, "points": len(pts)})
        else:
            print(f"  ❌ Failed: {sym}")
            summary["errors"].append({"symbol": sym, "type": "price"})
        time.sleep(SLEEP_SEC)

    # -------- 3. Stock fundamentals --------
    print("\n" + "=" * 50)
    print("FETCHING STOCK FUNDAMENTALS")
    print("=" * 50)

    # 3a. Load Compustat P/B data for industry percentile computation
    print(f"\n📊 Loading Compustat P/B data from {WRDS_CSV} (mktcap >= ${MKTCAP_FLOOR}M)…")
    industry_pbs = load_wrds_pb(WRDS_CSV)
    for gind, pairs in sorted(industry_pbs.items()):
        if gind in INDUSTRY_MAP.values():
            print(f"  GICS {gind}: {len(pairs)} firms with P/B > 0")

    # 3b. Fetch market cap + dividend yield from yfinance, P/B from Compustat
    print("\n📊 Fetching fundamentals for target stocks…")
    fundamentals = {}

    for sym in STOCKS:
        print(f"⏳ {sym}…")
        gind = INDUSTRY_MAP[sym]

        # P/B + percentile from Compustat
        pb, pb_pctile = compute_pb_percentile(sym, gind, industry_pbs)
        valuation = pb_to_valuation(pb_pctile)

        # Market cap + dividend yield from yfinance
        raw = fetch_fundamentals_yf(sym)
        if raw is not None:
            mc_raw = raw.get("marketcap_raw")
            div_raw = raw.get("div_y_raw")
            mc_millions = round(mc_raw / 1_000_000, 2) if mc_raw else None
            if div_raw is not None:
                div_pct = round(div_raw * 100, 2) if div_raw < 1 else round(div_raw, 2)
            else:
                div_pct = None
        else:
            mc_millions = None
            div_pct = None
            summary["errors"].append({"symbol": sym, "type": "fundamentals_yf"})

        sector_label = SECTOR_LABEL[sym]
        fundamentals[sym] = {
            "marketcap":         mc_millions,
            "pb_current":        pb,
            "pb_current_pctile": pb_pctile,
            "div_y":             div_pct,
            "valuation":         valuation,
            "sector":            sector_label,
        }
        print(f"  ✅ {sym}: mcap={mc_millions}M, P/B={pb}, pctile={pb_pctile}, "
              f"val={valuation}, div={div_pct}%, sector={sector_label}")
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
    write_json(os.path.join(crypto_run_dir, "summary.json"), summary)
    write_json(os.path.join(crypto_cur_dir, "summary.json"), summary)

    # -------- 5. Git commit & push --------
    print("\n" + "=" * 50)
    print("GIT COMMIT & PUSH")
    print("=" * 50)

    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors — skipping git commit/push to avoid overwriting good data.")
    else:
        git_commit_and_push(
            REPO_ROOT,
            [
                os.path.join(BASE_DIR, "cryptos"),
                os.path.join(BASE_DIR, "stocks"),
            ],
        )

    # -------- Done --------
    print("\n🏁 Done.")
    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors (see summary.json)")
    else:
        print("All symbols fetched successfully.")

    print(f"\nCrypto data: {crypto_cur_dir}")
    print(f"Stock data:  {stock_cur_dir}")
    print(f"\nCDN base URLs (after push):")
    print(f"  Cryptos: https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@latest/similarity_pilot2/cryptos/current/")
    print(f"  Stocks:  https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@latest/similarity_pilot2/stocks/current/")


if __name__ == "__main__":
    main()
