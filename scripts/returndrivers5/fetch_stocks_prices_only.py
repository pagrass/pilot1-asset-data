#!/usr/bin/env python3
"""
Fetch stock PRICE data only for Visual Similarity Pilot — Wave 2.

Stocks:
  Low-returns pair:  CRM (Salesforce), NOW (ServiceNow)
  High-returns pair: ARM (Arm), STM (STMicroelectronics)
  Anchors:           MSFT (Microsoft), CSCO (Cisco)

This is a prices-only variant of fetch_stocks.py — it does NOT compute or
write fundamentals.json (no P/B, market cap, or dividend yield).

Output: visualsimilarity/stock/current/ + visualsimilarity/stock/runs/run_YYYY-MM-DD/
"""

import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime

# ======================== Config ========================

STOCKS = ["NOW", "MSFT", "LRCX", "DDOG", "AKAM"]

REPO_ROOT = "/Users/paulgrass/Library/Mobile Documents/com~apple~CloudDocs/Documents/Programming/Git/pilot3-asset-data"
BASE_DIR  = os.path.join(REPO_ROOT, "returndrivers3")

SLEEP_SEC   = 30
MAX_RETRIES = 3
RETRY_DELAY = 10

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
        msg = f"returndrivers: update stock prices {datetime.now().strftime('%Y-%m-%d %H:%M')}"
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

    # -------- Fetch price data --------
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

    # -------- Save summary --------
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    write_json(os.path.join(stock_run_dir, "summary.json"), summary)
    write_json(os.path.join(stock_cur_dir, "summary.json"), summary)

    # -------- Git commit & push --------
    print(f"\n{'=' * 50}")
    print("GIT COMMIT & PUSH")
    print("=" * 50)

    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors — skipping git commit/push.")
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


if __name__ == "__main__":
    main()
