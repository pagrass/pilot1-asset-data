#!/usr/bin/env python3
"""
Fetch asset data for Visual Similarity Pilot.

Cryptos (Wave 1):
  Replication: SOL, BCH, XMR
  Own belief:  BTC, TRX, XRP

Output structure under visualsimilarity/:
  crypto/current/   + crypto/runs/run_YYYY-MM-DD/
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta

# ======================== Config ========================

CRYPTOS = {
    "SOL-USD": "sol",
    "BCH-USD": "bch",
    "XMR-USD": "xmr",
    "BTC-USD": "btc",
    "TRX-USD": "trx",
    "XRP-USD": "xrp",
}

REPO_ROOT = "/Users/paulgrass/Library/Mobile Documents/com~apple~CloudDocs/Documents/Programming/Git/pilot3-asset-data"
BASE_DIR  = os.path.join(REPO_ROOT, "visualsimilarity")

MAX_RETRIES  = 3
RETRY_DELAY  = 10
SLEEP_SEC    = 2

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


def fetch_price_data(yahoo_ticker, max_retries=MAX_RETRIES):
    """Download 365-day price history via Yahoo Finance chart API.
    Returns list of [timestamp_ms, close]."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
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

            pts = []
            for ts, c in zip(timestamps, closes):
                if c is not None:
                    pts.append([int(ts * 1000), round(float(c), 2)])

            if not pts:
                raise ValueError("No valid close prices.")
            return pts

        except Exception as e:
            print(f"   ⚠️  Attempt {attempt}/{max_retries} for {yahoo_ticker}: {e}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)

    return None


def git_commit_and_push(repo_root, paths_to_add, branch="main"):
    """Stage specific paths, commit, and push."""
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
        msg = f"visualsimilarity: update crypto data {datetime.now().strftime('%Y-%m-%d %H:%M')}"
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

    crypto_run_dir = os.path.join(BASE_DIR, "crypto", "runs", f"run_{date_str}")
    crypto_cur_dir = os.path.join(BASE_DIR, "crypto", "current")

    for d in [crypto_run_dir, crypto_cur_dir]:
        os.makedirs(d, exist_ok=True)

    summary = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "cryptos": [],
        "errors": [],
    }

    print("=" * 50)
    print("FETCHING CRYPTO PRICE DATA")
    print("=" * 50)

    for yahoo_ticker, slug in CRYPTOS.items():
        print(f"⏳ {yahoo_ticker}…")
        pts = fetch_price_data(yahoo_ticker)
        if pts:
            out_name = slug + "_365d.json"
            out_path = os.path.join(crypto_run_dir, out_name)
            write_json(out_path, {"prices": pts})
            copy_to_current(out_path, crypto_cur_dir)

            # Compute return for display
            first_price = pts[0][1]
            last_price = pts[-1][1]
            ret = round((last_price - first_price) / first_price * 100, 1)

            print(f"  ✅ {out_name} ({len(pts)} points, ret={ret:+.1f}%)")
            summary["cryptos"].append({
                "symbol": yahoo_ticker, "slug": slug,
                "points": len(pts), "return_pct": ret,
            })
        else:
            print(f"  ❌ Failed: {yahoo_ticker}")
            summary["errors"].append({"symbol": yahoo_ticker, "type": "price"})
        time.sleep(SLEEP_SEC)

    # Save summary
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    write_json(os.path.join(crypto_run_dir, "summary.json"), summary)
    write_json(os.path.join(crypto_cur_dir, "summary.json"), summary)

    # Git commit & push
    print("\n" + "=" * 50)
    print("GIT COMMIT & PUSH")
    print("=" * 50)

    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors — skipping git commit/push.")
    else:
        git_commit_and_push(
            REPO_ROOT,
            [os.path.join(BASE_DIR, "crypto")],
        )

    # Done
    print("\n🏁 Done.")
    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors (see summary.json)")
    else:
        print("All cryptos fetched successfully.")

    print(f"\nCrypto data: {crypto_cur_dir}")
    print(f"\nCDN base URL (after push):")
    print(f"  https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@latest/visualsimilarity/crypto/current/")


if __name__ == "__main__":
    main()
