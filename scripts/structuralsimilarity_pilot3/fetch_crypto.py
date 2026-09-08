#!/usr/bin/env python3
"""
Fetch crypto data for the Structural Similarity PILOT 3 (Wave 1).

Wave 1 is the same survey as the Investment Decisions Pilot 2 Wave 1
(pure copy: survey name + data folder swapped, design untouched), so the
slate is that survey's slate:
  Stage 1 (replication, also shown in pre-study): ETH, XMR, BNB
  Stage 2 (own beliefs):                          BTC, HYPE, XRP

SLATE NOTE 2026-09-08 (pilot 3): the pilot-3 folder was first created on
2026-08-31 with the pilot-1/2 slate (TRX in stage 2). TRX -> XRP is carried
over from the decisions pilot 2 (2026-08-31): TRX had drifted to a ~0%
12-month return (+1.0% on 2026-09-07) and in pilot-1 W1 barely discriminated
the two arms (AUC 0.574, d=+0.10 vs 0.294/0.780 for BTC/HYPE; 22% of
participants answered |x|<2 there) and carried no information about the
internalised model. Re-screened ~80 candidates on 2026-09-07 data: market
still bear; XMR (+95) and HYPE (+68) remain the only non-fringe up-assets
(ZEC +2146, JST +215, DASH +151 w/ 41 days >10% are ludicrous/fringe);
XRP -53 is a clean, well-known negative. Slate span -53 .. +95.

Series end at the last COMPLETED UTC day (yesterday's close == today's
00:00 UTC open; crypto trades 24/7 so these are the same number).
The in-progress "today" bar is always dropped, so a fetch returns identical
data no matter what hour it runs. Pin at Wave 1 launch; do NOT re-fetch
while Wave 1 is in the field (all participants must see identical charts).

TRAILING-GAP BACKFILL: Yahoo's DAILY crypto series sometimes omits recent
days even though the data exists at 1h granularity (observed for Sat/Sun
2026-08-29/30 on all six tickers). Any completed UTC day missing from the
tail of the daily series is refilled from the 1h series (range=7d), using
that day's last hourly close -- the same quantity Yahoo records as the
daily close, agreeing to ~0.02% on days where both exist. Backfilled days
are listed in summary.json. The 1h window only reaches 7 days back, so this
covers trailing holes only; the run warns loudly if a gap survives.

Output structure under structuralsimilarity_pilot3/:
  crypto/current/   + crypto/runs/run_YYYY-MM-DD/
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

# ======================== Config ========================

CRYPTOS = {
    "ETH-USD": "eth",
    "XMR-USD": "xmr",
    "BNB-USD": "bnb",
    "BTC-USD": "btc",
    "HYPE32196-USD": "hype",
    "XRP-USD": "xrp",
}

REPO_ROOT = "/Users/paulgrass/Documents/Programming/Git/pilot3-asset-data"
BASE_DIR  = os.path.join(REPO_ROOT, "structuralsimilarity_pilot3")
CDN_BASE  = "https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@main/structuralsimilarity_pilot3/crypto/current/"

# CDN alias: @main, NOT @latest. This repo has no git tags, so jsDelivr
# resolves "@latest" to version:null and falls back to the default branch
# with sticky per-file caching -- on 2026-08-31 that served two different
# price vintages across the six files at the same time, through repeated
# purges. Branch URLs purge reliably. Keep @main in the QSF too.

MAX_RETRIES  = 3
RETRY_DELAY  = 10
SLEEP_SEC    = int(os.environ.get("FETCH_SLEEP", "2"))
# Auto commit+push at the end (default on). Set FETCH_PUSH=0 to fetch/write only.
PUSH = os.environ.get("FETCH_PUSH", "1") != "0"

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
    Returns list of [timestamp_ms, close], truncated to the last completed
    UTC day (the in-progress bar is dropped even if Yahoo fills it)."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
        f"?range=1y&interval=1d"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    today_utc_ms = int(
        datetime.now(timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp() * 1000
    )

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
                ts_ms = int(ts * 1000)
                if c is not None and ts_ms < today_utc_ms:
                    c = float(c)
                    # sub-$1 assets (e.g. JST) need finer precision, else the
                    # chart quantizes into visible price steps
                    pts.append([ts_ms, round(c, 2 if c >= 1 else 4)])

            if not pts:
                raise ValueError("No valid close prices.")
            return pts

        except Exception as e:
            print(f"   ⚠️  Attempt {attempt}/{max_retries} for {yahoo_ticker}: {e}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)

    return None


def fetch_hourly_daily_closes(yahoo_ticker):
    """Daily closes derived from the 1h series (last hourly bar of each UTC day).
    Used only to refill trailing days the daily series omits."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
        f"?range=7d&interval=1h"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        result = data["chart"]["result"][0]
        out = {}
        for ts, c in zip(result["timestamp"],
                         result["indicators"]["quote"][0]["close"]):
            if c is None:
                continue
            dt = datetime.fromtimestamp(ts, timezone.utc)
            out[dt.strftime("%Y-%m-%d")] = float(c)   # later bars overwrite earlier
        return out
    except Exception as e:
        print(f"   \u26a0\ufe0f  hourly backfill fetch failed for {yahoo_ticker}: {e}")
        return {}


def backfill_trailing_days(yahoo_ticker, pts):
    """Append any completed UTC days missing from the tail of the daily series."""
    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    last_have = datetime.fromtimestamp(pts[-1][0] / 1000, timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    missing, d = [], last_have + timedelta(days=1)
    while d < today_utc:
        missing.append(d)
        d += timedelta(days=1)
    if not missing:
        return pts, []

    hourly = fetch_hourly_daily_closes(yahoo_ticker)
    added = []
    for d in missing:
        key = d.strftime("%Y-%m-%d")
        if key in hourly:
            c = hourly[key]
            pts.append([int(d.timestamp() * 1000), round(c, 2 if c >= 1 else 4)])
            added.append(key)
    still = [d.strftime("%Y-%m-%d") for d in missing if d.strftime("%Y-%m-%d") not in added]
    if added:
        print(f"   \U0001f527 backfilled from 1h series: {', '.join(added)}")
    if still:
        print(f"   \u26a0\ufe0f  STILL MISSING (no 1h data either): {', '.join(still)}")
    return pts, added


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
        msg = f"structuralsimilarity_pilot3: update crypto data {datetime.now().strftime('%Y-%m-%d %H:%M')}"
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
        "data_note": "series end at last completed UTC day; in-progress bar dropped",
        "cryptos": [],
        "errors": [],
    }

    print("=" * 50)
    print("FETCHING CRYPTO PRICE DATA (structuralsimilarity_pilot3)")
    print("=" * 50)

    for yahoo_ticker, slug in CRYPTOS.items():
        print(f"⏳ {yahoo_ticker}…")
        pts = fetch_price_data(yahoo_ticker)
        backfilled = []
        if pts:
            pts, backfilled = backfill_trailing_days(yahoo_ticker, pts)
            out_name = slug + "_365d.json"
            out_path = os.path.join(crypto_run_dir, out_name)
            write_json(out_path, {"prices": pts})
            copy_to_current(out_path, crypto_cur_dir)

            # Compute return for display
            first_price = pts[0][1]
            last_price = pts[-1][1]
            ret = round((last_price - first_price) / first_price * 100, 1)
            last_day = datetime.fromtimestamp(pts[-1][0] / 1000, timezone.utc).strftime("%Y-%m-%d")

            print(f"  ✅ {out_name} ({len(pts)} points, ret={ret:+.1f}%, last day {last_day})")
            summary["cryptos"].append({
                "symbol": yahoo_ticker, "slug": slug,
                "points": len(pts), "return_pct": ret,
                "last_day_utc": last_day, "last_close": last_price,
                "backfilled_from_1h": backfilled,
            })
        else:
            print(f"  ❌ Failed: {yahoo_ticker}")
            summary["errors"].append({"symbol": yahoo_ticker, "type": "price"})
        time.sleep(SLEEP_SEC)

    # Save summary
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    write_json(os.path.join(crypto_run_dir, "summary.json"), summary)
    write_json(os.path.join(crypto_cur_dir, "summary.json"), summary)

    # Staleness check: every series must end on the last completed UTC day
    expected = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    stale = [c for c in summary["cryptos"] if c["last_day_utc"] != expected]
    summary["expected_last_day_utc"] = expected
    summary["stale"] = [{"slug": c["slug"], "last_day_utc": c["last_day_utc"]} for c in stale]
    write_json(os.path.join(crypto_run_dir, "summary.json"), summary)
    write_json(os.path.join(crypto_cur_dir, "summary.json"), summary)
    if stale:
        print("\n\u26a0\ufe0f  STALE: expected all series to end " + expected + "; "
              + ", ".join(f"{c['slug']}={c['last_day_utc']}" for c in stale))
    else:
        print("\n\u2705 all series end on " + expected + " (last completed UTC day)")

    # Git commit & push
    print("\n" + "=" * 50)
    print("GIT COMMIT & PUSH")
    print("=" * 50)

    if summary["errors"]:
        print(f"⚠️  {len(summary['errors'])} errors — skipping git commit/push.")
    elif not PUSH:
        print("ℹ️  FETCH_PUSH=0 — skipping git commit/push (data written locally).")
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
    print(f"  {CDN_BASE}")
    print(f"\nTo purge the jsDelivr cache after an update:")
    for slug in CRYPTOS.values():
        print(f"  curl -s https://purge.jsdelivr.net/gh/pagrass/pilot1-asset-data@main/structuralsimilarity_pilot3/crypto/current/{slug}_365d.json")


if __name__ == "__main__":
    main()
