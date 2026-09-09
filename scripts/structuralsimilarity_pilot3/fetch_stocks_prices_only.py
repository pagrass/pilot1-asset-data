#!/usr/bin/env python3
"""
Fetch stock PRICE data only for Structural Similarity — Wave 2 (pilot 3).

*** THIS is the launch-day script for W2. ***
fundamentals.json is PINNED from the last full fetch (valuation tiers and
net profit must not flap between now and launch) — this script refreshes
ONLY the price series and never touches fundamentals.json. Do NOT run the
full fetch_stocks.py on launch day.

Slate (2026-09-09, pilot 3 = profit-chart redesign; quadrant logic on
(12-mo return sign x last-FY profit-change sign)); fielded six + spares.
Returns quoted for a window starting 2025-09-16 (stable after the 09-10-2025
earnings shocks roll out):
  (+ret,+chg): CSCO +63 (rebound path), ANET +37 (mono-up)
  (+ret,-chg): AKAM +38 (decline)
  (-ret,+chg): ADBE -27 (mono-up), INTU -51 (mono-up)
  (-ret,-chg): EPAM -25 (flat-then-decline)
Matched pairs: ANET/AKAM (high side) and ADBE/EPAM (low side), each within
~2pp of return with opposite profit paths.
Spares (fetched, not wired in the W2 QSF): ORCL (-47 after the roll, mono-up;
optional 7th / swap for INTU), FTNT (+97, mono-up, weak +chg), TXN (+46,
vivid 3-yr decline, flat last FY).
Dropped 2026-09-09: PANW (FY2026 net income ~$0.31B, -73% y/y, loss quarter
-> not a comfortably positive base); SNPS (its -35% return was entirely the
2025-09-10 crash at the window start -> -10% once that day rolls out).

Output: structuralsimilarity_pilot3/stock/current/ + structuralsimilarity_pilot3/stock/runs/run_YYYY-MM-DD/

UNCONSOLIDATED-CLOSE WORKAROUND (opt-in, FILL_UNCONSOLIDATED=1)
--------------------------------------------------------------
Yahoo's daily series sometimes carries the most recent trading day with
close=null for many hours after the session (seen Sat 2026-08-29 for Fri
08-28: all six tickers null). The default behaviour DROPS such a day, which
is right for a launch pin but means a same-weekend refresh advances the
window start without adding a trading day.

With FILL_UNCONSOLIDATED=1 the script fills a trailing null close only when
BOTH of these agree to within 0.15%:
  (a) meta.regularMarketPrice, if meta.regularMarketTime falls on that date;
  (b) the last regular-session (<=16:00 ET) 5-minute bar for that date.
(b) is an actual trade aggregate, so this is a cross-check, not a guess.
Anything that fails the check is left out rather than filled. Every filled
bar is recorded in summary.json under "filled_bars" for provenance, and the
next ordinary fetch (once Yahoo consolidates) silently replaces it -- run
verify_filled_bars() then to confirm the filled value matched.
"""

import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

# ======================== Config ========================

STOCKS = ["INTU", "CSCO", "ANET", "AKAM", "ADBE", "EPAM", "ORCL", "FTNT", "TXN"]

# Opt-in: fill a trailing unconsolidated (close=null) trading day. See docstring.
FILL_UNCONSOLIDATED = os.environ.get("FILL_UNCONSOLIDATED", "0") == "1"
FILL_TOL_PCT = 0.15
FILLED_LOG = []

REPO_ROOT = "/Users/paulgrass/Documents/Programming/Git/pilot3-asset-data"
BASE_DIR  = os.path.join(REPO_ROOT, "structuralsimilarity_pilot3")

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


def _yahoo(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _intraday_close(ticker, day):
    """Last regular-session (<=16:00 ET) 5-minute close for `day`; None if absent."""
    try:
        r = _yahoo(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                   f"?range=5d&interval=5m")["chart"]["result"][0]
        ts, cl = r["timestamp"], r["indicators"]["quote"][0]["close"]
        vals = []
        for x, c in zip(ts, cl):
            if c is None:
                continue
            dt = datetime.fromtimestamp(x, ZoneInfo("America/New_York"))
            if dt.date() == day and dt.time() <= __import__("datetime").time(16, 0):
                vals.append(float(c))
        return vals[-1] if vals else None
    except Exception:
        return None


def _meta_close(ticker, day):
    """meta.regularMarketPrice, but only if its timestamp falls on `day`."""
    try:
        m = _yahoo(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                   f"?range=1d&interval=1d")["chart"]["result"][0]["meta"]
        t = m.get("regularMarketTime"); px = m.get("regularMarketPrice")
        if t is None or px is None:
            return None
        if datetime.fromtimestamp(t, ZoneInfo("America/New_York")).date() != day:
            return None
        return float(px)
    except Exception:
        return None


def fetch_price_data(ticker, max_retries=MAX_RETRIES):
    """Download 365-day price history via Yahoo Finance chart API."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?range=1y&interval=1d"
    )
    ET = ZoneInfo("America/New_York")
    for attempt in range(1, max_retries + 1):
        try:
            data = _yahoo(url, timeout=15)
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]
            today_et = datetime.now(ET).date()

            pts = []
            missing = []          # completed trading days Yahoo has not consolidated
            for ts, c in zip(timestamps, closes):
                day = datetime.fromtimestamp(ts, ET).date()
                if day >= today_et:
                    continue      # never ship an in-progress session
                if c is not None:
                    pts.append([int(ts * 1000), round(float(c), 2)])
                else:
                    missing.append((int(ts * 1000), day))

            if FILL_UNCONSOLIDATED:
                for ts_ms, day in missing:
                    a, b = _meta_close(ticker, day), _intraday_close(ticker, day)
                    if a is None or b is None:
                        print(f"   ⏭  {ticker} {day}: unconsolidated, no cross-validated price -> left out")
                        continue
                    spread = abs(a - b) / min(a, b) * 100
                    if spread > FILL_TOL_PCT:
                        print(f"   ⚠️  {ticker} {day}: sources disagree ({a:.2f} vs {b:.2f}, {spread:.2f}%) -> left out")
                        continue
                    px = round((a + b) / 2, 2)
                    pts.append([ts_ms, px])
                    FILLED_LOG.append({"ticker": ticker, "date": str(day), "close": px,
                                       "meta": a, "intraday_5m": b, "spread_pct": round(spread, 4)})
                    print(f"   🔧 {ticker} {day}: filled close {px:.2f} (meta {a:.2f} / intraday {b:.2f})")
                pts.sort(key=lambda x: x[0])
            elif missing:
                print(f"   ℹ️  {ticker}: {len(missing)} unconsolidated day(s) dropped "
                      f"({', '.join(str(d) for _, d in missing)}); set FILL_UNCONSOLIDATED=1 to fill")

            if not pts:
                raise ValueError("No valid close prices.")
            return pts
        except Exception as e:
            print(f"   ⚠️  Attempt {attempt}/{max_retries} for {ticker}: {e}")
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
    return None


def warn_early_window_shocks(sym, pts, days=10, thresh_pct=15.0):
    """Flag a large single-day move inside the first `days` trading days of
    the 365-day window. Such a day drops out of the window within two weeks,
    so the displayed 12-mo return will jump mid-fielding (SNPS 2026-09-09:
    -35% -> -10% overnight; ORCL -33% -> -47%). Screen slates on returns
    computed from ~2 weeks after the window start when this fires."""
    head = pts[:days + 1]
    worst = max((abs(head[i][1] / head[i - 1][1] - 1) * 100, i) for i in range(1, len(head)))
    if worst[0] >= thresh_pct:
        from datetime import datetime as _dt
        d = _dt.fromtimestamp(head[worst[1]][0] / 1000).strftime("%Y-%m-%d")
        alt = round((pts[-1][1] / head[-1][1] - 1) * 100, 1)
        print(f"  \u26a0\ufe0f  {sym}: {worst[0]:.1f}% single-day move on {d} within the first {days} trading days "
              f"of the window -> 12-mo return will shift to ~{alt:+.1f}% once it rolls out")


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
        msg = f"structuralsimilarity_pilot3: update stock prices, fundamentals pinned {datetime.now().strftime('%Y-%m-%d %H:%M')}"
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
            warn_early_window_shocks(sym, pts)
        else:
            print(f"  ❌ Failed: {sym}")
            summary["errors"].append({"symbol": sym, "type": "price"})
        time.sleep(SLEEP_SEC)

    # -------- Save summary --------
    summary["filled_bars"] = FILLED_LOG
    summary["fill_unconsolidated"] = FILL_UNCONSOLIDATED
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
