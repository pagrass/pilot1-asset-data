#!/usr/bin/env python3
"""
returndrivers4 — recompute CURRENT P/B + valuation labels (copy of fetch_stocks.py).

Why this copy exists:
  fetch_stocks.py takes P/B from the WRDS/Compustat CSV, which is FISCAL-2024
  (datadate 2024-12-31, year-end price) → ~18 months stale. Worst case ZS showed
  P/B 25.1 ("High") because its end-2024 book was depressed; its CURRENT P/B is ~9.

What changed vs fetch_stocks.py:
  - P/B for the 6 slate stocks = CURRENT Yahoo priceToBook (price now / latest reported book).
  - Percentile peer universe = same GICS sector 45 (IT/Technology) membership as the
    baseline, taken from the WRDS CSV, restricted to end-2024 mktcap >= $10B (the slate
    is all large-cap; matches the simpilot2 precedent of a market-cap floor), but valued
    with CURRENT Yahoo priceToBook (cached screen values reused where available).
  - Tertile thresholds exposed as constants (TERTILE_LOW / TERTILE_HIGH) — easy to tweak.
  - Fundamentals only (price *_365d.json already fetched today; not touched here).
  - Git auto-push DISABLED (PUSH=False) — writes locally; push manually when ready.

Output: returndrivers4/stock/current/fundamentals.json (+ runs/run_YYYY-MM-DD/)
"""

import csv, json, os, time
from datetime import datetime
import yfinance as yf
from curl_cffi import requests as curl_requests

_YF = curl_requests.Session(impersonate="chrome")

# ======================== Config ========================
STOCKS = ["CSCO", "CRM", "WDAY", "ZS", "AKAM", "TWLO"]
SECTOR_LABEL = {s: "Technology" for s in STOCKS}

WRDS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..",
                        "WRDS", "pb_ratio_sectors_40_45_with_industry.csv")
PEER_MKTCAP_FLOOR = 10000   # $M (end-2024) — large-cap peer universe for percentile
TERTILE_LOW  = 33           # <= this percentile -> "Low"
TERTILE_HIGH = 67           # >= this percentile -> "High"
SCREEN_CACHE = "/tmp/stock_screen.json"   # reuse current P/B already pulled (optional)

REPO_ROOT = "/Users/paulgrass/Library/Mobile Documents/com~apple~CloudDocs/Documents/Programming/Git/pilot3-asset-data"
BASE_DIR  = os.path.join(REPO_ROOT, "returndrivers4")
# NB: BASE_DIR resolves through iCloud; if that path isn't present, fall back to the
# local working copy the user is editing.
if not os.path.isdir(os.path.dirname(BASE_DIR)):
    BASE_DIR = "/Users/paulgrass/Documents/Programming/Git/pilot3-asset-data/returndrivers4"

PUSH = False
PEER_SLEEP = 0.5
SLATE_SLEEP = 1.0

# ======================== Helpers ========================
def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)

def num(x):
    try:
        v = float(x)
        return v if v == v and abs(v) != float("inf") else None
    except (TypeError, ValueError):
        return None

def yf_info(ticker, retries=3):
    for a in range(retries):
        try:
            info = yf.Ticker(ticker, session=_YF).info
            if info and info.get("marketCap") is not None:
                return info
        except Exception:
            pass
        if a < retries - 1:
            time.sleep(4)
    return None

def load_sector45_peers(csv_path, floor):
    """Return list of GICS-45 tickers with end-2024 mktcap >= floor ($M)."""
    peers = set()
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            if row["gsector"] != "45":
                continue
            try:
                if float(row["mktcap"]) >= floor and float(row["pb_ratio"]) > 0:
                    peers.add(row["tic"].strip().upper())
            except ValueError:
                continue
    return sorted(peers)

def load_screen_cache(path):
    cache = {}
    if os.path.exists(path):
        for r in json.load(open(path)):
            pb = num(r.get("pb"))
            if pb is not None and pb > 0:
                cache[r["ticker"].upper()] = pb
    return cache

def valuation_label(pctile):
    if pctile is None:
        return None
    if pctile <= TERTILE_LOW:
        return "Low"
    if pctile >= TERTILE_HIGH:
        return "High"
    return "Mid"

# ======================== Main ========================
def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    run_dir = os.path.join(BASE_DIR, "stock", "runs", f"run_{date_str}")
    cur_dir = os.path.join(BASE_DIR, "stock", "current")
    for d in (run_dir, cur_dir):
        os.makedirs(d, exist_ok=True)

    # 1) Build CURRENT-P/B peer universe (GICS-45, >= $10B)
    peer_tics = load_sector45_peers(WRDS_CSV, PEER_MKTCAP_FLOOR)
    cache = load_screen_cache(SCREEN_CACHE)
    print(f"Peer universe: {len(peer_tics)} GICS-45 tickers (end-2024 mktcap >= ${PEER_MKTCAP_FLOOR}M)")
    print(f"  reusing {sum(1 for t in peer_tics if t in cache)} current P/Bs from screen cache; fetching the rest…")
    peer_pbs, fetched, failed = [], 0, []
    for t in peer_tics:
        if t in cache:
            peer_pbs.append(cache[t]); continue
        info = yf_info(t, retries=2)
        pb = num(info.get("priceToBook")) if info else None
        if pb is not None and pb > 0:
            peer_pbs.append(pb)
        else:
            failed.append(t)
        fetched += 1
        time.sleep(PEER_SLEEP)
    # also make sure the slate themselves are in the peer pool (current values, added below)
    print(f"  fetched {fetched} live, {len(peer_pbs)} usable peer P/Bs, {len(failed)} unresolved")
    peer_pbs_sorted = sorted(peer_pbs)
    def pctile(pb):
        if pb is None or not peer_pbs_sorted:
            return None
        below = sum(1 for p in peer_pbs_sorted if p < pb)
        return round(100 * below / len(peer_pbs_sorted))

    # 2) Slate fundamentals (current P/B, mcap, div) from a single .info call each
    print("\nSlate (current Yahoo priceToBook):")
    fundamentals, errors = {}, []
    for sym in STOCKS:
        info = yf_info(sym)
        if not info:
            errors.append(sym); print(f"  ❌ {sym}: info fetch failed"); continue
        pb = num(info.get("priceToBook"))
        mc = num(info.get("marketCap"))
        dv = num(info.get("dividendYield"))
        div_pct = None
        if dv is not None:
            conv = round(dv * 100, 2)
            div_pct = conv if conv <= 20 else round(dv, 2)   # yfinance sometimes already %
        pc = pctile(pb)
        fundamentals[sym] = {
            "marketcap":         round(mc / 1e6, 2) if mc else None,
            "pb_current":        round(pb, 2) if pb is not None else None,
            "pb_current_pctile": pc,
            "div_y":             div_pct,
            "valuation":         valuation_label(pc),
            "sector":            SECTOR_LABEL[sym],
        }
        f = fundamentals[sym]
        print(f"  {sym:5s} P/B={f['pb_current']:<6} pctile={pc:<3} -> {f['valuation']:<4} "
              f"(mcap={f['marketcap']}M, div={div_pct})")
        time.sleep(SLATE_SLEEP)

    # 3) Write outputs (fundamentals only; prices untouched)
    meta = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "pb_source": "yahoo priceToBook (current)",
        "peer_universe": f"GICS sector 45, end-2024 mktcap>=${PEER_MKTCAP_FLOOR}M, current Yahoo P/B",
        "n_peers_used": len(peer_pbs_sorted),
        "tertile_thresholds": {"low<=": TERTILE_LOW, "high>=": TERTILE_HIGH},
        "peers_unresolved": failed,
    }
    # Keep fundamentals.json to the ORIGINAL schema (ticker keys only) so the survey's
    # consumption of it is unaffected; provenance goes to a sidecar.
    write_json(os.path.join(run_dir, "fundamentals.json"), fundamentals)
    write_json(os.path.join(cur_dir, "fundamentals.json"), fundamentals)
    write_json(os.path.join(run_dir, "fundamentals_meta.json"), meta)
    write_json(os.path.join(cur_dir, "fundamentals_meta.json"), meta)
    print(f"\n✅ wrote fundamentals.json + fundamentals_meta.json (current dir + {os.path.basename(run_dir)})")
    if errors:
        print(f"⚠️  errors for: {errors}")
    if PUSH:
        print("PUSH=True not implemented in this copy; push manually.")
    else:
        print("ℹ️  Git push disabled (PUSH=False). Review, then commit/push when ready.")

if __name__ == "__main__":
    main()
