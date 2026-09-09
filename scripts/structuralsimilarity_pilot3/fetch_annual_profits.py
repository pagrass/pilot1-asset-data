#!/usr/bin/env python3
"""
Fetch ANNUAL net profits (last 4 fiscal years) for the pilot-3 stock slate and
write profits.json — the data behind the NEW profit-history bar chart on the
W2 infoscreen (pilot-3 profit-chart redesign, 2026-08-31).

Consistency contract (decided 2026-08-31):
  * chart      = last 4 posted fiscal years (bars), $B
  * panel stat "Net profit (last fiscal year)"    = last bar
  * panel stat "Profit change (last fiscal year)" = last two bars' % change
  so the chart and both displayed numbers can never contradict each other.

FY fill: Yahoo's annual table lags fresh earnings releases by a few weeks,
and the fundamentals-timeseries trailingNetIncome can lag too (observed asOf
~Apr 2026 for CSCO while quoteSummary was fresh). Rule: if a new FY has ended
>=25 days ago but is not yet in the annual table, fill it from yfinance
netIncomeToCommon (quoteSummary TTM == the just-ended FY for these names; the
same source as the fielded pilot-2 "netprofit" stat, rolls promptly after
earnings). Record fill provenance; verify once Yahoo's annual table rolls.

Also writes netprofit_growth (= fychange_pct) into fundamentals.json so the
legacy <t>_profitgrowth embedded-data field keeps carrying the displayed
profit change (replaces the old add_profit_growth.py hand-pin). Run this
AFTER fetch_stocks.py / fetch_stocks_prices_only.py.

Selection rule reminder (2026-08-31/09-09): every FY in the 4-year window
must be comfortably positive. PANW was dropped on 2026-09-09 when its FY2026
came in at ~$0.31B (-73%, with a loss quarter) -- the assert below would
pass on the sign alone, so eyeball the printed series before pushing.

Output: structuralsimilarity_pilot3/stock/current/profits.json (+ runs/ copy)
"""
import json, os, time, urllib.request
from datetime import datetime, date, timedelta

import yfinance as yf
from curl_cffi import requests as curl_requests
_YF_SESSION = curl_requests.Session(impersonate="chrome")

STOCKS = ["INTU", "CSCO", "ANET", "AKAM", "ADBE", "EPAM", "ORCL", "FTNT", "TXN"]
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
CUR = os.path.join(REPO_ROOT, "structuralsimilarity_pilot3", "stock", "current")
RUN = os.path.join(REPO_ROOT, "structuralsimilarity_pilot3", "stock", "runs",
                   "run_" + datetime.now().strftime("%Y-%m-%d"))
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

def get(url):
    req = urllib.request.Request(url, headers=UA)
    return json.load(urllib.request.urlopen(req, timeout=20))

out, errors = {}, []
for t in STOCKS:
    try:
        d = get("https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/"
                + t + "?type=annualNetIncome,trailingNetIncome&period1=1400000000&period2=1800000000&merge=false")
        ann, ttm = [], None
        for r in d["timeseries"]["result"]:
            for item in (r.get("annualNetIncome") or []):
                if item: ann.append((item["asOfDate"], item["reportedValue"]["raw"]))
            for item in (r.get("trailingNetIncome") or []):
                if item: ttm = (item["asOfDate"], item["reportedValue"]["raw"])
        ann = sorted(ann)
        fill = None
        last_end = date.fromisoformat(ann[-1][0])
        next_end = date(last_end.year + 1, last_end.month, last_end.day)
        if ttm and (date.fromisoformat(ttm[0]) - last_end).days > 300:
            ann.append(ttm)                        # timeseries TTM already rolled to new FY
            fill = {"source": "trailingNetIncome", "asOf": ttm[0]}
        elif date.today() >= next_end + timedelta(days=25):
            info = yf.Ticker(t, session=_YF_SESSION).info
            ni = info.get("netIncomeToCommon")
            assert ni and ni > 0, "no fresh netIncomeToCommon for %s" % t
            ann.append((next_end.isoformat(), float(ni)))
            fill = {"source": "netIncomeToCommon", "asOf": next_end.isoformat()}
        ann = ann[-4:]
        vals = [round(v / 1e9, 2) for _, v in ann]
        assert len(vals) == 4 and all(v > 0 for v in vals), "need 4 positive FYs: %s" % vals
        out[t] = {
            "fy_ends":   [a for a, _ in ann],
            "fy_labels": ["FY " + a[:4] for a, _ in ann],
            "fy_values_bn": vals,
            "np_last_fy_bn": vals[-1],
            "fychange_pct": round((vals[-1] / vals[-2] - 1) * 100, 1),
            "fill_last": fill,
        }
        print(t, out[t]["fy_labels"], vals, "%+.1f%%" % out[t]["fychange_pct"],
              ("(filled: %s)" % fill["source"]) if fill else "")
    except Exception as e:
        errors.append("%s: %s" % (t, e)); print("ERR", t, e)
    time.sleep(0.4)

if errors:
    raise SystemExit("errors — not writing: %s" % errors)
os.makedirs(CUR, exist_ok=True); os.makedirs(RUN, exist_ok=True)
payload = {"fetched_at": datetime.now().isoformat(timespec="seconds"), "stocks": out}
for d in (CUR, RUN):
    json.dump(payload, open(os.path.join(d, "profits.json"), "w"), indent=1)
print("wrote", os.path.join(CUR, "profits.json"))

# mirror the displayed change into fundamentals.json (legacy <t>_profitgrowth ED)
for d in (CUR, RUN):
    fp = os.path.join(d, "fundamentals.json")
    if not os.path.exists(fp):
        continue
    f = json.load(open(fp))
    for t, rec in out.items():
        if t in f:
            f[t]["netprofit_growth"] = rec["fychange_pct"]
    json.dump(f, open(fp, "w"), indent=2)
    print("netprofit_growth written into", fp)
