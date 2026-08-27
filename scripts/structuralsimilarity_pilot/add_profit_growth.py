#!/usr/bin/env python3
"""Add netprofit_growth (12-mo profit growth, %) to structuralsimilarity_pilot fundamentals.json.

Convention (pinned 2026-08-26 from yfinance annual income statements + netIncomeToCommon TTM):
latest completed 12-month reporting period vs the preceding comparable 12 months.
  MSFT: FY26 (Jun) vs FY25          = +31.3   (TTM == just-closed FY26)
  INTU: TTM (= FY26, Jul) vs FY25   = +18.0
  CSCO: TTM (= FY26, Jul) vs FY25   = +30.3
  NOW:  FY25 (Dec) vs FY24          = +22.7   (prior-TTM not derivable from yfinance quarterly depth)
  AKAM: FY25 (Dec) vs FY24          = -10.5
  FFIV: FY25 (Sep) vs FY24          = +22.2
Screened for one-offs: NOW's FY23 tax-release year is outside the window used.
Values are static between earnings reports (next slate earnings: Oct 2026) — the
prices-only launch fetch never touches fundamentals.json, so this pin holds.
"""
import json, os

GROWTH = {"MSFT": 31.3, "INTU": 18.0, "CSCO": 30.3, "NOW": 22.7, "AKAM": -10.5, "FFIV": 22.2}
PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                    "structuralsimilarity_pilot", "stock", "current", "fundamentals.json")

f = json.load(open(PATH))
for tk, g in GROWTH.items():
    f[tk]["netprofit_growth"] = g
json.dump(f, open(PATH, "w"), indent=2)
print("updated", os.path.abspath(PATH))
for tk in GROWTH:
    print(f"  {tk}: netprofit={f[tk]['netprofit']}  netprofit_growth={f[tk]['netprofit_growth']:+.1f}%")
