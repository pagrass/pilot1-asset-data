#!/usr/bin/env python3
"""Add netprofit_growth (12-mo profit growth, %) to structuralsimilarity_pilot2 fundamentals.json.

Convention (pinned 2026-08-26; EPAM added 2026-08-27, ORCL swap 2026-08-28 from yfinance annual income statements + netIncomeToCommon TTM):
latest completed 12-month reporting period vs the preceding comparable 12 months.
  ORCL: FY26 (May) vs FY25          = +37.3   (TTM == just-closed FY26; $12.44B -> $17.09B, smooth accelerating series)
  INTU: TTM (= FY26, Jul) vs FY25   = +18.0
  CSCO: TTM (= FY26, Jul) vs FY25   = +30.3
  EPAM: FY25 (Dec) vs FY24          = -16.9   ($454.5M -> $377.7M; clean multi-quarter decline, no one-offs)
  AKAM: FY25 (Dec) vs FY24          = -10.5
  FFIV: FY25 (Sep) vs FY24          = +22.2
Screened for one-offs: none in the windows used (EPAM annual series 2022-2025 smooth).
Values are static between earnings reports (next slate earnings: Oct 2026) — the
prices-only launch fetch never touches fundamentals.json, so this pin holds.
"""
import json, os

GROWTH = {"ORCL": 37.3, "INTU": 18.0, "CSCO": 30.3, "AKAM": -10.5, "FFIV": 22.2, "EPAM": -16.9}
PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                    "structuralsimilarity_pilot2", "stock", "current", "fundamentals.json")

f = json.load(open(PATH))
for tk, g in GROWTH.items():
    f[tk]["netprofit_growth"] = g
json.dump(f, open(PATH, "w"), indent=2)
print("updated", os.path.abspath(PATH))
for tk in GROWTH:
    print(f"  {tk}: netprofit={f[tk]['netprofit']}  netprofit_growth={f[tk]['netprofit_growth']:+.1f}%")
