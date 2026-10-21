Structural similarity UK pilot 2: frozen GBP crypto snapshot

Folder: structuralsim_ukpilot2
Snapshot: ukpilot2_2026-10-20_v1
Freshly collected: 20 October 2026
Window: 19 October 2025 through 19 October 2026 (365 elapsed calendar days).
The incomplete current UTC day is excluded.

Files used by the survey:
crypto/snapshots/ukpilot2_2026-10-20_v1/gbp/<slug>_365d.json
crypto/snapshots/ukpilot2_2026-10-20_v1/gbp/manifest.json

Same asset roles and currency method as the first UK pilot:
Replication: ETH, XMR, BNB. Own forecasts: BTC, HYPE, XRP.
ETH/XMR/BNB/BTC/XRP are directly fetched Yahoo GBP series.
Yahoo HYPE32196-GBP returned HTTP 404 again; HYPE uses fresh USD daily
closes divided by same-date historical GBPUSD closes, or the latest
preceding FX observation on non-trading days. Never use a future FX rate.
Both groups retain one prior winner and two prior losers.
HYPE omits the unavailable historical observation on 2 February 2026;
no interpolation. Other assets each contain 366 daily observations.

Source responses, endpoint dates, FX mapping and hashes are preserved.
The daily price parser and output precision match the preceding UK pilot.
Do not change these files after this snapshot is used for fielding.
QSF delivery should be pinned to the publication commit via jsDelivr.
This folder contains public prices and source code only.


Wave 2 stock alternatives: refreshed 21 October 2026

Latest completed US close: 20 October 2026.
Calendar-anniversary price window: 20 October 2025 to 20 October 2026,
252 common trading observations per stock. Prices and all earnings are GBP.
Earnings: latest sixteen consecutive reported quarters, translated using
each quarter's mean daily GBP-per-USD rate, then summed into four annual bars.

Frozen, separately named alternatives (choose one before fielding):
stock/snapshots/ukpilot2_2026-10-21_stock_otex_v1/gbp/
stock/snapshots/ukpilot2_2026-10-21_stock_orcl_v1/gbp/

Both contain FICO, OLED, DBX, CSCO, INTU and ANET. The seventh company is
OTEX (OpenText) or ORCL (Oracle). Displayed annual price/profit correlations:
OpenText +0.063064; Oracle -0.072250. Neither variant refreshes automatically.

Each snapshot includes seven price series, profits.json, fundamentals.json,
slate.json, currency_policy.json and manifest.json. The manifest hashes every
other JSON file. The QSF must use the selected snapshot's immutable commit URL
and its specific manifest hash; this data publication does not edit surveys.

All eight SEC quarter series were rechecked; their underlying USD earnings
values and displayed GBP profit-growth rates are unchanged since 20 October.
Source prices, FX, compact verified SEC facts and company fundamentals are in
sources/ukpilot2_2026-10-21_stocks_v1/. Complete SEC responses remain locally
archived; their SHA256 hashes and the exact facts underlying the selected
quarters, including annual-minus-nine-month calculations, are retained here.
Comparison: stock/reviews/comparison_2026-10-21.json.

The Wave 1 crypto snapshot and its source files are unchanged.

Final W2 selection, 21 October 2026: stock/snapshots/ukpilot2_2026-10-21_stock_final_v1/gbp. Autodesk replaces FICO; Oracle is retained. Data end at 20 October 2026. Frozen snapshot with seven stocks and exact hashes in manifest.json. Two fixed reverse orders begin with Cisco and Dropbox; see stock/reviews/final_selection_2026-10-21.json. Crypto data are unchanged.
