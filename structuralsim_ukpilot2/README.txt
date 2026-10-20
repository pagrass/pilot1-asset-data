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
