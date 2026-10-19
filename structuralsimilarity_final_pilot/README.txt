Structural similarity final pilot — current crypto data fetched 19 October 2026.

Default: crypto/snapshots/pilot_2026-10-19_v1/gbp/ (historical GBP prices).
Window: 18 October 2025 through 18 October 2026; current UTC day excluded.
Direct GBP quotes: ETH, XMR, BNB, BTC, XRP. HYPE uses its USD series and historical daily GBP/USD; native GBP pair unavailable. Non-trading days use preceding available FX close. Raw sources, conversion mapping and hashes are retained. HYPE omits one missing 2 February 2026 price; no interpolation.

Pre-study / W1 replication: ETH -37.7%, XMR +95.4%, BNB -33.5%.
W1 own predictions: BTC -23.0%, HYPE +117.0%, XRP -44.4%.
Use the exact same GBP snapshot for the fresh pre-study and W1; freeze once the pre-study begins.

The fresh USD series and a fixed-rate GBP alternative are retained for comparison. The default uses historical GBP and includes currency movements. See currency_policy.json for the distinction and prospective stock/earnings translation guidance.

Survey QSFs and Prolific settings are delivered separately in Downloads/Structural_Similarity_Final_Pilot_2026-10-19/. This folder contains public numeric source data and provenance only.

Hosting: load the six GBP price JSON files and manifest.json from structuralsimilarity_final_pilot/crypto/snapshots/pilot_2026-10-19_v1/gbp/ using a jsDelivr URL pinned to the Git publication commit. Do not use a moving branch or current alias in a fielded survey.
