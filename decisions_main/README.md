# Decisions experiment: crypto snapshot, 12 October 2026

Frozen daily USD price data for the decisions experiment pre-study and Wave 1.
Window: **11 October 2025 through 11 October 2026**, 365 elapsed calendar days.
Both surveys must use the same snapshot, pinned to the publication commit.

| Stage | Asset | 12-month return |
|---|---|---:|
| Pre-study / W1 replication | Ethereum (ETH) | -32.4% |
| Pre-study / W1 replication | Monero (XMR) | +80.1% |
| Pre-study / W1 replication | BNB | -33.7% |
| W1 own predictions | Bitcoin (BTC) | -24.5% |
| W1 own predictions | Hyperliquid (HYPE) | +135.0% |
| W1 own predictions | XRP | -41.0% |

Returns use the displayed, rounded price endpoints, exactly as the surveys do.
Prices are Yahoo daily `quote.close`, with USD prices rounded using the established
survey rule (two decimals for values >= 1; four otherwise). Current-day observations
are excluded. Raw public responses, retrieval URLs, and SHA-256 hashes are retained.
There are 366 observations per asset except HYPE (365): its missing 2 February 2026
observation is omitted, not filled or interpolated. The line chart connects adjacent
available observations. This gap is also present in the previous snapshot.

All six signs are preserved relative to the previous snapshot. Each stage still
has one winner and two losers. ETH and BNB now have similar losses; HYPE's gain
is larger. The intended direction contrast remains, but effect sizes and power
cannot be established from stimulus returns alone.

`crypto/snapshots/launch_2026-10-12_v1/manifest.json` documents the files and assets.
`validation.json` records integrity and old-versus-new return checks.
`source_snapshots/launch_2026-10-12_v1/` preserves the public input data and parser.
To reproduce parsing without another download, import `fetch_assets.py` and call
`price_series(symbol, slug, "crypto", date(2026,10,11), 365, source_folder, True)`.

QSFs are delivered locally, not stored here: their inherited Prolific integration
and study routing settings should not be published with the public asset data.
