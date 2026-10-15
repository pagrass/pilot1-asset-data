Decisions main study — Wave 2 stock data
Latest publication: 2026-10-15; completed price session: 2026-10-14.
Current data: current/ (copy of the frozen snapshot).
Current immutable snapshot: snapshots/d3_2026-10-15_v1/
All earlier snapshots are retained unchanged for reproducibility.
Public raw source responses: source_snapshots/d3_2026-10-15_v1/
Seven selected pairs: CSCO/INTU, NTAP/EPAM, AKAM/NOW, FFIV/CHKP, ZM/ADBE, TXN/QCOM, CRM/ORCL.
All current series have 252 daily closing observations from 2025-10-14 to 2026-10-14.
The current requested start is 2025-10-14, exactly 365 elapsed days before the closing-price date.
Yahoo split-adjusted quote.close, USD, rounded to cents; excludes dividend reinvestment.
Valuation categories use refreshed Yahoo P/B and the inherited historical Technology peer reference, not refreshed peers.
No valuation-category changes relative to the previous publication.
Private survey QSF files and participant data are deliberately absent.
The live-CDN W2 QSF fetches stock/current/ through jsDelivr @main with browser cache revalidation.
No stock prices or fundamentals are embedded in that QSF.
The downloaded manifest supplies SHA-256 checksums; mismatched/unavailable files block continuation.
The snapshot identifier and price window are recorded per pair.
To update stocks: publish a new archived snapshot, update current/, purge all current jsDelivr URLs, and verify hashes/CORS.
The revised QSF must be imported and published in Qualtrics once; later data updates require no QSF edit.
The earlier D1 embedded draft is superseded by the live-CDN version.
Crypto data are unchanged.

Consecutive main-study numbering: p1 CSCO/INTU; p2 NTAP/EPAM; p3 AKAM/NOW; p4 FFIV/CHKP; p5 ZM/ADBE; p6 TXN/QCOM; p7 CRM/ORCL.
Use pair_id_crosswalk.json when comparing earlier QSF drafts or pilot IDs.
Day 1 version 2 changed pair metadata only. Day 2 adds the October 13 close and refreshes fundamentals; all overlapping historical prices are unchanged.

Day 2 version 2 restores positive dividend yields for CSCO, INTU, NTAP, TXN, QCOM, CRM, and ORCL using the archived Yahoo dividendYield when dividendRate is absent. Each matches the prior annual dividend rate divided by the October 13 close. All other fundamentals and all prices are unchanged.
Day 3 adds the October 14 close and rolls the start to October 14, 2025. All overlapping historical prices are unchanged. All seven pair rankings, sign patterns, and valuation labels are preserved.
Day 3 dividend yields are recalculated using the previously observed indicated annual rates because Yahoo omitted dividendRate and supplied yields based on the previous close. Current lastDividendValue and exDividendDate are unchanged; each reported yield agrees with the archived rate divided by the previous close. See verified_dividend_rates.json for provenance. This is a carried-forward annual rate, not a newly retrieved dividendRate.
For future refreshes use source_snapshots/d3_2026-10-15_v1/refresh_stocks.py and build_d3.py, with a new snapshot identifier and dates. Include verified_dividend_rates.json and the peer benchmark. The dividend fallback checks for changed dividends/dates and unexplained yields and stops for review instead of silently publishing stale data. Refresh or review the archived rates when those checks fail.
