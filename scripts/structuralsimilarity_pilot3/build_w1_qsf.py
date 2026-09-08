#!/usr/bin/env python3
"""
Build the Structural Similarity PILOT 3 Wave 1 QSF.

Source = as-fielded export of "Model Spillovers - Investment Decisions -
Pilot 2 - Wave 1" (SV_42Yb7SVgakNrxoq; XRP slate, jsDelivr @main). Wave 1
is design-identical across the structural-similarity pilots and the
decisions pilots (field-level diff vs pilot-1 W1 on 2026-09-08: only the
data BASE URL and the TRX->XRP slate rename differ), so the pilot-3 W1 is a
pure copy with two edits:
  1. SurveyName -> "Model Spillovers - Structural Similarity - Pilot 3 - Wave 1"
  2. data BASE URL in the six *_infoscreen question JS ->
     .../@main/structuralsimilarity_pilot3/crypto/current/

Usage:  python3 build_w1_qsf.py <source.qsf> [<out.qsf>]
Default source/out live in ~/Downloads.
"""
import json, os, re, sys

SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Investment_Decisions_-_Pilot_2_-_Wave_1 (1).qsf")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot3_-_Wave_1_draft1.qsf")

OLD_BASE = "https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@main/decisions_pilot2/crypto/current/"
NEW_BASE = "https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@main/structuralsimilarity_pilot3/crypto/current/"
NEW_NAME = "Model Spillovers - Structural Similarity - Pilot 3 - Wave 1"
SLUGS = ["eth", "xmr", "bnb", "btc", "hype", "xrp"]
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "..", "structuralsimilarity_pilot3", "crypto", "current")

def strict_load(path):
    # Qualtrics rejects NaN/Infinity constants; refuse to read them either.
    with open(path) as f:
        return json.load(f, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))

q = strict_load(SRC)
assert q["SurveyEntry"]["SurveyName"] == "Model Spillovers - Investment Decisions - Pilot 2 - Wave 1", q["SurveyEntry"]["SurveyName"]
q["SurveyEntry"]["SurveyName"] = NEW_NAME

swapped = []
for el in q["SurveyElements"]:
    if el["Element"] != "SQ":
        continue
    p = el["Payload"]
    js = p.get("QuestionJS") or ""
    if OLD_BASE in js:
        p["QuestionJS"] = js.replace(OLD_BASE, NEW_BASE)
        swapped.append(p["DataExportTag"])

# ---- checks ----
assert sorted(swapped) == sorted(s + "_infoscreen" for s in SLUGS), swapped
s = json.dumps(q)
assert "decisions_pilot" not in s, "stale decisions_pilot reference left"
assert "@latest" not in s.replace("chart.js@2.9.4", ""), "@latest alias left (must be @main)"
assert s.count(NEW_BASE) == 6, s.count(NEW_BASE)
# the JS fetches BASE + ticker + "_365d.json"; every slate file must exist in the repo
for slug in SLUGS:
    fp = os.path.join(DATA_DIR, slug + "_365d.json")
    assert os.path.exists(fp), "missing data file " + fp
# slate consistency: flow ED cur_ticker values == slugs
flow = json.dumps([e for e in q["SurveyElements"] if e["Element"] == "FL"])
tickers = sorted(set(t.lower() for t in re.findall(r'"Field":\s*"cur_ticker"[^}]*?"Value":\s*"([A-Z]+)"', flow)))
assert tickers == sorted(SLUGS), tickers
# every infoscreen resolves its file name from the piped cur_ticker ED field
for el in q["SurveyElements"]:
    if el["Element"] == "SQ" and (el["Payload"].get("DataExportTag") or "").endswith("_infoscreen"):
        js = el["Payload"]["QuestionJS"]
        assert "${e://Field/cur_ticker}" in js and 'BASE + ticker + "_365d.json"' in js, el["Payload"]["DataExportTag"]

with open(OUT, "w") as f:
    json.dump(q, f, separators=(",", ":"), ensure_ascii=False)
strict_load(OUT)
print("OK ->", OUT)
print("swapped BASE in:", ", ".join(swapped))
print("survey name:", NEW_NAME)
