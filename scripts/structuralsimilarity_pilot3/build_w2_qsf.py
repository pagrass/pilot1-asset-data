#!/usr/bin/env python3
"""
Build the Structural Similarity PILOT 3 Wave 2 QSF, draft 2 (2026-09-09).

Source = draft 1 (2026-08-31; built off the fielded pilot-2 W2 export with the
profit-chart redesign, second comprehension question, definitions/JS etc.).
Draft 2 applies, on top of draft 1:

  1. jsDelivr alias @latest -> @main in the six *_infoscreen JS (the repo has
     no git tags; @latest resolves to HEAD with sticky per-file caching and
     served mixed vintages on 2026-08-31 -- see commit 4d36932). W1 already
     uses @main.
  2. Slate v2: ORCL -> ADBE, SNPS -> EPAM, PANW -> AKAM, FTNT -> ANET
     (INTU, CSCO stay). DataExportTags, block descriptions, the cur_ticker/
     cur_name/cur_shortname flow entries, and the order label
     orclfirst -> adbefirst (branch logic + ED value).
       adbefirst: ADBE, EPAM, AKAM, CSCO, INTU, ANET
       cscofirst: CSCO, INTU, ANET, ADBE, EPAM, AKAM
  3. FL_19 embedded-data declarations rebuilt for the fielded slate, incl. the
     pilot-3 provenance fields (<t>_fyvals, <t>_fylabels, <t>_fychange) that
     the infoscreen JS sets but draft 1 never declared (draft 1 still carried
     the pilot-2 akam/ffiv/epam declarations and none for snps/ftnt/panw).
  4. Profits-arm $100 feedback sentence in the six *_percent JS: "over the
     past 12 months" -> "in its last fiscal year" (the base the bonus and the
     panel use since the fiscal-year rebase).
  5. info_reliance: new option "The net profit chart" (choice 6, shown after
     "The net profit figures"); toggle ADD_CHART_OPTION.

Not touched (Paul's side): Prolific completion codes (still pilot-2's, plus
one cc=REPLACEWITHSCREENOUTCODE), bridge_belief (in Trash).

Usage:  python3 build_w2_qsf.py [<source.qsf>] [<out.qsf>]
"""
import copy, json, os, re, sys

SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot3_-_Wave_2_draft1.qsf")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot3_-_Wave_2_draft2.qsf")

ADD_CHART_OPTION = True

OLD_BASE = "https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@latest/structuralsimilarity_pilot3/stock/current/"
NEW_BASE = "https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@main/structuralsimilarity_pilot3/stock/current/"

# old slug -> (new slug, ticker, cur_name, cur_shortname, block description)
SWAP = {
    "orcl": ("adbe", "ADBE", "Adobe Inc.", "Adobe", "Stocks (Adobe)"),
    "snps": ("epam", "EPAM", "EPAM Systems, Inc.", "EPAM Systems", "Stocks (EPAM Systems)"),
    "panw": ("akam", "AKAM", "Akamai Technologies, Inc.", "Akamai", "Stocks (Akamai)"),
    "ftnt": ("anet", "ANET", "Arista Networks, Inc.", "Arista Networks", "Stocks (Arista Networks)"),
}
# old slug -> (ticker, cur_name, cur_shortname, block description) as in draft 1
OLD_NAMES = {
    "orcl": ("ORCL", "Oracle Corporation", "Oracle", "Stocks (Oracle)"),
    "snps": ("SNPS", "Synopsys, Inc.", "Synopsys", "Stocks (Synopsys)"),
    "panw": ("PANW", "Palo Alto Networks, Inc.", "Palo Alto Networks", "Stocks (Palo Alto Networks)"),
    "ftnt": ("FTNT", "Fortinet, Inc.", "Fortinet", "Stocks (Fortinet)"),
}
ORDER_RENAME = ("orclfirst", "adbefirst")
SLATE = ["adbe", "epam", "akam", "csco", "intu", "anet"]
ORDERS = {"adbefirst": ["ADBE", "EPAM", "AKAM", "CSCO", "INTU", "ANET"],
          "cscofirst": ["CSCO", "INTU", "ANET", "ADBE", "EPAM", "AKAM"]}
PER_TICKER_FIELDS = ["365d_pct", "12m_max", "12m_min", "current", "sector", "valuation",
                     "pb_current", "pb_pctile", "div_y", "marketcap_raw",
                     "netprofit", "profitgrowth", "fyvals", "fylabels", "fychange"]
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "..", "structuralsimilarity_pilot3", "stock", "current")

OLD_FEEDBACK = "earned $100 in profits over the past 12 months, you expect it to earn"
NEW_FEEDBACK = "earned $100 in profits in its last fiscal year, you expect it to earn"


def strict_load(path):
    with open(path) as f:
        return json.load(f, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))


q = strict_load(SRC)
src_elem_order = [e["Element"] for e in q["SurveyElements"]]
assert q["SurveyEntry"]["SurveyName"] == "Model Spillovers - Structural Similarity - Pilot 3 - Wave 2"

SQ = {e["PrimaryAttribute"]: e["Payload"] for e in q["SurveyElements"] if e["Element"] == "SQ"}
checks = []


def check(cond, msg):
    checks.append((bool(cond), msg))
    assert cond, msg


# ---------------- 1. CDN alias ----------------
n_base = 0
for p in SQ.values():
    js = p.get("QuestionJS")
    if js and OLD_BASE in js:
        p["QuestionJS"] = js.replace(OLD_BASE, NEW_BASE)
        n_base += 1
check(n_base == 6, "BASE swapped in 6 infoscreens (got %d)" % n_base)

# ---------------- 2. slate swap: tags ----------------
retagged = []
for p in SQ.values():
    tag = p.get("DataExportTag") or ""
    for old, (new, *_r) in SWAP.items():
        if tag.startswith(old + "_"):
            p["DataExportTag"] = new + tag[len(old):]
            retagged.append((tag, p["DataExportTag"]))
check(len(retagged) == 5 * len(SWAP), "%d tags retagged (got %d)" % (5 * len(SWAP), len(retagged)))

# block descriptions
BL = [e for e in q["SurveyElements"] if e["Element"] == "BL"][0]["Payload"]
blocks = list(BL.values()) if isinstance(BL, dict) else BL
n_bl = 0
for b in blocks:
    for old, (new, tick, name, short, desc) in SWAP.items():
        if b["Description"] == OLD_NAMES[old][3]:
            b["Description"] = desc
            n_bl += 1
check(n_bl == len(SWAP), "%d block descriptions renamed" % len(SWAP))

# flow: cur_ticker / cur_name / cur_shortname entries + order label + sequences
FL = [e for e in q["SurveyElements"] if e["Element"] == "FL"][0]["Payload"]


def walk(node):
    for el in node.get("Flow", []):
        yield el
        yield from walk(el)


n_cur = 0
for el in walk(FL):
    if el.get("Type") == "EmbeddedData":
        d = {x.get("Field"): x for x in el["EmbeddedData"]}
        if "cur_ticker" in d and d["cur_ticker"].get("Value"):
            tick = d["cur_ticker"]["Value"]
            for old, (new, ntick, nname, nshort, _) in SWAP.items():
                if tick == OLD_NAMES[old][0]:
                    d["cur_ticker"]["Value"] = ntick
                    d["cur_name"]["Value"] = nname
                    d["cur_shortname"]["Value"] = nshort
                    n_cur += 1
check(n_cur == 2 * len(SWAP), "cur_* flow entries swapped (%d tickers x 2 orders)" % len(SWAP))

# order label rename (ED value + branch logic operand/description)
n_ord = 0
for el in walk(FL):
    if el.get("Type") == "EmbeddedData":
        for x in el["EmbeddedData"]:
            if x.get("Field") == "order" and x.get("Value") == ORDER_RENAME[0]:
                x["Value"] = ORDER_RENAME[1]
                n_ord += 1
    if el.get("Type") == "Branch":
        lg = json.dumps(el.get("BranchLogic"))
        if '"RightOperand": "%s"' % ORDER_RENAME[0] in lg:
            el["BranchLogic"] = json.loads(lg.replace(ORDER_RENAME[0], ORDER_RENAME[1]))
            n_ord += 1
check(n_ord == 2, "order label renamed in ED value + branch (got %d)" % n_ord)

# verify the two presentation orders
n_seq = 0
for el in walk(FL):
    if el.get("Type") == "Branch":
        lg = json.dumps(el.get("BranchLogic"))
        for oname, seq in ORDERS.items():
            if '"LeftOperand": "order"' in lg and '"RightOperand": "%s"' % oname in lg:
                seen = [x["Value"] for sub in el.get("Flow", []) if sub.get("Type") == "EmbeddedData"
                        for x in sub["EmbeddedData"] if x.get("Field") == "cur_ticker"]
                check(seen == seq, "order %s = %s (got %s)" % (oname, seq, seen))
                n_seq += 1
check(n_seq == 2, "both order branches verified")

# ---------------- 3. FL_19 declarations ----------------
fl19 = [el for el in FL["Flow"] if el.get("FlowID") == "FL_19"][0]
keep = [x for x in fl19["EmbeddedData"]
        if not re.match(r"^[a-z]{3,5}_(%s)$" % "|".join(PER_TICKER_FIELDS), x.get("Field", ""))]
template = [x for x in fl19["EmbeddedData"] if x.get("Field") == "orcl_365d_pct"][0]
new_fields = []
for t in SLATE:
    for f in PER_TICKER_FIELDS:
        x = copy.deepcopy(template)
        x["Field"] = "%s_%s" % (t, f)
        x["Description"] = x["Field"]
        x.pop("Value", None)
        new_fields.append(x)
fl19["EmbeddedData"] = keep + new_fields
fields = [x["Field"] for x in fl19["EmbeddedData"]]
check(len(keep) == 10, "10 non-ticker FL_19 fields kept (got %d)" % len(keep))
check(len(new_fields) == 6 * len(PER_TICKER_FIELDS), "per-ticker FL_19 fields rebuilt")
check(len(fields) == len(set(fields)), "no duplicate FL_19 fields")
for x in new_fields:
    check("Value" not in x and x.get("Type") == template.get("Type"), "declaration shape ok")

# ---------------- 4. feedback wording ----------------
n_fb = 0
for p in SQ.values():
    js = p.get("QuestionJS")
    if js and OLD_FEEDBACK in js:
        p["QuestionJS"] = js.replace(OLD_FEEDBACK, NEW_FEEDBACK)
        n_fb += 1
check(n_fb == 6, "feedback sentence rebased in 6 percent questions (got %d)" % n_fb)

# ---------------- 5. info_reliance chart option ----------------
ir = [p for p in SQ.values() if p.get("DataExportTag") == "info_reliance"][0]
if ADD_CHART_OPTION:
    check("6" not in ir["Choices"], "choice 6 free")
    ir["Choices"]["6"] = {"Display": "The net profit chart"}
    ir["ChoiceOrder"] = ["1", "2", "6", "3", "4", "5"]
    ir["NextChoiceId"] = max(ir.get("NextChoiceId", 6), 7)
    check(ir.get("RecodeValues") in (None, {}), "no recode map to extend")

# ---------------- global checks ----------------
s = json.dumps(q)
check("@latest" not in s.replace("chart.js@2.9.4", ""), "no @latest left")
check(s.count(NEW_BASE) == 6, "6 x new BASE")
for old, (tick, name, short, desc) in OLD_NAMES.items():
    check(old + "_" not in s, "no '%s_' tag left" % old)
    for bad in (tick, name, short, desc):
        check(bad not in s, "no '%s' left" % bad)
check(ORDER_RENAME[0] not in s, "old order label gone")
check(OLD_FEEDBACK not in s, "old feedback wording gone")
for slug in SLATE:
    check(os.path.exists(os.path.join(DATA_DIR, slug + "_365d.json")), "data file for " + slug)
prof = strict_load(os.path.join(DATA_DIR, "profits.json"))["stocks"]
fund = strict_load(os.path.join(DATA_DIR, "fundamentals.json"))
for slug in SLATE:
    check(slug.upper() in prof and slug.upper() in fund, "profits+fundamentals for " + slug)
    check(len(prof[slug.upper()]["fy_values_bn"]) == 4 and min(prof[slug.upper()]["fy_values_bn"]) > 0,
          "4 positive FYs for " + slug)
tickers = sorted(set(t.lower() for el in walk(FL) if el.get("Type") == "EmbeddedData"
                     for x in el["EmbeddedData"] if x.get("Field") == "cur_ticker" and x.get("Value")
                     for t in [x["Value"]]))
check(tickers == sorted(SLATE), "flow cur_ticker set == slate (%s)" % tickers)
tags = sorted(p["DataExportTag"] for p in SQ.values() if re.match(r"^[a-z]{4}_(binary|percent|conf|time|infoscreen)$", p.get("DataExportTag", "")))
check(tags == sorted("%s_%s" % (t, k) for t in SLATE for k in ("binary", "percent", "conf", "time", "infoscreen")), "stock tags == slate")
# schema hygiene (lessons from the 08-26 / 08-28 import failures)
check([e["Element"] for e in q["SurveyElements"]] == src_elem_order, "element order unchanged")
check(all(not ("QuestionJS" in p and p["QuestionJS"] is None) for p in SQ.values()), "no null QuestionJS")
qc = [e for e in q["SurveyElements"] if e["Element"] == "QC"][0]
check(qc.get("Payload") is None, "QC payload null")
for el in walk(FL):
    if el.get("Type") == "EmbeddedData":
        for x in el["EmbeddedData"]:
            if x.get("Value") not in (None, ""):
                check(x.get("Type") == "Custom", "ED with Value must be Custom: %s" % x.get("Field"))
# the comprehension gate still tests both questions in all four branches
n_gate = sum(1 for el in walk(FL) if el.get("Type") == "Branch"
             and "QID608" in json.dumps(el.get("BranchLogic")) and "QID662" in json.dumps(el.get("BranchLogic")))
check(n_gate == 4, "4 two-question gate branches")
# every infoscreen resolves its file from the piped cur_ticker and fetches all three data files
for p in SQ.values():
    if (p.get("DataExportTag") or "").endswith("_infoscreen"):
        js = p["QuestionJS"]
        check("${e://Field/cur_ticker}" in js and 'BASE+ticker+"_365d.json"' in js
              and 'BASE+"fundamentals.json"' in js and 'BASE+"profits.json"' in js, "infoscreen JS shape " + p["DataExportTag"])

with open(OUT, "w") as f:
    json.dump(q, f, separators=(",", ":"), ensure_ascii=False)
strict_load(OUT)
print("OK -> %s  (%d/%d checks)" % (OUT, sum(1 for c, _ in checks if c), len(checks)))
print("retagged:", ", ".join("%s->%s" % rt for rt in retagged))
print("FL_19 fields:", len(fields))
