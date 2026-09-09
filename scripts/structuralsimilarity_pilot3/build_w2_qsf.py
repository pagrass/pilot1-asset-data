#!/usr/bin/env python3
"""
Build the Structural Similarity PILOT 3 Wave 2 QSF, draft 3 (2026-09-09).

Source = draft 1 (2026-08-31; built off the fielded pilot-2 W2 export with the
profit-chart redesign, second comprehension question, definitions/JS etc.).
This builder applies, on top of draft 1:

  1. jsDelivr alias @latest -> @main in the *_infoscreen JS (the repo has no
     git tags; @latest resolves to HEAD with sticky per-file caching and
     served mixed vintages on 2026-08-31 -- see commit 4d36932). W1 already
     uses @main.
  2. Slate v2 renames: ORCL -> ADBE, SNPS -> EPAM, PANW -> AKAM, FTNT -> ANET
     (INTU, CSCO stay). DataExportTags, block descriptions, the cur_ticker/
     cur_name/cur_shortname flow entries, and the order label
     orclfirst -> adbefirst (branch logic + ED value).
  3. SEVENTH STOCK (draft 3): a fresh "Stocks (Oracle)" block cloned from the
     Adobe block (QID663-667, tags orcl_*), appended to half B of both
     presentation orders, plus its flow entries and FL_19 declarations:
       adbefirst: ADBE, EPAM, AKAM, CSCO, INTU, ANET, ORCL
       cscofirst: CSCO, INTU, ANET, ORCL, ADBE, EPAM, AKAM
     Participant-facing "six" -> "seven" (instructions x2, stocks_intro x1);
     screener duration 6 -> 7 minutes (toggle BUMP_MINUTES).
  4. FL_19 embedded-data declarations rebuilt for the fielded slate, incl. the
     pilot-3 provenance fields (<t>_fyvals, <t>_fylabels, <t>_fychange) that
     the infoscreen JS sets but draft 1 never declared (draft 1 still carried
     the pilot-2 akam/ffiv/epam declarations and none for snps/ftnt/panw).
  5. Profits-arm $100 feedback sentence in the *_percent JS: "over the past
     12 months" -> "in its last fiscal year" (the base the bonus and the panel
     use since the fiscal-year rebase).
  6. info_reliance: new option "The net profit chart" (choice 6, shown after
     "The net profit figures"); toggle ADD_CHART_OPTION.

Not touched (Paul's side): Prolific completion codes (still pilot-2's, plus
one cc=REPLACEWITHSCREENOUTCODE), bridge_belief (in Trash).

Usage:  python3 build_w2_qsf.py [<source.qsf>] [<out.qsf>]
"""
import copy, json, os, re, sys

SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot3_-_Wave_2_draft1.qsf")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot3_-_Wave_2_draft3.qsf")

ADD_CHART_OPTION = True
BUMP_MINUTES = ("6&nbsp;minutes", "7&nbsp;minutes")   # screener text; set to None to leave

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

# seventh stock: cloned from the Adobe block (the draft-1 Oracle block, QID369-373)
SEVENTH = {"slug": "orcl", "ticker": "ORCL", "name": "Oracle Corporation", "short": "Oracle",
           "desc": "Stocks (Oracle)", "clone_from_desc": "Stocks (Adobe)",
           "qids": ["QID663", "QID664", "QID665", "QID666", "QID667"],
           "block_id": "BL_stsimOracle7", "block_key": "59",
           "flow_ids": {"adbefirst": ("FL_1101", "FL_1102"), "cscofirst": ("FL_1103", "FL_1104")},
           "insert_after_ticker": "ANET"}   # appended after ANET in both orders (= end of half B)

SLATE = ["adbe", "epam", "akam", "csco", "intu", "anet", "orcl"]
ORDERS = {"adbefirst": ["ADBE", "EPAM", "AKAM", "CSCO", "INTU", "ANET", "ORCL"],
          "cscofirst": ["CSCO", "INTU", "ANET", "ORCL", "ADBE", "EPAM", "AKAM"]}
N = len(SLATE)
PER_TICKER_FIELDS = ["365d_pct", "12m_max", "12m_min", "current", "sector", "valuation",
                     "pb_current", "pb_pctile", "div_y", "marketcap_raw",
                     "netprofit", "profitgrowth", "fyvals", "fylabels", "fychange"]
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "..", "structuralsimilarity_pilot3", "stock", "current")

OLD_FEEDBACK = "earned $100 in profits over the past 12 months, you expect it to earn"
NEW_FEEDBACK = "earned $100 in profits in its last fiscal year, you expect it to earn"

# participant-facing stock-count wording: (tag, old, new, expected count)
SEVEN_TEXT = [
    ("instructions", "about six real&nbsp;<strong>stocks</strong>", "about seven real&nbsp;<strong>stocks</strong>", 1),
    ("instructions", "for each of the six stocks independently", "for each of the seven stocks independently", 1),
    ("stocks_intro", "of <b>six real ${e://Field/obj_entity_pl} </b>", "of <b>seven real ${e://Field/obj_entity_pl} </b>", 1),
]


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

# ---------------- 2. slate renames: tags ----------------
retagged = []
for p in SQ.values():
    tag = p.get("DataExportTag") or ""
    for old, (new, *_r) in SWAP.items():
        if tag.startswith(old + "_"):
            p["DataExportTag"] = new + tag[len(old):]
            retagged.append((tag, p["DataExportTag"]))
check(len(retagged) == 5 * len(SWAP), "%d tags retagged (got %d)" % (5 * len(SWAP), len(retagged)))
for old in SWAP:
    check(not any((p.get("DataExportTag") or "").startswith(old + "_") for p in SQ.values()), "no '%s_' tag left after rename" % old)

# block descriptions
BL = [e for e in q["SurveyElements"] if e["Element"] == "BL"][0]["Payload"]
check(isinstance(BL, dict), "BL payload is a dict")
blocks = list(BL.values())
n_bl = 0
for b in blocks:
    for old, (new, tick, name, short, desc) in SWAP.items():
        if b["Description"] == OLD_NAMES[old][3]:
            b["Description"] = desc
            n_bl += 1
check(n_bl == len(SWAP), "%d block descriptions renamed" % len(SWAP))

# flow: cur_ticker / cur_name / cur_shortname entries + order label
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

# the point of no return for stale names: everything below re-introduces ORCL on purpose
s_mid = json.dumps(q)
for old, (tick, name, short, desc) in OLD_NAMES.items():
    for bad in (tick, name, short, desc):
        check(bad not in s_mid, "no '%s' left after the renames" % bad)
check(ORDER_RENAME[0] not in s_mid, "old order label gone")

# ---------------- 3. seventh stock: clone the Adobe block ----------------
S = SEVENTH
src_block = [b for b in blocks if b["Description"] == S["clone_from_desc"]][0]
src_qids = [x["QuestionID"] for x in src_block["BlockElements"] if x.get("Type") == "Question"]
check(len(src_qids) == 5 == len(S["qids"]), "source block has 5 questions")
check(all(qid not in SQ for qid in S["qids"]), "new QIDs unused")
qid_map = dict(zip(src_qids, S["qids"]))
sq_elements = [e for e in q["SurveyElements"] if e["Element"] == "SQ"]
new_elems = []
for old_qid, new_qid in qid_map.items():
    src_el = [e for e in sq_elements if e["PrimaryAttribute"] == old_qid][0]
    el = copy.deepcopy(src_el)
    el["PrimaryAttribute"] = new_qid
    p = el["Payload"]
    p["QuestionID"] = new_qid
    tag = p["DataExportTag"]
    check(tag.startswith("adbe_"), "cloned tag is an adbe_* tag: " + tag)
    p["DataExportTag"] = S["slug"] + tag[len("adbe"):]
    js = json.dumps(p.get("QuestionJS") or "")
    check(old_qid not in js and "adbe" not in js.lower(), "cloned JS carries no QID/ticker literals: " + tag)
    new_elems.append(el)
# append, then normalize to the export convention [BL, FL, PL, PROJ, QC, RS, SCO, SO, all SQ, STAT]
# (draft 1 carried QID662 after STAT; SQ-after-STAT broke a Qualtrics import on 2026-08-26)
q["SurveyElements"].extend(new_elems)
_non_sq = [e for e in q["SurveyElements"] if e["Element"] not in ("SQ", "STAT")]
_sq = [e for e in q["SurveyElements"] if e["Element"] == "SQ"]
_stat = [e for e in q["SurveyElements"] if e["Element"] == "STAT"]
check(len(_stat) == 1, "one STAT element")
q["SurveyElements"] = _non_sq + _sq + _stat
SQ = {e["PrimaryAttribute"]: e["Payload"] for e in q["SurveyElements"] if e["Element"] == "SQ"}

new_block = copy.deepcopy(src_block)
new_block["ID"] = S["block_id"]
new_block["Description"] = S["desc"]
new_block["BlockElements"] = [{"Type": "Question", "QuestionID": qid_map[x["QuestionID"]]} if x.get("Type") == "Question" else copy.deepcopy(x)
                              for x in src_block["BlockElements"]]
check(S["block_key"] not in BL and all(b["ID"] != S["block_id"] for b in blocks), "new block key/ID unused")
BL[S["block_key"]] = new_block
blocks = list(BL.values())

# flow: append ED + block after the ANET entries in both order branches
n_flow_added = 0
for el in walk(FL):
    if el.get("Type") == "Branch":
        lg = json.dumps(el.get("BranchLogic"))
        for oname in ORDERS:
            if '"LeftOperand": "order"' in lg and '"RightOperand": "%s"' % oname in lg:
                sub = el["Flow"]
                idx = [i for i, x in enumerate(sub) if x.get("Type") == "EmbeddedData"
                       and any(d.get("Field") == "cur_ticker" and d.get("Value") == S["insert_after_ticker"] for d in x["EmbeddedData"])]
                check(len(idx) == 1, "one ANET ED entry in order " + oname)
                i = idx[0]
                check(sub[i + 1].get("Type") == "Standard", "ANET ED is followed by its block")
                ed = copy.deepcopy(sub[i])
                ed["FlowID"] = S["flow_ids"][oname][0]
                for d in ed["EmbeddedData"]:
                    if d.get("Field") == "cur_ticker": d["Value"] = S["ticker"]
                    if d.get("Field") == "cur_name": d["Value"] = S["name"]
                    if d.get("Field") == "cur_shortname": d["Value"] = S["short"]
                blk = copy.deepcopy(sub[i + 1])
                blk["FlowID"] = S["flow_ids"][oname][1]
                blk["ID"] = S["block_id"]
                sub.insert(i + 2, ed)
                sub.insert(i + 3, blk)
                n_flow_added += 1
check(n_flow_added == 2, "seventh stock inserted in both orders")
all_fids = re.findall(r'"FlowID":\s*"(FL_\d+)"', json.dumps(FL))
check(len(all_fids) == len(set(all_fids)), "FlowIDs unique")

# question counter (Survey Question Count keeps the max QID in SecondaryAttribute, Payload null)
qc = [e for e in q["SurveyElements"] if e["Element"] == "QC"][0]
check(qc.get("Payload") is None and str(qc.get("SecondaryAttribute")) == "662", "QC counter as expected before bump")
qc["SecondaryAttribute"] = "667"

# participant-facing stock count
for tag, old, new, n_exp in SEVEN_TEXT:
    p = [p for p in SQ.values() if p.get("DataExportTag") == tag][0]
    check(p["QuestionText"].count(old) == n_exp, "'%s' found %dx in %s" % (old, n_exp, tag))
    p["QuestionText"] = p["QuestionText"].replace(old, new)
if BUMP_MINUTES:
    p = [p for p in SQ.values() if p.get("DataExportTag") == "initial"][0]
    check(p["QuestionText"].count(BUMP_MINUTES[0]) == 1, "screener duration string found once")
    p["QuestionText"] = p["QuestionText"].replace(*BUMP_MINUTES)

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
                blocks_seen = [sub["ID"] for sub in el.get("Flow", []) if sub.get("Type") == "Standard"]
                check(len(blocks_seen) == N and len(set(blocks_seen)) == N, "order %s shows %d distinct blocks" % (oname, N))
                n_seq += 1
check(n_seq == 2, "both order branches verified")

# ---------------- 4. FL_19 declarations ----------------
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
check(len(new_fields) == N * len(PER_TICKER_FIELDS), "per-ticker FL_19 fields rebuilt")
check(len(fields) == len(set(fields)), "no duplicate FL_19 fields")
for x in new_fields:
    check("Value" not in x and x.get("Type") == template.get("Type"), "declaration shape ok")

# ---------------- 5. feedback wording ----------------
n_fb = 0
for p in SQ.values():
    js = p.get("QuestionJS")
    if js and OLD_FEEDBACK in js:
        p["QuestionJS"] = js.replace(OLD_FEEDBACK, NEW_FEEDBACK)
        n_fb += 1
check(n_fb == N, "feedback sentence rebased in %d percent questions (got %d)" % (N, n_fb))

# ---------------- 6. info_reliance chart option ----------------
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
check(s.count(NEW_BASE) == N, "%d x new BASE" % N)
for old in ("snps", "panw", "ftnt"):
    tick, name, short, desc = OLD_NAMES[old]
    for bad in (tick, name, short, desc, old + "_"):
        check(bad not in s, "no '%s' left" % bad)
check(OLD_FEEDBACK not in s, "old feedback wording gone")
for old, new, n_exp in [(o, n, k) for _, o, n, k in SEVEN_TEXT]:
    check(old not in s, "'%s' gone" % old)
check(re.search(r"\bsix\b", " ".join(p["QuestionText"] for p in SQ.values() if p.get("QuestionText"))) is None
      or all("six" not in p["QuestionText"] for p in SQ.values() if p.get("DataExportTag") in ("instructions", "stocks_intro")),
      "no 'six' left in instructions/stocks_intro")
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
alltags = [p.get("DataExportTag") for p in SQ.values()]
check(len(alltags) == len(set(alltags)), "DataExportTags unique")
qids = [e["PrimaryAttribute"] for e in q["SurveyElements"] if e["Element"] == "SQ"]
check(len(qids) == len(set(qids)) and all(SQ[k]["QuestionID"] == k for k in SQ), "QIDs unique and consistent")
block_qids = [x["QuestionID"] for b in blocks for x in b["BlockElements"] if x.get("Type") == "Question"]
check(all(k in SQ for k in block_qids) and len(block_qids) == len(set(block_qids)), "every block question exists exactly once")
check(sorted(b["ID"] for b in blocks) == sorted(set(b["ID"] for b in blocks)), "block IDs unique")
flow_block_ids = [el["ID"] for el in walk(FL) if el.get("Type") == "Standard"]
check(all(any(b["ID"] == bid for b in blocks) for bid in flow_block_ids), "every flow block exists")
# schema hygiene (lessons from the 08-26 / 08-28 import failures)
order_now = [e["Element"] for e in q["SurveyElements"]]
check(order_now[:8] == ["BL", "FL", "PL", "PROJ", "QC", "RS", "SCO", "SO"] and order_now[-1] == "STAT"
      and set(order_now[8:-1]) == {"SQ"} and len(order_now) == len(src_elem_order) + 5, "element order = export convention (all SQ, STAT last)")
check(all(not ("QuestionJS" in p and p["QuestionJS"] is None) for p in SQ.values()), "no null QuestionJS")
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
n_info = 0
for p in SQ.values():
    if (p.get("DataExportTag") or "").endswith("_infoscreen"):
        js = p["QuestionJS"]
        check("${e://Field/cur_ticker}" in js and 'BASE+ticker+"_365d.json"' in js
              and 'BASE+"fundamentals.json"' in js and 'BASE+"profits.json"' in js, "infoscreen JS shape " + p["DataExportTag"])
        n_info += 1
check(n_info == N, "%d infoscreens" % N)

with open(OUT, "w") as f:
    json.dump(q, f, separators=(",", ":"), ensure_ascii=False)
strict_load(OUT)
print("OK -> %s  (%d/%d checks)" % (OUT, sum(1 for c, _ in checks if c), len(checks)))
print("retagged:", ", ".join("%s->%s" % rt for rt in retagged))
print("seventh stock:", S["ticker"], "block", S["block_id"], "QIDs", ", ".join(S["qids"]))
print("FL_19 fields:", len(fields), "| questions:", len(SQ))
