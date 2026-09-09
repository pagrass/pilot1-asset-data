#!/usr/bin/env python3
"""
Structural Similarity PILOT 3 Wave 2 -- round-2 edits on Paul's Qualtrics
export of draft 3 (2026-09-09, SV_6yhw1qgNu0dSBsG; his edit: screener back to
"6 minutes" -- kept).

Source = ~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot_3_-_Wave_2.qsf
Output = ~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot3_-_Wave_2_draft4.qsf

Edits:
  1. ZERO ESTIMATES. The seven *_percent questions hid the confidence step and
     the $100 feedback whenever the typed number was 0 ("percent!==0"), while
     the JS validator accepts 0 -> a "no change" respondent was stuck behind
     an invisible forced-response slider. 0 is now a valid estimate in both
     directions: feedback reads "$100", step 3 shows.
  2. INSTRUCTIONS BOX, identical across arms (Paul 2026-09-09: no treatment
     differences in the box -- returns-arm participants see the profit data
     too and may want its definition). Both forecast objects are defined
     fully, with their measurement rules, for everybody:
       "A stock's return over the next 12 months is the percentage change
        from its current price to its price in 12 months."
       "A company's profit (net profit, also called earnings) is what it
        earns after subtracting all costs. Its change in profits over the
        next 12 months compares its total net profit over the next four
        reported quarters with its net profit in the last fiscal year shown."
     "earnings" kept once as an unbolded synonym; distinction sentence and
     task sentence unchanged; the page keeps its two pipes.
  3. COMP QUESTION 1: choice 2 "How each company's profits will change (its
     earnings)" -> "How each company's profits (net profit) will change";
     profit vocabulary is now profits / net profit everywhere, "earnings"
     appears only as the synonym in the definition.
  4. COMP QUESTIONS 1+2: choice order randomized ("Type": "All"; format as in
     the fielded Extreme Models comp3). Gate logic keys on choice IDs, which
     randomization does not touch.

Untouched: QID85 screener (Paul's "6 minutes"), Prolific codes, everything
else. Builder asserts the source is the SV_6yhw1qgNu0dSBsG export.
"""
import copy, json, os, re, sys

SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot_3_-_Wave_2.qsf")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.expanduser(
    "~/Downloads/Model_Spillovers_-_Structural_Similarity_-_Pilot3_-_Wave_2_draft4.qsf")

RANDOMIZE_COMP = True

DEF_RET_OLD = "A stock&rsquo;s <strong>return</strong> is the percentage change in its price over some period."
DEF_RET_NEW = "A stock&rsquo;s <strong>return</strong> over the next 12 months is the percentage change from its current price to its price in 12 months."
DEF_PROF_OLD = "A company&rsquo;s <strong>profit</strong> (net profit, also called <strong>earnings</strong>) is what it earns over some period after subtracting all costs, as reported in its financial statements."
DEF_PROF_NEW = ("A company&rsquo;s <strong>profit</strong> (net profit, also called earnings) is what it earns after subtracting all costs. "
                "Its <strong>change in profits</strong> over the next 12 months compares its total net profit over the next four reported quarters "
                "with its net profit in the last fiscal year shown.")

COMP1_CHOICE2_OLD = "How each company's <b>profits</b> will change (its earnings)"
COMP1_CHOICE2_NEW = "How each company's <b>profits</b> (net profit) will change"

ZERO_OLD = "if (!isNaN(percent)&&percent!==0){"
ZERO_NEW = "if (!isNaN(percent)){"

RANDOMIZATION = {"Advanced": None, "TotalRandSubset": "", "Type": "All"}


def strict_load(path):
    with open(path) as f:
        return json.load(f, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))


q = strict_load(SRC)
check_log = []


def check(cond, msg):
    check_log.append((bool(cond), msg))
    assert cond, msg


check(q["SurveyEntry"]["SurveyID"] == "SV_6yhw1qgNu0dSBsG", "source is Paul's export of draft 3")
check(q["SurveyEntry"]["SurveyName"] == "Model Spillovers - Structural Similarity - Pilot 3 - Wave 2", "survey name")
src_dump = json.dumps(q, sort_keys=True)
src_elem_order = [e["Element"] for e in q["SurveyElements"]]
SQ = {e["PrimaryAttribute"]: e["Payload"] for e in q["SurveyElements"] if e["Element"] == "SQ"}
by_tag = {p.get("DataExportTag"): p for p in SQ.values()}
FL = [e for e in q["SurveyElements"] if e["Element"] == "FL"][0]["Payload"]


def walk(node):
    for el in node.get("Flow", []):
        yield el
        yield from walk(el)


# ---------------- 1. zero estimates ----------------
pct = [p for p in SQ.values() if (p.get("DataExportTag") or "").endswith("_percent")]
check(len(pct) == 7, "7 percent questions")
n_zero = 0
for p in pct:
    check(p["QuestionJS"].count(ZERO_OLD) == 1, "zero guard found once in " + p["DataExportTag"])
    p["QuestionJS"] = p["QuestionJS"].replace(ZERO_OLD, ZERO_NEW)
    n_zero += 1
check(n_zero == 7, "zero guard removed in 7 questions")

# ---------------- 2. instructions ----------------
ins = by_tag["instructions"]
for old, new in ((DEF_RET_OLD, DEF_RET_NEW), (DEF_PROF_OLD, DEF_PROF_NEW)):
    check(ins["QuestionText"].count(old) == 1, "instructions string found once: " + old[:50])
    ins["QuestionText"] = ins["QuestionText"].replace(old, new)
check(ins["QuestionText"].count("${e://Field/") == 2, "instructions page keeps exactly 2 pipes (obj_gate_teach, obj_bonus_actual)")
check("obj_measure" not in json.dumps(q), "no arm-specific measurement pipe")

# ---------------- 3. comp question 1 wording ----------------
c1 = by_tag["compquestion"]
check(c1["Choices"]["2"]["Display"] == COMP1_CHOICE2_OLD, "comp1 choice 2 as expected")
c1["Choices"]["2"]["Display"] = COMP1_CHOICE2_NEW
check(c1["Choices"]["1"]["Display"] == "How each stock's <b>price</b> will change (its <b>return</b>)", "comp1 choice 1 unchanged")

# ---------------- 4. randomize comp choices ----------------
if RANDOMIZE_COMP:
    for tag in ("compquestion", "compquestion2"):
        p = by_tag[tag]
        check(not p.get("Randomization"), tag + " had no randomization")
        p["Randomization"] = copy.deepcopy(RANDOMIZATION)
        check(p["ChoiceOrder"] == ["1", "2", "3"] and set(p["Choices"]) == {"1", "2", "3"}, tag + " has 3 choices")

# ---------------- global checks ----------------
s = json.dumps(q)
check(ZERO_OLD not in s, "no zero guard left")
check(s.count(DEF_RET_NEW) == 1 and s.count(DEF_PROF_NEW) == 1, "both common definitions present once")
check("(its earnings)" not in s, "'(its earnings)' gone")
check(s.count("earnings") == 1, "'earnings' appears exactly once (definition synonym)")
src = json.loads(src_dump)
src_initial = [e["Payload"]["QuestionText"] for e in src["SurveyElements"] if e["Element"] == "SQ" and e["Payload"].get("DataExportTag") == "initial"][0]
check(by_tag["initial"]["QuestionText"] == src_initial and "6&nbsp;minutes" in src_initial, "screener untouched (Paul's 6 minutes)")
check([e["Element"] for e in q["SurveyElements"]] == src_elem_order, "element order unchanged")
check("@latest" not in s.replace("chart.js@2.9.4", ""), "no @latest")
check(all(not ("QuestionJS" in p and p["QuestionJS"] is None) for p in SQ.values()), "no null QuestionJS")
qc = [e for e in q["SurveyElements"] if e["Element"] == "QC"][0]
check(qc.get("Payload") is None, "QC payload null")
for el in walk(FL):
    if el.get("Type") == "EmbeddedData":
        for x in el["EmbeddedData"]:
            if x.get("Value") not in (None, ""):
                check(x.get("Type") == "Custom", "ED with Value must be Custom: %s" % x.get("Field"))
# only the intended questions changed
src_sq = {e["PrimaryAttribute"]: e["Payload"] for e in src["SurveyElements"] if e["Element"] == "SQ"}
changed = sorted(SQ[k]["DataExportTag"] for k in SQ if json.dumps(SQ[k], sort_keys=True) != json.dumps(src_sq[k], sort_keys=True))
expected = sorted([p["DataExportTag"] for p in pct] + ["instructions", "compquestion", "compquestion2"])
check(changed == expected, "changed questions == intended (%s)" % changed)
n_gate = sum(1 for el in walk(FL) if el.get("Type") == "Branch"
             and "QID608" in json.dumps(el.get("BranchLogic")) and "QID662" in json.dumps(el.get("BranchLogic")))
check(n_gate == 4, "4 two-question gate branches intact")

with open(OUT, "w") as f:
    json.dump(q, f, separators=(",", ":"), ensure_ascii=False)
strict_load(OUT)
print("OK -> %s  (%d/%d checks)" % (OUT, sum(1 for c, _ in check_log if c), len(check_log)))
print("changed questions:", ", ".join(changed))
