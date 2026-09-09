#!/usr/bin/env python3
"""Local behavioral harness for the 3-step elicitation page (binary -> percent -> confidence).
Builds harness_zero.html from a QSF's QID370/QID371/QID372 (Adobe block) and runs
scenarios in-page; results land in #results (read via headless Chrome --dump-dom)."""
import json, os, re, sys

QSF = sys.argv[1]
q = json.load(open(QSF))
SQ = {e["PrimaryAttribute"]: e["Payload"] for e in q["SurveyElements"] if e["Element"] == "SQ"}
so = [e for e in q["SurveyElements"] if e["Element"] == "SO"][0]["Payload"]
css = re.search(r"<style>(.*?)</style>", so["Header"], re.S).group(1)

ARM = sys.argv[2] if len(sys.argv) > 2 else "returns"
PIPES = {"cur_ticker": "ADBE", "cur_name": "Adobe Inc.", "cur_shortname": "Adobe", "structuralsim": ARM,
         "obj_adj": "return" if ARM == "returns" else "profit", "obj_phrase": "stock price" if ARM == "returns" else "profits",
         "obj_suffix": "" if ARM == "returns" else " (compared to its last fiscal year)",
         "obj_outcome": "return" if ARM == "returns" else "change in profits",
         "obj_choice_inc": "The stock's price will increase" if ARM == "returns" else "The company's profits will increase",
         "obj_choice_dec": "The stock's price will decrease" if ARM == "returns" else "The company's profits will decrease"}


def pipe(t):
    for k, v in PIPES.items():
        t = t.replace("${e://Field/%s}" % k, v)
    assert "${e://" not in t, re.findall(r"\$\{e://[^}]+\}", t)
    return t


def block(qid, inner):
    p = SQ[qid]
    return ('<div class="QuestionOuter" id="%s"><div class="Inner"><div class="QuestionText">%s</div>'
            '<div class="QuestionBody">%s</div></div></div>' % (qid, pipe(p["QuestionText"]), inner))


bin_inner = ('<fieldset><ul class="ChoiceStructure">'
             '<li><label><input type="radio" name="QR~QID370" value="4"> %s</label></li>'
             '<li><label><input type="radio" name="QR~QID370" value="5"> %s</label></li></ul></fieldset>'
             % (pipe(SQ["QID370"]["Choices"]["4"]["Display"]), pipe(SQ["QID370"]["Choices"]["5"]["Display"])))
pct_inner = '<div class="pctbox"><input class="InputText" type="text" style="width:80px;font-size:18px"></div>'
conf_inner = ('<div class="horizontalbar"><div class="track"><div class="handle"></div></div>'
              '<ul class="numbers"><li>0</li><li>10</li><li>20</li><li>30</li><li>40</li><li>50</li><li>60</li><li>70</li><li>80</li><li>90</li><li>100</li></ul>'
              '<div class="sliderToolTipBox"></div><input class="ResultsInput InputText" type="hidden" value=""></div>')

js = "\n".join("(function(){var ctx={questionId:'%s',getQuestionContainer:function(){return document.getElementById('%s');}};"
               "window.__ctx=ctx;\n%s\n})();" % (qid, qid, pipe(SQ[qid]["QuestionJS"]).replace("Qualtrics.SurveyEngine.addOnload(", "__reg('load',ctx,")
                                                   .replace("Qualtrics.SurveyEngine.addOnReady(", "__reg('ready',ctx,")
                                                   .replace("Qualtrics.SurveyEngine.addOnUnload(", "__reg('unload',ctx,"))
               for qid in ("QID370", "QID371", "QID372"))

page = """<!doctype html><html><head><meta charset="utf-8"><title>zero harness</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.7.1/jquery.min.js"></script>
<style>%s body{font-family:Arial;margin:0} .Skin .QuestionOuter{margin:20px auto} #results{font-family:monospace;font-size:12px;white-space:pre-wrap;max-width:80%%;margin:20px auto;color:#444}</style>
</head><body class="Skin"><script>document.documentElement.classList.add('sim-high');
var __H={load:[],ready:[]}; function __reg(k,ctx,f){__H[k].push([ctx,f]);}
window.$=function(id){ if(typeof id==='string'){ var n=document.getElementById(id); if(n){ n.enable=function(){}; n.disable=function(){}; } return n;} return jQuery(id); };
window.Qualtrics={SurveyEngine:{setEmbeddedData:function(){}}};
</script>
<div id="Questions">%s%s%s</div><div id="NextButton" style="display:none"></div><div id="results">running...</div>
<script>
%s
jQuery(function(){
  __H.load.forEach(function(x){ x[1].call(x[0]); });
  __H.ready.forEach(function(x){ x[1].call(x[0]); });
  setTimeout(function(){
    function vis(id){ return getComputedStyle(document.getElementById(id)).visibility; }
    var out=[];
    function scenario(dirVal, txt){
      var r=document.querySelector('#QID370 input[value="'+dirVal+'"]'); r.checked=true; jQuery(r).trigger('change');
      var inp=document.querySelector('#QID371 .InputText'); inp.value=txt; jQuery(inp).trigger('input');
      var fb=document.getElementById('dollarFeedback');
      out.push({dir:(dirVal==='4'?'increase':'decrease'), typed:JSON.stringify(txt), step2:vis('QID371'), step3:vis('QID372'),
                feedback_shown: fb?getComputedStyle(fb).display!=='none':null, feedback: fb?fb.textContent:null});
    }
    scenario('4','0'); scenario('4','5'); scenario('4',''); scenario('5','0'); scenario('5','10'); scenario('5','abc');
    document.getElementById('results').textContent='RESULTS '+JSON.stringify(out);
  }, 300);
});
</script></body></html>""" % (css, block("QID370", bin_inner), block("QID371", pct_inner), block("QID372", conf_inner), js)
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness_zero_%s.html" % ARM)
open(out, "w").write(page)
print(out)
