#!/usr/bin/env python3
"""Apply CDN delivery and supplied completion codes to the latest rolling W2 QSF.

Preserves all survey wording, choices, validation, randomization and timing.
Fetches current stock data initially, pins the loaded snapshot per respondent,
and checks downloaded file SHA-256 against the version manifest.
No upload, survey activation, or participant redirect request is performed.
"""
import argparse,copy,hashlib,json,re
from pathlib import Path
BASE='https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@main/structuralsimilarity_main/stock/'
CODES={'FL_69':'C4BX5P9C','FL_28':'C40WVJFQ','FL_1004':'CHULK0G8','FL_1012':'CHULK0G8','FL_652':'C13ZQNA8'}
def nodes(flow):
    for n in flow:
        yield n
        yield from nodes(n.get('Flow',[]))
def sha(raw):return hashlib.sha256(raw).hexdigest()
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    raw=a.source.read_bytes();d=json.loads(raw);before=copy.deepcopy(d)
    d['SurveyEntry']['SurveyDescription']='Main Wave 2; agreed seven-stock slate and rolling annual profits; stock data fetched from jsDelivr, with the loaded snapshot recorded and pinned per respondent.'
    opts=next(e['Payload'] for e in d['SurveyElements'] if e['Element']=='SO')
    opts['Header'],count=re.subn(r'<script>window\.__STRUCTURAL_MAIN_DATA__=.*?;</script>','',opts['Header'],count=1,flags=re.S);assert count==1
    flow=next(e['Payload']['Flow'] for e in d['SurveyElements'] if e['Element']=='FL');routes={}
    for n in nodes(flow):
        if n.get('Type')=='EndSurvey':
            assert n['FlowID'] in CODES
            n['Options']['EOSRedirectURL']='https://app.prolific.com/submissions/complete?cc='+CODES[n['FlowID']];routes[n['FlowID']]=CODES[n['FlowID']]
        for ed in n.get('EmbeddedData',[]):
            if ed.get('Field') in ['stimulus_snapshot_id','stock_window_start','stock_window_end','crypto_snapshot_id','crypto_window_start','crypto_window_end']:
                ed['Type']='Recipient';ed.pop('Value',None)
            if ed.get('Field')=='stimulus_delivery':ed['Value']='cdn_current_then_snapshot'
    assert routes==CODES
    modified=[]
    for e in d['SurveyElements']:
        if e['Element']!='SQ':continue
        q=e['Payload'];js=q.get('QuestionJS') or ''
        if 'stimulusFetch' not in js:continue
        js,n=re.subn(re.escape(BASE)+r'snapshots/[^/]+/',BASE+'current/',js);assert n==1
        old=re.search(r'  function stimulusFetch\(url\)\{.*?\n  \}\n(?=  var MONTHS)',js,re.S);assert old
        helper='''  var priorSnapshot="${e://Field/stimulus_snapshot_id}";
  if (/^launch_\\d{4}-\\d{2}-\\d{2}_v\\d+$/.test(priorSnapshot)) {
    BASE=BASE.replace(/current\\/$/,"snapshots/"+priorSnapshot+"/");
  }
  var loadedHashes={};
  function stimulusFetch(url){
    return fetch(url,{cache:'no-cache'}).then(function(r){
      if(!r.ok) throw new Error('Stimulus unavailable');
      return {json:function(){return r.text().then(function(raw){
        return window.crypto.subtle.digest('SHA-256',new TextEncoder().encode(raw)).then(function(buffer){
          loadedHashes[url]=Array.prototype.map.call(new Uint8Array(buffer),function(b){return ('0'+b.toString(16)).slice(-2);}).join('');
          return JSON.parse(raw);
        });
      });}};
    });
  }
  function calendarDay(ts){
    var parts=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(ts)),p={};
    parts.forEach(function(x){p[x.type]=x.value;});
    return p.year+'-'+p.month+'-'+p.day;
  }
  function chartDay(ts){var p=calendarDay(ts).split('-');return new Date(+p[0],+p[1]-1,+p[2]);}
'''
        js=js[:old.start()]+helper+js[old.end():]
        old='    stimulusFetch(BASE+"profits.json").then(function(r){return r.json();})'
        assert js.count(old)==1;js=js.replace(old,old+',\n    stimulusFetch(BASE+"manifest.json").then(function(r){return r.json();})')
        marker="    if(!((res[2].stocks||{})[TICK])) throw new Error('Missing profit observations');"
        check='''
    var manifest=res[3], row=(manifest.assets||[]).filter(function(x){return x.slug===ticker;})[0];
    var Pcheck=res[2].stocks[TICK], firstDay=calendarDay(pts[0][0]), lastDay=calendarDay(pts[pts.length-1][0]);
    if(!manifest.snapshot_id || !row || row.first_day!==firstDay || row.last_day!==lastDay ||
       row.points!==pts.length || Math.abs(row.first_close-pts[0][1])>1e-8 || Math.abs(row.last_close-pts[pts.length-1][1])>1e-8 ||
       res[2].snapshot_id!==manifest.snapshot_id || (res[1]._metadata||{}).snapshot_id!==manifest.snapshot_id ||
       Pcheck.latest_reported_quarter_end!==manifest.annual_earnings[TICK].latest_reported_quarter_end ||
       (/^launch_\\d{4}-\\d{2}-\\d{2}_v\\d+$/.test(priorSnapshot) && manifest.snapshot_id!==priorSnapshot)) {
      throw new Error('Stock data and version metadata do not match');
    }
    [ticker+'_365d.json','fundamentals.json','profits.json'].forEach(function(name){
      if(loadedHashes[BASE+name]!==manifest.files_sha256[name]) throw new Error('Stock file and version hashes do not match');
    });
    setED('stimulus_snapshot_id',manifest.snapshot_id);
    setED('stimulus_delivery','cdn_current_then_snapshot');
    setED('stock_window_start',firstDay);setED('stock_window_end',lastDay);
'''
        assert js.count(marker)==1;js=js.replace(marker,marker+check)
        js=js.replace('t:p[0],y:p[1]','t:chartDay(p[0]),y:p[1]')
        js=js.replace('min:new Date(pts[0][0]), max:new Date(pts[pts.length-1][0])','min:chartDay(pts[0][0]), max:chartDay(pts[pts.length-1][0])')
        js=js.replace('now=new Date(pts[pts.length-1][0]);','now=chartDay(pts[pts.length-1][0]);')
        q['QuestionJS']=js;modified.append(e['PrimaryAttribute'])
    assert len(modified)==7
    oldq={e['PrimaryAttribute']:e['Payload'] for e in before['SurveyElements'] if e['Element']=='SQ'}
    for e in d['SurveyElements']:
        if e['Element']=='SQ':
            expected=copy.deepcopy(oldq[e['PrimaryAttribute']])
            if e['PrimaryAttribute'] in modified:expected['QuestionJS']=e['Payload']['QuestionJS']
            assert expected==e['Payload'],e['PrimaryAttribute']
        elif e['Element'] not in ['SO','FL']:assert e==next(x for x in before['SurveyElements'] if x['Element']==e['Element'])
    assert opts==dict(next(e['Payload'] for e in before['SurveyElements'] if e['Element']=='SO'),Header=opts['Header'])
    # Only dynamic version fields and the five completion routes may change in flow.
    normalized=copy.deepcopy(before)
    nf=next(e['Payload']['Flow'] for e in normalized['SurveyElements'] if e['Element']=='FL')
    for n in nodes(nf):
        if n.get('Type')=='EndSurvey':n['Options']['EOSRedirectURL']='https://app.prolific.com/submissions/complete?cc='+CODES[n['FlowID']]
        for ed in n.get('EmbeddedData',[]):
            if ed.get('Field') in ['stimulus_snapshot_id','stock_window_start','stock_window_end','crypto_snapshot_id','crypto_window_start','crypto_window_end']:ed['Type']='Recipient';ed.pop('Value',None)
            if ed.get('Field')=='stimulus_delivery':ed['Value']='cdn_current_then_snapshot'
    assert nf==flow
    text=json.dumps(d);assert '__STRUCTURAL_MAIN_DATA__' not in text and 'SET_MAIN_W2' not in text
    assert not re.search(r'launch_\d{4}-\d{2}-\d{2}',text)
    a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(d,indent=2,ensure_ascii=False)+'\n')
    report=dict(source=str(a.source),source_sha256=sha(raw),output=str(a.output),output_sha256=sha(a.output.read_bytes()),chart_questions=modified,completion_routes=routes,stock_cdn_base=BASE+'current/',embedded_data_bundle=False,all_question_wording_choices_validation_randomization_timing_preserved=True,stock_dates_follow_et_calendar=True,manifest_versions_and_sha256_verified=True,respondent_snapshot_pinned_after_first_asset=True,latest_loaded_version_and_dates_recorded=True)
    a.output.with_suffix('.build.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
if __name__=='__main__':main()
