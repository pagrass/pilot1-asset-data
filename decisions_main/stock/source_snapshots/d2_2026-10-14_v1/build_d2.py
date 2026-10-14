from pathlib import Path
from datetime import datetime,date
from zoneinfo import ZoneInfo
import json,hashlib,shutil,math
R=Path(__file__).resolve().parent
repo=Path('/private/tmp/decisions-data-publish-20261012')
sid='d2_2026-10-14_v1';D=R/'decisions_main/stock/snapshots'/sid;S=R/'decisions_main/stock/source_snapshots'/sid
old=repo/'decisions_main/stock/snapshots/d1_2026-10-13_v2';ET=ZoneInfo('America/New_York')
def read(p):return json.loads(p.read_text())
def write(p,o):p.write_text(json.dumps(o,indent=2,ensure_ascii=False,allow_nan=False)+'\n')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
man=read(D/'manifest.json');assets={a['ticker']:a for a in man['assets']};funds=read(D/'fundamentals.json');oldfunds=read(old/'fundamentals.json')
assert len(assets)==14 and not man['errors'] and not man['missing_dates_relative_to_original_stocks']
expected=[datetime.fromtimestamp(p[0]/1000,ET).date().isoformat() for p in read(old/'csco_365d.json')['prices']]+['2026-10-13']
assert len(expected)==252 and len(set(expected))==252
changes=[];rows=[]
for t,a in assets.items():
 pts=read(D/(t.lower()+'_365d.json'))['prices'];prev=read(old/(t.lower()+'_365d.json'))['prices'];info=read(S/(t.lower()+'_info.json'))
 dates=[datetime.fromtimestamp(p[0]/1000,ET).date().isoformat() for p in pts]
 assert dates==expected,t
 assert pts[:-1]==prev,(t,'unexpected historical revision')
 assert a['last_close']==round(a['quote_price'],2) and a['quote_time'].startswith('2026-10-13'),(t,'quote mismatch')
 assert a['return_pct']==round((pts[-1][1]/pts[0][1]-1)*100,1)
 assert info['currency']=='USD' and funds[t]['sector']=='Technology',t
 assert abs(funds[t]['pb_current']-a['last_close']/info['bookValue'])<.011,(t,'PB mismatch')
 assert math.isclose(funds[t]['marketcap'],round(info['marketCap']/1e6,2))
 if funds[t]['div_y'] is not None:assert funds[t]['div_y']==round(100*info['dividendRate']/a['last_close'],2)
 if oldfunds[t]['valuation']!=funds[t]['valuation']:changes.append({'ticker':t,'old':oldfunds[t]['valuation'],'new':funds[t]['valuation']})
 rows.append({'ticker':t,'old_close':prev[-1][1],'new_close':pts[-1][1],'return_pct':a['return_pct'],'pb':funds[t]['pb_current'],'valuation':funds[t]['valuation']})
pm=read(old/'seven_pair_manifest.json');pm['snapshot_id']=sid;pm['price_window']=[expected[0],expected[-1]]
assert [p['pair_id'] for p in pm['pairs']]==[f'p{i}' for i in range(1,8)]
for p in pm['pairs']:
 a=assets[p['stock_a']]['return_pct'];b=assets[p['stock_b']]['return_pct']
 assert a>b,(p['pair_id'],'rank change')
 assert ((b<0<a and p['role']=='mixed_sign') or (0<b<a and p['role']=='both_up') or (b<a<0 and p['role']=='both_down')),(p['pair_id'],'sign change')
 p.update(a_return_pct=a,b_return_pct=b,gap_pp=round(a-b,1))
write(D/'seven_pair_manifest.json',pm)
summary={'snapshot_id':sid,'price_as_of':man['price_as_of'],'first_price_date':expected[0],'pair_count':7,'assets':[{'ticker':t,**{k:a[k] for k in ['first_date','last_date','n','first_close','last_close','return_pct']}} for t,a in sorted(assets.items())],'pairs':pm['pairs']}
write(D/'summary.json',summary)
for f in ['refresh_stocks.py','build_d2.py']:shutil.copy2(R/f,S/f)
man['files_sha256']={p.name:sha(p) for p in sorted(D.glob('*.json')) if p.name!='manifest.json'}
man['source_files_sha256']={p.name:sha(p) for p in sorted(S.iterdir()) if p.is_file()}
man['validation']={'fourteen_assets':True,'all_252_expected_sessions_present':True,'all_closes_match_regular_market_quote':True,'historical_overlap_unchanged':True,'all_pb_matches_price_divided_by_book_value':True,'pair_ordering_and_signs_preserved':True,'valuation_label_changes':changes}
write(D/'manifest.json',man)
current=R/'decisions_main/stock/current';current.mkdir(exist_ok=True)
for f in D.iterdir():shutil.copy2(f,current/f.name)
report={'snapshot_id':sid,'previous_snapshot_id':read(old/'manifest.json')['snapshot_id'],'assets':rows,'pairs':pm['pairs'],'validation':man['validation'],'files':len(list(D.glob('*.json')))}
write(R/'validation.json',report)
print(json.dumps({'snapshot':sid,'files':report['files'],'validation':report['validation'],'pairs':[{'pair':p['pair_id'],'stocks':p['stock_a']+'/'+p['stock_b'],'returns':[p['a_return_pct'],p['b_return_pct']]} for p in pm['pairs']]}))
