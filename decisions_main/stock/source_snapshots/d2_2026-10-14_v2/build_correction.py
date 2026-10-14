from pathlib import Path
import json,shutil,hashlib,ast,math
R=Path(__file__).resolve().parent
repo=Path('/private/tmp/decisions-data-publish-20261012')
base=repo/'decisions_main/stock';old=base/'snapshots/d2_2026-10-14_v1';oldraw=base/'source_snapshots/d2_2026-10-14_v1'
sid='d2_2026-10-14_v2';D=R/'decisions_main/stock/snapshots'/sid;S=R/'decisions_main/stock/source_snapshots'/sid
shutil.copytree(old,D,dirs_exist_ok=True);shutil.copytree(oldraw,S,dirs_exist_ok=True)
def read(p):return json.loads(p.read_text())
def write(p,v):p.write_text(json.dumps(v,indent=2,ensure_ascii=False,allow_nan=False)+'\n')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
# Repair the refresh parser for subsequent refreshes and save it with this snapshot.
p=S/'refresh_stocks.py';code=p.read_text().replace("SNAPSHOT = 'd2_2026-10-14_v1'", "SNAPSHOT = 'd2_2026-10-14_v2'")
helper='''def dividend_yield(info, closing_price):
    rate = number(info.get('dividendRate'))
    if rate is not None:
        return round(100 * rate / closing_price, 2)
    # Yahoo dividendYield uses percentage units (1.46 means 1.46%, not 146%).
    # Preserve the existing blank display for non-payers when the annual rate is absent.
    direct = number(info.get('dividendYield'))
    return round(direct, 2) if direct is not None and direct > 0 else None

'''
assert "def get_stock(ticker):" in code;code=code.replace('def get_stock(ticker):',helper+'def get_stock(ticker):')
line="div_y = None if rate is None else round(100 * rate / r['last_close'], 2)";assert line in code
code=code.replace(line,"div_y = dividend_yield(info, r['last_close'])")
method='Indicated annual dividendRate / frozen closing price * 100 when present; otherwise positive Yahoo dividendYield in percentage units. Missing or zero fallback yields retain the previous blank display for non-payers.'
code=code.replace('Indicated annual dividendRate / frozen closing price * 100; missing rate remains null.',method)
p.write_text(code)
# Exercise the actual parser functions without executing network setup.
tree=ast.parse(code);ns={'math':math};exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ['number','dividend_yield']],type_ignores=[]),str(p),'exec'),ns)
f=ns['dividend_yield'];assert f({'dividendRate':1.68,'dividendYield':9},115.07)==1.46;assert f({'dividendYield':1.46},115.07)==1.46;assert f({'dividendRate':0},100)==0;assert f({'dividendYield':0},100) is None;assert f({},100) is None
fund=read(D/'fundamentals.json');oldfund=read(old/'fundamentals.json');changes=[]
for t,v in fund.items():
 info=read(S/(t.lower()+'_info.json'));price=read(D/(t.lower()+'_365d.json'))['prices'][-1][1]
 y=f(info,price)
 if y!=v['div_y']:
  prior=read(base/'source_snapshots/d1_2026-10-13_v1'/(t.lower()+'_info.json'))
  assert y==round(prior['dividendRate']/price*100,2),(t,'independent yield cross-check')
  changes.append({'ticker':t,'before':v['div_y'],'after_percent':y,'source':'Yahoo dividendYield','cross_check':'Matches yesterday annual rate divided by October 13 close'})
 v['div_y']=y
 a=dict(v);b=dict(oldfund[t]);a.pop('div_y');b.pop('div_y');assert a==b
assert {x['ticker'] for x in changes}=={'CSCO','INTU','NTAP','TXN','QCOM','CRM','ORCL'}
write(D/'fundamentals.json',fund)
meta=read(D/'fundamentals_meta.json');meta['dividend_yield_method']=method;meta['dividend_correction']='October 14 correction: restore seven positive yields from the archived Yahoo dividendYield field after dividendRate was omitted. All seven match the prior annual dividend rate divided by the current frozen close. Other fundamentals and prices unchanged.';write(D/'fundamentals_meta.json',meta)
for name in ['summary.json','seven_pair_manifest.json']:
 v=read(D/name);v['snapshot_id']=sid;write(D/name,v)
shutil.copy2(Path(__file__),S/'build_correction.py')
man=read(D/'manifest.json');man['snapshot_id']=sid;man['status']='Day 2 dividend-field correction; October 13 prices and all other fundamentals unchanged.'
man['supersedes_snapshot_id']='d2_2026-10-14_v1'
man['validation']['dividend_fallback_verified_against_prior_annual_rate']=True;man['validation']['restored_dividend_tickers']=[x['ticker'] for x in changes]
man['files_sha256']={p.name:sha(p) for p in sorted(D.glob('*.json')) if p.name!='manifest.json'};man['source_files_sha256']={p.name:sha(p) for p in sorted(S.iterdir()) if p.is_file()};write(D/'manifest.json',man)
for p in old.glob('*_365d.json'):assert p.read_bytes()==(D/p.name).read_bytes()
current=R/'decisions_main/stock/current';shutil.copytree(D,current,dirs_exist_ok=True)
write(R/'validation.json',{'snapshot_id':sid,'changes':changes,'prices_unchanged':True,'other_fundamentals_unchanged':True,'parser_fallback_tests_passed':True,'yield_cross_checks_passed':True})
print(json.dumps({'snapshot_id':sid,'changes':changes,'prices_unchanged':True}))
