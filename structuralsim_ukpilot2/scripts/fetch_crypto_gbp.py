#!/usr/bin/env python3
"""Fetch the frozen GBP crypto snapshot for structuralsim_ukpilot2.
Run from the package root using Python >=3.9. Public raw responses are retained.
No commits, pushes, survey changes, cached reuse or stock requests occur here.
"""
import concurrent.futures
import copy
import hashlib
import importlib.util
import json
import math
import shutil
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
SNAP = 'ukpilot2_2026-10-20_v1'
ASOF = date(2026, 10, 20)
END = ASOF - timedelta(days=1)
START = END - timedelta(days=365)
RAW = ROOT/'sources'/SNAP
OUT = ROOT/'crypto'/'snapshots'/SNAP/'gbp'
RAW.mkdir(parents=True, exist_ok=True)
if OUT.exists():
    raise SystemExit('Snapshot output already exists; do not overwrite frozen stimuli.')
assert datetime.now(timezone.utc).date()==ASOF, 'Fresh snapshot must be collected on the recorded date'
spec=importlib.util.spec_from_file_location('price_parser', ROOT/'sources'/'price_parser.py')
f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)
started=datetime.now(timezone.utc).isoformat(timespec='seconds')

# Check native GBP availability afresh for every asset. Persist the HYPE 404.
def native(item):
    slug,usd_symbol=item
    symbol=usd_symbol.removesuffix('-USD')+'-GBP'
    if slug=='hype':
        url=f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={f.epoch(START)}&period2={f.epoch(END+timedelta(days=2))}&interval=1d&events=splits'
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=f.UA),timeout=30) as response:
                raw=response.read()
            (RAW/'hype_native_gbp_availability.json').write_bytes(raw)
        except urllib.error.HTTPError as error:
            payload=error.read()
            (RAW/'hype_native_gbp_unavailable.json').write_bytes(payload)
            assert error.code==404, f'Unexpected native HYPE error: {error.code}'
            return slug,None,dict(symbol=symbol,status=error.code,url=url,response_sha256=hashlib.sha256(payload).hexdigest())
    data,row=f.price_series(symbol,slug,'crypto',END,365,RAW,False)
    raw=json.loads((RAW/('crypto_'+slug+'_daily.json')).read_text())['chart']['result'][0]
    assert raw['meta']['currency']=='GBP'
    row.update(currency='GBP',currency_method='native_gbp_source_series',price_definition='Yahoo daily GBP quote.close; historical gaps omitted and disclosed, never interpolated')
    return slug,data,row

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    results=list(pool.map(native,f.CRYPTOS.items()))
files={};rows=[];fallbacks=[]
for slug,data,row in results:
    if data is not None:
        files[slug+'_365d.json']=data;rows.append(row)
    else:
        assert slug=='hype'
        usd,usdrow=f.price_series('HYPE32196-USD','hype','crypto',END,365,RAW,False)
        usd_raw=json.loads((RAW/'crypto_hype_daily.json').read_text())['chart']['result'][0]
        assert usd_raw['meta']['currency']=='USD'
        fxurl=f'https://query1.finance.yahoo.com/v8/finance/chart/GBPUSD=X?period1={f.epoch(START-timedelta(days=7))}&period2={f.epoch(END+timedelta(days=1))}&interval=1d'
        fx=f.get(fxurl,RAW,'gbpusd_daily',False)['chart']['result'][0]
        assert fx['meta']['currency']=='USD'
        tz=ZoneInfo(fx['meta']['exchangeTimezoneName'])
        rates={datetime.fromtimestamp(ts,tz).date():float(px) for ts,px in zip(fx['timestamp'],fx['indicators']['quote'][0]['close']) if px is not None and datetime.fromtimestamp(ts,tz).date()<=END}
        prices=[];mapping=[]
        for ts,close in usd['prices']:
            day=datetime.fromtimestamp(ts/1000,timezone.utc).date()
            fxday=max(d for d in rates if d<=day)
            assert (day-fxday).days<=4
            converted=Decimal(str(close))/Decimal(str(rates[fxday]))
            prices.append([ts,round(float(converted),2 if converted>=1 else 4)])
            mapping.append(dict(crypto_day=str(day),fx_day=str(fxday),usd_per_gbp=rates[fxday]))
        method='Yahoo HYPE daily USD quote.close divided by the same-date GBPUSD daily close, or latest preceding available FX close on non-trading days; never future FX; same output precision as the original UK pilot'
        r=copy.deepcopy(usdrow)
        r.update(symbol='HYPE32196-USD / GBPUSD=X',currency='GBP',source_currency='USD',first_close=prices[0][1],last_close=prices[-1][1],return_pct=round((prices[-1][1]/prices[0][1]-1)*100,1),currency_method='daily_historical_fx_conversion',fx_source_url=fxurl,price_definition=method,native_pair_unavailable=row)
        rows.append(r);files['hype_365d.json']={'prices':prices}
        fallbacks.append(dict(slug='hype',reason='Native GBP pair returned HTTP 404',method=method))
        fxpolicy=dict(source_url=fxurl,quote='USD per GBP',date_timezone=str(tz),non_trading_days='latest preceding FX close; never a future rate',last_fx_day=str(max(rates)))
        (RAW/'hype_fx_dates.json').write_bytes(f.encoded(dict(source_url=fxurl,fx_date_timezone=str(tz),usage=mapping)))
order=list(f.CRYPTOS)
rows.sort(key=lambda r:order.index(r['slug']))
for row in rows:
    pts=files[row['slug']+'_365d.json']['prices']
    assert row['first_day']==str(START) and row['last_day']==str(END)
    assert all(math.isfinite(px) and px>0 for ts,px in pts)
    assert all(a[0]<b[0] for a,b in zip(pts,pts[1:]))
    assert len(pts)==366-len(row['historical_missing_days_omitted'])
    assert round((pts[-1][1]/pts[0][1]-1)*100,1)==row['return_pct']
    assert (row['return_pct']>0)==(row['slug'] in ['xmr','hype']), 'Intended return pattern changed'
OUT.mkdir(parents=True)
for name,data in files.items(): (OUT/name).write_bytes(f.encoded(data))
manifest=dict(snapshot_id=SNAP,currency='GBP',currency_method='historical_gbp_prices_native_where_available_daily_fx_fallback',collection_date=str(ASOF),fetch_started_at_utc=started,fetch_finished_at_utc=datetime.now(timezone.utc).isoformat(timespec='seconds'),window_start=str(START),window_end=str(END),elapsed_calendar_days=365,assets=rows,errors=[],replication=['ETH','XMR','BNB'],self_directed=['BTC','HYPE','XRP'],raw_source_records=sorted(f.RAW_RECORDS,key=lambda r:r['key']),files_sha256={name:hashlib.sha256((OUT/name).read_bytes()).hexdigest() for name in sorted(files)},native_pair_fallbacks=fallbacks,fx_policy=fxpolicy if fallbacks else None,note='Fresh fetch; no prior source cache reused; current UTC day excluded; historical interior gaps omitted and disclosed.')
(OUT/'manifest.json').write_bytes(f.encoded(manifest))
print(json.dumps(dict(snapshot=SNAP,window=[str(START),str(END)],assets=[{k:r[k] for k in ['slug','return_pct','points','historical_missing_days_omitted','backfilled_from_1h']} for r in rows]),indent=2))
