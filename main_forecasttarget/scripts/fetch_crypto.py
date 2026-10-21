#!/usr/bin/env python3
"""Fetch immutable USD crypto data for Forecast Target main W1.

Never commits or pushes. Uses completed UTC days and Yahoo USD quotes.
Historical missing observations are omitted, never interpolated.
"""
import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

CRYPTOS = {'eth':'ETH-USD','xmr':'XMR-USD','bnb':'BNB-USD',
           'btc':'BTC-USD','hype':'HYPE32196-USD','xrp':'XRP-USD'}
UTC = timezone.utc
ET = ZoneInfo('America/New_York')
UA = {'User-Agent':'Mozilla/5.0'}
RAW_RECORDS = []


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(obj):
    return (json.dumps(obj, indent=2, allow_nan=False) + '\n').encode()


def get(url, cache, key, reuse):
    path = cache / (key + '.json')
    if reuse:
        raw = path.read_bytes()
    else:
        failure = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(urllib.request.Request(url,headers=UA), timeout=30) as response:
                    raw = response.read()
                json.loads(raw)
                cache.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
                break
            except Exception as exc:
                failure = exc
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
        else:
            raise RuntimeError(f'{key}: {failure}')
    RAW_RECORDS.append({'key':key, 'url':url, 'sha256':digest(raw)})
    return json.loads(raw)


def epoch(d):
    return int(datetime.combine(d,datetime.min.time(),UTC).timestamp())


def price_series(symbol, slug, kind, end, days, cache, reuse):
    start = end - timedelta(days=days)
    # Stocks use ET calendar dates; two extra UTC days cover ET timestamps.
    url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
           f'?period1={epoch(start)}&period2={epoch(end+timedelta(days=2))}'
           '&interval=1d&events=splits')
    result = get(url,cache,kind+'_'+slug+'_daily',reuse)['chart']['result'][0]
    assert result['meta']['currency'] == 'USD', (symbol, result['meta'].get('currency'))
    tz = ET if kind == 'stock' else UTC
    byday = {}
    nulls = []
    for ts, close in zip(result.get('timestamp',[]),result['indicators']['quote'][0]['close']):
        day = datetime.fromtimestamp(ts,tz).date()
        if not start <= day <= end:
            continue
        if close is None:
            nulls.append(str(day))
        else:
            px = float(close)
            byday[day] = [int(ts*1000), round(px, 2 if px >= 1 else 4)]
    backfilled = []
    historical_gaps = []
    if kind == 'crypto':
        missing = [start+timedelta(days=i) for i in range(days+1) if start+timedelta(days=i) not in byday]
        historical_gaps = [str(d) for d in missing if d < end-timedelta(days=6)]
        missing = [d for d in missing if d >= end-timedelta(days=6)]
        if missing:
            # Only fill complete daily values from the final 23:00 UTC hourly
            # bar. Never substitute the current/incomplete daily price.
            hurl=(f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
                  f'?period1={epoch(end-timedelta(days=6))}&period2={epoch(end+timedelta(days=1))}&interval=1h')
            hourly=get(hurl,cache,kind+'_'+slug+'_hourly',reuse)['chart']['result'][0]
            for ts, close in zip(hourly.get('timestamp',[]),hourly['indicators']['quote'][0]['close']):
                dt=datetime.fromtimestamp(ts,UTC)
                if dt.date() in missing and dt.hour==23 and close is not None:
                    px=float(close)
                    byday[dt.date()]=[epoch(dt.date())*1000,round(px,2 if px>=1 else 4)]
                    backfilled.append(str(dt.date()))
            missing=[str(x) for x in missing if x not in byday]
            if missing:
                raise ValueError(f'{symbol}: missing completed UTC days: {missing}')
    elif nulls:
        raise ValueError(f'{symbol}: unconsolidated completed-session close(s): {nulls}')
    if not byday or max(byday)!=end:
        raise ValueError(f'{symbol}: expected last completed date {end}, got {max(byday) if byday else None}')
    if kind=='stock' and (min(byday)-start).days>3:
        raise ValueError(f'{symbol}: unexpectedly missing window start')
    pts=[byday[x] for x in sorted(byday)]
    assert all(p[1]>0 for p in pts)
    row={'symbol':symbol,'slug':slug,'points':len(pts),'requested_start':str(start),
         'first_day':str(min(byday)),'last_day':str(max(byday)),
         'first_close':pts[0][1],'last_close':pts[-1][1],
         'return_pct':round((pts[-1][1]/pts[0][1]-1)*100,1),
         'backfilled_from_1h':backfilled, 'historical_missing_days_omitted':historical_gaps, 'source_url':url,
         'price_definition':'Yahoo quote.close; split-adjusted; no dividend reinvestment; historical missing daily values omitted, never interpolated'}
    print(kind, symbol, row['return_pct'], row['first_day'],row['last_day'],len(pts),flush=True)
    return {'prices':pts},row



def main():
    from concurrent.futures import ThreadPoolExecutor
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--end',type=date.fromisoformat,required=True)
    parser.add_argument('--snapshot-id',required=True)
    parser.add_argument('--reuse',action='store_true')
    args=parser.parse_args()
    assert args.end < datetime.now(UTC).date(), 'Refuse incomplete UTC day'
    root=args.output; cache=root/'sources'/args.snapshot_id
    dest=root/'crypto'/'snapshots'/args.snapshot_id/'usd'
    assert not dest.exists(), 'Immutable snapshot already exists'
    def fetch(item):
        slug,symbol=item
        return slug,price_series(symbol,slug,'crypto',args.end,365,cache,args.reuse)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results=list(pool.map(fetch,CRYPTOS.items()))
    dest.mkdir(parents=True)
    assets=[]; hashes={}
    for slug,(data,row) in results:
        assert row['first_day']==str(args.end-timedelta(days=365))
        assert row['last_day']==str(args.end)
        pts=data['prices']; assert all(a[0]<b[0] for a,b in zip(pts,pts[1:]))
        assert len(set(x[0] for x in pts))==len(pts)
        assert all(datetime.fromtimestamp(x[0]/1000,UTC).hour==0 for x in pts)
        row.update(currency='USD',min_close=min(x[1] for x in pts),max_close=max(x[1] for x in pts))
        fn=slug+'_365d.json'; raw=encoded(data)
        (dest/fn).write_bytes(raw); hashes[fn]=digest(raw); assets.append(row)
    manifest={'snapshot_id':args.snapshot_id,'currency':'USD',
      'created_at_utc':datetime.now(UTC).isoformat(),
      'window_start':str(args.end-timedelta(days=365)),'window_end':str(args.end),
      'date_convention':'Completed UTC calendar days; 365-day interval including both endpoints',
      'rounding':'Prices rounded to 2 decimals at USD 1 or above, otherwise 4; returns computed from displayed endpoint prices and shown to 1 decimal',
      'assets':assets,'files_sha256':hashes,'source_records':RAW_RECORDS}
    (dest/'manifest.json').write_bytes(encoded(manifest))
    (cache/'source_records.json').write_bytes(encoded(RAW_RECORDS))
    (root/'README.txt').write_text('Forecast Target main — USD cryptocurrency stimuli\n\nSnapshot: '+args.snapshot_id+'\nPrice window: '+manifest['window_start']+' to '+manifest['window_end']+' (UTC).\nSix unchanged assets: ETH, XMR, BNB, BTC, HYPE, XRP.\nYahoo USD quotes, no GBP conversion. Last complete UTC day only.\nDaily source gaps are documented in manifest.json, never interpolated.\nA recent missing daily close may use the close of the final 23:00 UTC hourly bar.\nPrice rounding matches W1. The survey derives returns, highs and lows from these files at runtime.\nSurvey QSF and participant data are intentionally excluded from this public data folder.\nExisting studies and snapshots are unchanged.\n')
    print(json.dumps({'snapshot':args.snapshot_id,'window_end':str(args.end),'assets':len(assets),'folder':str(dest)},indent=2))

if __name__=='__main__':
    main()
