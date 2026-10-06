#!/usr/bin/env python3
"""Fetch a complete, immutable Structural Similarity main-study asset snapshot.

Never commits or pushes. Annual bars are GAAP net income (not EPS). Yahoo
quote closes are split-adjusted, excluding dividends; adjclose is not used.
Requires Python >=3.9; stock fundamentals require yfinance and curl_cffi.
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

STOCKS = ['ADBE', 'EPAM', 'AKAM', 'CSCO', 'INTU', 'ANET', 'ORCL']
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


def annual_profits(symbol, asof, cache, reuse):
    url=(f'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}'
         f'?type=annualNetIncome,trailingNetIncome&period1=1400000000&period2={epoch(asof+timedelta(days=1))}&merge=false')
    payload=get(url,cache,symbol.lower()+'_earnings',reuse)
    annual={}; trailing={}
    for result in payload['timeseries']['result']:
        for metric,out in [('annualNetIncome',annual),('trailingNetIncome',trailing)]:
            for item in result.get(metric,[]):
                if item and item['asOfDate']<=str(asof):
                    out[item['asOfDate']]=float(item['reportedValue']['raw'])
    if not annual:
        raise ValueError(f'{symbol}: missing annual net income')
    # A TTM observation may fill a newly reported fiscal year only at its
    # exact fiscal year-end month/day. Never relabel an interim TTM as FY.
    last=max(annual); fill=None
    for ended,net in sorted(trailing.items()):
        if ended>last and ended[5:]==last[5:] and int(ended[:4])==int(last[:4])+1:
            annual[ended]=net
            fill={'source':'trailingNetIncome_at_fiscal_year_end','asOf':ended}
    entries=sorted(annual.items())[-4:]
    vals=[round(net/1e9,2) for ended,net in entries]
    if len(vals)!=4 or not all(x>0 for x in vals):
        raise ValueError(f'{symbol}: need four positive annual net-income bars: {vals}')
    return {'fy_ends':[x[0] for x in entries], 'fy_labels':['FY '+x[0][:4] for x in entries],
            'fy_values_bn':vals,'np_last_fy_bn':vals[-1],
            'fychange_pct':round((vals[-1]/vals[-2]-1)*100,1),
            'fill_last':fill,'source_url':url,'metric':'GAAP annual net income, USD billions'}


def peer_distribution(repo):
    rel='WRDS/pb_ratio_sectors_40_45_with_industry.csv'
    # Read tracked version deliberately; avoids accidental CSV edits or
    # cloud-only file stalls and records exact peer-set source hash.
    data=subprocess.run(['git','-C',str(repo),'show','HEAD:'+rel],capture_output=True,check=True).stdout
    rows=list(csv.DictReader(data.decode().splitlines()))
    peers={}
    for row in rows:
        try:
            if row['gsector']=='45' and float(row['pb_ratio'])>0 and float(row['mktcap'])>=10000:
                peers[row['tic'].strip().upper()]=float(row['pb_ratio'])
        except (ValueError,TypeError):
            continue
    if not peers:
        raise ValueError('empty WRDS technology peer distribution')
    return peers,{'path':rel,'sha256':digest(data),'peer_count':len(peers),
                  'note':'Historical tracked WRDS GICS-45 peer distribution; positive P/B; market cap >= $10bn; current Yahoo P/B ranked against historical peers; not a freshly downloaded peer distribution.'}


def fundamentals(symbol, prices, profit, peers, cache, reuse, stock_end):
    key=symbol.lower()+'_quote_fundamentals'
    path=cache/(key+'.json')
    if reuse:
        info=json.loads(path.read_bytes())
    else:
        import yfinance as yf
        from curl_cffi import requests
        info=yf.Ticker(symbol,session=requests.Session(impersonate='chrome')).info
        # Persist only the public fields used, never cookie/crumb/auth state.
        keys=['regularMarketTime','regularMarketPrice','marketCap','priceToBook','trailingPE',
              'netIncomeToCommon','dividendYield','bookValue','sharesOutstanding']
        info={k:info.get(k) for k in keys}
        path.write_bytes(encoded(info))
    RAW_RECORDS.append({'key':key,'source':'Yahoo quoteSummary via yfinance','sha256':digest(path.read_bytes())})
    timestamp=info.get('regularMarketTime')
    if timestamp is None or datetime.fromtimestamp(timestamp,ET).date()!=stock_end:
        raise ValueError(f'{symbol}: quote date differs from pinned stock close: {timestamp}')
    quote=float(info['regularMarketPrice'])
    close=prices['prices'][-1][1]
    if abs(quote/close-1)>.002:
        raise ValueError(f'{symbol}: quote and pinned close do not agree: {quote} / {close}')
    pb=round(float(info['priceToBook'])*close/quote,2)
    peer_vals=[v for s,v in peers.items() if s!=symbol]
    pct=round(100*sum(v<pb for v in peer_vals)/len(peer_vals))
    valuation='Low' if pct<=33 else 'High' if pct>=67 else 'Mid'
    dividend=info.get('dividendYield')
    if dividend is not None:
        dividend=round(dividend*100 if dividend*100<=20 else dividend,2)
    return {'marketcap':round(float(info['marketCap'])*close/quote/1e6,2),
            'pb_current':pb,'pb_current_pctile':pct,'pb_yahoo':pb,
            'pe_yahoo':round(float(info['trailingPE']),1) if info.get('trailingPE') else None,
            'div_y':dividend,'netprofit':round(profit['np_last_fy_bn']*1000),
            'netprofit_src':'annual_last_displayed_fy_rounded_bar',
            'netprofit_growth':profit['fychange_pct'],'valuation':valuation,'sector':'Technology',
            'netprofit_ttm':round(float(info['netIncomeToCommon'])/1e6) if info.get('netIncomeToCommon') else None,
            'quote_time_utc':datetime.fromtimestamp(timestamp,UTC).isoformat(),
            'pb_peer_note':'Current Yahoo P/B against historical tracked WRDS sector-45 peers; same pilot tier definition'}


def publish_local(folder, files, manifest, update_current):
    if folder.exists():
        raise FileExistsError(f'Frozen snapshot already exists: {folder}. Choose a new --snapshot-id; never overwrite fielded stimuli.')
    stage=folder.with_name(folder.name+'.staging')
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)
    hashes={}
    for name,obj in files.items():
        payload=encoded(obj)
        (stage/name).write_bytes(payload)
        hashes[name]=digest(payload)
    manifest=dict(manifest,files_sha256=hashes)
    (stage/'manifest.json').write_bytes(encoded(manifest))
    stage.rename(folder)
    if update_current:
        current=folder.parent.parent/'current'
        current.mkdir(parents=True,exist_ok=True)
        for path in folder.glob('*.json'):
            shutil.copyfile(path,current/path.name)
    return manifest


def verify(root, snapshot, kinds=('stock','crypto')):
    for kind in kinds:
        folder=root/kind/'snapshots'/snapshot
        manifest=json.loads((folder/'manifest.json').read_bytes())
        for name,want in manifest['files_sha256'].items():
            assert digest((folder/name).read_bytes())==want, str(folder/name)
        print(kind,'snapshot hashes verified',len(manifest['files_sha256']),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--as-of',type=date.fromisoformat,required=True,help='Collection date; both price endpoints must be earlier dates')
    parser.add_argument('--stock-end',type=date.fromisoformat,help='Last fully completed US trading session; required unless --only-crypto')
    parser.add_argument('--only-crypto',action='store_true',help='Refresh crypto without fetching or changing any stock data')
    parser.add_argument('--crypto-end',type=date.fromisoformat,required=True,help='Last fully completed UTC crypto day')
    parser.add_argument('--window-days',type=int,default=365,help='Elapsed calendar days, inclusive endpoints')
    parser.add_argument('--snapshot-id',required=True)
    parser.add_argument('--raw-cache',type=Path,default=Path('/private/tmp/model-spillovers/structuralsimilarity_main_raw'))
    parser.add_argument('--reuse-cache',action='store_true',help='Reproduce outputs offline from preserved public raw responses')
    parser.add_argument('--update-current',action='store_true',help='Also replace main-study current aliases; snapshots remain immutable')
    parser.add_argument('--verify-only',action='store_true')
    args=parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]+',args.snapshot_id):
        parser.error('snapshot-id may contain only letters, numbers, underscore and hyphen')
    root=args.repo_root/'structuralsimilarity_main'
    if args.verify_only:
        verify(root,args.snapshot_id,['crypto'] if args.only_crypto else ['stock','crypto']);return
    if not (args.crypto_end<args.as_of and (args.only_crypto or (args.stock_end is not None and args.stock_end<args.as_of))):
        parser.error('end dates must precede collection date; no in-progress bars')
    if any((root/kind/'snapshots'/args.snapshot_id).exists() for kind in (['crypto'] if args.only_crypto else ['stock','crypto'])):
        parser.error('snapshot already exists; use --verify-only or a new snapshot-id')
    args.raw_cache.mkdir(parents=True,exist_ok=True)
    fetched=datetime.now(UTC).isoformat(timespec='seconds')
    common={'snapshot_id':args.snapshot_id,'collection_date':str(args.as_of),'fetched_at_utc':fetched,
            'elapsed_calendar_days':args.window_days,'no_push_or_commit':True, 'missing_data_note':'Complete endpoints required. Historical interior crypto nulls omitted; no interpolated prices. Any omissions recorded per asset.',
            'freeze_note':'QSF must reference immutable snapshot path; never refresh during fielding.'}
    files={}; crypto_rows=[]
    for slug,symbol in CRYPTOS.items():
        data,row=price_series(symbol,slug,'crypto',args.crypto_end,args.window_days,args.raw_cache,args.reuse_cache)
        files[slug+'_365d.json']=data;crypto_rows.append(row)
    files['summary.json']=dict(common,cryptos=crypto_rows,errors=[],expected_last_day_utc=str(args.crypto_end),stale=[])
    crypto_files=files
    crypto_manifest=dict(common,last_completed_day=str(args.crypto_end),kind='crypto',
                         assets=crypto_rows,replication=['ETH','XMR','BNB'],self_directed=['BTC','HYPE','XRP'])
    if args.only_crypto:
        crypto_manifest['raw_source_records']=list(RAW_RECORDS)
        crypto_manifest['fetch_script_sha256']=digest(Path(__file__).read_bytes())
        publish_local(root/'crypto'/'snapshots'/args.snapshot_id,crypto_files,crypto_manifest,args.update_current)
        print('Prepared crypto-only local snapshot; nothing committed or pushed.',flush=True)
        return
    files={};stock_rows=[];profits={};funds={}
    peers,peer_meta=peer_distribution(args.repo_root)
    for symbol in STOCKS:
        data,row=price_series(symbol,symbol.lower(),'stock',args.stock_end,args.window_days,args.raw_cache,args.reuse_cache)
        files[symbol.lower()+'_365d.json']=data;stock_rows.append(row)
        profits[symbol]=annual_profits(symbol,args.as_of,args.raw_cache,args.reuse_cache)
        funds[symbol]=fundamentals(symbol,data,profits[symbol],peers,args.raw_cache,args.reuse_cache,args.stock_end)
        print(symbol,profits[symbol]['fy_values_bn'],profits[symbol]['fychange_pct'],funds[symbol]['valuation'],flush=True)
    files['profits.json']={'fetched_at':fetched,'stocks':profits}
    files['fundamentals.json']=funds
    files['summary.json']=dict(common,stocks=stock_rows,fundamentals=funds,errors=[],peer_distribution=peer_meta)
    stock_manifest=dict(common,last_completed_day=str(args.stock_end),kind='stock',assets=stock_rows,
                        annual_earnings=profits,peer_distribution=peer_meta)
    for manifest in [crypto_manifest,stock_manifest]:
        manifest['raw_source_records']=list(RAW_RECORDS)
        manifest['fetch_script_sha256']=digest(Path(__file__).read_bytes())
    # Both asset classes have passed validation before any frozen output is installed.
    publish_local(root/'crypto'/'snapshots'/args.snapshot_id,crypto_files,crypto_manifest,args.update_current)
    publish_local(root/'stock'/'snapshots'/args.snapshot_id,files,stock_manifest,args.update_current)
    verify(root,args.snapshot_id)
    print('Prepared local snapshot only; nothing committed, pushed, or published.',flush=True)

if __name__=='__main__':
    main()
