#!/usr/bin/env python3
"""Refresh the agreed seven-stock slate and four rolling annual profit bars.

Downloads fresh SEC companyfacts/submissions and Yahoo prices/quote fields.
Four bars sum sixteen sequential fiscal quarters, spaced one year apart.
Refuses to publish if the latest SEC report is missing or quarters have gaps.
Immutable snapshots and public source hashes preserve each stimulus version.
Does not commit, push, purge caches, or edit surveys. Requires pandas/yfinance.
"""
import argparse,json,sys,urllib.request,time,re
from pathlib import Path
from datetime import date,datetime,timedelta,timezone
sys.dont_write_bytecode=True
import pandas as pd
import numpy as np
import fetch_assets as shared
STOCKS=['ADBE','CTSH','AKAM','CSCO','INTU','ANET','ORCL']
CIKS={'ADBE':796343,'CTSH':1058290,'AKAM':1086222,'CSCO':858877,'INTU':896878,'ANET':1596532,'ORCL':1341439}
RELEASES={
 'ADBE':('2026-08-28','2026-09-10','https://news.adobe.com/news/2026/09/adobe-q3fy26-financial-results'),
 'CTSH':('2026-06-30','2026-07-29','https://news.cognizant.com/2026-07-29-Cognizant-Reports-Second-Quarter-2026-Results'),
 'AKAM':('2026-06-30','2026-08-06','https://www.sec.gov/Archives/edgar/data/1086222/000108622226000083/exhibit991-q22026.htm'),
 'CSCO':('2026-07-25','2026-08-12','https://investor.cisco.com/news/news-details/2026/CISCO-REPORTS-FOURTH-QUARTER-AND-FISCAL-YEAR-2026-EARNINGS/default.aspx'),
 'INTU':('2026-07-31','2026-08-25','https://investors.intuit.com/news-events/press-releases/detail/1320/intuit-reports-fourth-quarter-and-full-year-fiscal-2026-results-sets-fiscal-2027-guidance'),
 'ANET':('2026-06-30','2026-08-04','https://www.arista.com/company/news/press-release/24401-pr-20260804'),
 'ORCL':('2026-08-31','2026-09-10','https://investor.oracle.com/investor-news/news-details/2026/Oracle-Announces-Q1-Results-Driven-by-Triple-Digit-Growth-in-Cloud-Infrastructure-Revenues/default.aspx')}

def quarters(d,asof):
    tags=[t for t in ['NetIncomeLoss','ProfitLoss','ProfitLossAttributableToOwnersOfParent'] if t in d['facts']['us-gaap'] and 'USD' in d['facts']['us-gaap'][t]['units']]
    records=[]
    for priority,tag in enumerate(tags):
        for x in d['facts']['us-gaap'][tag]['units']['USD']:
            if 'start' not in x or x.get('filed','9999')>asof or x['end']>asof or x.get('form') not in ['10-K','10-K/A','10-Q','10-Q/A']:continue
            records.append(dict(x,source_tag=tag,priority=priority,days=(date.fromisoformat(x['end'])-date.fromisoformat(x['start'])).days))
    df=pd.DataFrame(records).sort_values(['priority','filed','accn'],ascending=[False,True,True]).drop_duplicates(['start','end'],keep='last')
    # Prefer the highest-priority standard net income tag, then latest filing.
    direct=df[df.days.between(65,115)].sort_values(['end','priority','filed'],ascending=[True,False,True]).drop_duplicates('end',keep='last')
    out={r['end']:{k:r[k] for k in ['start','end','val','filed','accn','source_tag']}|{'derived':False} for r in direct.to_dict('records')}
    for y in df[df.days.between(330,380)].sort_values(['end','priority','filed'],ascending=[True,False,True]).drop_duplicates('end',keep='last').to_dict('records'):
        if y['end'] in out:continue
        prev=df[(df.start==y['start']) & df.days.between(240,300) & (df.source_tag==y['source_tag'])]
        if prev.empty:continue
        p=prev.sort_values('filed').iloc[-1].to_dict()
        if not 65<=(date.fromisoformat(y['end'])-date.fromisoformat(p['end'])).days<=115:continue
        out[y['end']]=dict(start=str(date.fromisoformat(p['end'])+timedelta(days=1)),end=y['end'],val=float(y['val']-p['val']),filed=max(y['filed'],p['filed']),accn=y['accn'],source_tag=y['source_tag'],derived=True,source='annual minus nine months',annual_accn=y['accn'],ytd_accn=p['accn'],annual_value=y['val'],nine_month_value=p['val'])
    return [out[k] for k in sorted(out)]

def ctsh_supplement(cache,reuse):
    """SEC companyfacts can lag a published results release: verify its table."""
    from bs4 import BeautifulSoup
    url=RELEASES['CTSH'][2];path=cache/'CTSH_q2_2026_release.html'
    if not reuse:
        from curl_cffi import requests
        r=requests.get(url,impersonate='chrome',timeout=30);r.raise_for_status();raw=r.content
        path.write_bytes(raw)
    raw=path.read_bytes();soup=BeautifulSoup(raw,'html.parser')
    text=soup.get_text(' ',strip=True)
    assert 'June 30, 2026' in text and 'July 29, 2026' in text
    matched=[]
    for table in soup.find_all('table'):
        for tr in table.find_all('tr'):
            cells=[c.get_text(' ',strip=True) for c in tr.find_all(['td','th'])]
            if cells and ('Net income'==cells[0] or 'Net income' in cells[0]) and any(re.sub(r'[^0-9]','',c)=='636' for c in cells[1:]):matched.append(cells)
    assert matched, 'Cannot verify Q2 2026 GAAP net income in the public release'
    return dict(start='2026-04-01',end='2026-06-30',val=636000000,filed='2026-07-29',source_tag='NetIncomeLoss',derived=False,source='Q2 2026 public earnings release',source_url=url,source_sha256=shared.digest(raw),verified_table_rows=matched)

def diagnostics(folder):
    profits=json.loads((folder/'profits.json').read_text())['stocks'];rows=[]
    for t in STOCKS:
        p=json.loads((folder/(t.lower()+'_365d.json')).read_text())['prices'];v=profits[t]['annual_values_bn']
        rows.append(dict(symbol=t,P=round((p[-1][1]/p[0][1]-1)*100,1),G=profits[t]['annualchange_pct'],H=100*(v[-1]/v[0]-1),last_annual_profit_bn=v[-1],quarter_end=profits[t]['latest_reported_quarter_end'],bars=v))
    frame=pd.DataFrame(rows).set_index('symbol');cue=frame[['P','G','H']]
    return dict(assets=rows,pearson=cue.corr().to_dict(),spearman=cue.corr(method='spearman').to_dict(),price_vs_latest_annual_profit=float(frame.P.corr(frame.last_annual_profit_bn)),leave_one_out_P_G={t:float(cue.drop(t).P.corr(cue.drop(t).G)) for t in STOCKS},positive_price_returns=int((frame.P>0).sum()),negative_price_returns=int((frame.P<0).sum()),opposite_price_growth_sign=[t for t in STOCKS if frame.loc[t,'P']*frame.loc[t,'G']<0])

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--asof',type=date.fromisoformat,required=True);p.add_argument('--price-end',type=date.fromisoformat,required=True);p.add_argument('--snapshot-id',required=True);p.add_argument('--reuse-sources',action='store_true');p.add_argument('--update-current',action='store_true');a=p.parse_args()
    assert a.price_end<=a.asof
    root=a.repo/'structuralsimilarity_main';baseline=root/'stock/snapshots/launch_2026-10-05_v3';sources=root/'source_snapshots'/a.snapshot_id;target=root/'stock/snapshots'/a.snapshot_id
    if target.exists():raise FileExistsError('Never overwrite an immutable snapshot')
    sources.mkdir(parents=True,exist_ok=True);files={};profits={};funds={};rows=[];audit={};peers,peer_meta=shared.peer_distribution(a.repo)
    for t in STOCKS:
        print('Fetching',t,flush=True)
        cik=CIKS[t]
        securl=f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json';suburl=f'https://data.sec.gov/submissions/CIK{cik:010d}.json'
        oldua=shared.UA;shared.UA={'User-Agent':'Paul Grass research paul.grass@uni-bonn.de'}
        sec=shared.get(securl,sources,t+'_sec',a.reuse_sources);sub=shared.get(suburl,sources,t+'_submissions',a.reuse_sources);shared.UA=oldua
        assert int(sec['cik'])==cik
        q=quarters(sec,str(a.asof));expected,release_date,release_url=RELEASES[t]
        if t=='CTSH':
            public=ctsh_supplement(sources,a.reuse_sources)
            if q[-1]['end']<'2026-06-30':q.append(public)
            else:assert next(x for x in q if x['end']=='2026-06-30')['val']==public['val']
        q=q[-16:];assert len(q)==16
        for x,y in zip(q,q[1:]):
            assert 65<=(date.fromisoformat(y['end'])-date.fromisoformat(x['end'])).days<=115
            assert date.fromisoformat(y['start'])==date.fromisoformat(x['end'])+timedelta(days=1),f'{t}: quarter gap/overlap'
        recent=sub['filings']['recent'];filings=[]
        for i,form in enumerate(recent['form']):
            if form in ['10-Q','10-Q/A','10-K','10-K/A'] and recent['filingDate'][i]<=str(a.asof):
                filings.append({k:recent[k][i] for k in ['form','reportDate','filingDate','accessionNumber','primaryDocument']})
        latest=max(filings,key=lambda x:x['reportDate'])
        assert q[-1]['end']>=latest['reportDate'],f'{t}: latest SEC report missing in earnings: {latest}'
        assert q[-1]['end']>=expected,f'{t}: latest public earnings release missing'
        vals=[sum(float(x['val']) for x in q[i:i+4])/1e9 for i in (0,4,8,12)];ends=[q[i]['end'] for i in (3,7,11,15)];starts=[q[i]['start'] for i in (0,4,8,12)];g=100*(vals[-1]/vals[-2]-1);assert all(v>0 for v in vals);last=date.fromisoformat(ends[-1]);month=last.strftime('%b');year=last.year
        profit=dict(annual_starts=starts,annual_ends=ends,annual_labels=[date.fromisoformat(s).strftime('%b %Y') for s in ends],annual_values_bn=vals,np_last_annual_bn=vals[-1],annualchange_pct=round(g,1),annualchange_pct_unrounded=g,metric='GAAP net income; four-quarter totals spaced one year apart; USD billions',latest_reported_quarter_end=ends[-1],forecast_start=str(last+timedelta(days=1)),forecast_period_label=f'four fiscal quarters after {month} {year}',forecast_baseline_label=f'year ended {month} {year}',forecast_definition='Total GAAP net income for the next four fiscal quarters following the latest reported quarter shown, relative to the latest displayed four-quarter annual total. Identify quarters by fiscal sequence; actual future quarter-end days may differ for 52/53-week fiscal years.',quarterly_source_records=q,source_companyfacts_sha256=shared.digest((sources/(t+'_sec.json')).read_bytes()),source_companyfacts_url=securl)
        prices,row=shared.price_series(t,t.lower(),'stock',a.price_end,365,sources,a.reuse_sources)
        fund=shared.fundamentals(t,prices,{'np_last_fy_bn':vals[-1],'fychange_pct':round(g,1)},peers,sources,a.reuse_sources,a.price_end)
        fund.update(netprofit=round(vals[-1]*1000,3),netprofit_ttm=round(vals[-1]*1000,3),netprofit_src='rolling_annual_four_reported_quarters')
        audit[t]=dict(latest_quarter_end=q[-1]['end'],latest_quarter_net_income_usd=q[-1]['val'],latest_selected_quarter=q[-1],latest_sec_report=latest,checked_through=str(a.asof),public_release_quarter_end=expected,public_release_date=release_date,public_release_url=release_url,public_release_matches_latest_selected=q[-1]['end']==expected)
        rows.append(row);profits[t]=profit;funds[t]=fund;files[t.lower()+'_365d.json']=prices
        print(t,'latest quarter',ends[-1],'bars',vals,'growth',round(g,1),flush=True)
    metadata=dict(snapshot_id=a.snapshot_id,collection_date=str(a.asof),last_completed_day=str(a.price_end))
    funds['_metadata']=metadata;files['fundamentals.json']=funds;files['profits.json']=dict(snapshot_id=a.snapshot_id,stocks=profits)
    files['summary.json']=dict(**metadata,stocks=rows,fundamentals=funds,errors=[],earnings_definition='Four annual totals from sixteen consecutive reported fiscal quarters; GAAP net income, unadjusted for one-off items.')
    selected={t:dict(quarters=profits[t]['quarterly_source_records'],rolling=profits[t]['annual_values_bn'],rolling_growth=profits[t]['annualchange_pct_unrounded']) for t in STOCKS}
    (sources/'selected_rolling_records.json').write_bytes(shared.encoded(selected));(sources/'latest_quarter_audit.json').write_bytes(shared.encoded(audit))
    manifest=dict(**metadata,kind='stock',assets=rows,annual_earnings=profits,latest_quarter_audit=audit,previous_stock_snapshot='launch_2026-10-05_v3',growth_definition='Latest displayed four-quarter net-profit total relative to preceding displayed four-quarter total; rounded to one decimal for display and the preregistered main regressor.',pb_peer_distribution=peer_meta,sources=shared.RAW_RECORDS,sources_sha256={f.name:shared.digest(f.read_bytes()) for f in sources.iterdir() if f.is_file()},fetched_at_utc=datetime.now(timezone.utc).isoformat())
    shared.publish_local(target,files,manifest,a.update_current)
    # The obsolete EPAM file belongs only to the original, unfielded current set.
    if a.update_current:
        obsolete=root/'stock/current/epam_365d.json'
        if obsolete.exists():
            (sources/'obsolete_current_epam_365d.json').write_bytes(obsolete.read_bytes());obsolete.unlink()
    old=diagnostics(baseline) if baseline.exists() else None;new=diagnostics(target);oldbars={r['symbol']:r['bars'] for r in old['assets']} if old else {}
    analysis=dict(snapshot_id=a.snapshot_id,previous=old,current=new,annual_bars_unchanged={r['symbol']:r['bars']==oldbars.get(r['symbol']) for r in new['assets']},latest_quarter_audit=audit)
    (sources/'slate_diagnostics.json').write_bytes(shared.encoded(analysis))
    print(json.dumps(analysis,indent=2),flush=True)
if __name__=='__main__':main()
