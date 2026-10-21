from datetime import date, timedelta
ASOF="2026-10-21"

def extract(d):
    ns=d['facts'].get('us-gaap',{})
    tag=next(k for k in ['NetIncomeLoss','ProfitLoss','NetIncomeLossAvailableToCommonStockholdersBasic'] if k in ns)
    rows=[r for r in ns[tag]['units']['USD'] if r.get('start') and r.get('filed','9999')<=ASOF and r.get('form') in ['10-Q','10-K','20-F','6-K']]
    periods={}
    for r in sorted(rows,key=lambda r:(r.get('filed',''),r.get('accn',''))):periods[(r['start'],r['end'])]=r
    day=date.fromisoformat;quarters={}
    for (s,e),r in periods.items():
        if 65<=(day(e)-day(s)).days<=115:quarters[(s,e)]={**r,'tag':tag,'derived':False}
    for (s,e),annual in periods.items():
        if not 340<=(day(e)-day(s)).days<=380:continue
        nine=[r for (s2,e2),r in periods.items() if s2==s and 245<=(day(e2)-day(s2)).days<=295 and 65<=(day(e)-day(e2)).days<=115]
        if nine:
            n=max(nine,key=lambda r:r['end']);start=str(day(n['end'])+timedelta(days=1));key=(start,e)
            if key not in quarters:
                quarters[key]=dict(start=start,end=e,val=annual['val']-n['val'],filed=max(annual['filed'],n['filed']),tag=tag,derived=True,
                    annual_accn=annual['accn'],nine_month_accn=n['accn'],annual_value=annual['val'],nine_month_value=n['val'])
    byend={}
    for q in sorted(quarters.values(),key=lambda r:r['filed']):byend[q['end']]=q
    q=sorted(byend.values(),key=lambda r:r['end'])[-16:]
    assert len(q)==16
    for a,b in zip(q,q[1:]):assert (day(b['start'])-day(a['end'])).days==1
    vals=[sum(r['val'] for r in q[i:i+4])/1e9 for i in [0,4,8,12]]
    assert all(v>0 for v in vals)
    return dict(rolling=vals,rolling_labels=[q[i]['end'] for i in [3,7,11,15]],rolling_growth=100*(vals[-1]/vals[-2]-1),
        rolling_path=100*(vals[-1]/vals[0]-1),last_quarter_end=q[-1]['end'],quarters=q,source_tag=tag,latest_filed=max(x['filed'] for x in q))
