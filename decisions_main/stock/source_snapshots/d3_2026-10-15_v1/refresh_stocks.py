"""Fetch a new frozen decisions-main W2 snapshot; never edit pilot data or push Git."""
from pathlib import Path
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
import concurrent.futures, urllib.request, json, hashlib, csv, math, warnings, os
warnings.filterwarnings('ignore')
import yfinance as yf
from curl_cffi import requests

ROOT = Path(os.environ.get('DECISIONS_W2_OUTPUT_ROOT', Path(__file__).resolve().parent))
SNAPSHOT = 'd3_2026-10-15_v1'
DATA = ROOT / 'decisions_main/stock/snapshots' / SNAPSHOT
RAW = ROOT / 'decisions_main/stock/source_snapshots' / SNAPSHOT
OLD = ROOT.parent / 'pair_review'
ASOF = date(2026, 10, 14)
START = ASOF - timedelta(days=365)
ET = ZoneInfo('America/New_York')
YF_SESSION = requests.Session(impersonate='chrome')
ORIGINAL = 'CSCO INTU NTAP EPAM AKAM NOW FFIV CHKP ZM ADBE TXN QCOM CRM ORCL'.split()
CANDIDATES = []

def write(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, allow_nan=False) + '\n')

def number(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None

def dividend_yield(info, closing_price):
    rate = number(info.get('dividendRate'))
    if rate is not None:
        return round(100 * rate / closing_price, 2)
    # Some quote responses contain yesterday's yield despite an updated stock price.
    # Use the archived, independently checked annual rate when the live rate is absent.
    archived = json.loads((RAW / 'verified_dividend_rates.json').read_text()).get(info.get('symbol'))
    if archived is not None:
        assert info.get('lastDividendValue') == archived['last_dividend_value'], 'Review changed dividend'
        assert info.get('exDividendDate') == archived['ex_dividend_date'], 'Review changed dividend date'
        reported = number(info.get('dividendYield'))
        prev = number(info.get('regularMarketPreviousClose', info.get('previousClose')))
        expected = [round(100 * archived['annual_rate'] / closing_price, 2)]
        if prev and prev > 0: expected.append(round(100 * archived['annual_rate'] / prev, 2))
        assert reported is not None and any(abs(reported - x) < .011 for x in expected), 'Review dividend rate: reported yield no longer matches'
        return round(100 * archived['annual_rate'] / closing_price, 2)
    # Yahoo dividendYield uses percentage units (1.46 means 1.46%, not 146%).
    # Preserve the existing blank display for non-payers when the annual rate is absent.
    direct = number(info.get('dividendYield'))
    return round(direct, 2) if direct is not None and direct > 0 else None

def get_stock(ticker):
    p1 = int(datetime.combine(START, time.min, ET).timestamp())
    p2 = int(datetime.combine(ASOF + timedelta(days=1), time.min, ET).timestamp())
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={p1}&period2={p2}&interval=1d&events=splits'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    raw_path = RAW / (ticker.lower() + '_daily.json')
    if raw_path.exists():
        raw = raw_path.read_bytes()
    else:
        with urllib.request.urlopen(req, timeout=35) as r:
            raw = r.read()
    (RAW / (ticker.lower() + '_daily.json')).write_bytes(raw)
    result = json.loads(raw)['chart']['result'][0]
    assert result['meta']['currency'] == 'USD', ticker
    points = [[ts * 1000, round(float(c), 2)]
              for ts, c in zip(result['timestamp'], result['indicators']['quote'][0]['close'])
              if c is not None and START <= datetime.fromtimestamp(ts, ET).date() <= ASOF]
    assert len(points) >= 240 and all(p[1] > 0 for p in points), ticker
    dates = [datetime.fromtimestamp(p[0] / 1000, ET).date().isoformat() for p in points]
    assert dates[0] == START.isoformat() and dates[-1] == ASOF.isoformat(), (ticker, dates[0], dates[-1])
    assert len(set(dates)) == len(dates) and dates == sorted(dates), ticker
    write(DATA / (ticker.lower() + '_365d.json'), {'prices': points})
    info_path = RAW / (ticker.lower() + '_info.json')
    if info_path.exists():
        info = json.loads(info_path.read_text())
    else:
        info = yf.Ticker(ticker, session=YF_SESSION).info
    assert info and number(info.get('marketCap')) and info.get('symbol') == ticker, ticker
    write(RAW / (ticker.lower() + '_info.json'), info)
    changes = [(100 * (points[i][1] / points[i-1][1] - 1), dates[i]) for i in range(1, len(points))]
    jump = max(changes, key=lambda x: abs(x[0]))
    return {'ticker': ticker, 'role': 'original' if ticker in ORIGINAL else 'replacement_candidate',
            'name': info.get('shortName'), 'sector': info.get('sector'),
            'first_date': dates[0], 'last_date': dates[-1], 'n': len(points),
            'first_close': points[0][1], 'last_close': points[-1][1],
            'return_pct': round(100 * (points[-1][1] / points[0][1] - 1), 1),
            'max_daily_change_pct': round(jump[0], 2), 'max_daily_change_date': jump[1],
            'split_events': result.get('events', {}).get('splits', {}),
            'quote_time': datetime.fromtimestamp(info['regularMarketTime'], ET).isoformat(), 'quote_price': number(info.get('regularMarketPrice')), 'url': url, 'retrieved_at_utc': datetime.now(timezone.utc).isoformat(), 'info': info}

def main():
    DATA.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)
    results, errors = [], []
    # yfinance shares an internal session; serialize fundamentals requests.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        tasks = {pool.submit(get_stock, t): t for t in ORIGINAL + CANDIDATES}
        for task in concurrent.futures.as_completed(tasks):
            ticker = tasks[task]
            try:
                r = task.result(); results.append(r)
                print(ticker, r['return_pct'], r['last_close'], r['sector'], 'largest day', r['max_daily_change_pct'], flush=True)
            except Exception as e:
                errors.append({'ticker': ticker, 'error': str(e)})
                print('ERROR', ticker, str(e), flush=True)
    peers = {}
    if (RAW / 'inherited_technology_peer_pb.json').exists():
        peers = json.loads((RAW / 'inherited_technology_peer_pb.json').read_text())
    else:
        for row in csv.DictReader((OLD / 'peer_reference.csv').open()):
            pb, cap = number(row.get('pb_ratio')), number(row.get('mktcap'))
            if row['gsector'] == '45' and pb and pb > 0 and cap and cap >= 10000:
                peers[row['tic'].strip().upper()] = pb
    write(RAW / 'inherited_technology_peer_pb.json', peers)
    fundamentals = {}
    for r in sorted(results, key=lambda x: x['ticker']):
        info = r.pop('info'); t = r['ticker']; pb = number(info.get('priceToBook'))
        book = number(info.get('bookValue'))
        if pb is None or pb <= 0 or book is None or book <= 0:
            pb = None
        vals = [v for k, v in peers.items() if k != t]
        pc = round(100 * sum(v < pb for v in vals) / len(vals)) if pb is not None and r['sector'] == 'Technology' else None
        label = None if pc is None else 'Low' if pc <= 33 else 'High' if pc >= 67 else 'Mid'
        rate = number(info.get('dividendRate'))
        div_y = dividend_yield(info, r['last_close'])
        np = number(info.get('netIncomeToCommon'))
        fundamentals[t] = {'marketcap': round(float(info['marketCap']) / 1e6, 2),
                           'pb_current': None if pb is None else round(pb, 2),
                           'pb_current_pctile': pc, 'pb_yahoo': None if pb is None else round(pb, 2),
                           'pe_yahoo': number(info.get('trailingPE')),
                           'div_y': div_y, 'netprofit': None if np is None else round(np / 1e6),
                           'netprofit_src': 'netIncomeToCommon', 'valuation': label,
                           'sector': r['sector']}
    write(DATA / 'fundamentals.json', fundamentals)
    meta = {'as_of': ASOF.isoformat(), 'retrieved_on': '2026-10-15',
            'source': 'Yahoo Finance via yfinance .info',
            'valuation_method': 'Current Yahoo P/B ranked against inherited WRDS GICS-45 positive-P/B peers with historical marketcap >= $10B; self excluded; Low <=33, High >=67. Peer P/B values NOT refreshed.',
            'n_reference_peers': len(peers),
            'valuation_non_technology': 'No sector percentile or label assigned to non-Technology replacement candidates.',
            'dividend_yield_method': 'Indicated annual dividendRate / frozen closing price * 100 when present; otherwise archived indicated annual rate (see verified_dividend_rates.json) / frozen closing price * 100; positive Yahoo dividendYield only when no verified rate is available. Missing or zero fallback yields retain the previous blank display for non-payers.',
            'reference_benchmark': 'Unchanged inherited historical WRDS reference; no claim of a refreshed peer distribution.'}
    write(DATA / 'fundamentals_meta.json', meta)
    manifest = {'snapshot_id': SNAPSHOT, 'collection_date': '2026-10-15',
                'kind': 'stock', 'wave': 2, 'price_as_of': ASOF.isoformat(),
                'requested_start': START.isoformat(), 'elapsed_calendar_days': 365,
                'price_definition': 'Yahoo daily quote.close (split adjusted, no dividend reinvestment), rounded to cents.',
                'window_note': 'Latest completed US session: 2026-10-14. Requested start and first available close: 2025-10-14. Returns exclude dividends.',
                'assets': sorted(results, key=lambda x: (x['role'], x['ticker'])), 'errors': errors,
                'original_tickers': ORIGINAL, 'candidate_tickers': CANDIDATES,
                'status': 'Day 3 publication; same seven pairs numbered p1 through p7; fourteen stocks.'}
    dates_by_ticker = {r['ticker']: {datetime.fromtimestamp(p[0] / 1000, ET).date().isoformat()
                                   for p in json.loads((DATA / (r['ticker'].lower() + '_365d.json')).read_text())['prices']}
                       for r in results}
    # All these listings use the US trading calendar; compare to the original 18.
    reference_dates = set.union(*(dates_by_ticker[t] for t in ORIGINAL if t in dates_by_ticker))
    manifest['missing_dates_relative_to_original_stocks'] = {
        t: sorted(reference_dates - dates) for t, dates in dates_by_ticker.items() if reference_dates - dates}
    manifest['missing_data_policy'] = 'No interpolation; all fourteen stocks must have identical complete trading-date coverage.'
    manifest['files_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(DATA.glob('*.json')) if p.name != 'manifest.json'}
    manifest['source_files_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(RAW.glob('*.json'))}
    write(DATA / 'manifest.json', manifest)
    print('COMPLETE', len(results), 'ERRORS', errors)
    if any(e['ticker'] in ORIGINAL for e in errors):
        raise RuntimeError('At least one original stock failed; do not publish this snapshot.')

if __name__ == '__main__':
    main()
