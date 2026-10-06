#!/usr/bin/env python3
"""Refresh only crypto stimuli in existing main pre-study and Wave 1 QSFs.

Preserves survey text, choices, response coding, completion routes and flow.
Reads an immutable snapshot, updates the embedded bundle and fallback URLs.
Never commits, pushes, uploads or activates surveys.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path

CDN = 'https://cdn.jsdelivr.net/gh/pagrass/pilot1-asset-data@main/'
NAMES = {'pre': 'Model_Spillovers_Structural_Similarity_Main_PreStudy.qsf',
         'w1': 'Model_Spillovers_Structural_Similarity_Main_Wave_1.qsf'}
ASSETS = {'pre': ['eth', 'xmr', 'bnb'],
          'w1': ['eth', 'xmr', 'bnb', 'btc', 'hype', 'xrp']}

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def transform(obj, fn):
    if isinstance(obj, str): return fn(obj)
    if isinstance(obj, list): return [transform(v, fn) for v in obj]
    if isinstance(obj, dict): return {k: transform(v, fn) for k, v in obj.items()}
    return obj

def flow_nodes(nodes):
    for n in nodes:
        yield n
        yield from flow_nodes(n.get('Flow', []))

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--snapshot-id', required=True)
    p.add_argument('--source-dir', type=Path)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    folder = args.repo / 'structuralsimilarity_main/crypto/snapshots' / args.snapshot_id
    manifest = json.loads((folder / 'manifest.json').read_text())
    rows = {r['slug']: r for r in manifest['assets']}
    starts = {r['first_day'] for r in rows.values()}
    ends = {r['last_day'] for r in rows.values()}
    assert len(starts) == len(ends) == 1
    start, end = starts.pop(), ends.pop()
    source_dir = args.source_dir or args.repo / 'structuralsimilarity_main/surveys'
    args.output.mkdir(parents=True, exist_ok=True)
    reports = {}
    for tag, name in NAMES.items():
        source = source_dir / name
        raw = source.read_bytes()
        before = json.loads(raw)
        prefix = CDN + 'structuralsimilarity_main/crypto/snapshots/'
        d = transform(before, lambda s: re.sub(re.escape(prefix) + r'[^/]+/', prefix + args.snapshot_id + '/', s))
        d['SurveyEntry']['SurveyDescription'] = 'Main study; frozen crypto snapshot ' + args.snapshot_id
        flow = next(e['Payload']['Flow'] for e in d['SurveyElements'] if e['Element'] == 'FL')
        metadata = {'stimulus_snapshot_id': args.snapshot_id,
                    'crypto_window_start': start, 'crypto_window_end': end}
        found = set()
        for n in flow_nodes(flow):
            for f in n.get('EmbeddedData', []):
                if f.get('Field') in metadata:
                    f['Value'] = metadata[f['Field']]
                    found.add(f['Field'])
        assert found == set(metadata)
        bundle, hashes = {}, {}
        for slug in ASSETS[tag]:
            filename = slug + '_365d.json'
            data = (folder / filename).read_bytes()
            hashes[filename] = digest(data)
            assert hashes[filename] == manifest['files_sha256'][filename]
            bundle[prefix + args.snapshot_id + '/' + filename] = json.loads(data)
        options = next(e['Payload'] for e in d['SurveyElements'] if e['Element'] == 'SO')
        header, count = re.subn(r'<script>window\.__STRUCTURAL_MAIN_DATA__=.*?;</script>', '', options['Header'], count=1, flags=re.S)
        assert count == 1
        options['Header'] = '<script>window.__STRUCTURAL_MAIN_DATA__=' + json.dumps(bundle, separators=(',', ':')).replace('</', '<\\/') + ';</script>' + header
        old_q = {e['PrimaryAttribute']: e['Payload'] for e in before['SurveyElements'] if e['Element'] == 'SQ'}
        new_q = {e['PrimaryAttribute']: e['Payload'] for e in d['SurveyElements'] if e['Element'] == 'SQ'}
        assert old_q.keys() == new_q.keys()
        for qid in old_q:
            expected = transform(old_q[qid], lambda s: re.sub(re.escape(prefix) + r'[^/]+/', prefix + args.snapshot_id + '/', s))
            assert expected == new_q[qid], qid
        output = args.output / name
        output.write_text(json.dumps(d, indent=2, ensure_ascii=False) + '\n')
        reports[tag] = dict(source=str(source), source_sha256=digest(raw), output=str(output),
                            output_sha256=digest(output.read_bytes()), snapshot_id=args.snapshot_id,
                            crypto_window_start=start, crypto_window_end=end,
                            embedded_data_sha256=hashes,
                            asset_returns={s.upper(): rows[s]['return_pct'] for s in ASSETS[tag]},
                            question_text_controls_and_routes_preserved=True)
    (args.output / 'crypto_refresh_manifest.json').write_text(json.dumps(reports, indent=2) + '\n')
    print(json.dumps(reports, indent=2))

if __name__ == '__main__':
    main()
