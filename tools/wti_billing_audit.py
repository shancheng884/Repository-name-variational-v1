#!/usr/bin/env python3
"""Standalone account billing audit. Never submits orders or writes live state."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.wti_billing import round_context, lighter_billing, variational_billing, net_audit, merge_cash_exports

BASE_URL = 'https://mainnet.zklighter.elliot.ai'


def get_json(path, params, token):
    response = requests.get(BASE_URL + '/api/v1/' + path, params=params,
                            headers={'authorization': token}, timeout=(3, 10))
    if response.status_code != 200:
        raise ValueError(f'lighter_read_http_{response.status_code}')
    data = response.json()
    if not isinstance(data, dict) or data.get('code') != 200:
        raise ValueError('lighter_read_response_unverified')
    return data


def download_export(url, *, getter=requests.get, sleeper=time.sleep):
    parsed = urlsplit(url)
    host = parsed.hostname or ''
    if (parsed.scheme != 'https' or parsed.username or parsed.password
            or not (host.endswith('.amazonaws.com') or host in {'data.lighter.xyz', 'mainnet.zklighter.elliot.ai'})):
        raise ValueError('export_download_host_not_allowed')
    for attempt in range(4):
        # Never forward an account authorization token to a download host.
        response = getter(url, timeout=(3, 10), allow_redirects=False)
        if response.status_code == 200:
            content = response.content
            if len(content) > 4 * 1024 * 1024:
                raise ValueError('export_too_large_for_bounded_round_audit')
            if content.lstrip().startswith(b'<'):
                raise ValueError('export_download_not_csv')
            return content
        if response.status_code == 404 and attempt < 3 and b'NoSuchKey' in response.content:
            sleeper(2 ** (attempt + 1))
            continue
        raise ValueError(f'export_download_http_{response.status_code}')
    raise ValueError('export_generation_timeout')


async def fetch_lighter_sources(state):
    from lighter.signer_client import SignerClient
    from tools.wti_account_probe import _load_env, _close_client
    p, actions, lo, hi = round_context(state)
    account, key_index, secret = _load_env()
    client = SignerClient(url=BASE_URL, account_index=account, api_private_keys={key_index: secret})
    try:
        token, error = client.create_auth_token_with_expiry(api_key_index=key_index)
        if error or not token:
            raise ValueError('billing_read_auth_unavailable')
        exports = {}
        for kind in ('trade', 'funding'):
            reply = await asyncio.to_thread(get_json, 'export', {
                'type': kind, 'account_index': account, 'market_id': 145,
                'start_timestamp': int(lo.timestamp() * 1000),
                'end_timestamp': int(hi.timestamp() * 1000)}, token)
            if not isinstance(reply.get('data_url'), str):
                raise ValueError('export_download_link_missing')
            exports[kind] = await asyncio.to_thread(download_export, reply['data_url'])
        raw_funding = await asyncio.to_thread(get_json, 'positionFunding', {
            'account_index': account, 'market_ids': '145', 'limit': 100,
            'start_timestamp': int(lo.timestamp()), 'end_timestamp': int(hi.timestamp())}, token)
        raw_trades = {}
        for action in actions:
            r = action['legs']['lighter']['result']
            oid = r.get('native_order_id') or r.get('order_id')
            if not oid or not str(oid).isdigit():
                raise ValueError('native_lighter_order_id_missing')
            raw_trades[action['action_id']] = await asyncio.to_thread(get_json, 'trades', {
                'account_index': account, 'market_id': 145, 'order_index': int(oid),
                'sort_by': 'timestamp', 'sort_dir': 'desc', 'limit': 100}, token)
        return {'account_index': account, 'trades_csv': exports['trade'],
                'funding_csv': exports['funding'], 'raw_funding': raw_funding,
                'raw_trades': raw_trades, 'window_start': lo.isoformat(), 'window_end': hi.isoformat()}
    finally:
        await _close_client(client)


def build_audit(state, sources, *, var_trades=None, var_transfers=None, var_manifest=None, var_ledger_har=None):
    try:
        lighter = lighter_billing(state, sources['trades_csv'], sources['funding_csv'],
            sources['raw_funding'], sources['raw_trades'], account_index=sources['account_index'])
    except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
        lighter = {'status': 'unverified', 'blockers': [str(exc)]}
    try:
        var = variational_billing(state, var_trades, var_transfers, var_manifest, ledger_har=var_ledger_har)
    except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
        var = {'status': 'unverified', 'blockers': [str(exc)]}
    result = net_audit(state, lighter, var)
    result['execution_snapshot_sha256'] = hashlib.sha256(
        json.dumps(state, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    result['observed_at'] = datetime.now(timezone.utc).isoformat()
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-path', type=Path, default=ROOT / 'log/wti_live_state.json')
    parser.add_argument('--lighter-bundle', type=Path, help='Offline sources bundle produced by this tool')
    parser.add_argument('--variational-trades-csv', type=Path)
    parser.add_argument('--variational-transfers-csv', type=Path, nargs='+')
    parser.add_argument('--variational-coverage-json', type=Path)
    parser.add_argument('--variational-ledger-har', type=Path, help='Sanitized raw ledger, bound by coverage manifest')
    parser.add_argument('--register-for-reporting', action='store_true',
                        help='Reconstruct only evidence-backed legacy fields and create a separately verified archive')
    parser.add_argument('--output-dir', type=Path, help='New artifact directory only; never overwrites existing paths')
    args = parser.parse_args(argv)
    try:
        state = json.loads(args.state_path.read_text(encoding='utf-8'))
        original_state = state
        if args.lighter_bundle:
            raw = json.loads(args.lighter_bundle.read_text(encoding='utf-8'))
            sources = {**raw, 'trades_csv': raw['trades_csv'].encode('utf-8'),
                       'funding_csv': raw['funding_csv'].encode('utf-8')}
        else:
            sources = asyncio.run(fetch_lighter_sources(state))
        var_trades = args.variational_trades_csv.read_bytes() if args.variational_trades_csv else None
        cash_sources = [p.read_bytes() for p in args.variational_transfers_csv or []]
        manifest = json.loads(args.variational_coverage_json.read_text(encoding='utf-8')) if args.variational_coverage_json else None
        if len(cash_sources) > 1:
            hashes = [hashlib.sha256(raw).hexdigest() for raw in cash_sources]
            if not manifest or manifest.get('cash_export_source_hashes') != hashes:
                raise ValueError('cash_export_source_hashes_unverified')
            var_cash = merge_cash_exports(cash_sources)
        else:
            var_cash = cash_sources[0] if cash_sources else None
        ledger_har = args.variational_ledger_har.read_bytes() if args.variational_ledger_har else None
        registration = None
        if args.register_for_reporting:
            if not args.output_dir or var_trades is None or var_cash is None or not manifest or ledger_har is None:
                raise ValueError('historical_registration_requires_complete_archive_inputs')
            from tools.lib.wti_billing_registration import prepare_registered_snapshot
            state, registration = prepare_registered_snapshot(
                original_state, var_trades, var_cash, manifest, ledger_har)
        result = build_audit(state, sources, var_trades=var_trades, var_transfers=var_cash,
                             var_manifest=manifest, var_ledger_har=ledger_har)
        if args.register_for_reporting and result.get('status') != 'verified_from_exports_and_final_fills':
            raise ValueError('historical_registration_audit_not_verified')
        if args.output_dir:
            args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
            bundle = {**sources, 'trades_csv': sources['trades_csv'].decode('utf-8-sig'),
                      'funding_csv': sources['funding_csv'].decode('utf-8-sig')}
            (args.output_dir / 'lighter-sources.json').write_text(json.dumps(bundle, sort_keys=True), encoding='utf-8')
            (args.output_dir / 'audit.json').write_text(json.dumps(result, sort_keys=True, indent=2), encoding='utf-8')
            (args.output_dir / 'execution-snapshot.json').write_text(json.dumps(state, sort_keys=True), encoding='utf-8')
            if registration is not None:
                (args.output_dir / 'original-execution-snapshot.json').write_text(
                    json.dumps(original_state, sort_keys=True), encoding='utf-8')
                (args.output_dir / 'registration.json').write_text(
                    json.dumps(registration, sort_keys=True, indent=2), encoding='utf-8')
            if var_trades is not None and var_cash is not None:
                evidence = {'trades_csv': var_trades.decode('utf-8-sig'),
                            'cash_sources_csv': [raw.decode('utf-8-sig') for raw in cash_sources],
                            'merged_cash_csv': var_cash.decode('utf-8-sig'), 'coverage': manifest,
                            'ledger_har': ledger_har.decode('utf-8') if ledger_har is not None else None}
                (args.output_dir / 'variational-sources.json').write_text(json.dumps(evidence, sort_keys=True), encoding='utf-8')
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
        return 0 if result['status'] == 'verified_from_exports_and_final_fills' else 2
    except Exception as exc:
        # Network errors may contain signed URLs. Do not log exception text.
        print(json.dumps({'status': 'unavailable', 'error_type': type(exc).__name__,
                          'orders_attempted': False, 'state_modified': False,
                          'cost_budget_modified': False, 'live_authorization': False}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
