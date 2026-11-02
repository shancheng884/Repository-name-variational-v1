"""Bounded fee-only proof builder. No orders, live ledger writes or funding queries."""
from __future__ import annotations
import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import sys
import uuid
import re
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.wti_exit_cost import fingerprint, var_fee_capture, lighter_fee_capture, cost_policy
from tools.lib.runtime_files import write_json_atomic
from tools.lib.wti_readonly_adapter import VariationalForwarderClient
from tools.wti_account_probe import _load_env, _close_client
from tools.wti_billing_audit import get_json, download_export


def funding_debits(actions, reply, now):
    if (reply.get('code') != 200 or reply.get('next_cursor') or reply.get('cursor')
            or not isinstance(reply.get('position_fundings'), list)
            or len(reply['position_fundings']) >= 100):
        raise ValueError('funding_debit_page_unverified')
    events = []
    for action in actions:
        leg = action['legs']['lighter']
        at = datetime.fromisoformat(leg['response_received_at'])
        qty = D(leg['result']['filled_qty'])*(1 if leg['request']['side'] == 'buy' else -1)
        events.append((at, qty))
    total, seen = D(0), set()
    for row in reply['position_fundings']:
        at = datetime.fromtimestamp(int(row['timestamp']), timezone.utc)
        if (row.get('funding_id') is None or row['funding_id'] in seen or row.get('market_id') != 145
                or not min(x[0] for x in events) <= at <= now
                or any(abs((at-x[0]).total_seconds()) < 1 for x in events)):
            raise ValueError('funding_debit_identity_or_boundary_unverified')
        exposure = sum((qty for t, qty in events if t < at), D(0))
        if (not exposure or D(str(row.get('position_size'))) != abs(exposure)
                or row.get('position_side') != ('long' if exposure > 0 else 'short')):
            raise ValueError('funding_debit_position_mismatch')
        change = D(str(row['change']))
        if not change.is_finite():
            raise ValueError('funding_debit_invalid')
        total += max(D(0), -change)
        seen.add(row['funding_id'])
    return total


async def build(state, capture, *, cached=None):
    from lighter.signer_client import SignerClient
    now = datetime.now(timezone.utc)
    p = state['open_position']
    ids = {x['entry_execution_id'] for x in p.get('layers', [p])}
    actions = [e for e in state['execution_history'] if e['action_id'] in ids]
    addresses = {h['value'].lower() for entry in capture['log']['entries']
                 if '/api/transfers?' in entry['request']['url']
                 for h in entry['request'].get('headers', [])
                 if h['name'].lower() == 'vr-connected-address'}
    if len(addresses) != 1 or not re.fullmatch(r'0x[0-9a-f]{40}', next(iter(addresses), '')):
        raise ValueError('var_fee_capture_wallet_unverified')
    address = next(iter(addresses))
    async with VariationalForwarderClient('ws://127.0.0.1:8768') as forwarder:
        response = await forwarder.request({'type': 'VAR_API_ORDERS', 'account': address,
            'requestId': uuid.uuid4().hex, 'status': 'cleared',
            'limit': 100, 'offset': 0, 'orderBy': 'created_at', 'order': 'desc', 'requestTimeoutMs': 10000})
    result = response.get('result') or {}
    if response.get('ok') is not True or result.get('ok') is not True or result.get('addressUsed') is not True:
        raise ValueError('var_fee_wallet_binding_missing')
    rows = result.get('orders')
    while isinstance(rows, dict):
        rows = rows.get('result', rows.get('orders', rows.get('data')))
    for action in actions:
        leg = action['legs']['variational']; fill = leg['result']
        matches = [x for x in rows or [] if x.get('rfq_id') == fill['native_client_order_id']
                   and x.get('order_id') == fill['native_order_id']]
        if (len(matches) != 1 or matches[0].get('status') != 'cleared'
                or D(matches[0]['qty']) != D(fill['filled_qty'])
                or D(matches[0]['price']) != D(fill['average_price'])
                or matches[0].get('side') != leg['request']['side']):
            raise ValueError('var_fee_order_binding_unverified')
    var = var_fee_capture(state, capture, expected_address=address, now=now)
    var.update(raw=capture, expected_address=address)
    account, key, secret = _load_env()
    client = SignerClient(url='https://mainnet.zklighter.elliot.ai', account_index=account,
                          api_private_keys={key: secret})
    try:
        token, error = client.create_auth_token_with_expiry(api_key_index=key)
        if error or not token:
            raise ValueError('lighter_fee_read_auth_unavailable')
        lo = min(datetime.fromisoformat(e['intent_at']).timestamp() for e in actions)-5
        raw = None
        if cached and cached.get('position_fingerprint') == fingerprint(state):
            candidate = ((cached.get('fee_proofs') or {}).get('lighter') or {}).get('raw')
            if candidate:
                lighter_fee_capture(state, candidate, account_index=account)
                raw = candidate
        if raw is None:
            export = await asyncio.to_thread(get_json, 'export', {'type': 'trade', 'account_index': account,
                'market_id': 145, 'start_timestamp': int(lo*1000), 'end_timestamp': int(now.timestamp()*1000)}, token)
            csv_content = await asyncio.to_thread(download_export, export['data_url'])
            raw = {'trades_csv': csv_content.decode('utf-8-sig'), 'trades': {}}
            for action in actions:
                oid = action['legs']['lighter']['result']['native_order_id']
                raw['trades'][action['action_id']] = await asyncio.to_thread(get_json, 'trades',
                    {'account_index': account, 'market_id': 145, 'order_index': int(oid),
                     'sort_by': 'timestamp', 'sort_dir': 'desc', 'limit': 100}, token)
        metadata = await asyncio.to_thread(get_json, 'orderBookDetails', {'market_id': 145}, token)
        markets = metadata.get('order_book_details') or []
        if (len(markets) != 1 or markets[0].get('market_id') != 145
                or D(str(markets[0].get('taker_fee', 'NaN'))) != 0
                or markets[0].get('is_taker_fee_enabled') is not True):
            raise ValueError('future_lighter_fee_policy_unverified')
    finally:
        await _close_client(client)
    lighter = lighter_fee_capture(state, raw, account_index=account)
    lighter.update(raw=raw, account_index=account)
    policy_url = 'https://docs.variational.io/omni/trading/fees'
    policy = await asyncio.to_thread(requests.get, policy_url, timeout=(3, 10), allow_redirects=False)
    if policy.status_code != 200 or 'There are no trading fees on Omni' not in policy.text:
        raise ValueError('future_var_zero_fee_policy_unverified')
    expiry = min(datetime.fromisoformat(var['observed_at'])+timedelta(minutes=15), now+timedelta(minutes=15))
    proof = {'schema_version': 2, 'position_fingerprint': fingerprint(state),
        'observed_at': now.isoformat(), 'expires_at': expiry.isoformat(),
        'fee_evidence_verified': True, 'entry_fees_usd': str(D(var['fee_usd'])+D(lighter['fee_usd'])),
        'exit_fee_reserve_usd': '0', 'funding_excluded_from_exit_cost': True,
        'fee_proofs': {'variational': var, 'lighter': lighter},
        'exit_fee_policy': {'variational_url': policy_url, 'variational_sha256': hashlib.sha256(policy.content).hexdigest(),
                            'lighter_metadata': metadata},
        'funding_queries_attempted': False}
    return proof, cost_policy(state, proof, now=now)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-path', type=Path, default=ROOT/'log/wti_live_state.json')
    parser.add_argument('--var-fees-har', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    state = json.loads(args.state_path.read_text())
    proof, (applied, audit) = asyncio.run(build(state, json.loads(args.var_fees_har.read_text())))
    if audit['mode'] != 'real_cost_plus_remaining_reserve':
        raise ValueError(audit['reason'])
    args.output_dir.mkdir(exist_ok=False)
    write_json_atomic(args.output_dir/'evidence.json', proof)
    write_json_atomic(args.output_dir/'state.json', state)
    write_json_atomic(args.output_dir/'audit.json', audit)
    print(json.dumps(audit))


if __name__ == '__main__':
    main()
