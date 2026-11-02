"""Offline validation of sanitized, authenticated Omni cash-ledger captures."""
from __future__ import annotations

import base64
import hashlib
import json
import re
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

from tools.lib.wti_billing import dec, round_context, time_at


def verified_raw_ledger(state, content: bytes, capture: dict, csv_proof: dict, cash_rows):
    if (not isinstance(capture, dict)
            or capture.get('sha256') != hashlib.sha256(content).hexdigest()
            or capture.get('account_scope_confirmed') is not True
            or not re.fullmatch(r'0x[0-9a-fA-F]{40}', str(capture.get('wallet_address', '')))
            or not capture.get('company_id') or len(content) > 4 * 1024 * 1024):
        raise ValueError('raw_ledger_capture_identity_or_hash_unverified')
    _, actions, lo, hi = round_context(state)
    wallet, company = capture['wallet_address'].lower(), capture['company_id']
    rfqs = set()
    close_rfqs = set()
    for action in actions:
        leg = action['legs']['variational']
        ids = {str(part['native_client_order_id']) for part in (leg['request'], leg['result'])
               if part.get('native_client_order_id')}
        if len(ids) != 1:
            raise ValueError('raw_ledger_execution_rfq_missing')
        rfqs.update(ids)
        if action['kind'] == 'close':
            close_rfqs.update(ids)
    expected = {frozenset(('deposit', 'withdrawal', 'fee')),
                frozenset(('funding',)), frozenset(('realized_pnl',))}
    pages, observed, export_errors = {}, [], []
    entries = json.loads(content)['log']['entries']
    if not isinstance(entries, list) or len(entries) > 100:
        raise ValueError('raw_ledger_capture_unbounded')
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError('raw_ledger_entry_object_required')
        request, response = entry['request'], entry['response']
        if not isinstance(request, dict) or not isinstance(response, dict):
            raise ValueError('raw_ledger_response_object_required')
        url = urlsplit(request['url'])
        if (url.scheme != 'https' or url.hostname != 'omni.variational.io'
                or url.port not in (None, 443) or url.username or url.password or url.fragment):
            raise ValueError('raw_ledger_origin_unverified')
        headers = {}
        for header in request.get('headers', []):
            key = header['name'].lower()
            if key in headers:
                raise ValueError('raw_ledger_duplicate_header')
            headers[key] = header['value']
        if any(h['name'].lower() in {'cookie', 'authorization', 'set-cookie'}
               for h in request.get('headers', []) + response.get('headers', [])):
            raise ValueError('raw_ledger_must_be_sanitized')
        if headers.get('vr-connected-address', '').lower() != wallet:
            raise ValueError('raw_ledger_wallet_mismatch')
        raw = response.get('content') or {}
        body = raw.get('text')
        if raw.get('encoding') == 'base64':
            body = base64.b64decode(body, validate=True).decode('utf-8')
        elif raw.get('encoding') not in (None, ''):
            raise ValueError('raw_ledger_encoding_unsupported')
        data = json.loads(body)
        if not isinstance(data, dict):
            raise ValueError('raw_ledger_body_object_required')
        if url.path.startswith('/api/exports') and (url.query or data.get('download_url')):
            raise ValueError('raw_ledger_signed_download_url_forbidden')
        if url.path.startswith('/api/exports/'):
            if response['status'] == 200 and data.get('status') == 'failed':
                export_errors.append({'id': data.get('id'), 'error': data.get('error')})
            continue
        if url.path == '/api/exports':
            continue
        if url.path != '/api/transfers' or request['method'] != 'GET' or response['status'] != 200:
            raise ValueError('raw_ledger_read_response_unverified')
        query = parse_qs(url.query, keep_blank_values=True)
        allowed = {'order_by', 'order', 'limit', 'offset', 'created_at_gte', 'created_at_lte', 'type'}
        if (set(query) != allowed or any(len(v) != 1 for v in query.values())
                or query['order_by'] != ['created_at'] or query['order'] != ['desc']
                or query['offset'] != ['0'] or not 0 < int(query['limit'][0]) <= 1000
                or time_at(query['created_at_gte'][0]) > lo
                or time_at(query['created_at_lte'][0]) < hi):
            raise ValueError('raw_ledger_query_scope_incomplete')
        kinds = query['type'][0].split(',')
        kind = frozenset(kinds)
        if kind not in expected or len(kinds) != len(kind):
            raise ValueError('raw_ledger_category_unverified')
        at = time_at(entry['startedDateTime'])
        if at < hi:
            raise ValueError('raw_ledger_captured_before_round_closed')
        observed.append(at)
        rows, page = data.get('result'), data.get('pagination')
        # Only an explicitly complete first page is accepted. A missing page is
        # never silently inferred from a CSV, an export error, or a row limit.
        if (not isinstance(rows, list) or not isinstance(page, dict)
                or 'next_page' not in page or page['next_page'] is not None
                or type(page.get('object_count')) is not int
                or page['object_count'] != len(rows) or len(rows) >= int(query['limit'][0])):
            raise ValueError('raw_ledger_pagination_incomplete')
        canonical = json.dumps({'query': query, 'data': data}, sort_keys=True)
        if kind in pages and pages[kind][0] != canonical:
            raise ValueError('raw_ledger_conflicting_repeated_query')
        pages[kind] = (canonical, rows)
    if set(pages) != expected or (max(observed) - min(observed)).total_seconds() > 600:
        raise ValueError('raw_ledger_categories_or_capture_window_incomplete')
    by_id, totals = {}, {k: Decimal(0) for k in ('fee', 'funding', 'realized_pnl')}
    seen = set()
    ignored = []
    for kinds, (_, rows) in pages.items():
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('raw_ledger_row_object_required')
            rid = row.get('id')
            if not rid or rid in seen or row.get('company') != company:
                raise ValueError('raw_ledger_duplicate_id_or_company_mismatch')
            seen.add(rid)
            kind = row.get('transfer_type')
            if kind not in kinds:
                raise ValueError('raw_ledger_response_category_mismatch')
            if not lo <= time_at(row.get('created_at')) <= hi:
                continue
            if kind in {'deposit', 'withdrawal'} or (
                    kind == 'fee' and row.get('fee_type') in {'deposit', 'withdrawal'}):
                ignored.append(rid)
                continue
            instrument = row.get('reference_instrument') or {}
            if not instrument.get('underlying') or not instrument.get('instrument_type'):
                raise ValueError('raw_ledger_unallocated_cashflow')
            if instrument['underlying'] != 'USOILP' or instrument['instrument_type'] != 'swap':
                continue
            if (row.get('status') != 'confirmed' or row.get('asset') != 'USDC'
                    or instrument.get('settlement_asset') != 'USDC'):
                raise ValueError('raw_ledger_cashflow_status_or_currency_unverified')
            if kind == 'fee' and (row.get('fee_type') != 'trade' or row.get('rfq_id') not in rfqs):
                raise ValueError('raw_ledger_trade_fee_identity_unverified')
            if kind == 'realized_pnl' and row.get('rfq_id') not in close_rfqs:
                raise ValueError('raw_ledger_close_rfq_mismatch')
            totals[kind] += dec(row.get('qty'))
            by_id[rid] = row
    covered = set(csv_proof['covered_cashflow_types'])
    selected_csv = {r['id']: r for r in cash_rows if r['id'] in csv_proof['cashflow_ids']}
    selected_raw = {key: r for key, r in by_id.items() if r['transfer_type'] in covered}
    if set(selected_csv) != set(selected_raw):
        raise ValueError('raw_ledger_csv_cashflow_ids_mismatch')
    for key, row in selected_csv.items():
        raw = selected_raw[key]
        if (dec(row['qty']) != dec(raw['qty']) or time_at(row['created_at']) != time_at(raw['created_at'])
                or row['asset'] != raw['asset'] or row['status'] != raw['status']
                or row['transfer_type'] != raw['transfer_type']):
            raise ValueError('raw_ledger_csv_cashflow_mismatch')
    if (abs(totals['realized_pnl'] - dec(csv_proof['realized_pnl_usdc'])) > Decimal('0.000001')
            or totals['funding'] != dec(csv_proof['funding_usdc'])
            or not any(r['transfer_type'] == 'realized_pnl' for r in by_id.values())):
        raise ValueError('raw_ledger_realized_or_funding_mismatch')
    return {**csv_proof, 'status': 'verified_from_raw_ledger_and_exports',
            'fee_usdc': str(-totals['fee']), 'funding_usdc': str(totals['funding']),
            'cashflow_ids': sorted(by_id), 'covered_cashflow_types': ['fee', 'funding', 'realized_pnl'],
            'missing_cashflow_types': [], 'coverage_basis': 'wallet_bound_complete_raw_ledger_plus_matched_csv_and_rfq',
            'zero_is_from_complete_cashflow_window_not_missing_api_fields': True,
            'raw_ledger_sha256': capture['sha256'], 'raw_ledger_company_id': company,
            'raw_ledger_observed_at': max(observed).isoformat(), 'export_task_errors': export_errors,
            'excluded_deposit_withdrawal_fee_ids': sorted(ignored)}
