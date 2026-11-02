"""Pure billing reconciliation. Never imports a gateway or modifies execution state."""
from __future__ import annotations

import csv
import hashlib
import io
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


def dec(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation) as exc:
        raise ValueError('invalid_decimal') from exc
    if not result.is_finite():
        raise ValueError('nonfinite_decimal')
    return result


def time_at(value: Any, *, naive_utc=False) -> datetime:
    at = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if at.tzinfo is None:
        if not naive_utc:
            raise ValueError('timezone_missing')
        at = at.replace(tzinfo=timezone.utc)
    return at.astimezone(timezone.utc)


def csv_rows(content: bytes, required: set[str]) -> list[dict[str, str]]:
    text = content.decode('utf-8-sig')
    if text.lstrip().startswith('<'):
        raise ValueError('export_is_xml_or_html_not_csv')
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or not required.issubset(reader.fieldnames):
        raise ValueError('csv_schema_mismatch')
    rows = list(reader)
    if any(None in row or any(v is None for v in row.values()) for row in rows):
        raise ValueError('csv_truncated_row')
    return rows


def merge_cash_exports(contents: list[bytes]) -> bytes:
    required = {'id', 'created_at', 'qty', 'asset', 'transfer_type', 'status',
                'underlying', 'instrument_type'}
    records, fields = {}, []
    for content in contents:
        local_ids = set()
        reader = csv.DictReader(io.StringIO(content.decode('utf-8-sig')))
        for field in reader.fieldnames or []:
            if field not in fields:
                fields.append(field)
        for row in csv_rows(content, required):
            key = row['id']
            if key in local_ids:
                raise ValueError('duplicate_cash_id_within_export')
            local_ids.add(key)
            if not key or (key in records and records[key] != row):
                raise ValueError('conflicting_or_missing_cash_export_id')
            records[key] = row
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(sorted(records.values(), key=lambda row: (row['created_at'], row['id'])))
    return output.getvalue().encode()


def _is_var_wti(row):
    # The official CSV uses USOIL; the command API uses USOILP for this swap.
    return row['underlying'] in {'USOIL', 'USOILP'} and row['instrument_type'] == 'swap'


def round_context(state):
    if state.get('asset') != 'WTI':
        raise ValueError('state_not_wti')
    p = state.get('last_closed_position')
    if not isinstance(p, dict) or not p.get('exit_execution_id'):
        raise ValueError('closed_position_missing')
    entries = [x['entry_execution_id'] for x in p.get('layers', []) if x.get('entry_execution_id')]
    entries = entries or [p.get('entry_execution_id')]
    ids = list(dict.fromkeys([*entries, p['exit_execution_id']]))
    actions = {}
    for e in state.get('execution_history', []):
        key = e.get('action_id')
        if key in actions and actions[key] != e:
            raise ValueError('conflicting_execution_id')
        actions[key] = e
    chosen = []
    for key in ids:
        e = actions.get(key)
        if not e or e.get('kind') != ('close' if key == p['exit_execution_id'] else 'open'):
            raise ValueError('execution_identity_missing')
        chosen.append(e)
    starts = [time_at(p['opened_at'])]
    for action in chosen:
        starts.append(time_at(action['intent_at']))
        for leg in (action.get('legs') or {}).values():
            submitted = (leg.get('request') or {}).get('submitted_at')
            if submitted:
                starts.append(time_at(submitted))
    lo = min(starts) - timedelta(seconds=5)
    hi = time_at(p['closed_at']) + timedelta(seconds=5)
    if hi <= lo or dec(p.get('multiplier')) != 1 or p.get('market_id') != 145:
        raise ValueError('round_identity_or_window_invalid')
    if p.get('direction') not in {'long_var_short_lighter', 'short_var_long_lighter'}:
        raise ValueError('round_direction_invalid')
    return p, chosen, lo, hi


def _fill(e, venue):
    leg = e.get('legs', {}).get(venue) or {}
    q, r = leg.get('request') or {}, leg.get('result') or {}
    if (r.get('classification') != 'filled' or r.get('terminal_confirmed') is not True
            or q.get('side') not in {'buy', 'sell'} or dec(r.get('filled_qty')) <= 0
            or dec(r.get('average_price')) <= 0 or dec(r.get('filled_qty')) != dec(q.get('qty'))):
        raise ValueError('final_fill_or_quantity_unverified')
    return q, r


def gross_from_fills(state, *, verify_persisted=True):
    p, actions, _, _ = round_context(state)
    totals = {'variational': Decimal(0), 'lighter': Decimal(0)}
    balances = dict(totals)
    for e in actions:
        for venue in totals:
            q, r = _fill(e, venue)
            expected_buy = (venue == 'variational') == (p['direction'] == 'long_var_short_lighter')
            if e['kind'] == 'close':
                expected_buy = not expected_buy
            if (q['side'] == 'buy') != expected_buy:
                raise ValueError('fill_side_does_not_match_round_direction')
            side = Decimal(1) if q['side'] == 'sell' else Decimal(-1)
            qty, price = dec(r['filled_qty']), dec(r['average_price'])
            totals[venue] += side * qty * price
            balances[venue] += side * qty
    if any(x != 0 for x in balances.values()):
        raise ValueError('roundtrip_quantities_not_flat')
    gross = sum(totals.values())
    if verify_persisted and gross != dec(p.get('actual_gross_pnl_usd')):
        raise ValueError('persisted_gross_does_not_match_final_fills')
    return totals


def lighter_billing(state, trades: bytes, funding: bytes, raw_funding: dict,
                    raw_trades: dict, *, account_index: int):
    p, actions, lo, hi = round_context(state)
    trade_rows = csv_rows(trades, {'Market', 'Side', 'Date', 'Size', 'Price', 'Fee', 'Trade ID', 'Role', 'Type'})
    fund_rows = csv_rows(funding, {'Market', 'Side', 'Date', 'Position Size', 'Payment'})
    by_id = {}
    for row in trade_rows:
        key = row['Trade ID']
        if not key or key in by_id:
            raise ValueError('duplicate_or_missing_export_trade_id')
        if row['Market'] != 'WTI' or row['Type'] != 'trade' or not lo <= time_at(row['Date'], naive_utc=True) <= hi:
            raise ValueError('export_trade_outside_scope')
        by_id[key] = row
    matched, fee_total, proofs, exposure_events = set(), Decimal(0), [], []
    for action in actions:
        q, stored = _fill(action, 'lighter')
        raw = raw_trades.get(action['action_id'])
        if not isinstance(raw, dict) or raw.get('code') != 200 or not isinstance(raw.get('trades'), list):
            raise ValueError('raw_trade_page_missing')
        records = raw['trades']
        if not 0 < len(records) < 100 or raw.get('next_cursor'):
            raise ValueError('raw_trade_pagination_incomplete')
        qty, quote = Decimal(0), Decimal(0)
        for record in records:
            tid = str(record.get('trade_id'))
            row = by_id.get(tid)
            prefix = 'ask' if q['side'] == 'sell' else 'bid'
            oid = str(stored.get('native_order_id') or stored.get('order_id') or '')
            if (row is None or tid in matched or str(record.get(prefix + '_id')) != oid
                    or type(record.get('is_maker_ask')) is not bool
                    or record.get(prefix + '_account_id') != account_index or record.get('market_id') != 145
                    or row['Role'] != ('Maker' if bool(record.get('is_maker_ask')) == (q['side'] == 'sell') else 'Taker')
                    or ('Short' not in row['Side'] if p['direction'] == 'long_var_short_lighter' else 'Long' not in row['Side'])
                    or not row['Side'].startswith('Close' if action['kind'] == 'close' else 'Open')
                    or dec(row['Size']) != dec(record.get('size')) or dec(row['Price']) != dec(record.get('price'))):
                raise ValueError('export_trade_identity_mismatch')
            raw_at = datetime.fromtimestamp(int(record['timestamp']) / (1000 if int(record['timestamp']) > 100_000_000_000 else 1), timezone.utc)
            if abs((raw_at - time_at(row['Date'], naive_utc=True)).total_seconds()) >= 1:
                raise ValueError('export_trade_time_mismatch')
            if not lo <= raw_at <= hi:
                raise ValueError('raw_trade_outside_round_window')
            exposure_events.append((raw_at, dec(row['Size']) * (1 if q['side'] == 'buy' else -1)))
            fee = dec(row['Fee'])
            currency = row.get('Fee Currency') or row.get('Currency')
            if fee != 0 and currency not in {'USDC', 'USD'}:
                raise ValueError('nonzero_fee_currency_unverified')
            matched.add(tid)
            qty += dec(row['Size'])
            quote += dec(row['Size']) * dec(row['Price'])
            fee_total += fee
            proofs.append({'action_id': action['action_id'], 'trade_id': tid,
                           'native_order_id': oid, 'fee_usdc': str(fee),
                           'source': 'authenticated_lighter_csv_plus_exact_order_trade',
                           'zero_fee_unit_independent': fee == 0})
        if qty != dec(stored['filled_qty']) or abs(quote - qty * dec(stored['average_price'])) > Decimal('0.000001'):
            raise ValueError('export_trade_fill_coverage_incomplete')
    if matched != set(by_id):
        raise ValueError('unallocated_export_trades')
    if raw_funding.get('code') != 200 or raw_funding.get('next_cursor') or raw_funding.get('cursor'):
        raise ValueError('funding_pagination_or_response_unverified')
    payments = raw_funding.get('position_fundings')
    if not isinstance(payments, list) or len(payments) >= 100:
        raise ValueError('funding_page_incomplete')
    observed, seen = [], set()
    for item in payments:
        key = item.get('funding_id')
        at = datetime.fromtimestamp(int(item['timestamp']), timezone.utc)
        if key is None or key in seen or item.get('market_id') != 145 or not lo <= at <= hi:
            raise ValueError('funding_identity_or_time_mismatch')
        if any(abs((trade_at - at).total_seconds()) < 1 for trade_at, _ in exposure_events):
            raise ValueError('funding_fill_boundary_order_unverified')
        exposure = sum((qty for trade_at, qty in exposure_events if trade_at < at), Decimal(0))
        if (exposure == 0 or abs(exposure) != dec(item.get('position_size'))
                or item.get('position_side') != ('short' if exposure < 0 else 'long')):
            raise ValueError('funding_position_does_not_match_fill_timeline')
        seen.add(key)
        observed.append((at, str(item.get('position_side')), dec(item.get('position_size')), dec(item.get('change'))))
    exported = [(time_at(r['Date'], naive_utc=True), r['Side'], dec(r['Position Size']), dec(r['Payment'])) for r in fund_rows]
    if any(r['Market'] != 'WTI' for r in fund_rows) or sorted(exported) != sorted(observed):
        raise ValueError('funding_csv_api_mismatch')
    return {'status': 'verified', 'fee_usdc': str(fee_total),
            'funding_usdc': str(sum((x[3] for x in observed), Decimal(0))),
            'funding_ids': sorted(seen), 'funding_count': len(seen), 'trade_proofs': proofs,
            'source_hashes': {'trades_csv': hashlib.sha256(trades).hexdigest(),
                             'funding_csv': hashlib.sha256(funding).hexdigest()},
            'cost_budget_modified': False}


def variational_billing(state, trades: bytes | None, transfers: bytes | None, manifest: dict | None,
                        *, ledger_har: bytes | None = None):
    if trades is None or transfers is None or not manifest:
        return {'status': 'needs_exports', 'blockers': ['variational_trades_csv_transfers_csv_and_coverage_manifest_required']}
    p, actions, lo, hi = round_context(state)
    m = manifest
    covered = set(m.get('covered_cashflow_types') or {'funding', 'fee', 'realized_pnl'})
    partial = m.get('complete') is False and covered == {'funding', 'realized_pnl'}
    if (m.get('venue') != 'variational_omni' or not m.get('account_scope')
            or m.get('timezone') != 'UTC' or m.get('settlement_asset') != 'USDC'
            or m.get('cashflow_sign_convention') != 'signed_account_delta'
            or (m.get('complete') is not True and not partial)
            or (m.get('complete') is True and covered != {'funding', 'fee', 'realized_pnl'})
            or m.get('all_pages_exported') is not True
            or time_at(m.get('start_at')) > lo or time_at(m.get('end_at')) < hi
            or m.get('trades_sha256') != hashlib.sha256(trades).hexdigest()
            or m.get('transfers_sha256') != hashlib.sha256(transfers).hexdigest()):
        raise ValueError('variational_export_coverage_or_hash_unverified')
    ts = csv_rows(trades, {'id', 'created_at', 'side', 'underlying', 'instrument_type', 'price', 'qty', 'status'})
    cash = csv_rows(transfers, {'id', 'created_at', 'qty', 'asset', 'transfer_type', 'status', 'underlying', 'instrument_type'})
    if len(ts) >= 10000 or len(cash) >= 10000:
        raise ValueError('variational_export_row_limit_may_be_truncated')
    for rows in (ts, cash):
        ids = [r['id'] for r in rows]
        if any(not x for x in ids) or len(ids) != len(set(ids)):
            raise ValueError('variational_duplicate_or_missing_ledger_id')
    wanted = [r for r in ts if _is_var_wti(r)
              and lo <= time_at(r['created_at']) <= hi]
    used = set()
    for action in actions:
        q, fill = _fill(action, 'variational')
        submitted = time_at(q.get('submitted_at') or action['intent_at'])
        hits = [r for r in wanted if r['status'] == 'confirmed' and r['side'] == q['side']
                and dec(r['qty']) == dec(fill['filled_qty']) and dec(r['price']) == dec(fill['average_price'])
                and -2 <= (time_at(r['created_at']) - submitted).total_seconds() <= 5]
        if len(hits) != 1 or hits[0]['id'] in used:
            raise ValueError('variational_trade_match_missing_or_ambiguous')
        used.add(hits[0]['id'])
    if len(used) != len(wanted):
        raise ValueError('variational_unallocated_trades_in_round')
    fees, funding, realized = Decimal(0), Decimal(0), Decimal(0)
    proof_ids, ignored = [], []
    for r in cash:
        if not lo <= time_at(r['created_at']) <= hi:
            continue
        if r['transfer_type'] == 'fee' and r.get('fee_type') in {'deposit', 'withdrawal'}:
            ignored.append(r['id'])
            continue
        if r['transfer_type'] not in {'funding', 'fee', 'realized_pnl'}:
            continue
        if not r['underlying'] or not r['instrument_type']:
            raise ValueError('unallocated_variational_cashflow')
        if not _is_var_wti(r):
            continue
        if r['status'] != 'confirmed' or r['asset'] != 'USDC':
            raise ValueError('variational_cashflow_status_or_currency_unverified')
        amount = dec(r['qty'])
        if r['transfer_type'] == 'fee':
            if r.get('fee_type') != 'trade':
                raise ValueError('variational_fee_type_unverified')
            fees -= amount
        elif r['transfer_type'] == 'funding':
            funding += amount
        else:
            realized += amount
        proof_ids.append(r['id'])
    if abs(realized - gross_from_fills(state)['variational']) > Decimal('0.000001'):
        raise ValueError('variational_realized_ledger_does_not_match_fills')
    proof = {'status': 'partial_exports' if partial else 'verified_from_operator_attested_exports',
            'fee_usdc': None if partial else str(fees),
            'funding_usdc': str(funding), 'realized_pnl_usdc': str(realized),
            'matched_trade_ids': sorted(used), 'cashflow_ids': proof_ids,
            'excluded_deposit_withdrawal_fee_ids': ignored,
            'coverage_basis': 'operator_attested_complete_export_with_matching_hashes',
            'zero_is_from_complete_cashflow_window_not_missing_api_fields': not partial,
            'covered_cashflow_types': sorted(covered),
            'missing_cashflow_types': ['fee'] if partial else [],
            'export_failures': m.get('export_failures') or [],
            'cost_budget_modified': False}
    if ledger_har is not None:
        from tools.lib.wti_variational_ledger import verified_raw_ledger
        return verified_raw_ledger(state, ledger_har, m.get('raw_ledger_capture'), proof, cash)
    return proof


def net_audit(state, lighter, variational):
    p, _, _, _ = round_context(state)
    totals = gross_from_fills(state)
    result = {'mode': 'read_only_billing', 'orders_attempted': False, 'state_modified': False,
              'cost_budget_modified': False, 'live_authorization': False,
              'entry_execution_id': p['entry_execution_id'], 'exit_execution_id': p['exit_execution_id'],
              'gross_pnl_usdc': str(sum(totals.values())), 'gross_by_venue': {k: str(v) for k, v in totals.items()},
              'lighter': lighter, 'variational': variational, 'actual_net_pnl_usdc': None,
              'slippage_already_in_gross_not_subtracted_again': True}
    if lighter.get('status') == 'verified' and variational.get('status') in {
            'verified_from_operator_attested_exports', 'verified_from_raw_ledger_and_exports'}:
        fees = dec(lighter['fee_usdc']) + dec(variational['fee_usdc'])
        funding = dec(lighter['funding_usdc']) + dec(variational['funding_usdc'])
        result.update(status='verified_from_exports_and_final_fills', fees_usdc=str(fees),
                      funding_usdc=str(funding), actual_net_pnl_usdc=str(sum(totals.values()) - fees + funding))
    else:
        result['status'] = 'partial_evidence'
        result['blockers'] = [name for name, proof in (('lighter_billing', lighter), ('variational_ledger', variational))
                              if not str(proof.get('status', '')).startswith('verified')]
        if lighter.get('status') == 'verified' and variational.get('status') == 'partial_exports':
            result['blockers'] = ['variational_fee_export_scope_incomplete']
            result['net_before_unverified_variational_fees_usdc'] = str(
                sum(totals.values()) - dec(lighter['fee_usdc'])
                + dec(lighter['funding_usdc']) + dec(variational['funding_usdc']))
    return result
