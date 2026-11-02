"""Deterministic registration of independently verified historical WTI rounds."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json

from tools.lib.wti_billing import csv_rows, dec, gross_from_fills, round_context, time_at


def canonical_sha256(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _is_wti_swap(row):
    return row.get('underlying') in {'USOIL', 'USOILP'} and row.get('instrument_type') == 'swap'


def prepare_registered_snapshot(original, trades_csv, transfers_csv, manifest, ledger_har):
    """Derive missing legacy receipt fields without changing the supplied snapshot."""
    if not isinstance(original, dict):
        raise ValueError('historical_snapshot_object_required')
    state = deepcopy(original)
    position, actions, lo, hi = round_context(state)
    if not isinstance(ledger_har, bytes) or not isinstance(manifest, dict):
        raise ValueError('historical_registration_requires_raw_ledger_evidence')
    if (manifest.get('trades_sha256') != hashlib.sha256(trades_csv).hexdigest()
            or manifest.get('transfers_sha256') != hashlib.sha256(transfers_csv).hexdigest()
            or not isinstance(manifest.get('raw_ledger_capture'), dict)):
        raise ValueError('historical_registration_source_hash_unverified')

    close = next((action for action in actions if action.get('kind') == 'close'), None)
    if close is None:
        raise ValueError('historical_close_action_missing')
    leg = close['legs']['variational']
    request = leg.get('request') or {}
    result = leg.get('result') or {}
    reconstruction = None
    try:
        from tools.lib.wti_billing import _fill
        _fill(close, 'variational')
    except (KeyError, ValueError, TypeError, ArithmeticError):
        if not (result.get('classification') == 'unknown'
                and result.get('status') == 'unknown'
                and result.get('terminal_confirmed') is False
                and result.get('filled_qty') is None
                and result.get('average_price') is None):
            raise ValueError('historical_receipt_not_reconstructable_unknown')
        submitted = time_at(request.get('submitted_at') or close['intent_at'])
        rows = csv_rows(trades_csv, {'id', 'created_at', 'side', 'underlying',
                                    'instrument_type', 'price', 'qty', 'status'})
        candidates = [row for row in rows
            if _is_wti_swap(row) and row.get('status') == 'confirmed'
            and row.get('side') == request.get('side')
            and dec(row.get('qty')) == dec(request.get('qty'))
            and dec(row.get('price')) > 0
            and lo <= time_at(row.get('created_at')) <= hi
            and -2 <= (time_at(row['created_at']) - submitted).total_seconds() <= 5]
        if len(candidates) != 1 or not candidates[0].get('id'):
            raise ValueError('historical_close_trade_missing_or_ambiguous')
        trade = candidates[0]
        rfq_ids = {str(part.get('native_client_order_id'))
                   for part in (request, result, leg.get('submission_result') or {})
                   if part.get('native_client_order_id')}
        if len(rfq_ids) != 1:
            raise ValueError('historical_close_rfq_missing_or_conflicting')
        leg['result'] = {**result,
            'classification': 'filled', 'status': 'filled', 'terminal_confirmed': True,
            'filled_qty': trade['qty'], 'average_price': trade['price'],
            'native_client_order_id': next(iter(rfq_ids)),
            'trade_ids': [trade['id']], 'trade_ids_confirmed': True,
            'source': 'official_confirmed_trade_csv_legacy_receipt_reconstruction'}
        reconstruction = {
            'action_id': close['action_id'], 'venue': 'variational',
            'original_result_sha256': canonical_sha256(result),
            'rfq_id': next(iter(rfq_ids)), 'trade_id': trade['id'],
            'side': trade['side'], 'qty': trade['qty'], 'price': trade['price'],
            'confirmed_trade_at': trade['created_at'],
            'source': 'official_confirmed_trade_csv_plus_exact_closing_rfq_cashflow',
            'trades_csv_sha256': hashlib.sha256(trades_csv).hexdigest(),
            'raw_ledger_sha256': manifest['raw_ledger_capture'].get('sha256'),
        }

    totals = gross_from_fills(state, verify_persisted=False)
    gross = sum(totals.values())
    stored = position.get('actual_gross_pnl_usd')
    if stored is not None and dec(stored) != gross:
        raise ValueError('historical_gross_conflicts_with_final_fills')
    position['actual_gross_pnl_usd'] = str(gross)
    gross_from_fills(state)

    # This binds the reconstructed final fill to the complete official cashflow
    # window, wallet, company, and exact closing RFQ before it can be archived.
    from tools.lib.wti_billing import variational_billing
    proof = variational_billing(state, trades_csv, transfers_csv, manifest,
                                ledger_har=ledger_har)
    if proof.get('status') != 'verified_from_raw_ledger_and_exports':
        raise ValueError('historical_variational_evidence_not_verified')
    return state, {
        'schema_version': 1, 'registered_for_reporting': True,
        'original_snapshot_sha256': canonical_sha256(original),
        'snapshot_sha256': canonical_sha256(state),
        'reconstruction': reconstruction,
        'verification': {
            'variational_status': proof['status'],
            'raw_ledger_sha256': proof['raw_ledger_sha256'],
            'raw_ledger_company_id': proof['raw_ledger_company_id'],
            'cashflow_ids': proof['cashflow_ids'],
            'gross_usdc': str(gross),
        },
    }


def _action_binding(action):
    keys = ('action_id', 'kind', 'direction', 'intent_at', 'completed_at',
            'outcome', 'market_id', 'multiplier')
    fill_keys = ('classification', 'status', 'terminal_confirmed', 'filled_qty',
                 'average_price', 'order_id', 'native_order_id',
                 'native_client_order_id', 'trade_ids')
    request_keys = ('side', 'qty', 'submitted_at', 'client_order_id',
                    'native_client_order_id', 'native_order_id', 'order_id',
                    'symbol', 'instrument_key', 'instrument_type', 'market_id',
                    'reduce_only', 'venue')
    legs = {}
    for venue in ('variational', 'lighter'):
        part = action.get('legs', {}).get(venue) or {}
        request, result = part.get('request') or {}, part.get('result') or {}
        legs[venue] = {
            'request': {key: request.get(key) for key in request_keys if key in request},
            'result': {key: result.get(key) for key in fill_keys if key in result},
        }
    return {**{key: action.get(key) for key in keys if key in action}, 'legs': legs}


def historical_round_binding(state):
    position, actions, _, _ = round_context(state)
    opened = next((action for action in actions if action.get('kind') == 'open'), None)
    closed = next((action for action in actions if action.get('kind') == 'close'), None)
    if (opened is None or closed is None
            or (opened.get('direction') is not None
                and opened.get('direction') != position.get('direction'))
            or (closed.get('direction') is not None
                and closed.get('direction') != position.get('direction'))
            or opened.get('intent_at') != position.get('opened_at')
            or (closed.get('completed_at') is not None
                and closed.get('completed_at') != position.get('closed_at'))):
        raise ValueError('historical_position_does_not_match_execution_identity')
    for action in actions:
        if action.get('market_id') is not None and action['market_id'] != position.get('market_id'):
            raise ValueError('historical_market_identity_mismatch')
        if action.get('multiplier') is not None and dec(action['multiplier']) != dec(position.get('multiplier')):
            raise ValueError('historical_multiplier_identity_mismatch')
    pkeys = ('entry_execution_id', 'exit_execution_id', 'direction', 'opened_at',
             'closed_at', 'market_id', 'multiplier')
    return canonical_sha256({
        'position': {key: position.get(key) for key in pkeys if key in position},
        'actions': [_action_binding(action) for action in actions],
    })
