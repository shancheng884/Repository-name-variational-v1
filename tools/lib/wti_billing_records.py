"""Independent audit archives and at-most-once billing notifications."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from tools.lib.runtime_files import write_json_atomic
from tools.lib.wti_billing import dec, gross_from_fills, round_context
from tools.lib.wti_reporting import closed_rounds
from tools.lib.wti_billing_registration import (
    canonical_sha256, historical_round_binding, prepare_registered_snapshot,
)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def round_fingerprint(state):
    p, actions, _, _ = round_context(state)
    gross_from_fills(state)
    economics = []
    for action in actions:
        legs = {}
        for venue in ('variational', 'lighter'):
            q, r = action['legs'][venue]['request'], action['legs'][venue]['result']
            legs[venue] = {'side': q['side'], 'qty': str(dec(r['filled_qty']).normalize()),
                          'price': str(dec(r['average_price']).normalize()),
                          'order_id': r.get('native_order_id') or r.get('order_id'),
                          'rfq': q.get('native_client_order_id') or r.get('native_client_order_id')}
        economics.append({'id': action['action_id'], 'kind': action['kind'], 'legs': legs})
    return _digest({'exit_id': p['exit_execution_id'], 'direction': p['direction'],
                    'opened_at': p['opened_at'], 'closed_at': p['closed_at'], 'executions': economics})


def _read(path):
    if path.is_symlink() or path.stat().st_size > 10 * 1024 * 1024:
        raise ValueError('billing_archive_path_or_size_invalid')
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('billing_archive_object_required')
    return data


def load_verified_records(directory: Path, live_state: dict):
    """Recompute evidence; never trust a hand-edited net number or live-state flag."""
    if not directory.exists():
        return {}
    from tools.wti_billing_audit import build_audit
    records = {}
    live_rounds = {r.get('exit_execution_id'): r for r in closed_rounds(live_state)}
    for folder in sorted(directory.iterdir()):
        if folder.is_symlink():
            raise ValueError('billing_archive_symlink_forbidden')
        if not folder.is_dir():
            continue
        report = _read(folder / 'audit.json')
        if report.get('status') != 'verified_from_exports_and_final_fills':
            continue
        snapshot = _read(folder / 'execution-snapshot.json')
        lighter = _read(folder / 'lighter-sources.json')
        var = _read(folder / 'variational-sources.json')
        sources = {**lighter, 'trades_csv': lighter['trades_csv'].encode(),
                   'funding_csv': lighter['funding_csv'].encode()}
        from tools.lib.wti_billing import merge_cash_exports
        cash_sources = [raw.encode() for raw in var['cash_sources_csv']]
        coverage = var['coverage']
        if len(cash_sources) > 1 and coverage.get('cash_export_source_hashes') != [
                hashlib.sha256(raw).hexdigest() for raw in cash_sources]:
            raise ValueError('billing_archive_cash_source_hashes_mismatch')
        merged = merge_cash_exports(cash_sources) if len(cash_sources) > 1 else cash_sources[0]
        if merged != var['merged_cash_csv'].encode():
            raise ValueError('billing_archive_merged_cash_mismatch')
        rebuilt = build_audit(snapshot, sources, var_trades=var['trades_csv'].encode(),
            var_transfers=merged, var_manifest=coverage,
            var_ledger_har=var['ledger_har'].encode() if var.get('ledger_har') is not None else None)
        canonical = lambda r: {k: v for k, v in r.items() if k != 'observed_at'}
        if canonical(rebuilt) != canonical(report):
            raise ValueError('billing_archive_recomputation_mismatch')
        p, _, _, _ = round_context(snapshot)
        key = p['exit_execution_id']
        current = live_rounds.get(key)
        if not current:
            continue
        fingerprint = round_fingerprint(snapshot)
        registration_path = folder / 'registration.json'
        if registration_path.exists():
            registration = _read(registration_path)
            original = _read(folder / 'original-execution-snapshot.json')
            if (registration.get('schema_version') != 1
                    or registration.get('registered_for_reporting') is not True
                    or registration.get('original_snapshot_sha256') != canonical_sha256(original)):
                raise ValueError('billing_archive_registration_identity_mismatch')
            original_var = var['trades_csv'].encode()
            expected, expected_registration = prepare_registered_snapshot(
                original, original_var, merged, coverage,
                var['ledger_har'].encode() if var.get('ledger_har') is not None else None)
            if (expected != snapshot or expected_registration != registration
                    or registration.get('snapshot_sha256') != canonical_sha256(snapshot)):
                raise ValueError('billing_archive_historical_reconstruction_mismatch')
            original_position, original_actions, _, _ = round_context(original)
            live_actions = {}
            for action in live_state.get('execution_history') or []:
                action_id = action.get('action_id')
                if action_id in live_actions and live_actions[action_id] != action:
                    raise ValueError('billing_archive_live_execution_identity_conflict')
                live_actions[action_id] = action
            selected_live = [live_actions.get(action['action_id']) for action in original_actions]
            if any(action is None for action in selected_live):
                raise ValueError('billing_archive_live_execution_missing')
            live_binding_state = {'asset': 'WTI',
                'last_closed_position': original_position,
                'execution_history': selected_live}
            if (current.get('closed_at') != original_position['closed_at']
                    or historical_round_binding(original) != historical_round_binding(live_binding_state)
                    or (current.get('actual_gross_pnl_usd') is not None
                        and dec(current['actual_gross_pnl_usd']) != dec(report['gross_pnl_usdc']))):
                raise ValueError('billing_archive_live_round_mismatch')
        else:
            candidate = {**live_state, 'last_closed_position': p}
            if (current.get('closed_at') != p['closed_at']
                    or dec(current.get('actual_gross_pnl_usd')) != dec(report['gross_pnl_usdc'])
                    or round_fingerprint(candidate) != fingerprint):
                raise ValueError('billing_archive_live_round_mismatch')
        record = {'exit_execution_id': key, 'closed_at': p['closed_at'],
                  'round_fingerprint': fingerprint, 'gross_usdc': report['gross_pnl_usdc'],
                  'fees_usdc': report['fees_usdc'], 'funding_usdc': report['funding_usdc'],
                  'net_usdc': report['actual_net_pnl_usdc']}
        if key in records and records[key] != record:
            raise ValueError('billing_archive_conflicting_round')
        records[key] = record
    return records


def billing_rounds(state, records):
    rows = [dict(r) for r in closed_rounds(state)]
    for row in rows:
        proof = records.get(row.get('exit_execution_id'))
        if proof is None:
            continue
        row.update(actual_gross_pnl_usd=proof['gross_usdc'], actual_fees_usd=proof['fees_usdc'], funding_cashflow_usd=proof['funding_usdc'],
                   funding_cashflow_confirmed=True, actual_net_pnl_usd=proof['net_usdc'],
                   actual_net_pnl_status='confirmed_from_independent_billing')
    return rows


@contextmanager
def _delivery_lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as handle:
        if os.name == 'nt':
            import msvcrt
            handle.seek(0, 2)
            if handle.tell() == 0:
                handle.write(b'0'); handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if os.name == 'nt':
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def deliver_once(path: Path, key: str, message: str, notifier, *, strict_message=True):
    with _delivery_lock(path.with_suffix(path.suffix + '.lock')):
        memory = _read(path) if path.exists() else {'schema_version': 1, 'events': {}}
        if memory.get('schema_version') != 1 or not isinstance(memory.get('events'), dict):
            raise ValueError('billing_delivery_state_invalid_preserve_for_review')
        events = memory['events']
        event = events.get(key)
        message_hash = hashlib.sha256(message.encode()).hexdigest()
        if event:
            if strict_message and event.get('message_sha256') != message_hash:
                raise ValueError('billing_notified_record_changed_requires_review')
            if event.get('status') == 'sending':
                event['status'] = 'delivery_unknown'
                write_json_atomic(path, memory)
            if event.get('status') not in {'pending', 'sent', 'delivery_unknown'}:
                raise ValueError('billing_delivery_status_invalid')
            if event['status'] != 'pending':
                return event['status']
        else:
            event = events[key] = {'message_sha256': message_hash}
        event['message_sha256'] = message_hash
        event['status'] = 'sending'
        write_json_atomic(path, memory)
        try:
            ok, detail = notifier.send_now(message)
        except Exception as exc:
            ok, detail = False, 'uncertain:' + type(exc).__name__
        event.update(status='sent' if ok else 'pending' if detail in {
            'http_status_429', 'missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID'
        } else 'delivery_unknown', detail=detail, updated_at=datetime.now(timezone.utc).isoformat())
        write_json_atomic(path, memory)
        return event['status']


def billing_message(record):
    from tools.lib.pnl_baseline import BEIJING_TIMEZONE
    from tools.lib.wti_billing import time_at
    at = time_at(record['closed_at']).astimezone(BEIJING_TIMEZONE).isoformat()
    return '\n'.join(['[Var/Lighter] WTI 账单核实（非新成交）',
        f"平仓时间（北京）：{at}", f"平仓动作ID：{record['exit_execution_id']}",
        f"实际毛收益：{record['gross_usdc']} USDC", f"双边实际手续费：{record['fees_usdc']} USDC",
        f"资金费净额：{record['funding_usdc']} USDC", f"实际净收益：{record['net_usdc']} USDC",
        '原始流水、最终成交及分页已核对；未改变交易门槛和风控预算'])


def publish_billing_updates(records, path, notifier):
    return {key: deliver_once(path, 'billing:' + key, billing_message(record), notifier)
            for key, record in sorted(records.items())}


def deliver_billing_correction_once(path: Path, key: str, message: str, records, notifier):
    """Send one consolidated correction and suppress its per-round duplicates."""
    with _delivery_lock(path.with_suffix(path.suffix + '.lock')):
        memory = _read(path) if path.exists() else {'schema_version': 1, 'events': {}}
        if memory.get('schema_version') != 1 or not isinstance(memory.get('events'), dict):
            raise ValueError('billing_delivery_state_invalid_preserve_for_review')
        events = memory['events']
        digest = hashlib.sha256(message.encode()).hexdigest()
        event = events.get(key)
        if event:
            if event.get('message_sha256') != digest:
                raise ValueError('billing_correction_changed_requires_review')
            if event.get('status') == 'sending':
                event.update(status='delivery_unknown', detail='interrupted_while_sending')
                status = 'delivery_unknown'
            elif event.get('status') in {'sent', 'delivery_unknown'}:
                status = event['status']
            elif event.get('status') == 'pending':
                status = None
            else:
                raise ValueError('billing_delivery_status_invalid')
        else:
            event = events[key] = {'message_sha256': digest}
            status = None
        if status is None:
            event.update(message_sha256=digest, status='sending')
            write_json_atomic(path, memory)
            try:
                ok, detail = notifier.send_now(message)
            except Exception as exc:
                ok, detail = False, 'uncertain:' + type(exc).__name__
            status = ('sent' if ok else 'pending' if detail in {
                'http_status_429', 'missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID'
            } else 'delivery_unknown')
            event.update(status=status, detail=detail,
                         updated_at=datetime.now(timezone.utc).isoformat())
        # The consolidated report covers each round. Even an ambiguous send is
        # terminal for individual notices, preventing a later timer from spamming.
        covered_status = 'sent' if status == 'sent' else 'delivery_unknown'
        for exit_id, record in records.items():
            billing_key = 'billing:' + exit_id
            existing = events.get(billing_key)
            if existing and existing.get('status') in {'sent', 'delivery_unknown'}:
                continue
            events[billing_key] = {
                'message_sha256': hashlib.sha256(billing_message(record).encode()).hexdigest(),
                'status': covered_status,
                'detail': 'included_in_daily_correction:' + key,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }
        write_json_atomic(path, memory)
        return status
