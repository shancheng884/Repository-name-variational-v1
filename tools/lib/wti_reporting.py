"""Read-only trade reporting with a durable, independent notification outbox."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from tools.lib.runtime_files import read_json, write_json_atomic


def number(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def closed_rounds(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Rebuild legacy rounds, retaining unknown fills rather than treating them as zero."""
    result: dict[str, dict[str, Any]] = dict(state.get('closed_positions') or {})
    entries: list[dict[str, Any]] = []
    history = {x.get('action_id'):x for x in state.get('execution_history', [])}
    seen: set[str] = set()
    for execution in state.get('execution_history') or []:
        key = execution.get('action_id')
        if not key or key in seen:
            continue
        seen.add(key)
        if execution.get('outcome') not in {'filled', 'positions_flat_confirmed'}:
            continue
        if execution.get('kind') == 'open':
            entries.append(execution)
            continue
        if execution.get('kind') != 'close':
            continue
        authoritative = result.get(key) or {}
        scoped_ids = {x.get('entry_execution_id') for x in authoritative.get('layers') or []}
        chosen = [history[x] for x in scoped_ids if x in history] if scoped_ids else entries
        row = {'exit_execution_id': key, 'closed_at': execution.get('completed_at'),
               'exit_reason': execution.get('exit_reason'), 'actual_gross_pnl_usd': None,
               'actual_fees_usd': None, 'actual_net_pnl_usd': None,
               'actual_net_pnl_status': 'evidence_incomplete', 'layers_count': len(entries),
               'strategy_evidence_excluded': execution.get('strategy_evidence_excluded') is True,
               'strategy_evidence_exclusion_reason': execution.get(
                   'strategy_evidence_exclusion_reason'
               )}
        gross, fees, volume = Decimal(0), Decimal(0), Decimal(0)
        fills_ok, fees_ok = bool(chosen), bool(chosen)
        direction = execution.get('direction')
        for venue in ('variational', 'lighter'):
            entry_qty = Decimal(0)
            for item in [*chosen, execution]:
                leg = (item.get('legs') or {}).get(venue) or {}
                fill = leg.get('result') or {}
                request = leg.get('request') or {}
                qty, price = number(fill.get('filled_qty')), number(fill.get('average_price'))
                fee = number(fill.get('fee_usd'))
                if (fill.get('classification') != 'filled' or fill.get('terminal_confirmed') is not True
                        or qty is None or price is None or qty <= 0 or price <= 0):
                    fills_ok = False
                    continue
                multiplier = number(item.get('multiplier')) if venue == 'lighter' else Decimal(1)
                if multiplier is None or multiplier <= 0:
                    fills_ok = False
                    continue
                sign = Decimal(1) if request.get('side') == 'sell' else Decimal(-1)
                expected = ('buy' if venue == 'variational' else 'sell')
                if direction == 'short_var_long_lighter':
                    expected = 'sell' if venue == 'variational' else 'buy'
                if item is execution:
                    expected = 'sell' if expected == 'buy' else 'buy'
                if request.get('side') != expected or item.get('direction') != direction:
                    fills_ok = False
                gross += sign * qty * price * multiplier
                volume += qty * price * multiplier
                if item is execution:
                    if qty != entry_qty:
                        fills_ok = False
                else:
                    entry_qty += qty
                if fill.get('fee_confirmed') is not True or fee is None:
                    fees_ok = False
                else:
                    fees += fee
        if fills_ok:
            row['actual_gross_pnl_usd'] = str(gross)
            row['four_leg_volume_usd'] = str(volume)
        if fills_ok and fees_ok:
            row['actual_fees_usd'] = str(fees)
        result[key] = {**row, **authoritative}
        entries = [e for e in entries if e['action_id'] not in scoped_ids] if scoped_ids else []
    latest = state.get('last_closed_position') or {}
    if latest.get('exit_execution_id'):
        key = latest['exit_execution_id']
        result[key] = {**result.get(key, {}), **latest}
    return list(result.values())


def trade_message(execution: dict[str, Any], state: dict[str, Any], health=None, record=None) -> str:
    from tools.lib.wti_equity_reporting import fixed_trade_message
    return fixed_trade_message(execution, state, health or {}, record or {})


class TradeReporter:
    def __init__(self, path: Path, state: dict[str, Any], notifier: Any, health_path=None,
                 equity_directory=None, snapshot_reader=None, snapshot_refresher=None):
        self.path, self.notifier = path, notifier
        self.health_path = health_path
        self.equity_directory = Path(equity_directory or path.parent/'wti_equity_events')
        self.snapshot_reader = snapshot_reader
        self.snapshot_refresher = snapshot_refresher
        self.outbox = read_json(path)
        if path.exists() and (not self.outbox or not isinstance(self.outbox.get('events'), dict)
                              or not isinstance(self.outbox.get('imported_ids'), list)):
            raise ValueError('wti_notification_outbox_invalid_preserve_for_review')
        if not self.outbox:
            # Deployment must not replay years of already-completed trades.
            self.outbox = {'imported_ids': [x['action_id'] for x in state.get('execution_history', [])],
                           'events': {}}
            write_json_atomic(path, self.outbox)
        for event in self.outbox['events'].values():
            if event.get('status') == 'sending':
                event['status'] = 'delivery_unknown'
        write_json_atomic(path, self.outbox)

    def poll(self, state: dict[str, Any]) -> None:
        events = self.outbox['events']
        health = read_json(self.health_path) if self.health_path else {}
        if self.snapshot_reader is not None:
            from tools.lib.wti_equity_reporting import record_daily_snapshot
            record_daily_snapshot(self.outbox, self.snapshot_reader())
            local_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))
            day = local_now.date().isoformat()
            boundary = (self.outbox.get('daily_capitals') or {}).get(day) or {}
            attempts = self.outbox.setdefault('boundary_snapshot_attempts', {})
            if (local_now.hour == 0 and local_now.minute < 2
                    and not boundary.get('day_open_verified') and self.snapshot_refresher is not None
                    and attempts.get(day, 0) < 2):
                # The cached snapshot was attempt one. At most one additional read,
                # including after a reporter/process restart; no repeated polling.
                attempts[day] = 2
                write_json_atomic(self.path, self.outbox)
                try:
                    self.snapshot_refresher()
                    record_daily_snapshot(self.outbox, self.snapshot_reader())
                except Exception as exc:
                    self.outbox.setdefault('boundary_snapshot_errors', {})[day] = type(exc).__name__
        try:
            observed = datetime.fromisoformat(health['account_snapshot_observed_at'])
            age = (datetime.now(timezone.utc)-observed).total_seconds()
            v,l = number(health['variational_equity_usd']),number(health['lighter_equity_usd'])
            if self.snapshot_reader is None and -5 <= age <= 15 and health.get('risk_data_complete') is True and v is not None and l is not None and min(v,l)>0:
                local = observed.astimezone(timezone(timedelta(hours=8)))
                daily = self.outbox.setdefault('daily_capitals', {})
                daily.setdefault(local.date().isoformat(), {'observed_at':observed.isoformat(),
                    'capital_usd':str(v+l),'day_open_verified':local.hour==0 and local.minute<2})
                self.outbox['latest_equity'] = {'observed_at':observed.isoformat(),'equity_usd':str(v+l)}
        except (ValueError, KeyError, TypeError, ArithmeticError):
            pass
        for execution in state.get('execution_history', []):
            key = execution['action_id']
            if key not in self.outbox['imported_ids'] and key not in events:
                if execution.get('outcome') not in {'filled', 'positions_flat_confirmed'}:
                    continue
                record = read_json(self.equity_directory/(key+'.json'))
                events[key] = {'status': 'pending', 'message': trade_message(execution, state, health, record),
                               'accounting_method':'account_equity_attribution_v1',
                               'attribution_status':(record.get('attribution') or {}).get('status','unverified')}
        write_json_atomic(self.path, self.outbox)
        for event in events.values():
            if event['status'] != 'pending':
                continue
            event['status'] = 'sending'
            write_json_atomic(self.path, self.outbox)
            try:
                ok, detail = self.notifier.send_now(event['message'])
            except Exception as exc:
                ok, detail = False, f'uncertain:{type(exc).__name__}'
            event['detail'] = detail
            event['updated_at'] = datetime.now(timezone.utc).isoformat()
            # Telegram has no idempotency key. Never resend an ambiguous delivery.
            event['status'] = ('sent' if ok else 'pending' if detail in {
                'missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID', 'http_status_429'
            } else 'delivery_unknown')
            write_json_atomic(self.path, self.outbox)

    async def run(self, state_path: Path) -> None:
        while True:
            try:
                await asyncio.to_thread(self.poll, read_json(state_path))
            except (OSError, ValueError, KeyError):
                import logging
                logging.getLogger('wti_live').exception('wti_trade_reporting_failed')
            await asyncio.sleep(10)
