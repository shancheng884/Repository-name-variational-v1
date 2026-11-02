"""Bounded, nonblocking decision evidence. Never a trading state or retry source."""
from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime, timezone
from decimal import Decimal
from functools import wraps
import itertools
import json
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
from queue import Queue, Empty, Full
import re
import threading
import time

_TRACE = ContextVar('wti_decision_trace', default=None)


def pick(value, keys):
    return {k: value[k] for k in keys if isinstance(value, dict) and k in value}


def sample_evidence(row):
    return pick(row, ('sample_id', 'signal_sample_id', 'sample_kind', 'logged_at',
        'sample_quality', 'sample_quality_reasons', 'long_edge_bps', 'short_edge_bps',
        'variational_qty', 'lighter_qty', 'var_bid', 'var_ask', 'lighter_buy_price',
        'lighter_sell_price', 'var_quote_age_seconds', 'lighter_book_age_seconds',
        'lighter_pair_drift_bps', 'execution_refresh_attempt',
        'execution_refresh_quote_timestamp_advanced', 'execution_refresh_deadline_exceeded',
        'session_id', 'session_closing_at', 'collector_commit', 'run_id'))


def crossing_evidence(row, thresholds):
    result = {}
    for direction, field in (('long_var_short_lighter','long_edge_bps'),
                              ('short_var_long_lighter','short_edge_bps')):
        try:
            edge = Decimal(str(row[field]))
            crossed = [i+1 for i,v in enumerate(thresholds[direction])
                       if v is not None and edge.is_finite() and edge >= Decimal(str(v))]
            result[direction] = {'edge_bps':str(edge),'crossed_tiers':crossed}
        except (KeyError,TypeError,ValueError,ArithmeticError):
            result[direction] = {'crossing_unverified':True}
    return result


def state_evidence(state):
    state = state if isinstance(state, dict) else {}
    pending = state.get('pending_action')
    pending = pending if isinstance(pending, dict) else {}
    history = state.get('execution_history')
    latest = history[-1] if isinstance(history,list) and history and isinstance(history[-1],dict) else {}
    return {'status': state.get('status'), 'open_position': bool(state.get('open_position')),
        'pending_action_id': pending.get('action_id'), 'pending_kind': pending.get('kind'),
        'pending_execution_evidence':execution_evidence({'pending_action':pending},None),
        'latest_execution':pick(latest,('action_id','kind','outcome','completed_at')),
        'manual_review_reason': state.get('manual_review_reason'),
        'account_reconciliation': pick(state.get('last_account_reconciliation'),
            ('observed_at', 'verified', 'market_id', 'runtime_pid', 'runtime_run_id',
             'variational_qty', 'lighter_qty', 'active_order_count'))}


def gate_evidence(runtime):
    return {k: getattr(runtime, k, None) for k in ('entry_authorized',
        'entry_gate_block_reason', 'entry_authorization_permanently_revoked',
        'startup_reconciled', 'manage_existing_only', 'account_read_recovery_required')} | {
        'account_risk': pick(getattr(runtime, 'last_risk', {}),
            ('risk_action', 'risk_reason', 'risk_data_complete', 'observed_at'))}


def execution_marker(state):
    history = state.get('execution_history') if isinstance(state, dict) else None
    return history[-1].get('action_id') if isinstance(history, list) and history and isinstance(history[-1], dict) else None


def execution_evidence(state, previous):
    state = state if isinstance(state, dict) else {}
    actions = []
    history = state.get('execution_history')
    for action in reversed((history if isinstance(history, list) else [])[-8:]):
        if not isinstance(action, dict): continue
        if action.get('action_id') == previous:
            break
        actions.append(action)
    pending = state.get('pending_action')
    pending = pending if isinstance(pending, dict) else {}
    if pending and not any(x.get('action_id') == pending.get('action_id') for x in actions):
        actions.append(pending)
    result = []
    for action in actions:
        legs = {}
        source_legs = action.get('legs')
        for venue, leg in (source_legs if isinstance(source_legs,dict) else {}).items():
            if not isinstance(leg, dict): continue
            legs[venue] = {
                **pick(leg, ('submission_started', 'submitted_at', 'response_received_at', 'lookup_checked_at')),
                'request': pick(leg.get('request'), ('client_order_id', 'side', 'qty', 'submitted_at', 'intent_at')),
                'result': pick(leg.get('result'), ('classification', 'terminal_confirmed', 'status',
                    'filled_qty', 'average_price', 'native_order_id', 'native_client_order_id', 'lookup_reason'))}
        result.append({**pick(action, ('action_id', 'kind', 'outcome', 'intent_at', 'completed_at')), 'legs': legs})
    return result


def safe_error(exc):
    try: text = str(exc)
    except Exception: text = ''
    match = re.search(r'(?:HTTP |http_status_)(\d{3})', text)
    status = getattr(exc,'http_status',None)
    result = {'error_type': type(exc).__name__, 'http_status': status if isinstance(status,int) else int(match[1]) if match else None}
    for key in ('phase','retryable','diagnostic_code','operation'):
        if hasattr(exc,key):result[key]=getattr(exc,key)
    return result


def clean(value, depth=0):
    if depth > 9:
        return 'truncated'
    if isinstance(value, dict):
        return {str(k)[:80]: clean(v, depth+1) for k,v in list(value.items())[:80]
                if str(k).lower() not in {'authorization','auth','api_key','access_key','jwt'}
                and not any(word in str(k).lower() for word in
                           ('secret', 'password', 'token', 'private_key', 'cookie', 'headers'))}
    if isinstance(value, (list, tuple)):
        return [clean(v, depth+1) for v in value[:80]]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return re.sub(r'(?i)(bearer\s+|token=|password=)\S+', r'\1[redacted]', str(value))[:400]


class AuditWriter:
    def __init__(self, root: Path, *, name: str, run_id: str, queue_size=2048,
                 max_bytes=10*1024*1024, backups=7, start=True):
        self.root, self.name, self.run_id = Path(root), name, str(run_id)
        self.session_id = f'{name}:{os.getpid()}:{time.time_ns()}'
        self.queue = Queue(maxsize=queue_size)
        self.sequence, self.cycles = itertools.count(1), itertools.count(1)
        self.dropped = self.write_errors = self.written = 0
        self.max_bytes, self.backups = max_bytes, backups
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.in_flight = False
        self.thread = threading.Thread(target=self._run, name=f'wti-{name}-audit', daemon=True)
        if start:
            self.thread.start()

    def emit(self, stage, data, *, trace=None, evidence='runtime_exact'):
        try:
            event = {'schema_version': 1, 'event_id': f'{self.session_id}:{next(self.sequence)}',
                'observed_at': datetime.now(timezone.utc).isoformat(), 'run_id': self.run_id,
                'trace_id': trace if trace is not None else _TRACE.get(), 'stage': str(stage),
                'evidence_level': evidence, 'data': clean(data), 'orders_attempted_by_auditor': False}
            self.queue.put_nowait(event)
            return True
        except Full:
            self.dropped += 1
        except Exception:
            self.dropped += 1
        return False

    def stats(self):
        return {'session_id': self.session_id, 'run_id': self.run_id,
            'alive': self.thread.is_alive(), 'queued': self.queue.qsize(),
            'written': self.written, 'dropped': self.dropped, 'write_errors': self.write_errors,
            'in_flight':self.in_flight,
            'coverage_complete': self.dropped == 0 and self.write_errors == 0 and self.queue.empty() and not self.in_flight,
            'durability':'best_effort_flush_no_trading_thread_fsync'}

    def _health(self, final=False):
        payload = {**self.stats(), 'updated_at': datetime.now(timezone.utc).isoformat(),
                   'pid': os.getpid(), 'status': 'stopped' if final else 'degraded' if self.write_errors else 'running'}
        if final: payload['alive'] = False
        tmp = self.root / f'{self.name}.health.tmp'
        tmp.write_text(json.dumps(payload, sort_keys=True)+'\n')
        tmp.replace(self.root / f'{self.name}.health.json')

    def _run(self):
        handler = lock = None
        owned = False
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            lock = (self.root/f'{self.name}.writer.lock').open('a+b')
            if os.name != 'nt':
                import fcntl
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            owned = True
            handler = RotatingFileHandler(self.root/f'{self.name}.jsonl', maxBytes=self.max_bytes,
                                           backupCount=self.backups, encoding='utf-8', delay=True)
            self.ready.set()
            last_health = 0.
            while not self.stop.is_set() or not self.queue.empty():
                try:
                    item = self.queue.get(timeout=.25)
                except Empty:
                    item = None
                if item is not None:
                    self.in_flight = True
                    try:
                        # Explicit writes propagate IO errors to audit counters,
                        # never through logging.handleError into strategy code.
                        line = json.dumps(item, ensure_ascii=True, sort_keys=True)+'\n'
                        if len(line.encode()) > min(32768, self.max_bytes):
                            self.dropped += 1
                            raise ValueError('audit_event_too_large')
                        if handler.stream is None:
                            handler.stream = handler._open()
                            os.chmod(self.root/f'{self.name}.jsonl', 0o600)
                        if handler.stream.tell()+len(line.encode()) >= self.max_bytes:
                            handler.doRollover()
                            if handler.stream is None: handler.stream = handler._open()
                        handler.stream.write(line); handler.stream.flush()
                        self.written += 1
                    except Exception:
                        self.write_errors += 1
                    finally:
                        self.in_flight = False
                        self.queue.task_done()
                if time.monotonic()-last_health >= 1:
                    try: self._health()
                    except Exception: self.write_errors += 1
                    last_health = time.monotonic()
        except Exception:
            self.write_errors += 1
        finally:
            self.ready.set()
            try:
                if owned: self._health(final=True)
            except Exception: pass
            if handler: handler.close()
            if lock: lock.close()

    def close(self, timeout=1):
        self.stop.set()
        if self.thread.is_alive(): self.thread.join(timeout=timeout)


def emit(runtime, stage, **data):
    try:
        writer = getattr(runtime, 'decision_audit', None)
        if writer is not None:
            trace = _TRACE.get()
            if trace is None:
                sample_id = (data.get('sample') or {}).get('sample_id')
                if sample_id: trace = f'{getattr(runtime,"run_id","unknown")}:ingest:{sample_id}'
            writer.emit(stage, data, trace=trace)
    except Exception:
        pass


def audited_cycle(kind):
    def decorate(fn):
        @wraps(fn)
        async def wrapper(self, row, *args, **kwargs):
            writer = getattr(self, 'decision_audit', None)
            if writer is None:
                return await fn(self, row, *args, **kwargs)
            try: trace = f'{self.run_id}:{row.get("sample_id")}:{next(writer.cycles)}'
            except Exception: return await fn(self, row, *args, **kwargs)
            token = _TRACE.set(trace)
            result = None
            failure = None
            emit(self, 'cycle_start', kind=kind, sample=sample_evidence(row), gate=gate_evidence(self))
            try:
                result = await fn(self, row, *args, **kwargs)
                return result
            except BaseException as exc:
                failure = safe_error(exc)
                raise
            finally:
                emit(self, 'cycle_end', kind=kind, error=failure, state=state_evidence(result),
                     gate=gate_evidence(self), action=pick(getattr(self, 'last_action', {}),
                         ('action', 'reason', 'direction', 'edge_bps', 'status', 'tier', 'risk_action')))
                _TRACE.reset(token)
        return wrapper
    return decorate
