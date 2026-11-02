"""Diagnostic-only entry eligibility and unchanged-parameter shadow replay.

Nothing in this module writes a trading state or grants order authorization.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Any

from tools.lib.wti_reporting import number
from tools.lib.wti_research import stamp, digest, profiles, threshold_audit


FRESHNESS_REASONS = {
    'wti_current_sample_or_session_not_fresh',
    'wti_market_sample_or_session_unavailable',
    'wti_sample_or_session_unverified',
}


def transport_failures(text: str, *, now: datetime) -> list[dict]:
    """Extract bounded, structured errors without exposing raw response secrets."""
    result = []
    pattern = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) \| WARNING \| '
                         r'wti_execution_refresh_unavailable trigger=([^ ]*) error_type=(\w+) reason=(.*)$')
    for line in text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        try:
            at = datetime.fromisoformat(match[1].replace(',', '.')).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        age = (now - at).total_seconds()
        if age < 0:
            continue
        http = re.search(r'(?:HTTP |http_status_)(\d{3})', match[4])
        close = re.search(r'(?:sent|received) (\d{4})', match[4])
        result.append({'at': at.isoformat(), 'age_seconds': age,
            'error_type': match[3], 'trigger_checks': match[2].split(','),
            'http_status': int(http[1]) if http else None,
            'websocket_close_code': int(close[1]) if close else None,
            'evidence_scope': 'historical_transport_failure_not_current_lock'})
    return result[-10:]


def _age(value, now):
    try:
        return (now - stamp(value)).total_seconds()
    except (ValueError, TypeError):
        return None


def entry_diagnostics(health: dict, rows: list[dict], *, now: datetime) -> dict:
    """Split policy eligibility from quote freshness without promoting raw False."""
    health_age = _age(health.get('updated_at'), now)
    blockers = []
    required = ('startup_reconciled', 'manage_existing_only',
                'entry_authorization_permanently_revoked', 'risk_data_complete',
                'account_read_recovery_required', 'pending_actions_total')
    if any(k not in health for k in required) or health_age is None or not -1 <= health_age <= 15:
        blockers.append('health_unverified_or_stale')
    if health.get('status') != 'running':
        blockers.append('runtime_not_running')
    if health.get('execution_mode') != 'live_bounded_gradient_pilot':
        blockers.append('gradient_execution_mode_unverified')
    for field in ('manage_existing_only', 'entry_authorization_permanently_revoked',
                  'account_read_recovery_required'):
        if health.get(field) is True:
            blockers.append(field)
    if health.get('startup_reconciled') is not True:
        blockers.append('startup_not_reconciled')
    if health.get('risk_data_complete') is not True or health.get('account_risk_action') not in {'normal', 'warning'}:
        blockers.append('account_risk_unavailable_or_blocked')
    if health.get('pending_actions_total') != 0:
        blockers.append('pending_action_requires_reconciliation')
    if health.get('market_session') != 'open':
        blockers.append('session_not_open')
    for field in ('manual_review_reason', 'runtime_cycle_error', 'runtime_error', 'sample_gap_event'):
        if health.get(field):
            blockers.append(field)
    gate = health.get('entry_gate_block_reason')
    if gate and gate not in FRESHNESS_REASONS:
        blockers.append(str(gate))
    risk_reason = health.get('entry_block_reason') or health.get('risk_reason')
    if health.get('risk_action') not in {'normal', 'warning'} and risk_reason not in FRESHNESS_REASONS:
        blockers.append(str(risk_reason or 'risk_action_unverified'))
    pilot = health.get('gradient_pilot') or {}
    if pilot.get('status') not in {'available', 'in_position'}:
        blockers.append('gradient_round_not_available')

    d = health.get('market_sample_freshness_diagnostics') or {}
    quote_age = number(d.get('quote_age_effective_seconds'))
    book_age = number(d.get('book_age_effective_seconds'))
    extra = Decimal(str(max(0, health_age or 0)))
    quote_age = quote_age + extra if quote_age is not None else None
    book_age = book_age + extra if book_age is not None else None
    checks = d.get('checks') or {}
    other_ok = bool(checks) and all(v is True for k, v in checks.items()
                                  if k not in {'quote_age_effective', 'book_age_effective'})
    fresh = bool(other_ok and quote_age is not None and book_age is not None
                 and -1 <= quote_age <= 2 and 0 <= book_age <= 2)
    eligible = not blockers
    # This is an observation, never a replacement for the live order gate.
    until_age = _age(health.get('entry_authorization_valid_until'), now)
    submit = bool(eligible and fresh and health.get('wti_entry_authorized') is True
                  and until_age is not None and until_age <= 0)
    phase = ('strategy_blocked' if blockers else 'waiting_fresh_quote' if not fresh
             else 'authorized_waiting_signal' if submit else 'live_authorization_unavailable')
    bursts = []
    for r in rows:
        age = _age(r.get('logged_at'), now)
        if r.get('sample_kind') != 'burst' or age is None or not 0 <= age <= 900:
            continue
        failures = list(r.get('sample_quality_reasons') or [])
        for key, low in (('var_quote_age_seconds', -1), ('lighter_book_age_seconds', 0)):
            value = number(r.get(key))
            if value is None or not low <= value <= 2:
                failures.append(key + '_outside_2s')
        bursts.append({k: r.get(k) for k in ('sample_id', 'logged_at', 'signal_sample_id',
            'sample_quality', 'var_quote_age_seconds', 'lighter_book_age_seconds')} | {
                'failed_checks': sorted(set(failures)),
                'evidence_scope': 'capture_only_not_current_authorization'})
    latest = max((r for r in rows if r.get('sample_kind', 'baseline') == 'baseline'
                  and _age(r.get('logged_at'), now) is not None and _age(r.get('logged_at'), now) >= 0),
                 key=lambda r: stamp(r['logged_at']), default={})
    directions = {}
    for direction, field in (('long_var_short_lighter', 'long_edge_bps'),
                             ('short_var_long_lighter', 'short_edge_bps')):
        thresholds = ((health.get('gradient_threshold_profiles') or {}).get(direction) or {}).get('thresholds_bps')
        edge = number(latest.get(field))
        first = number(thresholds[0]) if isinstance(thresholds, list) and thresholds else None
        directions[direction] = {'edge_bps': None if edge is None else str(edge),
            'thresholds_bps': thresholds,
            'below_first_tier_bps': str(first - edge) if first is not None and edge is not None else None,
            'signal_threshold_reached': None if first is None or edge is None else edge >= first}
    if submit and any(v['signal_threshold_reached'] is True for v in directions.values()):
        phase = 'authorized_signal_requires_execution_checks'
    raw_error = str(health.get('execution_refresh_error') or '')
    http_error = re.search(r'(?:HTTP |http_status_)(\d{3})', raw_error)
    error_type = re.match(r'^(\w+):', raw_error)
    return {'schema_version': 1, 'mode': 'read_only_diagnostic', 'as_of': now.isoformat(),
        'source_health_at': health.get('updated_at'), 'source_run_id': health.get('run_id'),
        'strategy_entry_eligible': eligible, 'strategy_blockers': sorted(set(blockers)),
        'quote_fresh_now': fresh, 'entry_phase': phase,
        'raw_live_entry_authorized': health.get('wti_entry_authorized'),
        'observed_live_gate_open': submit, 'diagnostic_grants_authorization': False,
        'quote_age_effective_seconds': str(quote_age), 'book_age_effective_seconds': str(book_age),
        'quote_age_limit_seconds': 2, 'signal_at': latest.get('logged_at'),
        'signal_age_seconds': _age(latest.get('logged_at'), now), 'directions': directions,
        'refresh': {'last_status': health.get('execution_refresh_status'),
            'last_attempt_at': health.get('execution_refresh_at'),
            'last_attempt_age_seconds': _age(health.get('execution_refresh_at'), now),
            'last_transport_error': ({'error_type': error_type[1] if error_type else 'unknown',
                                      'http_status': int(http_error[1]) if http_error else None}
                                     if raw_error else None),
            'source_failed_checks': d.get('failed_checks'), 'recent_bursts': bursts[-20:],
            'recent_failure_counts': dict(Counter(x for b in bursts for x in b['failed_checks']))},
        'orders_attempted': False, 'state_modified': False}


def same_parameter_shadow(rows, *, health, config, as_of):
    """Warm prior-only thresholds; replay last completed session at unchanged risk settings."""
    from tools.lib.wti_replay import ReplayConfig, replay_wti_gradient
    clean, audit = threshold_audit(rows, as_of=as_of, config=config)
    complete = [r for r in clean if stamp(r['session_closing_at']) <= as_of]
    base = {'mode': 'shadow_only', 'live_authorization': False, 'orders_attempted': False,
            'state_modified': False, 'eligible_for_live': False, 'input_sha256': digest(rows),
            'live_config_snapshot': config, 'threshold_audit': audit,
            'actual_pnl_usd': None, 'risk_limits': {'quote_seconds': 2, 'gap_seconds': 120},
            'limitations': ['historical_account_health_and_equity_path_unavailable',
                            'quotes_do_not_prove_fills_fees_or_profitability',
                            'no_lower_threshold_counterfactual_is_live_approved']}
    if not complete:
        return {**base, 'status': 'no_completed_session'}
    session = complete[-1]['session_id']
    start = min(stamp(r['logged_at']) for r in clean if r['session_id'] == session)
    training = [r for r in rows if stamp(r['logged_at']) < start
                and r.get('sample_kind', 'baseline') == 'baseline']
    test = [r for r in rows if start <= stamp(r['logged_at']) < as_of
            and r.get('session_id') == session]
    equities = [number(health.get(k)) for k in ('variational_equity_usd', 'lighter_equity_usd')]
    if any(x is None or x <= 0 for x in equities) or health.get('risk_data_complete') is not True:
        return {**base, 'status': 'capacity_snapshot_unverified'}
    equity = min(equities)
    floors = [Decimal(x) for x in config['tier_thresholds_bps']]
    expected = [floors[0], floors[1], floors[1] + Decimal('.25'),
                floors[1] + Decimal('.50'), floors[1] + Decimal('.75')]
    if floors != expected or Decimal(str(config.get('max_equity_multiple', '5'))) != 5:
        return {**base, 'status': 'unsupported_live_parameter_mismatch'}
    replay_config = ReplayConfig(entry_threshold_bps=Decimal(config['entry_threshold_bps']),
        exit_target_bps=Decimal(config['exit_target_bps']), roundtrip_cost_bps=Decimal(config['roundtrip_cost_bps']),
        max_hold_seconds=float(config['max_hold_seconds']), max_sample_gap_seconds=120,
        entry_cutoff_seconds=float(config['entry_cutoff_seconds']),
        max_slippage_bps=Decimal(str(config['max_slippage_bps'])),
        max_order_notional_usd=Decimal(config['max_notional_usd']))
    replay = replay_wti_gradient(test, config=replay_config, second_tier_threshold_bps=floors[1],
        layer_notional_usd=Decimal(config['layer_notional_usd']), aggregate_max_notional_usd=equity * 5,
        max_layers=5, equity_usd=equity, expected_market_id=config['market_id'],
        expected_multiplier=Decimal(config['multiplier']),
        max_completed_roundtrips=int(config.get('max_completed_roundtrips', 1)),
        elastic_capacity_enabled=config.get('elastic_capacity_enabled', False) is True,
        initial_threshold_history=training)
    # Count signal opportunities independently of portfolio fills, using only prior data.
    history, cached, at_cache = list(training), None, None
    counts = {d: Counter() for d in ('long_var_short_lighter', 'short_var_long_lighter')}
    clean_ids = {r['sample_id'] for r in clean}
    counted_ids = set()
    for row in sorted(test, key=lambda r: stamp(r['logged_at'])):
        if row.get('sample_kind', 'baseline') != 'baseline':
            continue
        at = stamp(row['logged_at'])
        if cached is None or (at - at_cache).total_seconds() >= 300:
            cached, at_cache = profiles(history, at, config), at
        if row.get('sample_id') in clean_ids and row['sample_id'] not in counted_ids:
            counted_ids.add(row['sample_id'])
            for direction, field in (('long_var_short_lighter', 'long_edge_bps'),
                                     ('short_var_long_lighter', 'short_edge_bps')):
                edge, threshold = number(row.get(field)), cached[direction].thresholds_bps[0]
                counts[direction]['valid_samples'] += 1
                counts[direction]['configured_floor_crossings'] += int(edge >= floors[0])
                counts[direction]['adaptive_first_tier_crossings'] += int(threshold is not None and edge >= threshold)
        history.append(row)
    return {**base, 'status': 'quote_path_complete_not_profitability_proven',
        'holdout_session_id': session, 'holdout_start': start.isoformat(),
        'prior_only_training_sha256': digest(training),
        'capacity_scenario': {'basis': 'frozen_current_smaller_venue_equity_not_historical',
                              'equity_usd': str(equity), 'observed_at': health.get('account_snapshot_observed_at')},
        'opportunities': {d: dict(v) for d, v in counts.items()}, 'same_parameter_replay': asdict(replay),
        'opportunity_scope': 'signal_crossings_not_account_or_execution_authorized_entries',
        'conservatism_conclusion': 'opportunity_filter_measured_profitability_not_identified'}
