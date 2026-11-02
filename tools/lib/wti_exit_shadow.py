"""Frozen, purged exit-basis research. Never an order or live-threshold source."""
from __future__ import annotations

from bisect import bisect_left, insort
from collections import Counter, defaultdict, deque
from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal

from tools.lib.wti_research import (
    DIRECTIONS, FIELDS, audit_rows, digest, stamp, profiles, _entry_gap, _exit_gap, _same_size,
)
from tools.lib.wti_replay import ReplayConfig, replay_wti_gradient
from tools.lib.wti_thresholds import _percentile
from tools.lib.wti_reporting import number

HORIZONS = (3600, 10800, 21600)
MIN_TRAIN_SESSIONS = 3
HOLDOUT_SESSIONS = 2
MIN_LABELS = 100
MIN_NONOVERLAPPING_LABELS = 6
QUANTILE = Decimal('95')


def causal_features(clean):
    """Features use prior captures only; session/gap boundaries reset history."""
    features, queues, ordered = {}, {}, {}
    previous = None
    for row in clean:
        at = stamp(row['logged_at'])
        if previous is None or row['session_id'] != previous['session_id'] or (
            at - stamp(previous['logged_at'])).total_seconds() > 120:
            queues = {d: deque() for d in DIRECTIONS}
            ordered = {d: [] for d in DIRECTIONS}
        opened = stamp(row['session_id'].split('|')[0])
        elapsed = (at - opened).total_seconds()
        phase = 'early' if elapsed < 21600 else 'middle' if elapsed < 57600 else 'late'
        by_direction = {}
        for direction in DIRECTIONS:
            queue, values = queues[direction], ordered[direction]
            while queue and (at - queue[0][0]).total_seconds() > 1800:
                _, old = queue.popleft()
                values.pop(bisect_left(values, old))
            gap = _entry_gap(row, direction)
            if len(queue) >= 30 and (at - queue[0][0]).total_seconds() >= 300:
                median = values[len(values) // 2]
                excess = gap - median
                bucket = 'below' if excess < 0 else 'normal' if excess < 5 else 'expanded' if excess < 15 else 'extreme'
                by_direction[direction] = {'phase': phase, 'state': bucket,
                    'prior_median_bps': str(median), 'excess_bps': str(excess)}
            queue.append((at, gap))
            insort(values, gap)
        features[row['sample_id']] = by_direction
        previous = row
    return features


def labels_for(clean, features, *, horizon, end_before):
    """No interpolation, no cross-session/abnormal-gap or different-size labels."""
    parts, part = [], []
    for row in clean:
        if part and (row['session_id'] != part[-1]['session_id'] or
                     (stamp(row['logged_at']) - stamp(part[-1]['logged_at'])).total_seconds() > 120):
            parts.append(part)
            part = []
        part.append(row)
    if part:
        parts.append(part)
    result, rejected = [], Counter()
    for part in parts:
        times = [stamp(r['logged_at']) for r in part]
        for i, entry in enumerate(part):
            if times[i] >= end_before:
                continue
            j = bisect_left(times, times[i] + timedelta(seconds=horizon))
            if j >= len(part):
                rejected['future_quote_missing_in_same_contiguous_session'] += 1
                continue
            end, at = part[j], times[j]
            if at >= end_before:
                rejected['purged_label_reaches_validation_boundary'] += 1
                continue
            if (at - times[i]).total_seconds() > horizon + 20:
                rejected['horizon_endpoint_too_late'] += 1
                continue
            if at >= stamp(entry['session_closing_at']) - timedelta(seconds=1800):
                rejected['label_reaches_session_entry_cutoff'] += 1
                continue
            if not _same_size(entry, end):
                rejected['exact_size_exit_quote_missing'] += 1
                continue
            if not features.get(entry['sample_id']):
                rejected['causal_feature_history_missing'] += 1
            for direction, feature in features.get(entry['sample_id'], {}).items():
                result.append({'sample_id': entry['sample_id'], 'at': entry['logged_at'],
                    'end_at': end['logged_at'], 'session_id': entry['session_id'],
                    'direction': direction, 'horizon_seconds': horizon, **feature,
                    'exit_delta_bps': str(_exit_gap(entry, end, direction) - _entry_gap(entry, direction))})
    return result, dict(rejected)


def nonoverlapping_count(labels):
    ends, count = {}, 0
    for label in sorted(labels, key=lambda x: (x['session_id'], x['at'])):
        key = label['session_id']
        if key not in ends or stamp(label['at']) >= ends[key]:
            count += 1
            ends[key] = stamp(label['end_at'])
    return count


def fit_model(labels):
    """Worst session P95 avoids letting a dense session dominate the bound."""
    cells = defaultdict(list)
    for label in labels:
        direction = label['direction']
        for key in ((direction, label['phase'], label['state']),
                    (direction, '*', label['state']), (direction, '*', '*')):
            cells[key].append(label)
    fitted, rejected = {}, {}
    for key, rows in cells.items():
        sessions = defaultdict(list)
        for row in rows:
            sessions[row['session_id']].append(Decimal(row['exit_delta_bps']))
        support = {'raw_overlapping_labels': len(rows), 'training_sessions': len(sessions),
                   'nonoverlapping_labels': nonoverlapping_count(rows)}
        if len(rows) < MIN_LABELS or len(sessions) < MIN_TRAIN_SESSIONS or support['nonoverlapping_labels'] < MIN_NONOVERLAPPING_LABELS:
            rejected['|'.join(key)] = support
            continue
        bounds = {session: _percentile(values, QUANTILE) for session, values in sessions.items()}
        fitted['|'.join(key)] = {**support,
            'conservative_exit_delta_bps': str(max(bounds.values())),
            'per_session_p95_bps': {k: str(v) for k, v in bounds.items()}}
    return {'method': 'persistent_basis_residual_worst_session_p95_v1',
            'quantile': str(QUANTILE), 'cells': fitted, 'unsupported_cells': rejected,
            'training_labels_sha256': digest(labels), 'hyperparameters_selected_on_holdout': False}


def predict(model, direction, feature):
    if not feature:
        return None
    for key in ((direction, feature['phase'], feature['state']),
                (direction, '*', feature['state']), (direction, '*', '*')):
        name = '|'.join(key)
        if name in model['cells']:
            cell = model['cells'][name]
            return {'exit_delta_bps': Decimal(cell['conservative_exit_delta_bps']), 'cell': name}
    return None


def model_thresholds(row, direction, feature, model, control, config):
    prediction = predict(model, direction, feature)
    if prediction is None or any(x is None for x in control):
        return (None,) * 5
    entry_gap = _entry_gap(row, direction)
    # Convert the entry-midpoint PnL unit to the existing right-price edge unit.
    right = Decimal(row['var_ask'] if direction == DIRECTIONS[0] else row['lighter_buy_price'])
    v = Decimal(row['var_ask'] if direction == DIRECTIONS[0] else row['var_bid'])
    l = Decimal(row['lighter_sell_price'] if direction == DIRECTIONS[0] else row['lighter_buy_price'])
    reserve = Decimal(config['roundtrip_cost_bps']) + Decimal(config['exit_target_bps'])
    required = (entry_gap + prediction['exit_delta_bps'] + reserve) * (v + l) / (2 * right)
    floors = [Decimal(x) for x in config['tier_thresholds_bps']]
    first = max(floors[0], required)
    values = []
    for i in range(5):
        candidates = [floors[i], first + control[i] - control[0]]
        if i:
            candidates.append(values[-1] + control[i] - control[i-1])
        values.append(max(candidates))
    return tuple(values)


def _replay_sessions(rows, clean_ids, training, schedule, config, health, session_ids):
    equity = min(Decimal(health[k]) for k in ('variational_equity_usd', 'lighter_equity_usd'))
    cfg = ReplayConfig(Decimal(config['entry_threshold_bps']), Decimal(config['exit_target_bps']),
        Decimal(config['roundtrip_cost_bps']), float(config['max_hold_seconds']), 120,
        float(config['entry_cutoff_seconds']), Decimal(config['max_slippage_bps']), Decimal(config['max_notional_usd']))
    results = []
    for session in session_ids:
        test = [r if r.get('sample_id') in clean_ids
                else dict(r, sample_quality='invalid') for r in rows if r.get('session_id') == session]
        if not test:
            continue
        start = min(stamp(r['logged_at']) for r in test)
        warm = [r for r in training if stamp(r['logged_at']) < start and r.get('sample_kind', 'baseline') == 'baseline']
        report = replay_wti_gradient(test, config=cfg,
            second_tier_threshold_bps=Decimal(config['tier_thresholds_bps'][1]),
            layer_notional_usd=Decimal(config['layer_notional_usd']),
            aggregate_max_notional_usd=equity * 5, equity_usd=equity, max_layers=5,
            expected_market_id=config['market_id'], expected_multiplier=Decimal(config['multiplier']),
            max_completed_roundtrips=int(config.get('max_completed_roundtrips', 1)),
            initial_threshold_history=warm, research_thresholds_by_sample=schedule,
            elastic_capacity_enabled=config.get('elastic_capacity_enabled', False) is True)
        results.append({'session_id': session, 'report': asdict(report)})
    trades = [t for x in results for t in x['report']['trades']]
    unresolved = sum(x['report']['unresolved_position'] for x in results)
    return {'sessions': results, 'closed_trades': len(trades), 'unresolved_sessions': unresolved,
        'indicative_closed_net_usd': str(sum((Decimal(t['net_pnl_usd']) for t in trades), Decimal(0))),
        'worst_closed_net_bps': str(min((Decimal(t['net_pnl_bps']) for t in trades), default=Decimal(0))) if trades else None,
        'max_closed_hold_seconds': max((t['duration_seconds'] for t in trades), default=None),
        'forced_exit_count': sum(t['exit_reason'] != 'target_reached' for t in trades),
        'actual_pnl_usd': None, 'unclosed_pnl_unknown': unresolved > 0,
        'path_drawdown_verified': False,
        'session_start_scope': 'independent_simulated_flat_starts_not_exchange_account_proofs'}


def exit_shadow(rows, *, config, health, as_of):
    if Decimal(config['roundtrip_cost_bps']) != 10:
        raise ValueError('research_requires_unchanged_10bps_budget')
    if float(config['max_hold_seconds']) != 21600 or float(config['entry_cutoff_seconds']) != 1800:
        raise ValueError('unsupported_live_hold_or_cutoff_do_not_silently_substitute')
    floors = [Decimal(x) for x in config['tier_thresholds_bps']]
    if floors != [floors[0], floors[1], floors[1]+Decimal('.25'), floors[1]+Decimal('.5'), floors[1]+Decimal('.75')]:
        raise ValueError('unsupported_live_tier_floors')
    if Decimal(str(config.get('max_equity_multiple', '5'))) != 5:
        raise ValueError('unsupported_live_equity_multiple')
    equities = [number(health.get(k)) for k in ('variational_equity_usd', 'lighter_equity_usd')]
    if health.get('risk_data_complete') is not True or any(x is None or x <= 0 for x in equities):
        raise ValueError('capacity_snapshot_unverified')
    clean, quality = audit_rows(rows, as_of=as_of, market_id=config['market_id'], multiplier=Decimal(config['multiplier']))
    sessions = list(dict.fromkeys(r['session_id'] for r in clean if stamp(r['session_closing_at']) <= as_of))
    qualified_split = len(sessions) >= MIN_TRAIN_SESSIONS + HOLDOUT_SESSIONS
    train_ids = sessions[:-2] if qualified_split else sessions[:3]
    test_ids = sessions[-2:] if qualified_split else sessions[3:]
    base = {'schema_version': 1, 'mode': 'offline_shadow_only', 'as_of': as_of.isoformat(),
        'orders_attempted': False, 'state_modified': False, 'live_thresholds_modified': False,
        'live_authorization': False, 'eligible_for_live': False, 'input_sha256': digest(rows),
        'quality': quality, 'live_config_snapshot': config, 'completed_sessions': sessions,
        'training_sessions': train_ids, 'holdout_sessions': test_ids,
        'required_training_sessions': 3, 'required_holdout_sessions': 2,
        'missing_completed_sessions': max(0, 5-len(sessions)),
        'validation_status': 'split_ready_not_live_approved' if qualified_split else 'insufficient_independent_sessions',
        'comparison_scope': 'registered_oos' if qualified_split else 'exploratory_only_does_not_satisfy_two_holdouts',
        'risk_limits': {'capture_seconds': 2, 'abnormal_gap_seconds': 120, 'cost_budget_bps': '10'},
        'capacity_scenario': {'basis': 'frozen_current_equity_not_historical', 'smaller_equity_usd': str(min(equities))},
        'acceptance_blockers': ['historical_account_risk_equity_and_executable_refresh_not_fully_observed',
                                'actual_fills_funding_and_path_drawdown_not_verified',
                                'two_sessions_do_not_establish_statistical_profitability'],
        'model_choice': 'all_1h_3h_6h_results_reported_no_winner_selected'}
    if not test_ids:
        return {**base, 'status': 'no_unseen_completed_session'}
    split = min(stamp(r['logged_at']) for r in clean if r['session_id'] in test_ids)
    cutoff = split - timedelta(seconds=120)
    features = causal_features(clean)
    training = [r for r in clean if r['session_id'] in train_ids and stamp(r['logged_at']) < cutoff]
    models, training_reports = {}, {}
    for horizon in HORIZONS:
        labels, rejected = labels_for(training, features, horizon=horizon, end_before=cutoff)
        models[horizon] = fit_model(labels)
        training_reports[str(horizon)] = {'model': models[horizon], 'rejected_label_reasons': rejected,
            'label_count': len(labels), 'purged_cutoff_exclusive': cutoff.isoformat()}
    base.update(training_reports=training_reports, training_sha256=digest(training),
                holdout_start=split.isoformat(), label_end_cutoff_exclusive=cutoff.isoformat())
    # This single prior-only schedule is shared by all arms to avoid repeated
    # expensive percentile scans and any differing control refresh times.
    history = [r for r in rows if stamp(r['logged_at']) < split and r.get('sample_kind', 'baseline') == 'baseline']
    initial_history = list(history)
    test = sorted((r for r in rows if r.get('session_id') in test_ids), key=lambda r: stamp(r['logged_at']))
    clean_ids = {r['sample_id'] for r in clean}
    checked_bursts, burst_quality = audit_rows(
        [dict(r, sample_kind='baseline') for r in rows if r.get('sample_kind') == 'burst'],
        as_of=as_of, market_id=config['market_id'], multiplier=Decimal(config['multiplier']))
    replay_ids = clean_ids | {r['sample_id'] for r in checked_bursts}
    base['execution_refresh_quality'] = burst_quality
    control, shadows = {}, {h: {} for h in HORIZONS}
    counts = {h: Counter() for h in HORIZONS}
    control_counts, seen = Counter(), set()
    cached, refreshed_at = None, None
    for row in test:
        if row.get('sample_kind', 'baseline') != 'baseline':
            continue
        at, key = stamp(row['logged_at']), row.get('sample_id')
        if cached is None or (at-refreshed_at).total_seconds() >= 300:
            cached, refreshed_at = profiles(history, at, config), at
        if key in clean_ids and key not in seen:
            seen.add(key)
            control[key] = {d: cached[d].thresholds_bps for d in DIRECTIONS}
            for direction in DIRECTIONS:
                edge = Decimal(row[FIELDS[direction]])
                control_counts[direction] += int(control[key][direction][0] is not None and edge >= control[key][direction][0])
            for horizon in HORIZONS:
                shadows[horizon][key] = {}
                for direction in DIRECTIONS:
                    values = model_thresholds(row, direction, features.get(key, {}).get(direction),
                                              models[horizon], control[key][direction], config)
                    shadows[horizon][key][direction] = values
                    counts[horizon]['supported_direction_samples'] += int(values[0] is not None)
                    counts[horizon][direction] += int(values[0] is not None and Decimal(row[FIELDS[direction]]) >= values[0])
        history.append(row)
    control_replay = _replay_sessions(test, replay_ids, initial_history, control, config, health, test_ids)
    comparisons = {}
    for horizon in HORIZONS:
        calibration_labels, rejects = labels_for([r for r in clean if r['session_id'] in test_ids],
                                                features, horizon=horizon, end_before=as_of)
        covered, scored = 0, []
        for label in calibration_labels:
            prediction = predict(models[horizon], label['direction'], label)
            if prediction:
                covered += int(Decimal(label['exit_delta_bps']) <= prediction['exit_delta_bps'])
                scored.append(label)
        comparisons[str(horizon)] = {'signal_crossings': dict(counts[horizon]),
            'calibration': {'overlapping_scored_labels': len(scored),
                'nonoverlapping_labels': {d: nonoverlapping_count([x for x in scored if x['direction']==d]) for d in DIRECTIONS},
                'p95_bound_coverage': None if not scored else str(Decimal(covered)/len(scored)),
                'rejected_reasons': rejects, 'used_for_training_or_selection': False},
            'replay': _replay_sessions(test, replay_ids, initial_history, shadows[horizon], config, health, test_ids)}
    return {**base, 'status': 'research_completed_not_live_approved',
        'control_signal_crossings': dict(control_counts), 'control_replay': control_replay,
        'exit_models': comparisons,
        'conclusion': 'unproven_more_profitable_entries_even_if_more_shadow_signals'}
