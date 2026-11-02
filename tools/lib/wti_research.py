"""Offline WTI diagnostics. No gateway, account mutation, or order submission."""
from __future__ import annotations

from bisect import bisect_left
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from typing import Any

from tools.basis_collector import edge_bps
from tools.lib.market_sessions import session_observation_for_sample
from tools.lib.wti_execution import validate_market_sample, WTIExecutionError
from tools.lib.wti_reporting import number
from tools.lib.wti_thresholds import build_wti_gradient_threshold_profiles, _percentile

DIRECTIONS = ('long_var_short_lighter', 'short_var_long_lighter')
FIELDS = dict(zip(DIRECTIONS, ('long_edge_bps', 'short_edge_bps')))
CAPTURE_SECONDS = 2
GAP_SECONDS = 120


def stamp(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('timezone_missing')
    return parsed.astimezone(timezone.utc)


def digest(rows: list[dict[str, Any]]) -> str:
    content = '\n'.join(json.dumps(r, sort_keys=True, separators=(',', ':')) for r in rows)
    return hashlib.sha256(content.encode()).hexdigest()


def audit_rows(rows: list[dict[str, Any]], *, as_of: datetime, market_id: int,
               multiplier: Decimal) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    unique, conflicts, excluded = {}, set(), Counter()
    duplicates = 0
    for row in rows:
        if not isinstance(row, dict):
            excluded['non_object'] += 1
            continue
        key = str(row.get('sample_id') or '')
        if not key:
            excluded['sample_id_missing'] += 1
            continue
        if key in unique:
            if row != unique[key]:
                conflicts.add(key)
            else:
                duplicates += 1
        else:
            unique[key] = row
    clean = []
    for key, row in unique.items():
        try:
            if key in conflicts:
                raise ValueError('conflicting_sample_id')
            at = stamp(row.get('logged_at'))
            if not as_of - timedelta(days=7) < at < as_of:
                raise ValueError('outside_prior_7d')
            if row.get('sample_kind', 'baseline') != 'baseline':
                raise ValueError('not_baseline')
            if row.get('record_kind') != 'basis_market_sample':
                raise ValueError('record_kind_invalid')
            precision = row.get('lighter_supported_size_decimals')
            if not isinstance(precision, int) or isinstance(precision, bool):
                raise ValueError('size_precision_invalid')
            context = validate_market_sample(row, expected_lighter_market_id=market_id,
                expected_lighter_multiplier=multiplier,
                expected_lighter_size_decimals=precision, now=at,
                max_quote_age_seconds=CAPTURE_SECONDS, max_book_age_seconds=CAPTURE_SECONDS)
            if row.get('lighter_continuity_ok') is not True:
                raise ValueError('book_continuity_unverified')
            vq, lq = number(row.get('variational_qty')), number(row.get('lighter_qty'))
            if vq is None or lq is None or vq != lq * multiplier:
                raise ValueError('hedge_quantity_mismatch')
            step = Decimal(1).scaleb(-precision)
            if (lq < context.lighter_min_base or lq % step != 0
                    or lq * min(context.lighter_buy_price, context.lighter_sell_price) * multiplier < context.lighter_min_quote):
                raise ValueError('market_minimum_or_quantity_step_not_met')
            for direction, prices in (
                (DIRECTIONS[0], ('lighter_sell_price', 'var_ask')),
                (DIRECTIONS[1], ('var_bid', 'lighter_buy_price')),
            ):
                stored = number(row.get(FIELDS[direction]))
                expected = edge_bps(Decimal(row[prices[0]]), Decimal(row[prices[1]]))
                if stored is None or abs(stored - expected) > Decimal('0.000001'):
                    raise ValueError('edge_price_inconsistent')
            clean.append(row)
        except (ValueError, WTIExecutionError, KeyError, TypeError, ArithmeticError) as exc:
            excluded[str(exc)] += 1
    clean.sort(key=lambda r: (stamp(r['logged_at']), r['sample_id']))
    # Different IDs for the same capture must not overweight a percentile or vote.
    timestamps = Counter(stamp(r['logged_at']) for r in clean)
    duplicate_times = {t for t, n in timestamps.items() if n > 1}
    excluded['ambiguous_same_timestamp'] += sum(timestamps[t] for t in duplicate_times)
    clean = [r for r in clean if stamp(r['logged_at']) not in duplicate_times]
    return clean, {'input_rows': len(rows), 'accepted_rows': len(clean),
                   'duplicate_rows': duplicates, 'conflicting_ids': len(conflicts),
                   'excluded_reasons': dict(excluded), 'accepted_sha256': digest(clean),
                   'capture_limit_seconds': CAPTURE_SECONDS, 'gap_limit_seconds': GAP_SECONDS}


def profiles(rows, as_of, config):
    floors = tuple(Decimal(x) for x in config['tier_thresholds_bps'])
    return build_wti_gradient_threshold_profiles(rows, as_of=as_of,
        market_id=config['market_id'], multiplier=Decimal(config['multiplier']),
        first_floor_bps=floors[0], second_floor_bps=floors[1], tier_floors_bps=floors,
        roundtrip_cost_bps=Decimal(config['roundtrip_cost_bps']),
        exit_target_bps=Decimal(config['exit_target_bps']))


def threshold_audit(rows, *, as_of, config):
    clean, audit = audit_rows(rows, as_of=as_of, market_id=config['market_id'],
                              multiplier=Decimal(config['multiplier']))
    live = profiles(rows, as_of, config)
    audited = profiles(clean, as_of, config)
    reserve = Decimal(config['roundtrip_cost_bps']) + Decimal(config['exit_target_bps'])
    explanations = {}
    floors = [Decimal(x) for x in config['tier_thresholds_bps']]
    for direction, p in live.items():
        quantiles = (p.p975_bps, p.p9825_bps, p.p99_bps, p.p995_bps, p.p998_bps)
        tiers = []
        for i, floor in enumerate(floors):
            candidates = {'configured_floor': floor}
            if quantiles[i] is not None:
                candidates['prior_7d_percentile_plus_reserve'] = quantiles[i] + reserve
            if i == 0 and p.fast_p95_bps is not None:
                candidates['prior_24h_p95_plus_reserve'] = p.fast_p95_bps + reserve
            if i and p.thresholds_bps[i - 1] is not None:
                candidates['previous_tier_plus_spacing'] = p.thresholds_bps[i - 1] + p.tier_spacing_bps
            applied = p.thresholds_bps[i]
            tiers.append({'tier': i + 1, 'candidates_bps': {k: str(v) for k, v in candidates.items()},
                          'applied_bps': None if applied is None else str(applied),
                          'binding_components': [k for k, v in candidates.items() if v == applied]})
        vals = [Decimal(r[FIELDS[direction]]) for r in clean]
        tail_cut = _percentile(vals, Decimal('97.5'))
        origins = Counter(r['session_id'] for r in clean
                          if tail_cut is not None and Decimal(r[FIELDS[direction]]) >= tail_cut)
        explanations[direction] = {'production_profile': p.as_dict(),
            'audited_profile_diagnostic_only': audited[direction].as_dict(), 'tiers': tiers,
            'tail_session_counts': dict(origins), 'cost_budget_bps': config['roundtrip_cost_bps'],
            'target_bps': config['exit_target_bps']}
    return clean, {'quality': audit, 'directions': explanations,
                   'live_thresholds_modified': False, 'orders_attempted': False}


def _same_size(entry, exit):
    return all(number(entry.get(k)) == number(exit.get(k)) for k in ('variational_qty', 'lighter_qty'))


def _entry_gap(row, direction):
    v = Decimal(row['var_ask'] if direction == DIRECTIONS[0] else row['var_bid'])
    l = Decimal(row['lighter_sell_price'] if direction == DIRECTIONS[0] else row['lighter_buy_price'])
    mid = (v + l) / 2
    return ((l - v) if direction == DIRECTIONS[0] else (v - l)) / mid * 10000


def _exit_gap(entry, exit, direction):
    ev = Decimal(entry['var_ask'] if direction == DIRECTIONS[0] else entry['var_bid'])
    el = Decimal(entry['lighter_sell_price'] if direction == DIRECTIONS[0] else entry['lighter_buy_price'])
    v = Decimal(exit['var_bid'] if direction == DIRECTIONS[0] else exit['var_ask'])
    l = Decimal(exit['lighter_buy_price'] if direction == DIRECTIONS[0] else exit['lighter_sell_price'])
    return ((l - v) if direction == DIRECTIONS[0] else (v - l)) / ((ev + el) / 2) * 10000


def _segments(clean, rows):
    """Invalid captured baselines are barriers, not silently skipped observations."""
    accepted_ids = {r['sample_id'] for r in clean}
    bad_times = sorted(stamp(r['logged_at']) for r in rows if isinstance(r, dict)
                       and r.get('sample_kind', 'baseline') == 'baseline'
                       and r.get('logged_at') and _parseable(r.get('logged_at'))
                       and r.get('sample_id') not in accepted_ids)
    parts, current = [], []
    for row in clean:
        at = stamp(row['logged_at'])
        if current:
            previous = current[-1]
            prev_at = stamp(previous['logged_at'])
            start = bisect_left(bad_times, prev_at)
            barrier = start < len(bad_times) and bad_times[start] <= at
            if (row['session_id'] != previous['session_id'] or barrier
                    or not 0 < (at - prev_at).total_seconds() <= GAP_SECONDS):
                parts.append(current)
                current = []
        current.append(row)
    if current:
        parts.append(current)
    return parts


def _parseable(value):
    try:
        stamp(value)
        return True
    except (ValueError, TypeError):
        return False


def shadow_validation(rows, clean, *, as_of, config, horizon_seconds=3600,
                      holdout_sessions=2, min_training_sessions=3, min_labels=100):
    if not 0 < horizon_seconds <= min(21600, float(config['max_hold_seconds'])):
        raise ValueError('shadow_horizon_outside_existing_hold_limit')
    if holdout_sessions < 2 or min_training_sessions < 3 or min_labels < 100:
        raise ValueError('shadow_validation_minimums_cannot_be_relaxed')
    sessions = list(dict.fromkeys(r['session_id'] for r in clean
                                  if stamp(r['session_closing_at']) <= as_of))
    base = {'mode': 'shadow_only', 'live_authorization': False, 'orders_attempted': False,
            'production_parameters_changed': False, 'eligible_for_live': False,
            'horizon_seconds': horizon_seconds, 'holdout_sessions_required': holdout_sessions,
            'completed_sessions_available': len(sessions),
            'minimum_training_sessions': min_training_sessions,
            'required_total_completed_sessions': min_training_sessions + holdout_sessions,
            'missing_completed_sessions': max(0, min_training_sessions + holdout_sessions - len(sessions)),
            'method': 'frozen_prior_exit_gap_p90_v1', 'costs_are_assumed_not_verified_fills': True,
            'overlapping_training_labels_are_not_independent': True,
            'replay_scope': 'matched_single_slot_no_adds_not_full_five_tier_portfolio',
            'safety': {'capture_seconds': 2, 'max_gap_seconds': 120,
                       'max_hold_seconds': config['max_hold_seconds'],
                       'max_notional_usd': min(Decimal(config['layer_notional_usd']), Decimal(config['max_notional_usd']))}}
    if len(sessions) < min_training_sessions + holdout_sessions:
        return {**base, 'status': 'insufficient_independent_sessions', 'session_count': len(sessions)}
    holdout_ids = set(sessions[-holdout_sessions:])
    split = min(stamp(r['logged_at']) for r in clean if r['session_id'] in holdout_ids)
    cutoff = split - timedelta(seconds=GAP_SECONDS)
    training = [r for r in clean if stamp(r['logged_at']) < cutoff and r['session_id'] not in holdout_ids]
    parts = _segments(clean, rows)
    labels = {d: [] for d in DIRECTIONS}
    label_sessions = set()
    for part in parts:
        times = [stamp(r['logged_at']) for r in part]
        for i, entry in enumerate(part):
            if entry['session_id'] in holdout_ids or times[i] >= cutoff:
                continue
            j = bisect_left(times, times[i] + timedelta(seconds=horizon_seconds))
            if j >= len(part) or times[j] >= cutoff or (times[j] - times[i]).total_seconds() > horizon_seconds + 20:
                continue
            if not _same_size(entry, part[j]):
                continue
            for direction in DIRECTIONS:
                labels[direction].append(_exit_gap(entry, part[j], direction))
            label_sessions.add(entry['session_id'])
    base.update({'training_cutoff_exclusive': cutoff.isoformat(), 'holdout_start': split.isoformat(),
                 'holdout_session_ids': sorted(holdout_ids), 'training_session_ids': sorted(label_sessions),
                 'training_sha256': digest(training), 'input_sha256': digest(rows),
                 'training_labels': len(labels[DIRECTIONS[0]])})
    if len(label_sessions) < min_training_sessions or len(labels[DIRECTIONS[0]]) < min_labels:
        return {**base, 'status': 'insufficient_purged_training_labels'}
    predictions = {d: _percentile(labels[d], Decimal(90)) for d in DIRECTIONS}
    base['frozen_exit_gap_bps'] = {d: str(v) for d, v in predictions.items()}
    test = [p for p in parts if p[0]['session_id'] in holdout_ids]
    baseline = _replay(test, training, predictions=None, config=config)
    shadow = _replay(test, training, predictions=predictions, config=config)
    return {**base, 'status': 'research_complete_not_live_approved',
            'control': baseline, 'exit_model_shadow': shadow,
            'acceptance_blockers': ['no_independent_final_fill_and_fee_validation',
                                    'quote_path_does_not_prove_execution_or_profitability']}


def _replay(parts, training, *, predictions, config):
    reserve = Decimal(config['roundtrip_cost_bps'])
    target = Decimal(config['exit_target_bps'])
    notional_cap = min(Decimal(config['layer_notional_usd']), Decimal(config['max_notional_usd']))
    history = list(training)
    trades, blocked = [], Counter()
    votes = {d: deque(maxlen=3) for d in DIRECTIONS}
    active, cached_profiles, refresh_at = None, None, None
    unresolved = False
    for part in parts:
        votes = {d: deque(maxlen=3) for d in DIRECTIONS}
        for row in part:
            at = stamp(row['logged_at'])
            if active:
                entry, direction = active
                if not _same_size(entry, row):
                    blocked['exact_size_exit_quote_unavailable'] += 1
                    history.append(row)
                    continue
                gross_bps = _entry_gap(entry, direction) - _exit_gap(entry, row, direction)
                net_bps = gross_bps - reserve
                age = (at - stamp(entry['logged_at'])).total_seconds()
                cutoff = (stamp(row['session_closing_at']) - at).total_seconds() <= float(config['entry_cutoff_seconds'])
                if net_bps >= target or age >= float(config['max_hold_seconds']) or cutoff:
                    v = Decimal(entry['var_ask'] if direction == DIRECTIONS[0] else entry['var_bid'])
                    l = Decimal(entry['lighter_sell_price'] if direction == DIRECTIONS[0] else entry['lighter_buy_price'])
                    base = Decimal(entry['variational_qty']) * (v + l) / 2
                    trades.append({'entry_at': entry['logged_at'], 'exit_at': row['logged_at'],
                                   'direction': direction, 'net_bps': str(net_bps),
                                   'indicative_net_usd': str(base * net_bps / 10000),
                                   'reason': 'net_target' if net_bps >= target else 'max_hold' if age >= float(config['max_hold_seconds']) else 'session_cutoff'})
                    active = None
                    votes = {d: deque(maxlen=3) for d in DIRECTIONS}
                history.append(row)
                continue
            if (stamp(row['session_closing_at']) - at).total_seconds() <= float(config['entry_cutoff_seconds']):
                blocked['session_cutoff'] += 1
                history.append(row)
                continue
            if predictions is None and (refresh_at is None or (at - refresh_at).total_seconds() >= 300):
                cached_profiles = profiles(history, at, config)
                refresh_at = at
            candidates = []
            for direction in DIRECTIONS:
                if predictions is None:
                    threshold = cached_profiles[direction].thresholds_bps[0]
                    qualifies = threshold is not None and Decimal(row[FIELDS[direction]]) >= threshold
                else:
                    qualifies = (Decimal(row[FIELDS[direction]]) >= Decimal(config['entry_threshold_bps'])
                                 and _entry_gap(row, direction) >= predictions[direction] + reserve + target)
                votes[direction].append(qualifies)
                if qualifies and sum(votes[direction]) >= 2:
                    candidates.append(direction)
            if candidates:
                direction = max(candidates, key=lambda d: Decimal(row[FIELDS[d]]))
                v = Decimal(row['var_ask'] if direction == DIRECTIONS[0] else row['var_bid'])
                l = Decimal(row['lighter_sell_price'] if direction == DIRECTIONS[0] else row['lighter_buy_price'])
                qty = Decimal(row['variational_qty'])
                slip_reserve = Decimal(1) + Decimal(str(config.get('max_slippage_bps', '8'))) / 10000
                if max(qty * v, qty * l) * slip_reserve <= notional_cap:
                    active = row, direction
                else:
                    blocked['existing_per_leg_notional_cap'] += 1
            else:
                blocked['threshold_or_two_of_three_confirmation'] += 1
            history.append(row)
        if active:
            # No quote-based assumption that an outage or session-end position was flattened.
            unresolved = True
            break
    pnl = sum((Decimal(t['indicative_net_usd']) for t in trades), Decimal(0))
    return {'closed_trades': len(trades), 'unresolved_position': unresolved,
            'indicative_net_usd': str(pnl), 'blocked_reasons': dict(blocked), 'trades': trades,
            'execution_refresh_confirmed': False, 'actual_pnl_usd': None}
