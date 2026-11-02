from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import json

import pytest

from tools.basis_collector import edge_bps
from tools.lib.wti_research import audit_rows, threshold_audit, shadow_validation, _segments, _replay
from tools.wti_research_audit import live_config, main, cost_evidence, read_research_input

AS_OF = datetime(2026, 10, 29, 18, tzinfo=timezone.utc)
CONFIG = {'tier_thresholds_bps': ['30', '45', '45.25', '45.50', '45.75'],
          'market_id': 145, 'multiplier': '1', 'entry_threshold_bps': '30',
          'roundtrip_cost_bps': '10', 'exit_target_bps': '1', 'max_hold_seconds': 21600,
          'entry_cutoff_seconds': 1800, 'layer_notional_usd': '20', 'max_notional_usd': '20'}


def sample(day=0, index=0, **changes):
    opened = AS_OF.replace(hour=0) - timedelta(days=6 - day)
    at = opened + timedelta(seconds=index * 30)
    close = opened + timedelta(hours=2)
    row = {'sample_id': f'{day}:{index}', 'logged_at': at.isoformat(), 'asset': 'WTI',
           'record_kind': 'basis_market_sample', 'sample_kind': 'baseline',
           'sample_quality': 'valid', 'market_availability': 'both_feeds_live',
           'variational_underlying': 'USOILP', 'variational_instrument_type': 'swap',
           'variational_instrument_kind': 'commodity', 'variational_funding_interval_s': 0,
           'lighter_symbol': 'WTI', 'lighter_market_type': 'perp', 'lighter_market_status': 'active',
           'lighter_market_id': 145, 'lighter_multiplier': '1',
           'lighter_supported_size_decimals': 3, 'lighter_supported_price_decimals': 3,
           'lighter_min_base_amount': '0.06', 'lighter_min_quote_amount': '10',
           'variational_qty': '0.19', 'lighter_qty': '0.19',
           'var_bid': '100', 'var_ask': '100.01', 'lighter_bid': '100.5', 'lighter_ask': '100.51',
           'lighter_buy_price': '100.51', 'lighter_sell_price': '100.5',
           'lighter_continuity_ok': True, 'var_quote_age_seconds': '0.1', 'lighter_book_age_seconds': '0.1',
           'market_status': 'open', 'session_status': 'open',
           'session_status_source': 'variational_market_status_websocket',
           'session_status_feed_connected': True,
           'session_status_observed_at': at.isoformat(), 'session_status_feed_heartbeat_at': at.isoformat(),
           'session_schedule_source': 'variational_supported_assets_api',
           'session_id': f'{opened.isoformat()}|{close.isoformat()}', 'session_closing_at': close.isoformat()}
    row.update(changes)
    row['long_edge_bps'] = str(edge_bps(D(row['lighter_sell_price']), D(row['var_ask'])))
    row['short_edge_bps'] = str(edge_bps(D(row['var_bid']), D(row['lighter_buy_price'])))
    return row


def audit(rows):
    return audit_rows(rows, as_of=AS_OF, market_id=145, multiplier=D(1))


def history():
    return [sample(day, index) for day in range(5) for index in range(240)]


def test_threshold_explanation_matches_unchanged_production_formula():
    rows = history()
    before = deepcopy(rows)
    clean, report = threshold_audit(rows, as_of=AS_OF, config=CONFIG)
    for detail in report['directions'].values():
        for tier in detail['tiers']:
            if tier['applied_bps'] is not None:
                assert D(tier['applied_bps']) == max(map(D, tier['candidates_bps'].values()))
    assert rows == before and clean
    assert report['orders_attempted'] is False
    assert report['live_thresholds_modified'] is False


@pytest.mark.parametrize('changes,reason', [
    ({'var_quote_age_seconds': '2.001'}, 'variational_quote_stale_or_invalid'),
    ({'lighter_book_age_seconds': '2.001'}, 'lighter_book_stale_or_invalid'),
    ({'lighter_continuity_ok': False}, 'book_continuity_unverified'),
    ({'lighter_market_id': 1}, 'lighter_market_identity_or_precision_invalid'),
    ({'session_status_feed_connected': False}, 'exchange_session_observation_unverified'),
    ({'lighter_qty': '0.18'}, 'hedge_quantity_mismatch'),
    ({'lighter_qty': '0.059', 'variational_qty': '0.059'}, 'market_minimum_or_quantity_step_not_met'),
    ({'sample_kind': 'burst'}, 'not_baseline'),
])
def test_strict_quality_uses_two_second_capture_and_identity_checks(changes, reason):
    clean, report = audit([sample(**changes)])
    assert not clean
    assert report['excluded_reasons'][reason] == 1


def test_duplicate_conflict_future_timestamp_and_computed_edge_audit():
    row = sample()
    bad = sample(index=2)
    bad['long_edge_bps'] = '9999'
    future = sample(day=6, logged_at=(AS_OF + timedelta(seconds=1)).isoformat())
    conflict = {**row, 'var_quote_age_seconds': '0.3'}
    clean, report = audit([row, row, conflict, bad, future])
    assert not clean
    assert report['duplicate_rows'] == 1 and report['conflicting_ids'] == 1
    assert report['excluded_reasons']['edge_price_inconsistent'] == 1
    assert report['excluded_reasons']['outside_prior_7d'] == 1


def test_invalid_row_and_real_gap_are_barriers_not_training_coverage():
    a, b, c = sample(index=0), sample(index=2), sample(index=4)
    invalid = sample(index=1, sample_quality='invalid')
    clean, _ = audit([a, b, c, invalid])
    assert len(_segments(clean, [a, b, c, invalid])) == 2
    far = sample(index=20)
    assert len(_segments([a, far], [a, far])) == 2


def test_frozen_training_is_independent_of_future_holdout_prices():
    rows = history()
    clean, _ = audit(rows)
    first = shadow_validation(rows, clean, as_of=AS_OF, config=CONFIG)
    changed = [sample(d, i, lighter_buy_price='101', lighter_sell_price='100.9')
               if d >= 3 else sample(d, i) for d in range(5) for i in range(240)]
    later, _ = audit(changed)
    second = shadow_validation(changed, later, as_of=AS_OF, config=CONFIG)
    assert first['status'] == 'research_complete_not_live_approved'
    assert first['training_sha256'] == second['training_sha256']
    assert first['frozen_exit_gap_bps'] == second['frozen_exit_gap_bps']
    assert first['input_sha256'] != second['input_sha256']
    assert first['eligible_for_live'] is False
    assert first['training_cutoff_exclusive'] < first['holdout_start']


def test_ongoing_session_is_not_counted_as_completed_holdout():
    rows = history()
    ongoing = sample(6, 0, session_closing_at=(AS_OF + timedelta(hours=5)).isoformat())
    clean, _ = audit(rows + [ongoing])
    result = shadow_validation(rows + [ongoing], clean, as_of=AS_OF, config=CONFIG)
    assert ongoing['session_id'] not in result['holdout_session_ids']


def test_missing_sessions_and_purged_labels_never_pass_validation():
    rows = [sample(d, i) for d in range(4) for i in range(240)]
    clean, _ = audit(rows)
    result = shadow_validation(rows, clean, as_of=AS_OF, config=CONFIG)
    assert result['status'] == 'insufficient_independent_sessions'
    assert result['completed_sessions_available'] == 4 and result['missing_completed_sessions'] == 1
    rows = [sample(d, i) for d in range(5) for i in range(10)]
    clean, _ = audit(rows)
    assert shadow_validation(rows, clean, as_of=AS_OF, config=CONFIG)['status'] == 'insufficient_purged_training_labels'
    with pytest.raises(ValueError):
        shadow_validation(rows, clean, as_of=AS_OF, config=CONFIG, min_labels=99)
    with pytest.raises(ValueError):
        shadow_validation(rows, clean, as_of=AS_OF, config=CONFIG, horizon_seconds=21601)


def test_replay_preserves_cap_and_does_not_invent_exit_quote_after_gap():
    rows = [sample(index=i) for i in range(4)]
    predictions = {d: D(0) for d in ('long_var_short_lighter', 'short_var_long_lighter')}
    result = _replay([rows], [], predictions=predictions, config=CONFIG)
    assert result['unresolved_position'] is True
    assert result['actual_pnl_usd'] is None
    large = [sample(index=i, variational_qty='0.2', lighter_qty='0.2') for i in range(4)]
    result = _replay([large], [], predictions=predictions, config=CONFIG)
    assert result['blocked_reasons']['existing_per_leg_notional_cap'] > 0
    assert not result['unresolved_position'] and result['closed_trades'] == 0


def test_mismatched_quote_size_is_not_used_to_assume_flat_exit():
    rows = [sample(index=i) for i in range(3)]
    rows.append(sample(index=4, variational_qty='0.18', lighter_qty='0.18'))
    result = _replay([rows], [], predictions={d: D(0) for d in ('long_var_short_lighter', 'short_var_long_lighter')}, config=CONFIG)
    assert result['unresolved_position']
    assert result['blocked_reasons']['exact_size_exit_quote_unavailable'] == 1


def test_cost_evidence_unknown_fees_cannot_change_budget_or_verify_net():
    state = {'last_closed_position': {'entry_execution_id': 'entry', 'exit_execution_id': 'exit'}}
    report = cost_evidence(state, {'status': 'confirmed', 'fees_usd': '0'})
    assert not report['net_profit_verified'] and not report['cost_budget_modified']
    assert 'exact_order_usd_fees_incomplete' in report['remaining_blockers']


def test_cli_is_read_only_and_never_imports_a_submit_entry(tmp_path, capsys):
    root = tmp_path / 'samples' / 'WTI'
    root.mkdir(parents=True)
    data = root / '2026-10-23.jsonl'
    data.write_text('\n'.join(json.dumps(r) for r in [sample(index=i) for i in range(5)]) + '\n')
    state_path = tmp_path / 'state.json'
    state_path.write_text(json.dumps({'asset': 'WTI', 'gradient_pilot': {'config': CONFIG}}))
    before = {p: p.read_bytes() for p in (data, state_path)}
    assert main(['--sample-root', str(root.parent), '--state-path', str(state_path), '--as-of', AS_OF.isoformat(), '--shadow']) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['orders_attempted'] is False and result['state_modified'] is False
    assert result['live_authorization'] is False
    assert all(p.read_bytes() == value for p, value in before.items())


def test_config_must_be_the_current_wti_configuration():
    with pytest.raises(ValueError):
        live_config({'asset': 'ETH', 'gradient_pilot': {'config': CONFIG}})
    bad = {**CONFIG, 'max_notional_usd': '26'}
    with pytest.raises(ValueError):
        live_config({'asset': 'WTI', 'gradient_pilot': {'config': bad}})


def test_input_audit_distinguishes_corruption_from_in_progress_append(tmp_path):
    path = tmp_path / 'WTI'
    path.mkdir()
    (path / '2026-10-23.jsonl').write_text(json.dumps(sample()) + '\n{broken}\n' + '{partial')
    rows, proof = read_research_input(tmp_path, AS_OF)
    assert len(rows) == 1
    assert proof['malformed_or_unverifiable_timestamp_rows'] == 1
    assert proof['sources'][0]['incomplete_tail_lines'] == 1
