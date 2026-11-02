from copy import deepcopy
from datetime import timedelta
from decimal import Decimal as D
import json

import pytest

from test_wti_research import AS_OF, CONFIG, history, sample
from tools.lib.wti_entry_audit import entry_diagnostics, same_parameter_shadow, transport_failures
from tools.lib.wti_replay import ReplayConfig, replay_wti_gradient
from tools.wti_entry_audit import main, recent_rows


def health():
    return dict(updated_at=AS_OF.isoformat(), status='running', startup_reconciled=True,
        execution_mode='live_bounded_gradient_pilot',
        entry_authorization_valid_until=(AS_OF + timedelta(seconds=1.5)).isoformat(),
        manage_existing_only=False, entry_authorization_permanently_revoked=False,
        risk_data_complete=True, account_read_recovery_required=False,
        pending_actions_total=0, market_session='open', account_risk_action='normal',
        risk_action='normal', wti_entry_authorized=True,
        gradient_pilot={'status': 'available'}, variational_equity_usd='144', lighter_equity_usd='129',
        market_sample_freshness_diagnostics={'checks': {'sample_quality': True,
            'session_evidence': True, 'quote_age_effective': True, 'book_age_effective': True},
            'quote_age_effective_seconds': '.5', 'book_age_effective_seconds': '.1'})


def test_policy_eligibility_does_not_extend_two_second_order_authority():
    h = health()
    before = deepcopy(h)
    fresh = entry_diagnostics(h, [], now=AS_OF)
    expired = entry_diagnostics(h, [], now=AS_OF + timedelta(seconds=2))
    assert fresh['observed_live_gate_open'] is True
    assert expired['strategy_entry_eligible'] is True
    assert expired['entry_phase'] == 'waiting_fresh_quote'
    assert expired['observed_live_gate_open'] is False
    assert h == before
    h['wti_entry_authorized'] = False
    assert entry_diagnostics(h, [], now=AS_OF)['observed_live_gate_open'] is False


@pytest.mark.parametrize('change', [
    {'entry_authorization_permanently_revoked': True}, {'account_read_recovery_required': True},
    {'account_risk_action': 'block_entry'}, {'pending_actions_total': 1},
    {'entry_gate_block_reason': 'sample_gap_latched'}, {'runtime_cycle_error': 'error'},
    {'startup_reconciled': False}, {'gradient_pilot': {'status': 'completed'}},
    {'market_session': 'closed'}, {'manage_existing_only': True},
])
def test_policy_blocks_are_not_relabelled_as_cache_expiry(change):
    h = health() | change
    d = entry_diagnostics(h, [], now=AS_OF + timedelta(seconds=3))
    assert d['entry_phase'] == 'strategy_blocked'
    assert not d['strategy_entry_eligible'] and not d['observed_live_gate_open']


def test_stale_unknown_or_incomplete_health_fails_closed():
    assert not entry_diagnostics({}, [], now=AS_OF)['strategy_entry_eligible']
    assert not entry_diagnostics(health(), [], now=AS_OF + timedelta(seconds=16))['strategy_entry_eligible']
    h = health()
    h['market_sample_freshness_diagnostics'] = {}
    assert not entry_diagnostics(h, [], now=AS_OF)['observed_live_gate_open']
    h = health() | {'entry_authorization_valid_until': (AS_OF - timedelta(seconds=1)).isoformat()}
    assert not entry_diagnostics(h, [], now=AS_OF)['observed_live_gate_open']


def test_burst_failure_explains_bad_leg_and_does_not_create_signal_votes():
    r = sample(logged_at=AS_OF.isoformat(), sample_kind='burst', sample_quality='invalid',
        sample_quality_reasons=['variational_quote_age_invalid'], var_quote_age_seconds='3.1')
    d = entry_diagnostics(health(), [r], now=AS_OF)
    assert d['refresh']['recent_failure_counts']['var_quote_age_seconds_outside_2s'] == 1
    assert d['signal_at'] is None
    assert d['directions']['long_var_short_lighter']['signal_threshold_reached'] is None


def test_reached_signal_is_not_reported_as_waiting_for_signal_or_a_submit():
    h = health() | {'gradient_threshold_profiles': {
        'long_var_short_lighter': {'thresholds_bps': ['30', '45', '46', '47', '48']}}}
    r = sample(logged_at=AS_OF.isoformat())
    d = entry_diagnostics(h, [r], now=AS_OF)
    assert d['entry_phase'] == 'authorized_signal_requires_execution_checks'
    assert d['diagnostic_grants_authorization'] is False and d['orders_attempted'] is False


def test_transport_details_are_structured_not_raw_secret_responses():
    text = ('2026-10-29 15:00:00,000 | WARNING | wti_execution_refresh_unavailable '
            'trigger=quote_age_effective error_type=RuntimeError reason=HTTP 503 token=secret\n'
            '2026-10-29 16:00:00,000 | WARNING | wti_execution_refresh_unavailable '
            'trigger=book_age_effective error_type=ConnectionClosedOK reason=sent 1000 (OK)\n')
    result = transport_failures(text, now=AS_OF)
    assert result[0]['http_status'] == 503 and result[1]['websocket_close_code'] == 1000
    assert 'secret' not in json.dumps(result)
    assert all(x['evidence_scope'].startswith('historical') for x in result)
    h = health() | {'execution_refresh_error': 'RuntimeError:HTTP 503 token=secret'}
    assert 'secret' not in json.dumps(entry_diagnostics(h, [], now=AS_OF))


def test_same_parameters_warm_thresholds_and_frozen_capacity_are_explicit():
    cfg = CONFIG | {'max_slippage_bps': '8', 'max_completed_roundtrips': 1, 'max_equity_multiple': '5'}
    rows = history()
    saved = deepcopy(rows)
    report = same_parameter_shadow(rows, health=health(), config=cfg, as_of=AS_OF)
    assert report['eligible_for_live'] is False
    replay = report['same_parameter_replay']
    assert replay['config']['initial_threshold_history_rows'] == 4 * 240
    assert replay['config']['max_sample_gap_seconds'] == 120
    assert replay['config']['roundtrip_cost_bps'] == '10'
    assert replay['config']['max_order_notional_usd'] == '20'
    assert replay['config']['max_layers'] == 5
    assert replay['config']['max_completed_roundtrips'] == 1
    assert replay['config']['tier_capacity_notional_usd'] == ['120', '240', '380', '500', '640']
    assert report['capacity_scenario']['basis'].endswith('not_historical')
    assert rows == saved
    duplicated = same_parameter_shadow(rows + [rows[-1]], health=health(), config=cfg, as_of=AS_OF)
    assert duplicated['opportunities'] == report['opportunities']
    changed = [dict(r, var_quote_age_seconds='3') if r['session_id'] == rows[-1]['session_id'] else r for r in rows]
    later = same_parameter_shadow(changed, health=health(), config=cfg, as_of=AS_OF)
    assert later['holdout_session_id'] != report['holdout_session_id']
    bad = cfg | {'tier_thresholds_bps': ['30', '45', '46', '47', '48']}
    assert same_parameter_shadow(rows, health=health(), config=bad, as_of=AS_OF)['status'] == 'unsupported_live_parameter_mismatch'


def test_warm_history_cannot_contain_future_current_or_burst_rows():
    cfg = ReplayConfig(D(30), D(1), D(10), 21600, 120)
    row = sample(index=10)
    for warm in ([row], [sample(index=11)], [sample(index=1, sample_kind='burst')]):
        with pytest.raises(ValueError, match='warm_history_must_precede'):
            replay_wti_gradient([row], config=cfg, second_tier_threshold_bps=D(45),
                layer_notional_usd=D(20), aggregate_max_notional_usd=D(100), max_layers=5,
                initial_threshold_history=warm)


def test_cli_never_writes_health_or_trading_sources(tmp_path, capsys):
    folder = tmp_path / 'WTI'
    folder.mkdir()
    source = folder / '2026-10-29.jsonl'
    source.write_text(json.dumps(sample()) + '\n' + '{incomplete')
    hp = tmp_path / 'health.json'
    hp.write_text(json.dumps(health()))
    before = source.read_bytes(), hp.read_bytes()
    assert len(recent_rows(tmp_path)) == 1
    assert main(['--health-path', str(hp), '--sample-root', str(tmp_path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['orders_attempted'] is False and report['state_modified'] is False
    assert before == (source.read_bytes(), hp.read_bytes())
