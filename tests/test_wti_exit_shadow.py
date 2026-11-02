from copy import deepcopy
from datetime import timedelta
from decimal import Decimal as D
import json
from types import SimpleNamespace

import pytest

from test_wti_research import sample, AS_OF, CONFIG
from tools.lib.wti_exit_shadow import (
    causal_features, labels_for, fit_model, predict, model_thresholds,
    exit_shadow, nonoverlapping_count, _replay_sessions,
)
from tools.lib.wti_research import DIRECTIONS, stamp, _entry_gap
from tools.lib.wti_replay import ReplayConfig, replay_wti_gradient
from tools.wti_exit_shadow import main

CFG = CONFIG | {'max_slippage_bps': '8', 'max_equity_multiple': '5', 'max_completed_roundtrips': 1}
HEALTH = {'risk_data_complete': True, 'variational_equity_usd': '144', 'lighter_equity_usd': '129'}


def row(day=0, index=0, **changes):
    first = sample(day=day)
    opened = stamp(first['logged_at'])
    at, closing = opened + timedelta(seconds=index * 60), opened + timedelta(hours=20)
    return sample(day=day, index=index, logged_at=at.isoformat(),
        session_id=f'{opened.isoformat()}|{closing.isoformat()}', session_closing_at=closing.isoformat(),
        session_status_observed_at=at.isoformat(), session_status_feed_heartbeat_at=at.isoformat(), **changes)


def fast_profiles(rows, at, config):
    return {d: SimpleNamespace(thresholds_bps=tuple(D(x) for x in ('80', '81', '82', '83', '84')))
            for d in DIRECTIONS}


def test_features_are_causal_and_do_not_persist_across_gaps():
    data = [row(index=i) for i in range(60)]
    first = causal_features(data)
    changed = data[:50] + [row(index=i, lighter_sell_price='104', lighter_buy_price='104.1') for i in range(50,60)]
    later = causal_features(changed)
    assert all(first[r['sample_id']]==later[r['sample_id']] for r in data[:50])
    assert not first[data[0]['sample_id']] and first[data[31]['sample_id']]
    gapped = causal_features(data[:40] + data[50:])
    assert not gapped[data[50]['sample_id']]


def test_labels_do_not_interpolate_cross_gaps_sizes_or_purged_boundary():
    data = [row(index=i) for i in range(200)]
    features = causal_features(data)
    cutoff = stamp(data[150]['logged_at'])
    labels, rejected = labels_for(data, features, horizon=3600, end_before=cutoff)
    assert labels and all(stamp(x['end_at']) < cutoff for x in labels)
    assert rejected['purged_label_reaches_validation_boundary'] > 0
    changed = [r if i<60 else dict(r, lighter_qty='0.18', variational_qty='0.18') for i,r in enumerate(data)]
    _, bad = labels_for(changed, features, horizon=3600, end_before=AS_OF)
    assert bad['exact_size_exit_quote_missing'] > 0
    gapped = data[:50] + data[70:]
    labels, _ = labels_for(gapped, causal_features(gapped), horizon=3600, end_before=AS_OF)
    assert not any(stamp(x['at']) < stamp(data[50]['logged_at']) and stamp(x['end_at']) >= stamp(data[70]['logged_at']) for x in labels)


def fitted_labels():
    result = []
    for day in range(3):
        for i in range(100):
            at = stamp(row(day)['logged_at']) + timedelta(minutes=i*2)
            result.append({'session_id': str(day), 'at': at.isoformat(),
                'end_at': (at+timedelta(hours=1)).isoformat(), 'direction': DIRECTIONS[0],
                'phase': 'early', 'state': 'expanded', 'exit_delta_bps': str(-20+day)})
    return result


def test_model_uses_worst_session_p95_not_dense_session_average():
    labels = fitted_labels()
    fit = fit_model(labels)
    p = predict(fit, DIRECTIONS[0], {'phase': 'early', 'state': 'expanded'})
    assert p['exit_delta_bps'] == -18
    dense = labels + labels[:100]*10
    assert predict(fit_model(dense), DIRECTIONS[0], {'phase': 'early','state':'expanded'})['exit_delta_bps'] == -18
    assert nonoverlapping_count(labels) < len(labels)
    assert not fit_model(labels[:100])['cells']


def test_midpoint_model_converts_to_existing_edge_units_and_preserves_tier_spacing():
    r = row(lighter_sell_price='101', lighter_buy_price='101.01')
    model = fit_model(fitted_labels())
    feature = {'phase': 'early', 'state': 'expanded'}
    control = tuple(D(x) for x in ('80', '81', '82', '83', '84'))
    thresholds = model_thresholds(r, DIRECTIONS[0], feature, model, control, CFG)
    factor = (D(r['var_ask']) + D(r['lighter_sell_price'])) / (2*D(r['var_ask']))
    assert abs(thresholds[0]/factor - _entry_gap(r,DIRECTIONS[0]) - D(-7)) < D('1e-22')
    assert all(b-a >= 1 for a,b in zip(thresholds,thresholds[1:]))
    assert all(x >= D(floor) for x,floor in zip(thresholds,CFG['tier_thresholds_bps']))
    assert model_thresholds(r, DIRECTIONS[0], None, model, control, CFG) == (None,)*5


def test_research_replay_retains_capture_gap_cap_and_confirmation():
    data = [row(index=i,variational_qty='0.199',lighter_qty='0.199') for i in range(3)]
    cfg = ReplayConfig(D(30), D(1), D(10),21600,120)
    schedule = {r['sample_id']: {d:tuple(D(x) for x in ('30','60','61','62','63')) for d in DIRECTIONS} for r in data}
    before = deepcopy(data)
    replay = replay_wti_gradient(data, config=cfg, second_tier_threshold_bps=D(45),
        layer_notional_usd=D(20), aggregate_max_notional_usd=D(100),max_layers=5,
        research_thresholds_by_sample=schedule)
    assert replay.live_authorization is False and data == before
    assert replay.config['max_sample_gap_seconds']==120 and replay.config['max_order_notional_usd']=='20'
    assert replay.config['roundtrip_cost_bps']=='10'
    assert replay.unresolved_position  # No assumed terminal flatten.
    gapped = data[:2]+[row(index=5,variational_qty='0.199',lighter_qty='0.199')]
    gap_schedule = {r['sample_id']:{d:tuple(D(x) for x in ('30','60','61','62','63')) for d in DIRECTIONS} for r in gapped}
    drained = replay_wti_gradient(gapped,config=cfg,second_tier_threshold_bps=D(45),
        layer_notional_usd=D(20),aggregate_max_notional_usd=D(100),max_layers=5,
        research_thresholds_by_sample=gap_schedule)
    assert drained.sample_gap_count==1
    assert drained.trades[0].exit_reason=='sample_gap_requires_position_drain'
    assert D(drained.trades[0].assumed_cost_usd)>0
    summary = _replay_sessions(gapped,{r['sample_id'] for r in gapped},[],gap_schedule,
                               CFG,HEALTH,[gapped[0]['session_id']])
    assert summary['forced_exit_count']==1 and summary['closed_trades']==1
    assert D(summary['indicative_closed_net_usd']) < 0 and summary['actual_pnl_usd'] is None
    invalid = [dict(r,var_quote_age_seconds='2.001') for r in data]
    rejected = replay_wti_gradient(invalid,config=cfg,second_tier_threshold_bps=D(45),
        layer_notional_usd=D(20),aggregate_max_notional_usd=D(100),max_layers=5,
        research_thresholds_by_sample=schedule)
    assert not rejected.unresolved_position and rejected.sample_count==0
    for bad in (('29','45','46','47','48'), ('30','45','44','47','48')):
        with pytest.raises(ValueError,match='research_thresholds_violate'):
            replay_wti_gradient(data,config=cfg,second_tier_threshold_bps=D(45),
                layer_notional_usd=D(20),aggregate_max_notional_usd=D(100),max_layers=5,
                research_thresholds_by_sample={r['sample_id']:{d:tuple(map(D,bad)) for d in DIRECTIONS} for r in data})
    missing = replay_wti_gradient(data,config=cfg,second_tier_threshold_bps=D(45),
        layer_notional_usd=D(20),aggregate_max_notional_usd=D(100),max_layers=5,
        research_thresholds_by_sample={})
    assert missing.completed_trades==0 and not missing.unresolved_position


def test_registered_split_is_three_train_two_holdouts_and_models_stay_frozen(monkeypatch):
    monkeypatch.setattr('tools.lib.wti_exit_shadow.profiles', fast_profiles)
    rows = [row(d,i) for d in range(5) for i in range(650)]
    before=deepcopy(rows)
    first=exit_shadow(rows,config=CFG,health=HEALTH,as_of=AS_OF)
    assert first['validation_status']=='split_ready_not_live_approved'
    assert len(first['training_sessions'])==3 and len(first['holdout_sessions'])==2
    changed=[row(d,i,lighter_sell_price='100.8',lighter_buy_price='100.81') if d>=3 else row(d,i)
             for d in range(5) for i in range(650)]
    second=exit_shadow(changed,config=CFG,health=HEALTH,as_of=AS_OF)
    assert first['training_sha256']==second['training_sha256']
    assert first['training_reports']==second['training_reports']
    assert first['input_sha256']!=second['input_sha256'] and first['eligible_for_live'] is False
    assert rows==before
    for model in first['exit_models'].values():
        for session in model['replay']['sessions']:
            cfg=session['report']['config']
            assert cfg['max_order_notional_usd']=='20' and cfg['roundtrip_cost_bps']=='10'
            assert cfg['max_completed_roundtrips']==1 and cfg['max_layers']==5


def test_four_sessions_are_diagnostic_not_a_relaxed_validation(monkeypatch):
    monkeypatch.setattr('tools.lib.wti_exit_shadow.profiles',fast_profiles)
    rows=[row(d,i) for d in range(4) for i in range(100)]
    result=exit_shadow(rows,config=CFG,health=HEALTH,as_of=AS_OF)
    assert result['validation_status']=='insufficient_independent_sessions'
    assert result['missing_completed_sessions']==1 and len(result['holdout_sessions'])==1
    assert result['comparison_scope'].startswith('exploratory_only') and result['eligible_for_live'] is False
    with pytest.raises(ValueError,match='10bps'):
        exit_shadow(rows,config=CFG|{'roundtrip_cost_bps':'9'},health=HEALTH,as_of=AS_OF)


def test_unfinished_session_does_not_fill_the_missing_independent_holdout(monkeypatch):
    monkeypatch.setattr('tools.lib.wti_exit_shadow.profiles',fast_profiles)
    rows=[row(d,i) for d in range(4) for i in range(100)]
    ongoing=row(6,10)
    result=exit_shadow(rows+[ongoing],config=CFG,health=HEALTH,as_of=AS_OF)
    assert result['missing_completed_sessions']==1
    assert ongoing['session_id'] not in result['completed_sessions']
    assert ongoing['session_id'] not in result['holdout_sessions']


def test_cli_frozen_inputs_are_never_changed_and_no_network_is_used(tmp_path,capsys,monkeypatch):
    def denied(*args,**kwargs):
        raise AssertionError('network forbidden')
    monkeypatch.setattr('socket.create_connection',denied)
    folder=tmp_path/'oil_swap/basis_samples/WTI';folder.mkdir(parents=True)
    data=folder/'2026-10-23.jsonl';data.write_text(json.dumps(row())+'\n')
    state=tmp_path/'wti_live_state.json';state.write_text(json.dumps({'asset':'WTI','gradient_pilot':{'market_id':145,'multiplier':'1','config':CFG}}))
    health=tmp_path/'wti_live_risk_health.json';health.write_text(json.dumps(HEALTH|{'updated_at':AS_OF.isoformat()}))
    before={p:p.read_bytes() for p in (data,state,health)}
    assert main(['--snapshot-log-root',str(tmp_path)])==0
    report=json.loads(capsys.readouterr().out)
    assert report['orders_attempted'] is False and report['state_modified'] is False
    assert all(p.read_bytes()==b for p,b in before.items())
