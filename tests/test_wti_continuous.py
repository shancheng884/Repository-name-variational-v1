from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from types import SimpleNamespace
import asyncio
import pytest

from tools.lib.wti_continuous import ensure_continuous, position_subset
from tools.lib.wti_sizing import equity_gradient_slot_caps
from tools.lib.wti_live_strategy import decide_wti_gradient_action
from tools.lib.wti_execution import WTIExecutionStore
from tools.wti_live import parse_args, WTILiveRuntime
from test_wti_manual_trial import setup_trial, complete_round, CONFIG
from test_wti_execution import _executor, _sample, _session_for_sample, SimulatedWTIGateway
from test_wti_live_strategy import _row, _config


def test_equity_slots_match_eth_and_remove_only_exploratory_aggregate_cap():
    from main import v4_real_gradient_slot_caps
    for v,l in [('100','90'),('19','5'),('200','250')]:
        expected = v4_real_gradient_slot_caps(variational_equity_usd=D(v),lighter_equity_usd=D(l),
            child_notional_usd=D(20),max_venue_leverage=D(5))
        assert list(equity_gradient_slot_caps(variational_equity_usd=D(v),lighter_equity_usd=D(l),child_notional_usd=D(20))) == expected
    runtime=object.__new__(WTILiveRuntime)
    runtime.args=SimpleNamespace(gradient_live=True,continuous_manual_gradient=True,
        _validated_gradient_layer_notional=D(20),_validated_notional=D(20))
    runtime.manage_existing_only=False;runtime.experimental_exit_model={'enabled':True}
    runtime.last_risk={'risk_data_complete':True,'observed_at':datetime.now(timezone.utc).isoformat(),
                       'variational_equity_usd':'100','lighter_equity_usd':'90'}
    assert runtime._gradient_equity_capacity_by_tier() == tuple(map(D,['80','180','260','360','440']))
    runtime.args.continuous_manual_gradient=False
    assert runtime._gradient_equity_capacity_by_tier() == (D(20),)*5


def test_manual_continuous_cli_requires_explicit_adoption_and_excludes_rotation():
    base=['--mode','live','--gradient-live','--confirm-bounded-gradient-pilot','--entry-threshold-bps','30',
          '--gradient-second-tier-bps','45','--exit-target-bps','1','--roundtrip-cost-bps','10',
          '--continuous-manual-gradient','--confirm-continuous-manual-gradient','--adopt-gradient-pilot','a'*32,
          '--keep-collecting-after-pilot']
    assert parse_args(base).continuous_manual_gradient
    for extra in [['--rotation-test-id','b'*32],['--manual-wti-trial-id','b'*32],['--max-notional-usd','100']]:
        with pytest.raises(SystemExit): parse_args(base+extra)


def test_adoption_and_many_rounds_preserve_history_without_asset_rotation(tmp_path):
    store,opts=setup_trial(tmp_path); root=opts['root_pilot_id']; cfg=CONFIG|{'manual_continuous_equity':True}
    complete_round(store)
    for i in range(6):
        state=ensure_continuous(store,root_pilot_id=root,config=cfg,runtime_pid=123,runtime_run_id='this-run')
        assert state['manual_wti_trial']['status']=='superseded_by_continuous_gradient'
        previous=state['gradient_pilot']['pilot_id']
        store.initialize_gradient_pilot(market_id=145,multiplier=D(1),config=cfg,max_layers=5,
            manual_new_pilot_after=previous,runtime_pid=123,runtime_run_id='this-run')
        state=ensure_continuous(store,root_pilot_id=root,config=cfg,runtime_pid=123,runtime_run_id='this-run')
        current=state['gradient_pilot']['pilot_id']
        assert current!=previous and state.get('gradient_rotation_session') is None
        before=store.path.read_bytes()
        ensure_continuous(store,root_pilot_id=root,config=cfg,runtime_pid=123,runtime_run_id='this-run')
        assert store.path.read_bytes()==before
        complete_round(store)
    assert len(store.load()['gradient_pilot_history'])==6


@pytest.mark.parametrize('change',[{'active_order_count':1},{'runtime_run_id':'old'},
    {'observed_at':(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()}, {'variational_qty':'1'}])
def test_adoption_refuses_stale_or_mismatched_account_without_writes(tmp_path,change):
    store,opts=setup_trial(tmp_path);complete_round(store,**change);before=store.path.read_bytes()
    with pytest.raises(ValueError):
        ensure_continuous(store,root_pilot_id=opts['root_pilot_id'],config=CONFIG|{'manual_continuous_equity':True},
                          runtime_pid=123,runtime_run_id='this-run')
    assert store.path.read_bytes()==before


def test_active_continuous_session_does_not_repeat_first_adoption_proof_check(tmp_path):
    store,opts=setup_trial(tmp_path);complete_round(store)
    cfg=CONFIG|{'manual_continuous_equity':True}
    adopted=ensure_continuous(store,root_pilot_id=opts['root_pilot_id'],config=cfg,
                              runtime_pid=123,runtime_run_id='this-run')
    assert adopted['manual_continuous_gradient']['status']=='active'
    adopted['last_account_reconciliation'].update(
        runtime_pid=999,runtime_run_id='previous-run',
        observed_at=(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat(),
    )
    store.save(adopted)
    before=store.path.read_bytes()

    current=ensure_continuous(store,root_pilot_id=opts['root_pilot_id'],config=cfg,
                              runtime_pid=123,runtime_run_id='this-run')

    assert current['manual_continuous_gradient']['status']=='active'
    assert store.path.read_bytes()==before


def two_layers(tmp_path, gateway=None):
    sample=_sample(variational_qty='2',lighter_qty='2')
    ex,gateway=_executor(tmp_path,sample=sample,gateway=gateway)
    pilot=ex.store.initialize_gradient_pilot(market_id=145,multiplier=D(1),config=CONFIG|{'manual_continuous_equity':True},max_layers=5)
    for tier in (1,2):
        ex.open(sample=sample,direction='long_var_short_lighter',notional_usd=D(20),session=_session_for_sample(sample),
                layer_id='layer-'+str(tier),gradient_tier=tier,gradient_pilot_id=pilot['pilot_id'],aggregate_max_notional_usd=D(100))
    return ex,gateway,sample


def test_partial_close_retains_lower_tier_and_restart_never_submits_again(tmp_path):
    ex,gateway,sample=two_layers(tmp_path)
    before=ex.store.load();lower=position_subset(before['open_position'],['layer-1'])
    result=ex.close(sample=sample,session=_session_for_sample(sample),decision={'reason':'max_hold','close_layer_ids':['layer-2']})
    assert result['status']=='open' and result['open_position']==lower
    assert len(gateway.requests)==6
    assert result['gradient_pilot']['layers_opened'][0]['layer_id']=='layer-1'
    assert result['last_closed_position']['layers'][0]['layer_id']=='layer-2'
    ex.reconcile(sample=sample,session=_session_for_sample(sample))
    assert len(gateway.requests)==6
    final=ex.close(sample=sample,session=_session_for_sample(sample))
    assert final['status']=='flat' and len(final['closed_positions'])==2


def test_partial_close_missing_hedge_recovers_exactly_once(tmp_path):
    ex,gateway,sample=two_layers(tmp_path)
    old=ex._submit_leg
    def interrupted(state,**kwargs):
        if kwargs['reduce_only'] and kwargs['venue']=='lighter': raise RuntimeError('simulated_process_death')
        return old(state,**kwargs)
    ex._submit_leg=interrupted
    with pytest.raises(RuntimeError):
        ex.close(sample=sample,session=_session_for_sample(sample),decision={'reason':'max_hold','close_layer_ids':['layer-2']})
    assert len(gateway.requests)==5
    ex._submit_leg=old
    state=ex.reconcile(sample=sample,session=_session_for_sample(sample))
    assert state['status']=='open' and not state['pending_action'] and len(gateway.requests)==6
    ex.reconcile(sample=sample,session=_session_for_sample(sample))
    assert len(gateway.requests)==6


def test_partial_profit_chooses_highest_profitable_tier_not_losing_lower_tier(tmp_path):
    ex,_,_=two_layers(tmp_path);s=ex.store.load();p=s['open_position'];now=datetime.now(timezone.utc)
    for layer in p['layers']:
        layer.update(session_id='session-1',opened_at=(now-timedelta(seconds=30)).isoformat(),
                     var_entry_price='70',lighter_entry_price='71' if layer['tier']==2 else '69')
    p=position_subset(p,['layer-1','layer-2']);p['session_id']='session-1'
    decision=decide_wti_gradient_action(_row(now,var_bid='70',var_ask='70.01',lighter_buy_price='70.5'),p,
        s['gradient_pilot'],config=_config(entry_threshold_bps=D(30),exit_target_bps=D(1),roundtrip_cost_bps=D('3.5')),now=now)
    assert decision['action']=='close' and decision['close_layer_ids']==['layer-2']


def test_partial_unknown_submitted_leg_never_retries(tmp_path):
    ex,gateway,sample=two_layers(tmp_path)
    old=ex._submit_leg
    def unknown(state,**kwargs):
        result=old(state,**kwargs)
        if kwargs['reduce_only'] and kwargs['venue']=='lighter':
            leg=state['pending_action']['legs']['lighter']
            leg['result']={'classification':'unknown','terminal_confirmed':False,'status':'unknown'}
            ex.store.save(state)
            return leg['result']
        return result
    ex._submit_leg=unknown
    ex.close(sample=sample,session=_session_for_sample(sample),decision={'reason':'max_hold','close_layer_ids':['layer-2']})
    gateway.lookup_order=lambda *args,**kwargs:{'classification':'unknown','terminal_confirmed':False}
    before=len(gateway.requests)
    for _ in range(3): ex.reconcile(sample=sample,session=_session_for_sample(sample))
    assert len(gateway.requests)==before
    assert ex.store.load()['status']=='manual_review_required'


def test_continuous_runtime_rearms_without_rotator_and_reuses_current_account_proof(tmp_path):
    store,opts=setup_trial(tmp_path);complete_round(store)
    runtime=object.__new__(WTILiveRuntime)
    runtime.args=SimpleNamespace(continuous_manual_gradient=True,adopt_gradient_pilot=opts['root_pilot_id'])
    runtime.manage_existing_only=False;runtime.entry_authorization_permanently_revoked=False
    runtime._poll_rotation_drain=lambda:False;runtime.pilot_store=store
    runtime.run_id='this-run';runtime.market_metadata={'multiplier':'1'}
    runtime._gradient_pilot_config=lambda:CONFIG|{'manual_continuous_equity':True}
    runtime._reset_gradient_confirmation=lambda:None
    import os
    state=store.load();state['last_account_reconciliation']['runtime_pid']=os.getpid();store.save(state)
    state=asyncio.run(runtime._advance_continuous_gradient_round(state))
    first=state['gradient_pilot']['pilot_id']
    state=asyncio.run(runtime._advance_continuous_gradient_round(state))
    assert state['gradient_pilot']['pilot_id']==first and not state.get('gradient_rotation_session')


def test_eth_trade_and_daily_templates_keep_costs_unknown_instead_of_zero():
    from tools.lib.wti_reporting import trade_message
    from tools.daily_pnl_report import build_platform_daily_message
    from test_wti_reporting import execution
    from datetime import date
    e=execution('a','open'); e['decision']={'gradient_tier':2,'edge_bps':'60'}
    text=trade_message(e,{'execution_history':[e]}, {'updated_at':datetime.now(timezone.utc).isoformat(),
        'variational_equity_usd':'100','lighter_equity_usd':'90'})
    assert '[Var/Lighter] 开仓/加仓确认' in text and '当前档位：2 / 5' in text
    assert '资产：WTI' in text and '已开子单：未核实 / 未核实' in text
    close=execution('b','close')
    state={'asset':'WTI','status':'flat','execution_history':[e,close]}
    text=build_platform_daily_message(None,state,day=date(2026,10,29),asset='WTI')
    assert '平仓子单：1 笔' in text and '平仓批次' not in text
    assert '确认净盈亏：未核实' in text and '最新双边权益：未核实' in text


def test_daily_snapshot_does_not_turn_intraday_equity_into_opening_capital(tmp_path):
    from tools.lib.wti_reporting import TradeReporter
    from tools.lib.runtime_files import write_json_atomic
    at=datetime.now(timezone.utc).replace(hour=10)
    # Use an actual current timestamp to test admission, then check the explicit opening flag.
    at=datetime.now(timezone.utc)
    h=tmp_path/'health.json';write_json_atomic(h,{'account_snapshot_observed_at':at.isoformat(),
        'risk_data_complete':True,'variational_equity_usd':'100','lighter_equity_usd':'90'})
    reporter=TradeReporter(tmp_path/'outbox.json',{},object(),health_path=h);reporter.poll({})
    day=(at+timedelta(hours=8)).date().isoformat()
    snapshot=reporter.outbox['daily_capitals'][day]
    assert snapshot['capital_usd']=='190'
    local=at+timedelta(hours=8)
    assert snapshot['day_open_verified']==(local.hour==0 and local.minute<2)


def test_real_decimal_margin_metrics_are_json_safe_in_health(tmp_path,monkeypatch):
    import json
    from test_wti_live_runtime import test_health_separates_collector_age_from_decision_age_and_expires_authorization
    original=WTILiveRuntime._write_health
    def publish(runtime,**kwargs):
        runtime.last_risk.update(variational_maintenance_margin_usage_pct=D('1.25'),
                                 lighter_maintenance_margin_usage_pct=D('2.50'))
        original(runtime,**kwargs)
        h=json.loads(runtime.args.health_path.read_text())
        assert h['variational_maintenance_margin_usage_pct']=='1.25'
        assert h['lighter_maintenance_margin_usage_pct']=='2.50'
    monkeypatch.setattr(WTILiveRuntime,'_write_health',publish)
    test_health_separates_collector_age_from_decision_age_and_expires_authorization(tmp_path)


def test_daily_timer_has_beijing_midnight_schedule_without_watchdog_dependency():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    timer=(root/'deploy/systemd/var-lighter-daily-pnl.timer').read_text()
    service=(root/'deploy/systemd/var-lighter-daily-pnl.service').read_text()
    assert 'OnCalendar=*-*-* 16:01:00 UTC' in timer
    assert '--asset WTI' in service
    assert 'watchdog' not in timer+service
