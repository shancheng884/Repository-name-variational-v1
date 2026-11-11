import asyncio
import json
import logging
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main as app
from tools.lib import frozen_median_live as live
from tools.lib.frozen_median import LONG, SHORT, FrozenMedianGradient, Observation
from tools.live import LiveConfig, build_main_command
from test_auto_live_fuse import _eth_inventory_snapshot


def row(t, b=D(10), event=None):
    v = D(2000) * (1 - b / 20000)
    l = D(2000) * (1 + b / 20000)
    return Observation(t, t, event or str(t), "ETH:USDC:passive_indicative_bid_ask", "base",
                       v-D(".1"), v+D(".1"), l-D(".01"), l+D(".01"), t)


def fake_runtime(tmp_path, *, basis=D(22)):
    now = time.time()
    m = FrozenMedianGradient()
    m.signature = ("ETH:USDC:passive_indicative_bid_ask", "base")
    m.history = deque((int(now)-3600+i, D(10)) for i in range(3600))
    r = row(now, basis)
    m.observe(r, var_max_age=5, lighter_max_age=2)
    return SimpleNamespace(
        output_dir=tmp_path, live_inventory_frozen_median=True,
        frozen_median_model=m, frozen_median_last_row=r,
        frozen_median_rfq_context={}, live_inventory_basis_min_exit_pnl_bps=D(3),
        live_inventory_max_lighter_book_age_seconds=2,
        live_inventory_open_lots=[], pending_live_inventory_var_fill_matches=[],
        live_inventory_v4_exit_reconciliation_lot_ids=set(),
        live_inventory_v4_episode_id="ep-1",
    )


@pytest.mark.parametrize("direction,basis", [(LONG,D(22)),(SHORT,D(-2))])
def test_rfq_confirmation_uses_same_signed_edge_in_both_directions(tmp_path,direction,basis):
    rt = fake_runtime(tmp_path,basis=basis)
    r = rt.frozen_median_last_row
    le = r.lighter_bid if direction == LONG else r.lighter_ask
    lx = r.lighter_ask if direction == LONG else r.lighter_bid
    edge, threshold, levels, proof = live.rfq_confirmation(
        rt,direction=direction,var_bid=r.var_bid,var_ask=r.var_ask,
        lighter_entry=le,lighter_exit=lx,now=r.at,
    )
    assert D(proof['median_expected_net_bps']) > 3
    assert app.v4_edge_meets_threshold(edge,threshold,direction=direction)
    assert len(levels)==5
    assert proof['median_entry_execution_reserve_bps']=='2'
    assert proof['median_exit_execution_reserve_bps']=='2'
    # A source-to-RFQ shift affects executable prices, not an extra fee.
    moved = live.rfq_confirmation(rt,direction=direction,
        var_bid=r.var_bid+D('.4'),var_ask=r.var_ask+D('.4'),
        lighter_entry=le,lighter_exit=lx,now=r.at)[3]
    assert abs(D(moved['median_expected_net_bps'])-D(proof['median_expected_net_bps'])) < D('.01')


def test_wider_executable_spread_reduces_profit_once(tmp_path):
    rt=fake_runtime(tmp_path);r=rt.frozen_median_last_row
    def profit(width):
        return D(live.rfq_confirmation(rt,direction=LONG,
            var_bid=r.var_bid-width,var_ask=r.var_ask+width,
            lighter_entry=r.lighter_bid,lighter_exit=r.lighter_ask,now=r.at)[3]['median_expected_net_bps'])
    assert D('.99') < profit(D(0))-profit(D('.1')) < D('1.01')


def test_partial_source_or_old_reference_is_not_qualified_journal(tmp_path):
    rt=fake_runtime(tmp_path);r=rt.frozen_median_last_row
    live.observe(rt,asset='ETH',quote={'quote_semantics':'reference_price_only'},
        snapshot=SimpleNamespace(lighter_bid=r.lighter_bid,lighter_ask=r.lighter_ask),
        var_bid=r.var_bid,var_ask=r.var_ask,lighter_age=0,now=r.at)
    assert not list(tmp_path.glob('median_samples/*'))
    assert not rt.frozen_median_model.confirmations


def test_restore_never_adopts_legacy_position(tmp_path):
    rt=fake_runtime(tmp_path)
    with pytest.raises(ValueError,match='cannot_adopt'):
        live.initialize(rt,{'open_lots':[{'lot_id':1}]})


def test_closed_episode_cannot_reenter_until_account_flat_verified(tmp_path):
    rt=fake_runtime(tmp_path)
    m=rt.frozen_median_model;m.anchor=D(10);m.direction=LONG;m.episode_id='ep'
    m.orders={'id':{'lot_id':1,'tier':1,'state':'closed','generation':0}}
    assert live.active_tier(rt,direction=LONG,now=m.last_at)==0


def test_exit_confirmation_requires_distinct_quotes(tmp_path):
    rt=fake_runtime(tmp_path);lot={};t=rt.frozen_median_last_row.at
    assert not live.confirm_exit_signal(rt,lot,True,t)
    assert not live.confirm_exit_signal(rt,lot,True,t+.1)
    rt.frozen_median_last_row=row(t+1,D(10))
    assert live.confirm_exit_signal(rt,lot,True,t+1)
    assert not live.confirm_exit_signal(rt,lot,False,t+1.1)


def test_preflight_rejection_does_not_leave_a_fake_close(tmp_path):
    rt=fake_runtime(tmp_path);m=rt.frozen_median_model
    m.orders={'id':{'lot_id':1,'tier':1,'state':'closing','generation':0}}
    m.tiers['1']['armed']=False
    live.close_not_submitted(rt,{'median_order_id':'id','lot_id':1})
    assert m.orders['id']['state']=='open'
    assert m.tiers['1']['armed']


def test_final_fill_quantity_mismatch_cannot_unlock_tier(tmp_path):
    rt=fake_runtime(tmp_path);m=rt.frozen_median_model
    m.orders={'id':{'lot_id':1,'tier':1,'state':'closing','generation':0}}
    live.final_closed(rt,lot_id=1,quantities_match=False)
    assert m.orders['id']['state']=='closing'
    live.final_closed(rt,lot_id=1,quantities_match=True)
    assert m.orders['id']['state']=='closed'


def test_duplicate_close_and_risk_batch_recovery_are_idempotent(tmp_path):
    rt=fake_runtime(tmp_path);m=rt.frozen_median_model
    rt.live_inventory_entry_cost_confirmed=lambda lot:True
    m.orders={str(i):{'lot_id':i,'tier':i,'state':'open','generation':0} for i in (1,2)}
    lot={'lot_id':1,'portfolio_component_lot_ids':['1','2']}
    assert live.close_started(rt,lot)
    assert not live.close_started(rt,lot)
    assert all(o['state']=='closing' for o in m.orders.values())
    live.final_closed(rt,lot_id=1,quantities_match=False)
    assert all(o['state']=='closing' for o in m.orders.values())
    live.final_closed(rt,lot_id=1,quantities_match=True)
    live.final_closed(rt,lot_id=1,quantities_match=True)
    assert all(o['state']=='closed' for o in m.orders.values())


def test_missing_close_identity_cannot_submit_or_unlock(tmp_path):
    rt=fake_runtime(tmp_path)
    assert not live.close_started(rt,{'lot_id':99})
    assert rt.frozen_median_model.review_reason=='median_close_order_identity_missing'


def test_cold_runtime_reports_1s_warmup_not_old_7d_anchor(tmp_path,monkeypatch):
    async def run():
        rt,snapshot=loop_runtime(tmp_path,monkeypatch,D(22))
        rt.frozen_median_model.history.clear()
        rt.frozen_median_model.confirmations.clear()
        await rt.maybe_run_live_inventory_basis(snapshot)
        assert rt.live_inventory_basis_v4_history_reason=='median_1s_60m_warmup'
        rt.fetch_live_inventory_basis_quote.assert_not_called()
        rt.send_variational_place_order.assert_not_called()
    asyncio.run(run())


def test_real_startup_does_not_start_legacy_history_before_model_restore(tmp_path,monkeypatch):
    async def run():
        rt,_=loop_runtime(tmp_path,monkeypatch,D(22))
        del rt.frozen_median_model
        rt.setup_signal_handlers=lambda:None
        rt.run_startup_diagnostics=lambda:SimpleNamespace(blocking_errors=[])
        rt.print_startup_diagnostics=lambda d:None
        rt.log_startup_diagnostics=lambda d:None
        rt.print_startup_next_steps=lambda:None
        rt.telegram_notifier=None
        rt.append_live_inventory_run_config=AsyncMock()
        async def start_forwarder():
            assert rt.frozen_median_model.deviation==6
        rt.runtime.start=AsyncMock(side_effect=start_forwarder)
        rt.wait_for_variational_ready=AsyncMock(return_value=True)
        rt.requires_lighter_trading_credentials=lambda:False
        rt.requires_lighter_market_data=lambda:True
        rt.wait_for_ticker_resolution=AsyncMock(return_value='ETH')
        rt.activate_asset=AsyncMock()
        rt.lighter_prewarm_submit_ws=False
        rt.preflight_variational_api_command_client=AsyncMock()
        rt.prime_variational_page_streams=AsyncMock()
        rt.load_live_inventory_basis_v4_history=lambda **kw:pytest.fail('legacy history must not run')
        rt.refresh_live_inventory_basis_v4_history_loop=AsyncMock()
        rt.watch_variational_quote_freshness=AsyncMock()
        rt.reconcile_live_inventory_startup_state=AsyncMock(side_effect=IntentCaptured())
        with pytest.raises(IntentCaptured):
            await rt.run()
        assert rt.frozen_median_model.deviation==6
        assert len(rt.frozen_median_model.history)==0
        rt.refresh_live_inventory_basis_v4_history_loop.assert_not_called()
        rt.send_variational_place_order.assert_not_called()
        await rt.live_inventory_quote_watchdog_task
    asyncio.run(run())


def test_invalid_source_clears_current_signal_and_cannot_use_previous_quote(tmp_path):
    rt=fake_runtime(tmp_path);r=rt.frozen_median_last_row
    live.observe(rt,asset='ETH',quote={'quote_semantics':'reference_price_only'},
        snapshot=SimpleNamespace(lighter_bid=r.lighter_bid,lighter_ask=r.lighter_ask),
        var_bid=r.var_bid,var_ask=r.var_ask,lighter_age=0,now=r.at)
    assert rt.frozen_median_last_row is None
    with pytest.raises(ValueError,match='reference_not_ready'):
        live.rfq_confirmation(rt,direction=LONG,var_bid=r.var_bid,var_ask=r.var_ask,
            lighter_entry=r.lighter_bid,lighter_exit=r.lighter_ask,now=r.at)


def test_constructor_and_cli_keep_all_original_execution_guards(tmp_path,monkeypatch):
    cmd=build_main_command('ETH',LiveConfig(v4_live_mode=True,max_cycles=0),
        v4_real_gradient=True,v4_bidirectional=True,v4_continuous=True)
    monkeypatch.setattr(sys,'argv',cmd[1:]+['--live-inventory-frozen-median','--output-dir',str(tmp_path)])
    args=app.parse_args()
    assert args.live_inventory_basis_entry_mode=='concurrent'
    assert args.live_inventory_max_venue_leverage==5
    runtime=app.VariationalToLighterRuntime(args)
    runtime.sync_live_inventory_memory_from_state()
    assert runtime.live_inventory_frozen_median
    assert runtime.frozen_median_model.deviation==6
    assert runtime.live_inventory_lot_notional_usd==20
    assert runtime.live_inventory_max_venue_leverage==5


def test_state_restore_blocks_accidental_legacy_mode_restart(tmp_path):
    rt=fake_runtime(tmp_path)
    rt.live_inventory_frozen_median=False
    with pytest.raises(ValueError,match='require_frozen_median_mode'):
        live.initialize(rt,{'frozen_median':{},'open_lots':[{'lot_id':1}]})


@pytest.mark.parametrize('basis,direction', [(D(22),LONG),(D(-2),SHORT)])
def test_direction_selection_is_signed_not_largest_raw_subtraction(tmp_path,basis,direction):
    rt=fake_runtime(tmp_path,basis=basis)
    rt.live_inventory_basis_v4_entry_directions=lambda: (LONG,SHORT)
    rt.live_inventory_basis_v4_entry_direction=lambda: SHORT
    selected=app.VariationalToLighterRuntime.live_inventory_basis_v4_select_entry_direction(
        rt,signal_edges={LONG:basis,SHORT:basis},thresholds={LONG:D(16),SHORT:D(4)})
    assert selected==direction


@pytest.mark.parametrize('active',[False,True])
def test_only_verified_rollback_with_no_orders_resolves_pending(tmp_path,active):
    rt=fake_runtime(tmp_path);m=rt.frozen_median_model
    m.orders={'id':{'lot_id':1,'tier':1,'state':'pending','generation':0}}
    rt.fetch_all_variational_pending_orders=AsyncMock(return_value=[{}] if active else [])
    rt.fetch_lighter_active_orders=AsyncMock(return_value=[])
    assert asyncio.run(live.entry_rollback_verified(rt,asset='ETH',lot_id=1)) is not active
    assert m.orders['id']['state']==('pending' if active else 'closed')
    if not active:
        assert not m.tiers['1']['armed']


class IntentCaptured(BaseException):
    pass


def loop_runtime(tmp_path,monkeypatch,basis):
    cmd=build_main_command('ETH',LiveConfig(v4_live_mode=True,max_cycles=0),
        v4_real_gradient=True,v4_bidirectional=True,v4_continuous=True)
    monkeypatch.setattr(sys,'argv',cmd[1:]+['--live-inventory-frozen-median','--output-dir',str(tmp_path)])
    rt=app.VariationalToLighterRuntime(app.parse_args())
    rt.sync_live_inventory_memory_from_state()
    now=time.time();r=row(now,basis)
    model=rt.frozen_median_model
    model.signature=(r.instrument,r.size_key)
    model.history=deque((int(now)-3600+i,D(10)) for i in range(3599))
    model.observe(row(now-1,basis),var_max_age=5,lighter_max_age=2)
    snapshot=_eth_inventory_snapshot()
    snapshot.var_bid=r.var_bid;snapshot.var_ask=r.var_ask
    snapshot.var_mid=(r.var_bid+r.var_ask)/2
    snapshot.lighter_bid=r.lighter_bid;snapshot.lighter_ask=r.lighter_ask
    snapshot.lighter_mid=(r.lighter_bid+r.lighter_ask)/2
    snapshot.lighter_buy_fill_price=r.lighter_ask;snapshot.lighter_sell_fill_price=r.lighter_bid
    timestamp=datetime.now(timezone.utc).isoformat()
    def quote(qty,passive):
        return {'bid':str(r.var_bid),'ask':str(r.var_ask),'quoteId':'passive' if passive else 'rfq',
            'quoteTimestamp':timestamp,'source_quote_timestamp':timestamp,
            'received_at':timestamp,'received_monotonic':time.monotonic(),
            'reference_price':str((r.var_bid+r.var_ask)/2),
            'quote_asset':'ETH','quote_request_qty':str(qty),'quote_size_mode':'exact_base_qty_v1',
            'quote_source':'passive_browser_stream' if passive else 'api',
            'quote_semantics':'passive_indicative_bid_ask' if passive else 'executable_rfq',
            'raw':{'__indicative_quote_size':'base'}}
    rt.get_live_inventory_basis_quote=AsyncMock(side_effect=lambda **kw:(quote(kw['qty'],True),D(0)))
    rt.fetch_live_inventory_basis_quote=AsyncMock(side_effect=lambda **kw:(quote(kw['qty'],False),D(0)))
    rt.live_inventory_lighter_book_age_ok=lambda:(True,0)
    rt.maybe_recover_stale_variational_reference_feed=AsyncMock()
    rt.poll_live_inventory_maintenance_control=AsyncMock()
    rt.fetch_live_inventory_stablecoin_context=AsyncMock(return_value={})
    rt.live_inventory_account_risk_context=AsyncMock(return_value={
        'risk_action':'normal','risk_reason':'account_risk_normal',
        'variational_equity_usd':'100','lighter_equity_usd':'100'})
    async def depth(**kw):
        price=r.lighter_ask if kw['lighter_side']=='BUY' else r.lighter_bid
        return {'estimated_fill_price':str(price),'reference_price':str(price),
            'book_age_seconds':0,'book_exchange_timestamp_valid':True,'slippage_bps':'0',
            'requested_qty':str(kw['qty']),'filled_qty':str(kw['qty'])}
    rt.live_inventory_lighter_depth_context=depth
    rt.get_lighter_best_bid_ask=AsyncMock(return_value=(r.lighter_bid,r.lighter_ask))
    rt.live_inventory_profit_exit_passive_pnl=AsyncMock(return_value=D(20))
    rt.append_live_inventory_log=AsyncMock()
    rt.send_variational_place_order=AsyncMock(side_effect=AssertionError('must not submit'))
    rt.place_lighter_order_from_plan=AsyncMock(side_effect=AssertionError('must not submit'))
    rt.auto_live_lighter_precheck=AsyncMock(return_value=(True,'ok',D(0)))
    rt.auto_live_manual_review_required=False
    rt.live_inventory_v4_batch_entry_gate=lambda:(True,'ok',{})
    return rt,snapshot


@pytest.mark.parametrize('direction,basis',[(LONG,D(22)),(SHORT,D(-2))])
def test_real_signal_loop_persists_median_intent_before_either_concurrent_leg(
    tmp_path,monkeypatch,direction,basis,
):
    async def run():
        rt,snapshot=loop_runtime(tmp_path,monkeypatch,basis)
        original=rt.persist_live_inventory_memory
        async def persist(**kw):
            await original(**kw)
            if kw['reason']=='basis_entry_submission_started':
                raise IntentCaptured()
        rt.persist_live_inventory_memory=persist
        try:
            await rt.maybe_run_live_inventory_basis(snapshot)
        except IntentCaptured:
            pass
        else:
            pytest.fail(str([(c.args[0],c.args[1].get('reason')) for c in rt.append_live_inventory_log.call_args_list]))
        state=json.loads(rt.live_inventory_state_file.read_text())
        model=FrozenMedianGradient.restore(state['frozen_median'])
        assert model.direction==direction and model.anchor==10
        assert len(model.orders)==1
        assert next(iter(model.orders.values()))['state']=='pending'
        assert len(state['pending_actions'])==1
        rt.send_variational_place_order.assert_not_called()
        rt.place_lighter_order_from_plan.assert_not_called()
        # A restart never restores quote confirmations or resubmits the intent.
        rt.sync_live_inventory_memory_from_state()
        await rt.maybe_run_live_inventory_basis(snapshot)
        assert len(rt.frozen_median_model.orders)==1
        rt.send_variational_place_order.assert_not_called()
    asyncio.run(run())


def test_real_loop_closes_only_selected_high_tier_with_a_fresh_rfq(tmp_path,monkeypatch):
    async def run():
        rt,snapshot=loop_runtime(tmp_path,monkeypatch,D(10))
        model=rt.frozen_median_model
        model.anchor=D(10);model.direction=LONG;model.episode_id='ep'
        rt.live_inventory_v4_episode_id='ep'
        now=time.time()
        for lot_id,tier,basis in [(1,1,D(30)),(2,3,D(22))]:
            entry=row(now-100,basis)
            lot={'lot_id':lot_id,'median_order_id':str(lot_id),'median_anchor_bps':'10',
                'entry_gradient_tier':tier,'qty':'0.01','direction':LONG,'entry_var_side':'BUY',
                'entry_var_fill_price':str(entry.var_ask),'entry_lighter_fill_price':str(entry.lighter_bid),
                'entry_kind':'basis_v4_eth_short_p97_5','entry_cost_status':'final_fills_confirmed',
                'entered_at':datetime.fromtimestamp(now-100,timezone.utc).isoformat(),
                'entered_sample_index':1,'status':'open',
                'median_exit_signals':[['prior',now-1,True]]}
            rt.live_inventory_open_lots.append(lot)
            model.orders[str(lot_id)]={'lot_id':lot_id,'tier':tier,'state':'open','generation':0}
        original=rt.persist_live_inventory_memory
        async def persist(**kw):
            await original(**kw)
            if kw['reason']=='basis_exit_submission_started':
                raise IntentCaptured()
        rt.persist_live_inventory_memory=persist
        try:
            await rt.maybe_run_live_inventory_basis(snapshot)
        except IntentCaptured:
            pass
        else:
            pytest.fail(str([(c.args[0],c.args[1].get('reason')) for c in rt.append_live_inventory_log.call_args_list]))
        state=json.loads(rt.live_inventory_state_file.read_text())
        assert len(state['open_lots'])==2
        assert state['frozen_median']['orders']['1']['state']=='open'
        assert state['frozen_median']['orders']['2']['state']=='closing'
        assert len(state['pending_actions'])==1
        assert state['pending_actions'][0]['lot_id']==2
        assert rt.fetch_live_inventory_basis_quote.await_count==1
        rt.send_variational_place_order.assert_not_called()
        # An unresolved close restored from disk must not spend another RFQ
        # or submit a second close for the same component.
        rt.sync_live_inventory_memory_from_state()
        rt.frozen_median_model.orders['1']['state']='closing'
        await rt.maybe_run_live_inventory_basis(snapshot)
        assert rt.fetch_live_inventory_basis_quote.await_count==1
        rt.send_variational_place_order.assert_not_called()
    asyncio.run(run())


@pytest.mark.parametrize('age,reason',[(.3,'profit_exit_lighter_book_stale_before_submit'),(0,None)])
def test_profit_preflight_retains_book_age_and_uses_only_remaining_cost(tmp_path,monkeypatch,age,reason):
    async def run():
        rt,_=loop_runtime(tmp_path,monkeypatch,D(10))
        rt.live_inventory_basis_quote_reuse_validation=lambda **kw:(True,None,{})
        async def depth(**kw):
            return {'estimated_fill_price':'2001','reference_price':'2001',
                    'book_age_seconds':age,'book_exchange_timestamp_valid':True}
        rt.live_inventory_lighter_depth_context=depth
        result=await rt.live_inventory_profit_exit_preflight(asset='ETH',direction=LONG,
            qty=D('.01'),var_exit_price=D(2000),reuse_quote_id='rfq',context={
                'entry_var_price':D(1999),'entry_lighter_price':D(2002),
                'effective_min_exit_pnl_bps':D(5),'exit_reserve_bps':D(2)})
        assert result['reason']==reason
        assert result['ok'] is (reason is None)
        if result['ok']:
            assert D(result['effective_min_exit_pnl_bps'])==5
            assert D(result['exit_execution_reserve_bps'])==2
    asyncio.run(run())
