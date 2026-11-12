import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main as app
from tools.lib import median_wti
from tools.lib.frozen_median import LONG
from tools.live import build_main_command, load_config
from tools.wti_median_live import build_command
from variational.listener import VariationalMonitor
from test_frozen_median_live import loop_runtime, IntentCaptured


def runtime(tmp_path, monkeypatch):
    command=build_command(tmp_path)
    monkeypatch.setattr(sys,"argv",command[1:])
    rt=app.VariationalToLighterRuntime(app.parse_args())
    rt.sync_live_inventory_memory_from_state()
    rt.ticker=rt.variational_ticker="WTI"
    rt.lighter_market_index=145
    return rt


def instrument(**changes):
    return {"underlying":"USOILP","instrument_type":"swap","kind":"commodity",
            "settlement_asset":"USDC","funding_interval_s":0,**changes}


def health_file(tmp_path, now, **changes):
    h={"updated_at":now.isoformat(),"status":"running","variational_underlying":"USOILP",
       "market_status":"open","session_status":"open","market_status_feed_connected":True,
       "market_status_feed_heartbeat_at":now.isoformat(),"session_status_observed_at":now.isoformat(),
       "session_schedule_source":"variational_supported_assets_api","session_id":"session",
       "session_closing_at":(now+timedelta(hours=2)).isoformat(),**changes}
    p=tmp_path/"oil_health.json";p.write_text(json.dumps(h));return p


def test_launcher_keeps_every_eth_trade_parameter_except_asset_and_storage(tmp_path, monkeypatch):
    actual=build_command(tmp_path)
    cfg=replace(load_config(tmp_path/"live_config.json"),v4_live_mode=True,
                reversion_mode=False,calibration_mode=False,max_cycles=0)
    expected=build_main_command("ETH",cfg,v4_real_gradient=True,v4_bidirectional=True,v4_continuous=True)
    expected.append("--live-inventory-frozen-median")
    for flag in ("--live-inventory-basis-min-exit-pnl-bps","--live-inventory-basis-min-signal-reverted-exit-pnl-bps"):
        expected[expected.index(flag)+1]="1"
    expected[expected.index("--live-allowed-assets")+1]="WTI"
    assert actual[:-4]==expected
    rt=runtime(tmp_path,monkeypatch)
    assert rt.live_inventory_lot_notional_usd==20
    assert rt.frozen_median_model.distances==tuple(map(D,("5.6","6.6","7.6","8.6","9.6")))
    assert rt.runtime.monitor.asset_aliases=={"USOILP":"WTI"}
    assert not rt.frozen_median_model.history


def test_launcher_resumes_pending_identity_without_resetting_state(tmp_path):
    p=tmp_path/"log/wti_median/live_inventory_state.json";p.parent.mkdir(parents=True)
    original={"asset":"WTI","status":"pending","open_lots":[],"pending_actions":[{"rfq_id":"same-id"}]}
    p.write_text(json.dumps(original))
    cmd=build_command(tmp_path)
    assert "--live-inventory-i-accept-open-state-resume" in cmd
    assert json.loads(p.read_text())==original


def test_aliases_are_per_runtime_and_do_not_promote_wrong_contract():
    monitor=VariationalMonitor(asset_aliases={"USOILP":"WTI"})
    good={"instrument":instrument(),"bid":"100","ask":"100.006"}
    assert monitor._update_quote(good)
    assert monitor.signal_quotes["WTI"]["asset"]=="WTI"
    assert not monitor._update_quote({**good,"instrument":instrument(instrument_type="perpetual_future")})
    assert monitor.indicative_quote_version("WTI")==1
    other=VariationalMonitor();other._update_quote(good)
    assert "USOILP" in other.quotes and "WTI" not in other.quotes
    trade={"type":"fill","instrument":instrument(instrument_type="perpetual_future"),
           "side":"buy","qty":".2","price":"100","trade_id":"wrong"}
    assert monitor._update_trade_event(trade) is None
    assert not monitor.trade_events
    assert monitor._update_trade_event({**trade,"instrument":instrument(),"trade_id":"right"})
    assert monitor.trade_events[-1]["asset"]=="WTI"


def test_wti_journal_does_not_adopt_eth_history(tmp_path,monkeypatch):
    root=tmp_path/"log/wti_median/median_samples";root.mkdir(parents=True)
    import time
    row={"schema":1,"at":time.time(),"source_at":time.time(),"event_id":"eth-event",
         "instrument":"ETH:USDC:passive_indicative_bid_ask","size_key":"base",
         "var_bid":"100","var_ask":"100.01","lighter_bid":"100","lighter_ask":"100.01",
         "lighter_at":time.time()}
    (root/"today.jsonl").write_text(json.dumps(row)+"\n")
    rt=runtime(tmp_path,monkeypatch)
    assert not rt.frozen_median_model.history


def test_wti_passive_pair_needs_no_reference_ws_or_new_rfq(tmp_path,monkeypatch):
    async def run():
        rt=runtime(tmp_path,monkeypatch)
        rt.runtime.monitor._update_quote({"instrument":instrument(),"bid":"100","ask":"100.006"})
        rt.fetch_live_inventory_basis_quote=AsyncMock(side_effect=AssertionError("no extra RFQ"))
        quote,cost=await rt.get_live_inventory_basis_quote(asset="WTI",qty=D(".2"),priority="background")
        assert quote["quote_asset"]=="WTI" and quote["bid"]=="100"
        assert quote["quote_semantics"]=="passive_indicative_bid_ask"
        assert quote["rfq_consumed"] is False and cost==0
        rt.fetch_live_inventory_basis_quote.assert_not_called()
    asyncio.run(run())


@pytest.mark.parametrize("changes",[{"instrument_type":"perpetual_future"},{"funding_interval_s":3600},
    {"kind":"crypto"},{"settlement_asset":"USDT"}])
def test_position_identity_fails_closed(changes):
    p={"result":{"positions":[{"instrument":instrument(**changes),"qty":".2"}]}}
    assert app.VariationalToLighterRuntime.extract_variational_position_qty(p,asset="WTI") is None


def test_positions_and_queries_use_swap_not_perpetual(tmp_path,monkeypatch):
    async def run():
        rt=runtime(tmp_path,monkeypatch)
        p={"result":{"positions":[{"instrument":instrument(),"qty":"-.2"}]}}
        assert rt.extract_variational_position_qty(p,asset="WTI")==D("-.2")
        rt.send_variational_command=AsyncMock(return_value={"ok":True})
        await rt.fetch_variational_orders(asset="WTI",status="pending")
        assert "instrument" not in rt.send_variational_command.call_args.kwargs["payload"]
        await rt.send_variational_place_order(asset="WTI",side="BUY",amount=".2",
            expected_min_btc_qty=None,confirm=False)
        q=rt.send_variational_command.call_args.kwargs["payload"]
        assert {k:q[k] for k in median_wti.request_instrument("WTI")}==median_wti.request_instrument("WTI")
        assert q["type"]=="VAR_API_QUOTE" and q["confirm"] is False
    asyncio.run(run())


@pytest.mark.parametrize("case",["open","cutoff","closed","stale","unknown"])
def test_session_guards_preserve_management_and_deny_unsafe_entries(tmp_path,case):
    now=datetime.now(timezone.utc);changes={}
    if case=="cutoff":changes["session_closing_at"]=(now+timedelta(minutes=20)).isoformat()
    if case=="closed":changes["market_status"]="closed"
    if case=="stale":changes["updated_at"]=(now-timedelta(seconds=31)).isoformat()
    if case=="unknown":changes["market_status_feed_connected"]=False
    rt=SimpleNamespace(live_inventory_frozen_median=True,live_allowed_assets={"WTI"},
        median_wti_session_health_path=health_file(tmp_path,now,**changes))
    gate=median_wti.session(rt,now)
    assert gate["entry_allowed"]==(case=="open")
    assert gate["management_allowed"]==(case in ("open","cutoff"))


def test_wti_startup_refuses_active_orders_and_eth_exposure(tmp_path,monkeypatch):
    async def run():
        rt=runtime(tmp_path,monkeypatch)
        rt.fetch_all_variational_pending_orders=AsyncMock(return_value=[{"order_id":"existing"}])
        rt.fetch_lighter_active_orders=AsyncMock(return_value=[])
        with pytest.raises(RuntimeError,match="active orders"):
            await rt.reconcile_live_inventory_startup_state()
        rt.fetch_all_variational_pending_orders.return_value=[]
        rt.fetch_variational_positions=AsyncMock(return_value={"ok":True,"result":{"positions":[
            {"instrument":{"underlying":"ETH"},"qty":"1"}]}})
        rt.fetch_lighter_account=AsyncMock(return_value={"code":200,"accounts":[{"positions":[]}]})
        with pytest.raises(RuntimeError,match="ETH positions"):
            await rt.reconcile_live_inventory_startup_state()
    asyncio.run(run())


def test_wti_real_loop_persists_same_pending_identity_before_concurrent_legs(tmp_path,monkeypatch):
    async def run():
        rt,snapshot=loop_runtime(tmp_path,monkeypatch,D(22))
        rt.live_allowed_assets={"WTI"};rt.ticker=rt.variational_ticker="WTI";snapshot.asset="WTI"
        rt.frozen_median_model.signature=("WTI:USDC:passive_indicative_bid_ask","base")
        rt.median_wti_session_health_path=health_file(tmp_path,datetime.now(timezone.utc))
        for method in (rt.get_live_inventory_basis_quote,rt.fetch_live_inventory_basis_quote):
            original=method.side_effect
            def quote(original=original,**kw):
                q,ms=original(**kw);q["quote_asset"]="WTI";q["instrumentType"]="swap";return q,ms
            method.side_effect=quote
        save=rt.persist_live_inventory_memory
        async def persist(**kw):
            await save(**kw)
            if kw["reason"]=="basis_entry_submission_started":raise IntentCaptured()
        rt.persist_live_inventory_memory=persist
        with pytest.raises(IntentCaptured):await rt.maybe_run_live_inventory_basis(snapshot)
        state=json.loads(rt.live_inventory_state_file.read_text())
        assert state["asset"]=="WTI" and len(state["pending_actions"])==1
        assert len(state["frozen_median"]["orders"])==1
        rt.send_variational_place_order.assert_not_called()
        rt.place_lighter_order_from_plan.assert_not_called()
        rt.sync_live_inventory_memory_from_state()
        await rt.maybe_run_live_inventory_basis(snapshot)
        assert len(rt.frozen_median_model.orders)==1
        rt.send_variational_place_order.assert_not_called()
    asyncio.run(run())


def test_wti_per_tier_close_keeps_unselected_lots_and_never_repeats_pending(tmp_path,monkeypatch):
    import test_frozen_median_live as shared
    original=shared.loop_runtime
    def wti_loop(*args):
        rt,snapshot=original(*args)
        rt.live_allowed_assets={"WTI"};rt.ticker=rt.variational_ticker="WTI";snapshot.asset="WTI"
        rt.frozen_median_model.signature=("WTI:USDC:passive_indicative_bid_ask","base")
        rt.median_wti_session_health_path=health_file(tmp_path,datetime.now(timezone.utc))
        for method in (rt.get_live_inventory_basis_quote,rt.fetch_live_inventory_basis_quote):
            callback=method.side_effect
            def quote(callback=callback,**kw):
                q,ms=callback(**kw);q["quote_asset"]="WTI";q["instrumentType"]="swap";return q,ms
            method.side_effect=quote
        return rt,snapshot
    monkeypatch.setattr(shared,"loop_runtime",wti_loop)
    shared.test_real_loop_closes_only_selected_high_tier_with_a_fresh_rfq(tmp_path,monkeypatch)


def test_wti_native_daily_uses_existing_template_and_separate_ledger(tmp_path,monkeypatch,capsys):
    from tools import daily_pnl_report as daily
    from tools.lib.pnl_baseline import new_pnl_baseline
    baseline=new_pnl_baseline(asset="WTI",realized_pnl_usd="0",completed_cycles=0,
                              started_at="2026-11-12T00:00:00+00:00")
    p=tmp_path/"pnl.json";p.write_text(json.dumps(baseline))
    monkeypatch.setattr(daily,"send_platform_daily",lambda *a:pytest.fail("legacy WTI ledger"))
    monkeypatch.setattr(sys,"argv",["report","--asset","WTI","--native-inventory","--dry-run",
        "--day","2026-11-12","--baseline-path",str(p),"--state-path",str(tmp_path/"sent.json")])
    assert daily.main()==0
    out=capsys.readouterr().out
    assert "WTI" in out and "daily_pnl=DRY_RUN" in out
