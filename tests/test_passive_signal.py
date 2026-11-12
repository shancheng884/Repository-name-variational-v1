import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main as app
from tools.lib import passive_signal
from tools.lib.frozen_median import FrozenMedianGradient, LONG
from variational.listener import VariationalMonitor, CommandBroker
from test_frozen_median_live import row, loop_runtime


def update(monitor, *, asset="ETH", bid="2000", ask="2000.2", timestamp=None):
    return monitor._update_quote({"asset": asset, "bid": bid, "ask": ask,
                                  "timestamp": timestamp})


def test_passive_update_wakes_waiter_without_timer_or_requests():
    async def run():
        monitor = VariationalMonitor()
        task = asyncio.create_task(monitor.wait_for_indicative_quote(
            "ETH", after_version=0, timeout=30))
        await asyncio.sleep(0)
        update(monitor)
        assert await asyncio.wait_for(task, .2) == 1
        assert monitor.quotes["ETH"]["quote_version"] == 1
        assert not monitor.recent_rest_responses
    asyncio.run(run())


def test_arrival_before_wait_is_not_lost_and_other_assets_do_not_wake():
    async def run():
        monitor = VariationalMonitor()
        update(monitor)
        assert await monitor.wait_for_indicative_quote("ETH", after_version=0, timeout=30) == 1
        task = asyncio.create_task(monitor.wait_for_indicative_quote(
            "ETH", after_version=1, timeout=30))
        await asyncio.sleep(0)
        update(monitor, asset="WTI")
        await asyncio.sleep(0)
        assert not task.done()
        update(monitor, bid="2001", ask="2001.2")
        assert await asyncio.wait_for(task, .2) == 2
    asyncio.run(run())


def test_duplicates_invalid_pairs_and_mid_stream_do_not_wake():
    async def run():
        monitor = VariationalMonitor()
        update(monitor, timestamp="2026-11-12T00:00:00Z")
        task = asyncio.create_task(monitor.wait_for_indicative_quote(
            "ETH", after_version=1, timeout=.02))
        await asyncio.sleep(0)
        assert not update(monitor, timestamp="2026-11-12T00:00:00Z")
        for bid, ask in (("2", "2"), ("3", "2"), ("NaN", "2"), ("1", "Infinity")):
            update(monitor, bid=bid, ask=ask)
        monitor._update_quote({"asset": "ETH", "price": "2001", "__source_stream": "/prices"})
        assert await task == 1
        assert monitor.indicative_quote_version("ETH") == 1
    asyncio.run(run())


def test_coalesced_wakeup_and_timer_fallback_do_not_replay_old_quotes():
    async def run():
        monitor = VariationalMonitor()
        rt = SimpleNamespace(live_inventory_frozen_median=True,
                             is_live_inventory_enabled=lambda: True,
                             runtime=SimpleNamespace(monitor=monitor),
                             variational_ticker="ETH", ticker="ETH", paper_interval_seconds=.01)
        passive_signal.begin_cycle(rt)
        for i in range(3):update(monitor, bid=str(2000+i), ask=str(2000.2+i))
        await passive_signal.wait_next(rt)
        assert rt.median_signal_trigger == "new_var_quote"
        assert rt.median_signal_coalesced_versions == 2
        passive_signal.begin_cycle(rt)
        await passive_signal.wait_next(rt)
        assert rt.median_signal_trigger == "timer"
    asyncio.run(run())


def test_real_paper_loop_is_serial_and_wakes_on_passive_quote(monkeypatch):
    async def run():
        rt = app.VariationalToLighterRuntime.__new__(app.VariationalToLighterRuntime)
        monitor = VariationalMonitor()
        rt.runtime = SimpleNamespace(monitor=monitor)
        rt.logger = logging.getLogger("passive_signal_loop")
        rt.live_inventory_frozen_median = True
        rt.live_inventory_basis_v4_mode = True
        rt.stop_flag = False
        rt.variational_ticker = rt.ticker = "ETH"
        rt.live_allowed_assets = {"ETH"}
        rt.paper_interval_seconds = 30
        rt.is_live_inventory_enabled = lambda: True
        rt.is_auto_live_enabled = rt.is_paper_mode = lambda: False
        rt.maybe_enforce_live_disk_guard = AsyncMock()
        rt.get_cross_spread_snapshot = AsyncMock(return_value=SimpleNamespace(asset="ETH"))
        for method in ("maybe_run_paper_inventory", "maybe_close_paper_position",
                       "maybe_enter_paper_position", "maybe_run_auto_live"):
            setattr(rt, method, AsyncMock())
        calls = 0
        in_cycle = False
        first = asyncio.Event()
        release = asyncio.Event()
        async def evaluate(snapshot):
            nonlocal calls, in_cycle
            assert not in_cycle
            in_cycle = True
            calls += 1
            if calls == 1:
                first.set()
                await release.wait()
            else:
                rt.stop_flag = True
            in_cycle = False
        rt.maybe_run_live_inventory = evaluate
        task = asyncio.create_task(rt.paper_loop())
        await asyncio.wait_for(first.wait(), .5)
        update(monitor)
        update(monitor, bid="2001", ask="2001.2")
        await asyncio.sleep(0)
        assert calls == 1
        release.set()
        # Exit without a 30-second shutdown sleep; cancellation is normal.
        for _ in range(100):
            if calls == 2:break
            await asyncio.sleep(.001)
        assert calls == 2
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
    asyncio.run(run())


def test_quote_audit_preserves_captured_book_and_does_not_double_confirm(tmp_path, monkeypatch):
    now = time.time()
    rt = SimpleNamespace(live_inventory_run_id="run", live_inventory_sample_index=1,
                         variational_ticker="ETH", ticker="ETH", output_dir=tmp_path,
                         frozen_median_model=FrozenMedianGradient(deviation_bps=D("5.6"),
                             distances=tuple(map(D, ("5.6","6.6","7.6","8.6","9.6")))))
    model = rt.frozen_median_model
    model.history = deque((int(now)-3600+i,D(10)) for i in range(3599))
    r = row(now, D(16))
    model.observe(r, var_max_age=5, lighter_max_age=2)
    before = list(model.confirmations)
    mono = time.monotonic()
    snapshot = SimpleNamespace(lighter_bid=r.lighter_bid, lighter_ask=r.lighter_ask,
        lighter_quote_audit={"version": 7, "received_monotonic": mono-.1,
                             "received_at": "book-receive", "book_exchange_frame_at": "exchange-time"})
    quote = {"bid": str(r.var_bid), "ask": str(r.var_ask),
             "passive_indicative_version": 3, "passive_indicative_received_monotonic": mono-.2,
             "passive_indicative_received_at": "var-receive"}
    context = passive_signal.pair_audit(rt, quote, snapshot)
    snapshot.lighter_quote_audit["version"] = 8
    passive_signal.record_decision(rt, context)
    passive_signal.record_decision(rt, context)
    rows = [json.loads(x) for x in next((tmp_path/"median_quote_audit").glob("*.jsonl")).read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["var_quote"]["version"] == 3
    assert rows[0]["lighter_book"]["version"] == 7
    assert rows[0]["directions"][LONG]["first_tier_confirmations"] == 1
    assert rows[0]["directions"][LONG]["active_tier"] == 0
    assert rows[0]["rfq_consumed"] is False
    assert model.confirmations == deque(before, maxlen=3)


def test_captured_old_book_is_not_refreshed_by_later_update(tmp_path, monkeypatch):
    async def run():
        rt, snapshot = loop_runtime(tmp_path, monkeypatch, D(22))
        snapshot.lighter_quote_audit = {"version":1, "received_monotonic":time.monotonic()-3,
            "captured_monotonic":time.monotonic()-3, "book_age_seconds":0}
        await rt.maybe_run_live_inventory_basis(snapshot)
        rt.fetch_live_inventory_basis_quote.assert_not_called()
        rt.send_variational_place_order.assert_not_called()
        assert any(c.args[1].get("reason")=="basis_lighter_book_too_old"
                   for c in rt.append_live_inventory_log.call_args_list)
    asyncio.run(run())


def test_rest_message_path_preserves_source_clock_and_wakes():
    async def run():
        monitor = VariationalMonitor()
        task = asyncio.create_task(monitor.wait_for_indicative_quote(
            "ETH", after_version=0, timeout=30))
        await asyncio.sleep(0)
        await monitor.process_rest_event({
            "kind":"rest_response", "url":"https://omni.variational.io/api/quotes/indicative",
            "status":200, "body":json.dumps({"asset":"ETH", "bid":"2000",
                "ask":"2000.2", "timestamp":"2026-11-12T00:00:00Z"}),
        })
        assert await asyncio.wait_for(task, .2) == 1
        q = monitor.quotes["ETH"]
        assert q["timestamp"] == "2026-11-12T00:00:00Z"
        assert q["received_at"] != q["timestamp"]
    asyncio.run(run())


def test_actual_snapshot_captures_prices_and_book_version_atomically(tmp_path, monkeypatch):
    async def run():
        rt, prior = loop_runtime(tmp_path, monkeypatch, D(12))
        rt.get_variational_reference_quote = AsyncMock(return_value={
            "asset":"ETH", "bid":"2000", "ask":"2000.2", "reference_price":"2000.1"})
        rt.lighter_best_bid, rt.lighter_best_ask = D("2001"), D("2001.2")
        rt.lighter_order_book_offset = 777
        rt.record_lighter_order_book_clock({"timestamp":time.time()})
        rt.estimate_lighter_fill_price = AsyncMock(return_value=None)
        captured = await rt.get_cross_spread_snapshot()
        rt.lighter_order_book_offset = 778
        rt.lighter_best_bid = D("2002")
        rt.record_lighter_order_book_clock({"timestamp":time.time()})
        assert captured.lighter_bid == D("2001")
        assert captured.lighter_quote_audit["version"] == 1
        assert captured.lighter_quote_audit["offset"] == 777
        assert captured.lighter_quote_audit["book_exchange_timestamp_valid"] is True
        assert rt.lighter_signal_book_version == 2
    asyncio.run(run())


def test_legacy_scheduler_remains_timer_only():
    async def run():
        rt = SimpleNamespace(live_inventory_frozen_median=False, paper_interval_seconds=.001)
        passive_signal.begin_cycle(rt)
        await passive_signal.wait_next(rt)
        assert not hasattr(rt, "median_signal_trigger")
    asyncio.run(run())


@pytest.mark.parametrize("error", ["missing", "future", "invalid"])
def test_captured_bad_clock_cannot_borrow_validity_from_new_frame(tmp_path, monkeypatch, error):
    async def run():
        rt, snapshot = loop_runtime(tmp_path, monkeypatch, D(22))
        snapshot.lighter_quote_audit = {
            "version":1, "received_monotonic":time.monotonic(),
            "captured_monotonic":time.monotonic(), "book_age_seconds":0,
            "book_exchange_timestamp_valid":False, "book_timestamp_error":error,
        }
        # The mocked current book is valid. Only the captured one is invalid.
        await rt.maybe_run_live_inventory_basis(snapshot)
        rt.fetch_live_inventory_basis_quote.assert_not_called()
        rt.send_variational_place_order.assert_not_called()
        assert any(c.args[1].get("reason")=="basis_lighter_book_too_old"
                   for c in rt.append_live_inventory_log.call_args_list)
    asyncio.run(run())


def test_execution_rfq_feedback_is_not_a_passive_quote_before_or_after_result():
    async def run():
        monitor = VariationalMonitor()
        update(monitor)
        before = dict(monitor.signal_quotes["ETH"])
        rt = SimpleNamespace(live_inventory_frozen_median=True,
            is_live_inventory_enabled=lambda:True, runtime=SimpleNamespace(monitor=monitor))
        async def command():
            monitor._update_quote({"asset":"ETH", "bid":"2010", "ask":"2010.2", "quote_id":"own-rfq"})
            assert monitor.signal_quotes["ETH"] == before
            assert monitor.indicative_quote_version("ETH") == 1
            return {"ok":True, "result":{"quoteId":"own-rfq"}}
        await passive_signal.execution_command(rt, "ETH", command())
        # CDP response bodies can arrive after the command result.
        monitor._update_quote({"asset":"ETH", "bid":"2010", "ask":"2010.2", "quote_id":"own-rfq"})
        assert monitor.signal_quotes["ETH"] == before
        assert monitor.indicative_quote_version("ETH") == 1
        update(monitor, bid="2001", ask="2001.2")
        assert monitor.indicative_quote_version("ETH") == 2
    asyncio.run(run())


def test_cancelled_execution_releases_scope_without_concurrent_signal_task():
    async def run():
        monitor = VariationalMonitor()
        rt = SimpleNamespace(live_inventory_frozen_median=True,
            is_live_inventory_enabled=lambda:True, runtime=SimpleNamespace(monitor=monitor))
        started = asyncio.Event()
        async def command():
            started.set()
            await asyncio.Event().wait()
        task = asyncio.create_task(passive_signal.execution_command(rt, "ETH", command()))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert monitor._execution_quote_depth["ETH"] == 0
    asyncio.run(run())


def test_timeout_quarantines_delayed_cdp_until_broker_correlates_result():
    async def run():
        monitor = VariationalMonitor()
        broker = CommandBroker(quiet=True, monitor=monitor)
        update(monitor)
        rt = SimpleNamespace(live_inventory_frozen_median=True,
            is_live_inventory_enabled=lambda:True, runtime=SimpleNamespace(monitor=monitor))
        async def command():await asyncio.Event().wait()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(passive_signal.execution_command(
                rt, "ETH", command(), request_id="timed-out-rfq"), .01)
        assert monitor._unresolved_quote_requests == {"timed-out-rfq":"ETH"}
        late = {"asset":"ETH", "bid":"2010", "ask":"2010.2", "quote_id":"late-rfq"}
        monitor._update_quote(late)
        assert monitor.indicative_quote_version("ETH") == 1
        await broker._handle_command_result({"requestId":"timed-out-rfq",
            "type":"VAR_API_QUOTE_RESULT", "ok":True, "result":{"quoteId":"late-rfq"}})
        assert not monitor._unresolved_quote_requests
        monitor._update_quote(late)
        assert monitor.indicative_quote_version("ETH") == 1
        update(monitor, bid="2001", ask="2001.2")
        assert monitor.indicative_quote_version("ETH") == 2
    asyncio.run(run())


def test_median_reads_only_passive_cache_not_execution_feedback():
    async def run():
        rt = app.VariationalToLighterRuntime.__new__(app.VariationalToLighterRuntime)
        monitor = VariationalMonitor()
        rt.live_inventory_frozen_median = True
        rt.runtime = SimpleNamespace(monitor=monitor)
        update(monitor)
        monitor._update_quote({"asset":"ETH", "price":"2000.1", "__source_stream":"/prices"})
        monitor.begin_execution_quote("ETH")
        update(monitor, bid="2050", ask="2050.2")
        merged = await rt.get_variational_reference_quote("ETH")
        assert merged["bid"] == "2000"
        assert merged["passive_indicative_version"] == 1
        assert monitor.quotes["ETH"]["bid"] == "2050"
    asyncio.run(run())


@pytest.mark.parametrize("result", [
    {"ok":False, "result":{"ok":False,"step":"quotes/indicative","timedOut":True,"httpStatus":200}},
    {"ok":False, "result":{"ok":False,"step":"quotes/indicative","error":"fetch_error","httpStatus":0}},
])
def test_timeout_dictionary_without_quote_identity_keeps_quarantine(result):
    async def run():
        monitor = VariationalMonitor()
        broker = CommandBroker(quiet=True, monitor=monitor)
        update(monitor)
        rt = SimpleNamespace(live_inventory_frozen_median=True,
            is_live_inventory_enabled=lambda:True, runtime=SimpleNamespace(monitor=monitor))
        async def command():
            await broker._handle_command_result({"requestId":"request-1", **result})
            return result
        await passive_signal.execution_command(rt,"ETH",command(),request_id="request-1")
        monitor._update_quote({"asset":"ETH","bid":"2010","ask":"2010.2","quote_id":"late"})
        assert monitor.indicative_quote_version("ETH") == 1
        assert monitor._unresolved_quote_requests == {"request-1":"ETH"}
        await broker._handle_command_result({"requestId":"request-1","result":{"quoteId":"late"}})
        monitor._update_quote({"asset":"ETH","bid":"2010","ask":"2010.2","quote_id":"late"})
        assert monitor.indicative_quote_version("ETH") == 1
    asyncio.run(run())
