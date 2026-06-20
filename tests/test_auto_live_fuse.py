import asyncio
import json
import logging
import time
from collections import deque
from decimal import Decimal
from pathlib import Path

from main import (
    AutoLivePositionState,
    CrossSpreadSnapshot,
    LiveInventoryBasisState,
    OrderLifecycle,
    PendingAutoLiveMatch,
    PendingLiveInventoryVarFillMatch,
    VariationalToLighterRuntime,
    variational_api_amount_to_str,
)


def _runtime_for_fuse_test() -> VariationalToLighterRuntime:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.auto_live_manual_review_required = False
    runtime.auto_live_manual_review_reason = None
    runtime.auto_live_max_cycles = 1
    runtime.auto_live_completed_cycles = 0
    runtime.auto_live_next_cycle_id = 1
    runtime.auto_live_last_closed_monotonic = None
    runtime.auto_live_cooldown_seconds = 60.0
    runtime.auto_live_position = None
    runtime.auto_live_state_file = None
    runtime._last_auto_live_guard_log = None
    runtime._last_auto_live_precheck_failure_log = {}
    runtime.logger = logging.getLogger("test_auto_live_fuse")
    return runtime


def _position() -> AutoLivePositionState:
    return AutoLivePositionState(
        cycle_id=7,
        asset="BTC",
        direction="long_var_short_lighter",
        entered_at_iso="2026-06-01T00:00:00Z",
        entered_at_monotonic=1.0,
        entry_spread_pct=Decimal("0.01"),
        entry_median_pct=Decimal("0"),
        entry_deviation_bps=Decimal("1"),
        entry_var_mid=Decimal("100000"),
        entry_lighter_mid=Decimal("100000"),
        entry_var_execution_price=Decimal("100001"),
        entry_lighter_execution_price=Decimal("100000"),
        planned_notional_usd=Decimal("25"),
        planned_qty=Decimal("0.00025"),
    )


def test_manual_review_sets_runtime_level_auto_live_fuse() -> None:
    runtime = _runtime_for_fuse_test()
    position = _position()
    runtime.auto_live_position = position

    runtime.require_auto_live_manual_review(position, "exit_precheck_failed:test")

    assert runtime.auto_live_guard_reason() == "manual_review_required"
    assert runtime.auto_live_manual_review_required is True
    assert runtime.auto_live_manual_review_reason == "exit_precheck_failed:test"
    assert position.manual_review_required is True
    assert position.manual_review_reason == "exit_precheck_failed:test"


def test_manual_review_guard_takes_priority_over_max_cycles() -> None:
    runtime = _runtime_for_fuse_test()
    runtime.auto_live_completed_cycles = 1

    runtime.require_auto_live_manual_review(None, "exit_already_submitted")

    assert runtime.auto_live_guard_reason() == "manual_review_required"


def test_auto_live_precheck_failure_logging_is_throttled() -> None:
    runtime = _runtime_for_fuse_test()

    assert runtime.should_log_auto_live_precheck_failure(
        "entry",
        1,
        "BTC",
        "SELL",
        "hedge_price_deviation_exceeds_risk_limit",
        interval_seconds=10.0,
    ) is True
    assert runtime.should_log_auto_live_precheck_failure(
        "entry",
        1,
        "BTC",
        "SELL",
        "hedge_price_deviation_exceeds_risk_limit",
        interval_seconds=10.0,
    ) is False

    assert runtime.should_log_auto_live_precheck_failure(
        "entry",
        1,
        "BTC",
        "BUY",
        "hedge_price_deviation_exceeds_risk_limit",
        interval_seconds=10.0,
    ) is True


def test_auto_live_entry_actionable_edge_uses_taker_prices() -> None:
    long_edge = VariationalToLighterRuntime.auto_live_entry_actionable_edge_bps(
        "long_var_short_lighter",
        Decimal("100000"),
        Decimal("100080"),
        Decimal("100100"),
    )
    short_edge = VariationalToLighterRuntime.auto_live_entry_actionable_edge_bps(
        "short_var_long_lighter",
        Decimal("100000"),
        Decimal("99900"),
        Decimal("99920"),
    )
    bad_short_edge = VariationalToLighterRuntime.auto_live_entry_actionable_edge_bps(
        "short_var_long_lighter",
        Decimal("100000"),
        Decimal("100050"),
        Decimal("100080"),
    )

    assert f"{long_edge:.3f}" == "8.000"
    assert f"{short_edge:.3f}" == "8.000"
    assert f"{bad_short_edge:.3f}" == "-8.000"


def test_variational_api_quote_execution_price_uses_side() -> None:
    quote = {"bid": "99990", "ask": "100010"}
    nested_quote = {"result": {"bid": "99980", "ask": "100020"}}

    buy_price = VariationalToLighterRuntime.variational_api_quote_execution_price("BUY", quote)
    sell_price = VariationalToLighterRuntime.variational_api_quote_execution_price("SELL", quote)
    nested_buy_price = VariationalToLighterRuntime.variational_api_quote_execution_price("BUY", nested_quote)

    assert buy_price == Decimal("100010")
    assert sell_price == Decimal("99990")
    assert nested_buy_price == Decimal("100020")


def test_variational_api_amount_is_quantized_to_min_qty_tick() -> None:
    assert variational_api_amount_to_str(Decimal("0.0002443343566137633103278690968")) == "0.000244"
    assert variational_api_amount_to_str(Decimal("0.0000019")) == "0.000001"
    assert variational_api_amount_to_str(Decimal("0.0000009")) == "0.000000"


def test_live_inventory_final_pnl_waits_for_var_and_lighter_final_fills(tmp_path) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.live_inventory_dry_decisions = False
        runtime.records = {}
        runtime.record_order = deque()
        runtime.pending_auto_live_matches = []
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="BTC",
                side="buy",
                qty=Decimal("0.0003"),
                lot_id=1,
                role="live_inventory_entry",
                created_at_monotonic=time.monotonic(),
            ),
            PendingLiveInventoryVarFillMatch(
                asset="BTC",
                side="sell",
                qty=Decimal("0.0003"),
                lot_id=1,
                role="live_inventory_exit",
                created_at_monotonic=time.monotonic(),
            ),
        ]
        runtime.pending_live_inventory_actual_pnl = {}
        runtime.pending_live_inventory_final_pnl = {}
        runtime.auto_live_match_window_seconds = 10.0
        runtime.trade_event_min_timestamp = None
        runtime.last_variational_trade_event_at = None
        runtime.variational_ticker = "BTC"
        runtime.accepted_assets = {"BTC"}
        runtime._record_lock = asyncio.Lock()
        runtime.logger = logging.getLogger("test_auto_live_fuse")
        runtime.lighter_client_order_to_trade_key = {}
        runtime.orders_file = tmp_path / "order_metrics.jsonl"
        runtime._order_write_lock = asyncio.Lock()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.0003",
                "entry_var_fill_price": "100",
                "entry_lighter_fill_price": "110",
                "entry_var_price_source": "estimated_snapshot",
                "entry_lighter_price_source": "estimated_snapshot",
                "entry_cost_status": "final_fills_pending",
            }
        ]
        persist_reasons: list[str] = []

        async def fake_persist_live_inventory_memory(*, reason: str) -> None:
            persist_reasons.append(reason)

        runtime.persist_live_inventory_memory = fake_persist_live_inventory_memory

        runtime.remember_live_inventory_final_pnl_lot(
            asset="BTC",
            lot={
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.0003",
                "entry_var_fill_price": "100",
                "entry_lighter_fill_price": "110",
                "entry_edge_bps": "1000",
                "entry_snapshot_var_bid": "99",
                "entry_snapshot_var_ask": "101",
                "entry_snapshot_var_mid": "100",
                "entry_snapshot_var_buy_price": "100",
                "entry_snapshot_var_sell_price": "99",
                "entry_snapshot_var_full_spread_bps": "200",
                "entry_snapshot_var_spread_source": "test",
                "entry_var_order_quote_id": "entry-quote",
                "entry_var_order_quote_bid": "119",
                "entry_var_order_quote_ask": "120",
                "entry_var_order_quote_timestamp": "2026-06-15T00:00:00.050000Z",
                "entry_var_order_quote_execution_price": "120",
                "entered_at": "2026-06-15T00:00:00Z",
            },
        )
        key = runtime.live_inventory_final_pnl_key("BTC", 1)
        runtime.pending_live_inventory_final_pnl[key].update(
            {
                "exit_var_price": "111",
                "exit_estimated_var_price": "111",
                "exit_lighter_estimated_price": "112",
                "exit_var_order_quote_execution_price": "111",
                "estimated_pnl_usd": "0.003",
            }
        )

        await runtime.maybe_append_live_inventory_final_pnl_from_fill(
            {
                "asset": "BTC",
                "qty": "0.0003",
                "auto_live_cycle_id": 1,
                "auto_live_role": "live_inventory_entry",
                "lighter_filled_price": "110",
                "lighter_filled_at": "2026-06-15T00:00:00.100000Z",
            }
        )
        await runtime.process_variational_trade_event(
            {
                "asset": "BTC",
                "side": "buy",
                "qty": "0.0003",
                "status": "filled",
                "trade_id": "entry-var",
                "timestamp": "2026-06-15T00:00:00.200000Z",
                "price": "130",
            }
        )
        assert runtime.live_inventory_open_lots[0]["entry_var_fill_price"] == "130"
        assert runtime.live_inventory_open_lots[0]["entry_lighter_fill_price"] == "110"
        assert runtime.live_inventory_open_lots[0]["entry_cost_status"] == "final_fills_confirmed"
        assert runtime.live_inventory_open_lots[0]["entry_var_price_source"] == "final_fill"
        assert runtime.live_inventory_open_lots[0]["entry_lighter_price_source"] == "final_fill"
        assert "entry_final_fill_cost_update" in persist_reasons
        await runtime.maybe_append_live_inventory_final_pnl_from_fill(
            {
                "asset": "BTC",
                "qty": "0.0003",
                "auto_live_cycle_id": 1,
                "auto_live_role": "live_inventory_exit",
                "lighter_filled_price": "112",
                "lighter_filled_at": "2026-06-15T00:00:10.100000Z",
            }
        )
        await runtime.process_variational_trade_event(
            {
                "asset": "BTC",
                "side": "sell",
                "qty": "0.0003",
                "status": "filled",
                "trade_id": "exit-var",
                "timestamp": "2026-06-15T00:00:10.200000Z",
                "price": "111",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        final_rows = [row for row in rows if row["event"] == "live_inventory_final_pnl"]
        assert len(final_rows) == 1
        assert final_rows[0]["final_var_leg_pnl_usd"] == "-0.0057"
        assert final_rows[0]["final_lighter_leg_pnl_usd"] == "-0.0006"
        assert final_rows[0]["final_pnl_usd"] == "-0.0063"
        assert Decimal(final_rows[0]["entry_var_fill_drift_bps"]) == Decimal("3000")
        assert Decimal(final_rows[0]["exit_var_fill_drift_bps"]) == Decimal("0")
        assert Decimal(final_rows[0]["entry_estimated_edge_bps"]) == Decimal("1000")
        assert Decimal(final_rows[0]["entry_final_edge_bps"]) < Decimal("0")
        assert Decimal(final_rows[0]["entry_edge_capture_loss_bps"]) > Decimal("2500")
        assert Decimal(final_rows[0]["entry_var_final_vs_snapshot_buy_bps"]) == Decimal("3000")
        assert Decimal(final_rows[0]["entry_var_final_vs_snapshot_ask_bps"]) > Decimal("2800")
        assert final_rows[0]["entry_var_order_quote_id"] == "entry-quote"
        assert Decimal(final_rows[0]["entry_var_order_quote_vs_snapshot_buy_bps"]) == Decimal("2000")
        assert Decimal(final_rows[0]["entry_var_final_vs_order_quote_bps"]) == Decimal("833.3333333333333333333333333")
        assert Decimal(final_rows[0]["exit_var_final_vs_order_quote_bps"]) == Decimal("0")
        assert runtime.live_inventory_execution_loss_bps_samples

    asyncio.run(run())


def test_variational_api_order_quote_fields_uses_side_execution_price() -> None:
    buy_fields = VariationalToLighterRuntime.variational_api_order_quote_fields(
        "BUY",
        {
            "result": {
                "quoteId": "q1",
                "bid": "99",
                "ask": "101",
                "markPrice": "100",
                "quoteTimestamp": "2026-06-15T00:00:00Z",
            }
        },
    )
    sell_fields = VariationalToLighterRuntime.variational_api_order_quote_fields(
        "SELL",
        {"result": {"quote_id": "q2", "bid": "98", "ask": "102"}},
    )

    assert buy_fields["quote_id"] == "q1"
    assert buy_fields["quote_execution_price"] == "101"
    assert buy_fields["quote_mark_price"] == "100"
    assert sell_fields["quote_id"] == "q2"
    assert sell_fields["quote_execution_price"] == "98"


def test_extract_variational_position_qty_from_positions_result() -> None:
    result = {
        "ok": True,
        "result": {
            "positions": [
                {"instrument": {"underlying": "BTC"}, "qty": "0"},
                {"instrument": {"underlying": "ETH"}, "position_size": "0.011441"},
            ]
        },
    }

    assert VariationalToLighterRuntime.extract_variational_position_qty(result, asset="ETH") == Decimal("0.011441")
    assert VariationalToLighterRuntime.extract_variational_position_qty(result, asset="SOL") == Decimal("0")


def test_live_inventory_blocks_spread_reverted_exit_until_entry_cost_confirmed(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.0003",
                "entry_var_fill_price": "60000",
                "entry_lighter_fill_price": "60400",
                "entry_var_side": "BUY",
                "entry_cost_status": "final_fills_pending",
                "entered_sample_index": 0,
            }
        ]
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _inventory_entry_snapshot()
        snapshot.long_var_short_lighter_pct = Decimal("0.0001")

        await runtime.maybe_run_live_inventory(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

        assert submit_calls == []
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["reason"] == "entry_final_fill_cost_pending"

    asyncio.run(run())


def test_non_filled_event_does_not_consume_pending_match_or_double_hedge(tmp_path) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime.records["auto:BTC:buy:123"] = OrderLifecycle(
            trade_key="auto:BTC:buy:123",
            trade_id="auto:BTC:buy:123",
            side="buy",
            qty=Decimal("0.00022"),
            asset="BTC",
            mode="live",
            last_variational_status="submitted",
            synthetic_eager_fill=True,
            auto_live_cycle_id=1,
            auto_live_role="entry",
            auto_live_merge_path="synthetic_created",
        )
        runtime.record_order.append("auto:BTC:buy:123")
        runtime.pending_auto_live_matches = [
            PendingAutoLiveMatch(
                record_key="auto:BTC:buy:123",
                asset="BTC",
                side="buy",
                qty=Decimal("0.00022"),
                cycle_id=1,
                role="entry",
                created_at_monotonic=asyncio.get_running_loop().time(),
            )
        ]
        runtime.auto_live_match_window_seconds = 10.0
        runtime.trade_event_min_timestamp = None
        runtime.last_variational_trade_event_at = None
        runtime.variational_ticker = "BTC"
        runtime.accepted_assets = {"BTC"}
        runtime._record_lock = asyncio.Lock()
        runtime.logger = logging.getLogger("test_auto_live_fuse")
        runtime.lighter_client_order_to_trade_key = {}
        runtime.output_dir = Path(tmp_path)

        hedge_calls: list[str] = []
        append_calls: list[str] = []

        async def fake_place_lighter_order(record) -> None:
            hedge_calls.append(record.trade_key)

        async def fake_append_order_log(event_type, payload) -> None:
            append_calls.append(event_type)

        runtime.place_lighter_order = fake_place_lighter_order
        runtime.append_order_log = fake_append_order_log

        submitted_event = {
            "asset": "BTC",
            "side": "buy",
            "qty": "0.00022",
            "status": "submitted",
            "trade_id": "trade-1",
            "timestamp": "2026-06-02T08:50:10Z",
            "price": "100000",
        }
        filled_event = {
            "asset": "BTC",
            "side": "buy",
            "qty": "0.00022",
            "status": "filled",
            "trade_id": "trade-1",
            "timestamp": "2026-06-02T08:50:11Z",
            "price": "100001",
        }

        await runtime.process_variational_trade_event(submitted_event)

        assert len(runtime.pending_auto_live_matches) == 1
        assert hedge_calls == []

        await runtime.process_variational_trade_event(filled_event)

        assert len(runtime.pending_auto_live_matches) == 0
        assert hedge_calls == []
        assert append_calls == ["variational_fill"]
        assert "id:trade-1" in runtime.records
        assert runtime.records["auto:BTC:buy:123"].auto_live_merge_path == "synthetic_matched_real_var_fill"
        assert runtime.records["auto:BTC:buy:123"].matched_variational_trade_id == "trade-1"

    asyncio.run(run())


def test_live_inventory_blocks_trade_event_auto_hedge(tmp_path) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.live_inventory = True
        runtime.records = {}
        runtime.record_order = deque()
        runtime.pending_auto_live_matches = []
        runtime.auto_live_match_window_seconds = 10.0
        runtime.trade_event_min_timestamp = None
        runtime.last_variational_trade_event_at = None
        runtime.variational_ticker = "BTC"
        runtime.accepted_assets = {"BTC"}
        runtime._record_lock = asyncio.Lock()
        runtime.logger = logging.getLogger("test_auto_live_fuse")
        runtime.lighter_client_order_to_trade_key = {}
        runtime.output_dir = Path(tmp_path)

        hedge_calls: list[str] = []
        append_calls: list[str] = []

        async def fake_place_lighter_order(record) -> None:
            hedge_calls.append(record.trade_key)

        async def fake_append_order_log(event_type, payload) -> None:
            append_calls.append(event_type)

        runtime.place_lighter_order = fake_place_lighter_order
        runtime.append_order_log = fake_append_order_log

        await runtime.process_variational_trade_event(
            {
                "asset": "BTC",
                "side": "buy",
                "qty": "0.00022",
                "status": "filled",
                "trade_id": "trade-live-inventory",
                "timestamp": "2026-06-02T08:50:11Z",
                "price": "100001",
            }
        )

        assert hedge_calls == []
        assert append_calls == ["variational_fill", "lighter_blocked"]
        record = runtime.records["id:trade-live-inventory"]
        assert record.processing_stage == "blocked_by_mode"
        assert record.failure_reason == "live_inventory_blocks_trade_event_auto_hedge"

    asyncio.run(run())


def test_lighter_ws_sendtx_sends_tx_info_as_object() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_submit_timeout_seconds = 1.0
        runtime._lighter_submit_ws_lock = asyncio.Lock()

        class FakeWs:
            state = 1

            def __init__(self):
                self.sent: list[str] = []
                self.recv_messages = [json.dumps({"type": "jsonapi/sendtx", "data": {"code": 200, "tx_hash": "0xabc"}})]

            async def send(self, message):
                self.sent.append(message)

            async def recv(self):
                return self.recv_messages.pop(0)

        fake_ws = FakeWs()
        runtime._lighter_submit_ws = fake_ws

        response = await runtime.send_lighter_tx_ws(tx_type=14, tx_info='{"Nonce": 1}')

        sent = json.loads(fake_ws.sent[0])
        assert sent["type"] == "jsonapi/sendtx"
        assert sent["data"]["tx_type"] == 14
        assert sent["data"]["tx_info"] == {"Nonce": 1}
        assert response.code == 200
        assert response.tx_hash == "0xabc"

    asyncio.run(run())


def test_lighter_ws_prewarm_reuses_connection(monkeypatch) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_submit_timeout_seconds = 1.0
        runtime.lighter_submit_transport = "ws"
        runtime._lighter_submit_ws_lock = asyncio.Lock()
        runtime._lighter_submit_ws = None
        runtime.logger = logging.getLogger("test_lighter_ws_prewarm")

        class FakeWs:
            state = 1

            def __init__(self):
                self.sent: list[str] = []
                self.recv_messages = [
                    json.dumps({"type": "connected"}),
                    json.dumps({"type": "jsonapi/sendtx", "data": {"code": 200, "tx_hash": "0xabc"}}),
                ]

            async def send(self, message):
                self.sent.append(message)

            async def recv(self):
                return self.recv_messages.pop(0)

        fake_ws = FakeWs()
        connect_calls = 0

        async def fake_connect(*_args, **_kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return fake_ws

        monkeypatch.setattr("main.websockets.connect", fake_connect)
        monkeypatch.setattr("main.elapsed_ms_str", lambda *_args, **_kwargs: "0.001")

        await runtime.prewarm_lighter_submit_ws()
        response = await runtime.send_lighter_tx_ws(tx_type=14, tx_info='{"Nonce": 1}')

        assert connect_calls == 1
        assert response.code == 200
        assert len(fake_ws.sent) == 1
        assert json.loads(fake_ws.sent[0])["type"] == "jsonapi/sendtx"

    asyncio.run(run())


def test_market_ioc_uses_ioc_expiry() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.lighter_order_mode = "market-ioc"

    class FakeClient:
        ORDER_TYPE_LIMIT = 0
        ORDER_TYPE_MARKET = 1
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
        ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
        DEFAULT_IOC_EXPIRY = 0
        DEFAULT_28_DAY_ORDER_EXPIRY = -1

    runtime.lighter_client = FakeClient()

    order_kwargs = {
        "market_index": 1,
        "client_order_index": 123,
        "base_amount": 45,
        "price": 100000,
        "is_ask": False,
        "order_type": (
            runtime.lighter_client.ORDER_TYPE_MARKET
            if runtime.lighter_order_mode == "market-ioc"
            else runtime.lighter_client.ORDER_TYPE_LIMIT
        ),
        "time_in_force": (
            runtime.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
            if runtime.lighter_order_mode == "market-ioc"
            else runtime.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
        ),
        "reduce_only": False,
        "trigger_price": 0,
        "order_expiry": (
            runtime.lighter_client.DEFAULT_IOC_EXPIRY
            if runtime.lighter_order_mode == "market-ioc"
            else runtime.lighter_client.DEFAULT_28_DAY_ORDER_EXPIRY
        ),
    }

    assert order_kwargs["order_type"] == runtime.lighter_client.ORDER_TYPE_MARKET
    assert order_kwargs["time_in_force"] == runtime.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    assert order_kwargs["order_expiry"] == runtime.lighter_client.DEFAULT_IOC_EXPIRY


def test_create_lighter_order_ws_accepts_order_expiry() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)

        class FakeNonceManager:
            def next_nonce(self):
                return 1, 99

            def acknowledge_failure(self, _api_key_index):
                raise AssertionError("should not acknowledge failure")

            def hard_refresh_nonce(self, _api_key_index):
                raise AssertionError("should not refresh nonce")

        class FakeClient:
            nonce_manager = FakeNonceManager()

            def sign_create_order(self, **kwargs):
                assert kwargs["order_expiry"] == 0
                return 1, "{}", "0xabc", None

        runtime.lighter_client = FakeClient()

        async def fake_send_lighter_tx_ws(*, tx_type, tx_info):
            assert tx_type == 1
            assert tx_info == "{}"

            class Response:
                code = 200
                tx_hash = "0xabc"

            return Response()

        runtime.send_lighter_tx_ws = fake_send_lighter_tx_ws

        _order, response, error = await runtime.create_lighter_order_ws(
            market_index=1,
            client_order_index=123,
            base_amount=45,
            price=100000,
            is_ask=False,
            order_type=1,
            time_in_force=0,
            reduce_only=False,
            trigger_price=0,
            order_expiry=0,
        )

        assert error is None
        assert response.code == 200

    asyncio.run(run())


def test_place_lighter_order_from_plan_passes_reduce_only() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime._record_lock = asyncio.Lock()
        runtime._lighter_signer_lock = asyncio.Lock()
        runtime.lighter_submit_transport = "http"
        runtime.lighter_order_mode = "market-ioc"
        runtime.lighter_market_index = 1
        runtime.price_multiplier = Decimal("100")
        runtime.base_amount_multiplier = Decimal("100000000")
        runtime.risk_guard_max_base_amount = 1000000
        runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = None
        runtime.live_allowed_sides = {"buy", "sell"}
        runtime.live_allowed_assets = {"BTC"}
        runtime.live_max_qty = Decimal("0")
        runtime.live_max_notional_usd = Decimal("100")
        runtime.live_require_min_edge_bps = Decimal("0")
        runtime.live_cooldown_seconds = 0.0
        runtime.last_live_submit_monotonic_by_asset = {}
        runtime.lighter_client_order_to_trade_key = {}
        runtime.lighter_best_bid = Decimal("99990")
        runtime.lighter_best_ask = Decimal("100010")
        runtime.lighter_order_book_lock = asyncio.Lock()
        runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
        runtime.logger = logging.getLogger("test_auto_live_fuse")

        captured_kwargs = {}

        class FakeClient:
            ORDER_TYPE_LIMIT = 0
            ORDER_TYPE_MARKET = 1
            ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
            DEFAULT_IOC_EXPIRY = 0
            DEFAULT_28_DAY_ORDER_EXPIRY = -1

            async def create_order(self, **kwargs):
                captured_kwargs.update(kwargs)
                return None, "0xabc", None

        runtime.lighter_client = FakeClient()

        async def fake_append_order_log(_event_type, _payload) -> None:
            return None

        runtime.append_order_log = fake_append_order_log

        record, payload = await runtime.place_lighter_order_from_plan(
            asset="BTC",
            side="SELL",
            qty=Decimal("0.0001"),
            var_fill_price=Decimal("100000"),
            role="live_inventory_exit",
            reduce_only=True,
        )

        assert captured_kwargs["reduce_only"] is True
        assert record is not None
        assert record.lighter_reduce_only is True
        assert payload["lighter_reduce_only"] is True

    asyncio.run(run())


def test_reduce_only_lighter_order_bypasses_live_cooldown() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime._record_lock = asyncio.Lock()
        runtime._lighter_signer_lock = asyncio.Lock()
        runtime.lighter_submit_transport = "http"
        runtime.lighter_order_mode = "market-ioc"
        runtime.lighter_market_index = 1
        runtime.price_multiplier = Decimal("100")
        runtime.base_amount_multiplier = Decimal("100000000")
        runtime.risk_guard_max_base_amount = 1000000
        runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = None
        runtime.live_allowed_sides = {"buy", "sell"}
        runtime.live_allowed_assets = {"BTC"}
        runtime.live_max_qty = Decimal("0")
        runtime.live_max_notional_usd = Decimal("100")
        runtime.live_require_min_edge_bps = Decimal("0")
        runtime.live_cooldown_seconds = 999999.0
        runtime.last_live_submit_monotonic_by_asset = {"BTC": 999999999999.0}
        runtime.lighter_client_order_to_trade_key = {}
        runtime.lighter_best_bid = Decimal("99990")
        runtime.lighter_best_ask = Decimal("100010")
        runtime.lighter_order_book_lock = asyncio.Lock()
        runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
        runtime.logger = logging.getLogger("test_auto_live_fuse")

        captured_kwargs = {}

        class FakeClient:
            ORDER_TYPE_LIMIT = 0
            ORDER_TYPE_MARKET = 1
            ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
            DEFAULT_IOC_EXPIRY = 0
            DEFAULT_28_DAY_ORDER_EXPIRY = -1

            async def create_order(self, **kwargs):
                captured_kwargs.update(kwargs)
                return None, "0xabc", None

        runtime.lighter_client = FakeClient()

        async def fake_append_order_log(_event_type, _payload) -> None:
            return None

        runtime.append_order_log = fake_append_order_log

        record, payload = await runtime.place_lighter_order_from_plan(
            asset="BTC",
            side="BUY",
            qty=Decimal("0.000243"),
            var_fill_price=Decimal("98990"),
            role="live_inventory_exit",
            reduce_only=True,
        )

        assert captured_kwargs["reduce_only"] is True
        assert record is not None
        assert record.failure_reason is None
        assert payload["processing_stage"] == "live_submit_sent"

    asyncio.run(run())


def _live_inventory_runtime(tmp_path) -> VariationalToLighterRuntime:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.mode = "live"
    runtime.live_inventory = True
    runtime.live_inventory_dry_decisions = False
    runtime.live_inventory_sample_index = 0
    runtime.live_inventory_completed_cycles = 0
    runtime.live_inventory_max_cycles = 1
    runtime.live_inventory_next_lot_id = 1
    runtime.live_inventory_open_lots = []
    runtime.live_inventory_realized_pnl_usd = Decimal("0")
    runtime.pending_live_inventory_actual_pnl = {}
    runtime.pending_live_inventory_final_pnl = {}
    runtime.live_inventory_execution_loss_bps_samples = deque(maxlen=20)
    runtime.live_inventory_entry_bps = Decimal("50")
    runtime.live_inventory_exit_bps = Decimal("10")
    runtime.live_inventory_max_var_spread_bps = Decimal("5")
    runtime.live_inventory_max_var_snapshot_age_seconds = 5.0
    runtime.live_inventory_refresh_var_quote_before_entry = False
    runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("5")
    runtime.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics = False
    runtime.live_inventory_max_lighter_slippage_bps = Decimal("3")
    runtime.live_inventory_lot_notional_usd = Decimal("10")
    runtime.live_inventory_max_total_lots = 1
    runtime.live_inventory_min_hold_samples = 0
    runtime.live_inventory_max_hold_samples = 300
    runtime.live_inventory_state_file = Path(tmp_path) / "live_inventory_state.json"
    runtime.orders_file = Path(tmp_path) / "order_metrics.jsonl"
    runtime._order_write_lock = asyncio.Lock()
    runtime.lighter_order_book_lock = asyncio.Lock()
    runtime.lighter_order_book = {
        "bids": {Decimal("59990"): Decimal("1")},
        "asks": {Decimal("60010"): Decimal("1")},
    }
    runtime.lighter_best_bid = Decimal("59990")
    runtime.lighter_best_ask = Decimal("60010")
    runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
    runtime.base_amount_multiplier = Decimal("100000000")
    runtime.risk_guard_max_base_amount = 1000000
    runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
    runtime.lighter_min_base_amount = None
    runtime.lighter_min_quote_amount = None
    runtime.live_allowed_sides = {"buy", "sell"}
    runtime.live_allowed_assets = {"BTC"}
    runtime.live_max_qty = Decimal("0")
    runtime.live_max_notional_usd = Decimal("20")
    runtime.live_require_min_edge_bps = Decimal("0")
    runtime.live_cooldown_seconds = 0.0
    runtime.last_live_submit_monotonic_by_asset = {}
    runtime.live_inventory_var_reject_cooldown_until = {}
    runtime.live_inventory_var_reject_cooldown_seconds = 600.0
    runtime.logger = logging.getLogger("test_auto_live_fuse")
    return runtime


def _inventory_entry_snapshot() -> CrossSpreadSnapshot:
    return CrossSpreadSnapshot(
        asset="BTC",
        var_bid=Decimal("59990"),
        var_ask=Decimal("60000"),
        var_mid=Decimal("59995"),
        var_half_spread_bps=Decimal("1"),
        var_buy_price=Decimal("60000"),
        var_sell_price=Decimal("59990"),
        var_full_spread_bps=Decimal("2"),
        var_spread_source="test",
        var_timestamp="2999-06-16T03:25:20.000Z",
        var_source_url="wss://example.test/prices",
        var_source_stream="instrument_price:BTC",
        lighter_bid=Decimal("60400"),
        lighter_ask=Decimal("60420"),
        lighter_mid=Decimal("60410"),
        lighter_buy_price=Decimal("60420"),
        lighter_sell_price=Decimal("60400"),
        lighter_half_spread_bps=Decimal("1"),
        lighter_buy_fill_price=Decimal("60420"),
        lighter_sell_fill_price=Decimal("60400"),
        long_var_short_lighter_pct=Decimal("0.66666667"),
        short_var_long_lighter_pct=Decimal("-0.006"),
        long_median_5m_pct=None,
        short_median_5m_pct=None,
        long_sample_count_5m=1,
        short_sample_count_5m=1,
    )


def _eth_inventory_snapshot() -> CrossSpreadSnapshot:
    snapshot = _inventory_entry_snapshot()
    snapshot.asset = "ETH"
    snapshot.var_bid = Decimal("1753.00")
    snapshot.var_ask = Decimal("1753.25")
    snapshot.var_mid = Decimal("1753.125")
    snapshot.var_buy_price = Decimal("1753.25")
    snapshot.var_sell_price = Decimal("1753.00")
    snapshot.var_timestamp = "2999-06-16T03:25:20.000Z"
    snapshot.lighter_bid = Decimal("1755.00")
    snapshot.lighter_ask = Decimal("1755.10")
    snapshot.lighter_mid = Decimal("1755.05")
    snapshot.lighter_buy_price = Decimal("1755.10")
    snapshot.lighter_sell_price = Decimal("1755.00")
    snapshot.lighter_buy_fill_price = Decimal("1755.10")
    snapshot.lighter_sell_fill_price = Decimal("1755.00")
    return snapshot


def test_live_inventory_basis_real_entry_waits_for_var_fill_before_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.trade_event_min_timestamp = None
        runtime.pending_auto_live_matches = []
        runtime.auto_live_match_window_seconds = 60
        runtime._record_lock = asyncio.Lock()
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        runtime.lighter_client_order_to_trade_key = {}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.lighter_order_book = {
            "bids": {Decimal("1755.00"): Decimal("1")},
            "asks": {Decimal("1755.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1755.00")
        runtime.lighter_best_ask = Decimal("1755.10")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.live_inventory_basis_z_exit = Decimal("0")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("1")
        runtime.pending_live_inventory_var_fill_matches = []
        calls: list[dict] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "entry-quote",
                "bid": "1753.00",
                "ask": "1753.25",
                "quoteTimestamp": "2999-06-16T03:25:20.000Z",
            }, Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            calls.append({"venue": "var", **kwargs})
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append({"venue": "lighter", **kwargs})
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="",
                side=str(kwargs["side"]).lower(),
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            return record, {"trade_key": "entry-1"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

        assert [call["venue"] for call in calls] == ["var"]
        assert calls[0]["confirm"] is True
        assert calls[0]["reuse_quote_id"] is None
        assert runtime.live_inventory_open_lots == []
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        assert runtime.pending_live_inventory_var_fill_matches[0].role == "live_inventory_entry_pending_lighter"
        assert rows[-1]["event"] == "live_inventory_var_entry_submitted"

        await runtime.process_variational_trade_event(
            {
                "asset": "ETH",
                "side": "buy",
                "qty": calls[0]["amount"],
                "price": "1753.30",
                "status": "filled",
                "trade_id": "var-fill-1",
                "timestamp": "2999-06-16T03:25:21.000Z",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert [call["venue"] for call in calls] == ["var", "lighter"]
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_open_lots[0]["entry_var_price_source"] == "final_fill"
        assert rows[-1]["event"] == "live_inventory_entered"
        assert rows[-1]["entry_confirmation_mode"] == "var_fill_then_lighter"

    asyncio.run(run())


def test_live_inventory_basis_real_entry_rejected_does_not_submit_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.trade_event_min_timestamp = None
        runtime.pending_auto_live_matches = []
        runtime.auto_live_match_window_seconds = 60
        runtime._record_lock = asyncio.Lock()
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        runtime.lighter_client_order_to_trade_key = {}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.lighter_order_book = {
            "bids": {Decimal("1755.00"): Decimal("1")},
            "asks": {Decimal("1755.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1755.00")
        runtime.lighter_best_ask = Decimal("1755.10")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.pending_live_inventory_var_fill_matches = []
        calls: list[dict] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "entry-quote",
                "bid": "1753.00",
                "ask": "1753.25",
                "quoteTimestamp": "2999-06-16T03:25:20.000Z",
            }, Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            calls.append({"venue": "var", **kwargs})
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append({"venue": "lighter", **kwargs})
            return None, None

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())
        await runtime.process_variational_trade_event(
            {
                "asset": "ETH",
                "side": "buy",
                "qty": calls[0]["amount"],
                "price": "1753.30",
                "status": "rejected",
                "trade_id": "var-reject-1",
                "timestamp": "2999-06-16T03:25:21.000Z",
            }
        )

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert [call["venue"] for call in calls] == ["var"]
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.stop_flag is True
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "variational_rejected:pending_live_inventory_entry_pending_lighter"

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_survives_match_window(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.auto_live_match_window_seconds = 0
    runtime.pending_live_inventory_var_fill_matches = [
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.01"),
            lot_id=1,
            role="live_inventory_entry_pending_lighter",
            created_at_monotonic=time.monotonic() - 3600,
        ),
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="sell",
            qty=Decimal("0.01"),
            lot_id=2,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic() - 3600,
        ),
    ]

    runtime.prune_pending_live_inventory_var_fill_matches()

    assert len(runtime.pending_live_inventory_var_fill_matches) == 1
    assert runtime.pending_live_inventory_var_fill_matches[0].role == "live_inventory_entry_pending_lighter"


def test_live_inventory_basis_pending_entry_timeout_requires_manual_review(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 31,
                context={"direction": "long_var_short_lighter", "quote_id": "quote-1"},
            )
        ]

        async def fake_fetch_variational_positions():
            return {"ok": True, "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "0"}]}}

        runtime.fetch_variational_positions = fake_fetch_variational_positions

        timed_out = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert timed_out is True
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.stop_flag is True
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "basis_entry_var_fill_timeout"
        assert state["manual_review_context"]["lot_id"] == 1
        assert state["manual_review_context"]["variational_position_qty"] == "0"
        assert rows[-1]["event"] == "live_inventory_manual_review_required"
        assert rows[-1]["reason"] == "basis_entry_var_fill_timeout"

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_timeout_detects_var_position(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 31,
            )
        ]

        async def fake_fetch_variational_positions():
            return {"ok": True, "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "0.011535"}]}}

        runtime.fetch_variational_positions = fake_fetch_variational_positions

        timed_out = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert timed_out is True
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "basis_entry_var_fill_timeout_position_detected"
        assert state["manual_review_context"]["variational_position_qty"] == "0.011535"
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.stop_flag is True

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_orders_rejected_clears_without_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.live_inventory_next_lot_id = 2
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 3,
                context={"rfq_id": "rfq-rejected", "direction": "long_var_short_lighter"},
            )
        ]
        calls: list[str] = []

        async def fake_fetch_variational_orders(**_kwargs):
            return {
                "ok": True,
                "result": {
                    "orders": {
                        "result": [
                            {
                                "rfq_id": "rfq-rejected",
                                "order_id": "order-rejected",
                                "status": "rejected",
                                "clearing_status": "rejected_failed_taker_funding",
                                "side": "buy",
                                "qty": "20",
                            }
                        ]
                    }
                },
            }

        async def fake_place_lighter_order_from_plan(**_kwargs):
            calls.append("lighter")
            return None, None

        runtime.fetch_variational_orders = fake_fetch_variational_orders
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        resolved = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert resolved is True
        assert calls == []
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.stop_flag is False
        assert state["status"] == "flat"
        assert state["last_rejected_reason"] == "variational_order_rejected"
        assert rows[-1]["event"] == "live_inventory_var_entry_final_rejected"
        assert rows[-1]["clearing_status"] == "rejected_failed_taker_funding"

    asyncio.run(run())


def test_live_inventory_basis_taker_funding_reject_cooldown_blocks_next_entry(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_basis_z_entry = Decimal("1")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("0")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("20")
        runtime.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics = True
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 3,
                context={"rfq_id": "rfq-rejected", "direction": "long_var_short_lighter"},
            )
        ]
        calls: list[str] = []

        async def fake_fetch_variational_orders(**_kwargs):
            return {
                "ok": True,
                "result": {
                    "orders": {
                        "result": [
                            {
                                "rfq_id": "rfq-rejected",
                                "order_id": "order-rejected",
                                "status": "rejected",
                                "clearing_status": "rejected_failed_taker_funding",
                                "side": "buy",
                                "qty": "20",
                            }
                        ]
                    }
                },
            }

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "entry-quote",
                "bid": "1753.00",
                "ask": "1753.25",
                "quoteTimestamp": "2999-06-16T03:25:20.000Z",
            }, Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            calls.append("var")
            return {"ok": True}

        runtime.fetch_variational_orders = fake_fetch_variational_orders
        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order

        resolved = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")
        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert resolved is True
        assert calls == []
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert state["last_blocked_reason"] == "variational_taker_funding_reject_cooldown_active"
        assert rows[-1]["event"] == "live_inventory_entry_blocked"
        assert rows[-1]["reason"] == "variational_taker_funding_reject_cooldown_active"

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_orders_cleared_submits_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.mode = "live"
        runtime._record_lock = asyncio.Lock()
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        runtime.lighter_client_order_to_trade_key = {}
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 3,
                context={
                    "rfq_id": "rfq-cleared",
                    "direction": "long_var_short_lighter",
                    "var_side": "BUY",
                    "lighter_price": "1755.00",
                },
            )
        ]
        calls: list[dict] = []

        async def fake_fetch_variational_orders(**_kwargs):
            return {
                "ok": True,
                "result": {
                    "orders": {
                        "result": [
                            {
                                "rfq_id": "rfq-cleared",
                                "order_id": "order-cleared",
                                "status": "cleared",
                                "clearing_status": "success_trades_booked_into_pool",
                                "side": "buy",
                                "qty": "0.01141",
                                "price": "1751.58",
                                "execution_timestamp": "2026-06-18T00:53:48.608Z",
                            }
                        ]
                    }
                },
            }

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append(kwargs)
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="entry-1",
                side=str(kwargs["side"]).lower(),
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="submitted",
                var_fill_price=kwargs["var_fill_price"],
                lighter_fill_price=Decimal("1755.00"),
            )
            record.processing_stage = "lighter_filled"
            return record, {"trade_key": "entry-1"}

        runtime.fetch_variational_orders = fake_fetch_variational_orders
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        resolved = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert resolved is True
        assert len(calls) == 1
        assert calls[0]["qty"] == Decimal("0.01141")
        assert calls[0]["var_fill_price"] == Decimal("1751.58")
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots[0]["entry_var_price_source"] == "final_fill"
        assert rows[-2]["event"] == "variational_fill"
        assert rows[-1]["event"] == "live_inventory_entered"

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_before_timeout_does_not_stop(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 29,
            )
        ]

        timed_out = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        assert timed_out is False
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        assert runtime.stop_flag is False
        assert not runtime.live_inventory_state_file.exists()

    asyncio.run(run())


def test_live_inventory_entry_blocks_below_lighter_min_base_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.lighter_min_base_amount = Decimal("0.00020")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        log_line = runtime.orders_file.read_text(encoding="utf-8").strip()

        assert submit_calls == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "hedge_below_lighter_min_base_amount"
        assert "live_inventory_entry_blocked" in log_line

    asyncio.run(run())


def test_live_inventory_entry_blocks_below_lighter_min_quote_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = Decimal("15")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "hedge_below_lighter_min_quote_amount"

    asyncio.run(run())


def test_live_inventory_entry_blocks_high_var_spread_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_max_var_spread_bps = Decimal("1")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "var_spread_exceeds_live_inventory_limit"
        assert state["last_blocked_context"]["var_spread_bps"] == "2"

    asyncio.run(run())


def test_live_inventory_entry_blocks_stale_var_snapshot_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_max_var_snapshot_age_seconds = 5.0
        snapshot = _inventory_entry_snapshot()
        snapshot.var_timestamp = "2026-06-16T03:25:20.000Z"
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(snapshot)

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "variational_quote_snapshot_stale"
        assert state["last_blocked_context"]["var_snapshot_timestamp"] == "2026-06-16T03:25:20.000Z"

    asyncio.run(run())


def test_live_inventory_refreshes_var_quote_before_entry_and_reuses_quote_id(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("0")
        runtime.live_inventory_refresh_var_quote_before_entry = True
        runtime.lighter_min_base_amount = Decimal("0.00020")
        snapshot = _inventory_entry_snapshot()
        snapshot.var_timestamp = "2026-06-16T03:25:20.000Z"
        calls: list[dict] = []

        async def fake_send_variational_place_order(**kwargs):
            calls.append(kwargs)
            if not kwargs["confirm"]:
                return {
                    "ok": True,
                    "result": {
                        "quoteId": "fresh-entry-quote",
                        "bid": "60095",
                        "ask": "60100",
                        "quoteTimestamp": "2999-06-16T03:25:21.000Z",
                    },
                }
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="",
                side="sell",
                qty=Decimal("0.000330"),
                asset="BTC",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            return record, {"trade_key": "entry-1"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        entered = next(row for row in rows if row["event"] == "live_inventory_entered")

        assert [call["confirm"] for call in calls] == [False, True]
        assert calls[1]["reuse_quote_id"] == "fresh-entry-quote"
        assert entered["var_order_quote_id"] == "fresh-entry-quote"
        assert entered["var_order_quote_execution_price"] == "60100"
        assert entered["initial_snapshot_var_price"] == "60000"

    asyncio.run(run())


def test_live_inventory_entry_blocks_dynamic_threshold_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("70")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "edge_bps_below_dynamic_live_inventory_entry"
        assert Decimal(state["last_blocked_context"]["live_inventory_required_entry_bps"]) == Decimal("72")

    asyncio.run(run())


def test_live_inventory_entry_uses_recent_execution_loss_buffer_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("0")
        runtime.live_inventory_execution_loss_bps_samples.extend(
            [Decimal("50"), Decimal("60"), Decimal("65"), Decimal("70")]
        )
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "edge_bps_below_dynamic_live_inventory_entry"
        assert Decimal(state["last_blocked_context"]["live_inventory_recent_execution_loss_buffer_bps"]) == Decimal("70")
        assert Decimal(state["last_blocked_context"]["live_inventory_required_entry_bps"]) == Decimal("72")

    asyncio.run(run())


def test_live_inventory_diagnostic_can_ignore_recent_execution_loss_buffer_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("0")
        runtime.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics = True
        runtime.lighter_min_base_amount = Decimal("0.00020")
        runtime.live_inventory_execution_loss_bps_samples.extend(
            [Decimal("50"), Decimal("60"), Decimal("65"), Decimal("70")]
        )
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {
                "ok": True,
                "result": {
                    "quoteId": "diagnostic-entry",
                    "bid": "60000",
                    "ask": "60005",
                    "quoteTimestamp": "2026-06-15T00:00:00Z",
                },
            }

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="",
                side="sell",
                qty=Decimal("0.000330"),
                asset="BTC",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            return record, {"trade_key": "entry-1"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        entered = next(row for row in rows if row["event"] == "live_inventory_entered")

        assert sorted(submit_calls) == ["lighter", "var"]
        assert runtime.live_inventory_open_lots
        assert entered["var_order_quote_id"] == "diagnostic-entry"
        assert entered["var_order_quote_execution_price"] == "60005"

    asyncio.run(run())


def test_live_inventory_entry_blocks_lighter_depth_slippage_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("1")
        runtime.lighter_order_book = {
            "bids": {
                Decimal("59990"): Decimal("0.00005"),
                Decimal("59000"): Decimal("1"),
            },
            "asks": {Decimal("60010"): Decimal("1")},
        }
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "lighter_slippage_exceeds_live_inventory_limit"
        assert Decimal(state["last_blocked_context"]["lighter_order_book_slippage_bps"]) > Decimal("1")

    asyncio.run(run())


def test_live_inventory_entry_blocks_live_cooldown_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_cooldown_seconds = 3.0
        runtime.last_live_submit_monotonic_by_asset = {"BTC": time.monotonic()}
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "live_cooldown_active"
        assert state["last_blocked_context"]["live_cooldown_remaining_seconds"] is not None

    asyncio.run(run())


def test_variational_api_amount_to_str_truncates_to_min_qty_tick() -> None:
    assert variational_api_amount_to_str(Decimal("0.0002432227102505721546713663434")) == "0.000243"
    assert variational_api_amount_to_str(Decimal("0.01167603668610726774903526747"), asset="ETH") == "0.01167"


def test_live_inventory_entry_concurrent_submit_uses_formatted_var_amount(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.lighter_min_base_amount = Decimal("0.00020")
        submit_calls: list[str] = []
        var_amounts: list[str] = []

        async def fake_send_variational_place_order(**kwargs):
            submit_calls.append("var")
            var_amounts.append(kwargs["amount"])
            return {"ok": False, "error": "quote_qty_precision"}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert sorted(submit_calls) == ["lighter", "var"]
        assert var_amounts == ["0.000330"]
        assert state["status"] == "manual_review_required"
        assert state["manual_review_context"]["var_amount"] == "0.000330"
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0

    asyncio.run(run())


def test_live_inventory_exit_concurrent_submit_uses_formatted_var_amount(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.lighter_min_base_amount = Decimal("0.00020")
        runtime.live_inventory_max_hold_samples = 300
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "short_var_long_lighter",
                "qty": "0.0002430031523195129605107581521",
                "entry_var_side": "SELL",
                "entry_var_fill_price": "61116.43",
                "entry_lighter_fill_price": "61054.70",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 0,
                "status": "open",
            }
        ]
        submit_calls: list[str] = []
        var_amounts: list[str] = []

        async def fake_send_variational_place_order(**kwargs):
            submit_calls.append("var")
            var_amounts.append(kwargs["amount"])
            return {"ok": False, "error": "quote_qty_precision"}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert sorted(submit_calls) == ["lighter", "var"]
        assert var_amounts == ["0.000243"]
        assert state["status"] == "manual_review_required"
        assert state["manual_review_context"]["var_amount"] == "0.000243"
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_completed_cycles == 0

    asyncio.run(run())


def test_live_inventory_exit_waits_for_min_hold_samples(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_min_hold_samples = 10
        runtime.live_inventory_max_hold_samples = 300
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.000301",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "65636.88",
                "entry_lighter_fill_price": "65670.40",
                "entered_sample_index": 0,
                "status": "open",
            }
        ]
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        assert submit_calls == []
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_completed_cycles == 0

    asyncio.run(run())


def test_live_inventory_actual_pnl_logged_after_lighter_final_fill(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.pending_live_inventory_actual_pnl["exit-1"] = {
            "asset": "BTC",
            "lot_id": 1,
            "direction": "short_var_long_lighter",
            "qty": "0.000326",
            "entry_var_price": "60679.56",
            "entry_lighter_price": "60600.4",
            "exit_var_price": "60607.99",
            "exit_lighter_estimated_price": "60605.9",
            "estimated_pnl_usd": "0.02478067523956343718372446020",
            "estimated_pnl_bps": "12.51424099597953577778085240",
        }

        await runtime.maybe_append_live_inventory_actual_pnl(
            {
                "trade_key": "exit-1",
                "lighter_filled_price": "60605.8",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

        assert rows[-1]["event"] == "live_inventory_actual_pnl"
        assert rows[-1]["actual_pnl_status"] == "lighter_final_fill_confirmed"
        assert rows[-1]["exit_lighter_final_fill_price"] == "60605.8"
        assert rows[-1]["actual_pnl_usd"] == "0.02509222"
        assert "exit-1" not in runtime.pending_live_inventory_actual_pnl

    asyncio.run(run())
