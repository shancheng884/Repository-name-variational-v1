import asyncio
import json
import logging
from collections import deque
from decimal import Decimal
from pathlib import Path

from main import (
    AutoLivePositionState,
    CrossSpreadSnapshot,
    OrderLifecycle,
    PendingAutoLiveMatch,
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
    runtime.live_inventory_entry_bps = Decimal("50")
    runtime.live_inventory_exit_bps = Decimal("10")
    runtime.live_inventory_max_var_spread_bps = Decimal("5")
    runtime.live_inventory_lot_notional_usd = Decimal("10")
    runtime.live_inventory_max_total_lots = 1
    runtime.live_inventory_state_file = Path(tmp_path) / "live_inventory_state.json"
    runtime.orders_file = Path(tmp_path) / "order_metrics.jsonl"
    runtime._order_write_lock = asyncio.Lock()
    runtime.lighter_order_book_lock = asyncio.Lock()
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


def test_variational_api_amount_to_str_truncates_to_min_qty_tick() -> None:
    assert variational_api_amount_to_str(Decimal("0.0002432227102505721546713663434")) == "0.000243"


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
