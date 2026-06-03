import asyncio
import json
import logging
from collections import deque
from decimal import Decimal
from pathlib import Path

from main import AutoLivePositionState, OrderLifecycle, PendingAutoLiveMatch, VariationalToLighterRuntime


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
