import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

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


def test_live_inventory_common_order_qty_uses_coarser_lighter_step() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.base_amount_multiplier = 10_000

    qty = runtime.live_inventory_common_order_qty(
        asset="ETH",
        qty=Decimal("0.01075"),
    )

    assert qty == Decimal("0.0107")
    assert variational_api_amount_to_str(qty, asset="ETH") == "0.01070"


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
                "lighter_filled_base_amount": "0.0002",
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
                "lighter_filled_base_amount": "0.0002",
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
        assert final_rows[0]["final_lighter_leg_pnl_usd"] == "-0.0004"
        assert final_rows[0]["final_pnl_usd"] == "-0.0061"
        assert final_rows[0]["final_var_pnl_qty"] == "0.0003"
        assert final_rows[0]["final_lighter_pnl_qty"] == "0.0002"
        assert final_rows[0]["cross_venue_entry_qty_delta"] == "0.0001"
        assert final_rows[0]["cross_venue_exit_qty_delta"] == "0.0001"
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


def test_extract_lighter_position_qty_uses_position_sign() -> None:
    result = {
        "code": 200,
        "accounts": [
            {
                "positions": [
                    {"symbol": "BTC", "sign": -1, "position": "0.001"},
                    {"symbol": "ETH", "sign": 1, "position": "0.0210"},
                ]
            }
        ],
    }

    assert VariationalToLighterRuntime.extract_lighter_position_qty(result, asset="ETH") == Decimal("0.0210")
    assert VariationalToLighterRuntime.extract_lighter_position_qty(result, asset="BTC") == Decimal("-0.001")
    assert VariationalToLighterRuntime.extract_lighter_position_qty(result, asset="SOL") == Decimal("0")


def test_live_inventory_persists_pending_entry_submission(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="sell",
                qty=Decimal("0.0105"),
                lot_id=2,
                role="live_inventory_entry_pending_var_fill",
                created_at_monotonic=time.monotonic(),
                context={
                    "direction": "short_var_long_lighter",
                    "submitted_at": "2026-08-06T12:03:09Z",
                    "rfq_id": "rfq-2",
                    "lighter_started": True,
                },
            )
        ]

        await runtime.persist_live_inventory_memory(reason="basis_entry_submission_started")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["status"] == "pending"
        assert state["open_lots"] == []
        assert state["pending_actions"] == [
            {
                "asset": "ETH",
                "side": "sell",
                "qty": "0.0105",
                "lot_id": 2,
                "role": "live_inventory_entry_pending_var_fill",
                "direction": "short_var_long_lighter",
                "submitted_at": "2026-08-06T12:03:09Z",
                "rfq_id": "rfq-2",
                "submitted_order_id": None,
                "lighter_started": True,
                "lighter_record_key": None,
            }
        ]

    asyncio.run(run())


def test_live_inventory_startup_reconcile_rejects_hidden_exchange_position(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {
                    "positions": [
                        {"instrument": {"underlying": "ETH"}, "qty": "-0.0105"}
                    ]
                },
            }

        async def fake_fetch_lighter_account():
            return {"code": 200, "accounts": [{"positions": []}]}

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        try:
            await runtime.reconcile_live_inventory_startup_state()
        except RuntimeError as exc:
            assert "exchange position exists" in str(exc)
        else:
            raise AssertionError("startup reconcile should reject hidden exchange exposure")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "startup_reconcile_local_flat_but_exchange_position_open"
        assert state["manual_review_context"]["variational_position_qty"] == "-0.0105"
        assert runtime.stop_flag is True

    asyncio.run(run())


def test_live_inventory_startup_reconcile_requires_both_exchange_qtys_to_match(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 3,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.0105",
            }
        ]

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {
                    "positions": [
                        {"instrument": {"underlying": "ETH"}, "qty": "-0.0210"}
                    ]
                },
            }

        async def fake_fetch_lighter_account():
            return {
                "code": 200,
                "accounts": [
                    {"positions": [{"symbol": "ETH", "sign": 1, "position": "0.0210"}]}
                ],
            }

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        try:
            await runtime.reconcile_live_inventory_startup_state()
        except RuntimeError as exc:
            assert "do not match local open lots" in str(exc)
        else:
            raise AssertionError("startup reconcile should reject doubled exchange exposure")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["manual_review_reason"] == "startup_reconcile_exchange_position_mismatch"
        assert state["manual_review_context"]["expected_open_qty"] == "0.0105"
        assert state["manual_review_context"]["variational_position_qty"] == "-0.0210"
        assert state["manual_review_context"]["lighter_position_qty"] == "0.0210"

    asyncio.run(run())


def test_live_inventory_startup_reconcile_accepts_verified_flat_exchanges(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []

        async def fake_fetch_variational_positions():
            return {"ok": True, "result": {"positions": []}}

        async def fake_fetch_lighter_account():
            return {"code": 200, "accounts": [{"positions": []}]}

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        await runtime.reconcile_live_inventory_startup_state()

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert rows[-1]["event"] == "live_inventory_startup_reconcile_ok"
        assert rows[-1]["status"] == "both_exchanges_flat"

    asyncio.run(run())


def test_live_inventory_startup_reconcile_rejects_wrong_lighter_direction(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 4,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.0105",
            }
        ]

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "-0.0105"}]},
            }

        async def fake_fetch_lighter_account():
            return {
                "code": 200,
                "accounts": [{"positions": [{"symbol": "ETH", "sign": -1, "position": "0.0105"}]}],
            }

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        try:
            await runtime.reconcile_live_inventory_startup_state()
        except RuntimeError:
            pass
        else:
            raise AssertionError("startup reconcile should reject an unhedged Lighter direction")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["manual_review_reason"] == "startup_reconcile_exchange_position_mismatch"
        assert state["manual_review_context"]["expected_lighter_sign"] == "1"
        assert state["manual_review_context"]["lighter_position_qty"] == "-0.0105"

    asyncio.run(run())


def test_live_inventory_startup_reconcile_accepts_matching_open_pair(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 5,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.0105",
            }
        ]

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "-0.0105"}]},
            }

        async def fake_fetch_lighter_account():
            return {
                "code": 200,
                "accounts": [{"positions": [{"symbol": "ETH", "sign": 1, "position": "0.0105"}]}],
            }

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        await runtime.reconcile_live_inventory_startup_state()

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert rows[-1]["event"] == "live_inventory_startup_reconcile_ok"
        assert rows[-1]["status"] == "open_state_matches_both_exchanges"

    asyncio.run(run())


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
                "entered_sample_index": 1,
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


def test_confirmed_entry_fill_ledger_cannot_be_downgraded_to_pending(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    lot = {
        "lot_id": 1,
        "direction": "short_var_long_lighter",
        "qty": "0.0105",
        "entry_var_fill_price": "1900.25",
        "entry_lighter_fill_price": "1901.10",
        "entry_estimated_var_price": "1900.00",
        "entry_estimated_lighter_price": "1900.80",
        "entry_var_final_fill_qty": "0.0105",
        "entry_lighter_final_fill_qty": "0.0105",
        "entry_var_price_source": "final_fill",
        "entry_lighter_price_source": "final_fill",
        "entry_cost_status": "final_fills_confirmed",
    }
    runtime.live_inventory_open_lots = [lot]

    runtime.remember_live_inventory_final_pnl_lot(asset="ETH", lot=lot)
    updated = runtime.sync_live_inventory_open_lot_entry_cost(
        asset="ETH",
        lot_id=1,
    )

    pending = runtime.pending_live_inventory_final_pnl["ETH:1"]
    assert pending["entry_var_final_fill_price"] == "1900.25"
    assert pending["entry_lighter_final_fill_price"] == "1901.10"
    assert pending["entry_estimated_var_price"] == "1900.00"
    assert pending["entry_estimated_lighter_price"] == "1900.80"
    assert lot["entry_cost_status"] == "final_fills_confirmed"
    assert runtime.live_inventory_entry_cost_confirmed(lot) is True
    assert updated is False


def test_entry_cost_confirmation_accepts_consistent_final_fill_sources() -> None:
    assert VariationalToLighterRuntime.live_inventory_entry_cost_confirmed(
        {
            "entry_cost_status": "final_fills_pending",
            "entry_var_price_source": "final_fill",
            "entry_lighter_price_source": "final_fill",
            "entry_var_fill_price": "1900.25",
            "entry_lighter_fill_price": "1901.10",
        }
    ) is True


def test_reversion_signal_exit_floor_is_separate_from_normal_exit_floor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_min_signal_reverted_exit_pnl_bps = Decimal("3")
    runtime.live_inventory_basis_reversion_signal_exit_min_pnl_bps = Decimal("-1")

    runtime.live_inventory_basis_reversion_mode = False
    assert runtime.live_inventory_signal_reverted_exit_min_pnl_bps(
        time_decayed_min_exit_pnl_bps=Decimal("0.03")
    ) == Decimal("3")

    runtime.live_inventory_basis_reversion_mode = True
    assert runtime.live_inventory_signal_reverted_exit_min_pnl_bps(
        time_decayed_min_exit_pnl_bps=Decimal("0.03")
    ) == Decimal("-1")


def test_reversion_execution_reserve_is_directional() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_reversion_long_execution_reserve_bps = Decimal("4.5")
    runtime.live_inventory_basis_reversion_short_execution_reserve_bps = Decimal("3.5")

    assert runtime.live_inventory_basis_reversion_execution_reserve_bps(
        "long_var_short_lighter"
    ) == Decimal("4.5")
    assert runtime.live_inventory_basis_reversion_execution_reserve_bps(
        "short_var_long_lighter"
    ) == Decimal("3.5")


def _v4_rolling_anchor_rows(
    now: float,
    recent_rows: list[tuple[float, Decimal]],
    total_count: int = 5760,
) -> deque[tuple[float, Decimal]]:
    older_count = total_count - len(recent_rows)
    older_rows = [
        (
            now - 604_700 + index * 30,
            Decimal(index % 100),
        )
        for index in range(older_count)
    ]
    return deque([*older_rows, *recent_rows])


def test_v4_entry_threshold_uses_rolling_7d_anchor_with_recent_health() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold == Decimal("98.50")
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_baseline_window_seconds"] == 604800
    assert context["v4_baseline_count"] == 5760
    assert context["v4_anchor_effective_seconds"] == 172800
    assert Decimal("98") <= threshold < Decimal("99")


def test_v4_entry_threshold_adds_recent_entry_capture_loss_reserve() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_execution_loss_bps_samples = deque(
        [Decimal("0.64")] * 2 + [Decimal("3.19")] * 8,
        maxlen=20,
    )
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert context["v4_raw_entry_threshold_bps"] == "97"
    assert context["v4_entry_capture_sample_count"] == 10
    assert context["v4_entry_capture_calibration_ready"] is True
    assert context["v4_entry_capture_raw_p80_bps"] == "3.19"
    assert context["v4_entry_capture_calibration_weight"] == "0"
    assert context["v4_entry_execution_reserve_bps"] == "1.50"
    assert threshold == Decimal("98.50")
    assert context["v4_entry_threshold_bps"] == "98.50"


def test_v4_entry_threshold_uses_prior_for_immature_entry_capture_reserve() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_execution_loss_bps_samples = deque(
        [Decimal("3.19"), Decimal("0")],
        maxlen=20,
    )
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert context["v4_entry_capture_sample_count"] == 2
    assert context["v4_entry_capture_calibration_ready"] is False
    assert context["v4_entry_capture_raw_p80_bps"] == "3.19"
    assert context["v4_entry_capture_prior_bps"] == "1.50"
    assert context["v4_entry_execution_reserve_bps"] == "1.50"
    assert threshold == Decimal("98.50")


def test_v4_entry_capture_reserve_blends_prior_until_twenty_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_execution_loss_bps_samples = deque(
        [Decimal("3.00")] * 15,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_entry_calibration_context()

    assert context["ready"] is True
    assert context["fully_mature"] is False
    assert context["calibration_weight"] == Decimal("0.5")
    assert context["applied_bps"] == Decimal("2.250")


def test_v4_fast_1d_threshold_can_raise_rolling_7d_entry_gate() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (
            now - 21_600 + index * 30,
            Decimal("20") if index >= 677 else Decimal("0"),
        )
        for index in range(721)
    ]
    older_rows = [
        (
            now - 604_700 + index * 30,
            Decimal(index % 10),
        )
        for index in range(5760 - len(recent_rows))
    ]
    runtime.live_inventory_basis_v4_history = deque([*older_rows, *recent_rows])

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert context["v4_fast_ready"] is True
    assert context["v4_fast_threshold_applied"] is True
    assert Decimal(context["v4_fast_threshold_bps"]) > Decimal(
        context["v4_7d_entry_threshold_bps"]
    )
    assert threshold == Decimal(context["v4_fast_threshold_bps"]) + Decimal(
        "1.50"
    )


def test_v4_entry_threshold_cache_refreshes_on_baseline_cadence() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    first_threshold, first_context = (
        runtime.live_inventory_basis_v4_entry_threshold(now=now)
    )
    runtime.live_inventory_basis_v4_history.append(
        (now + 1, Decimal("1000"))
    )
    cached_threshold, cached_context = (
        runtime.live_inventory_basis_v4_entry_threshold(now=now + 1)
    )
    refreshed_threshold, refreshed_context = (
        runtime.live_inventory_basis_v4_entry_threshold(now=now + 31)
    )

    assert cached_threshold == first_threshold
    assert cached_context["v4_history_samples"] == first_context["v4_history_samples"]
    assert refreshed_context["v4_history_samples"] == (
        first_context["v4_history_samples"] + 1
    )
    assert refreshed_threshold is not None


def test_v4_exit_target_uses_conservative_prior_until_calibration_is_ready() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0.64"), Decimal("6.65")],
        maxlen=20,
    )
    lot: dict[str, object] = {}

    assert runtime.live_inventory_basis_v4_exit_shortfall_reserve_bps() == Decimal(
        "3.50"
    )
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "4.50"
    )
    context = runtime.live_inventory_basis_v4_exit_calibration_context()
    assert context["sample_count"] == 2
    assert context["ready"] is False
    assert context["fully_mature"] is False
    assert context["raw_p80_bps"] == Decimal("6.65")
    assert context["prior_bps"] == Decimal("3.50")
    assert context["calibration_weight"] == Decimal("0")
    assert runtime.live_inventory_basis_v4_confirm_exit_candidate(
        lot,
        eligible=True,
    ) == (False, 1)
    assert runtime.live_inventory_basis_v4_confirm_exit_candidate(
        lot,
        eligible=True,
    ) == (True, 2)
    assert runtime.live_inventory_basis_v4_confirm_exit_candidate(
        lot,
        eligible=False,
    ) == (False, 0)
    assert "v4_exit_confirmation_count" not in lot


def test_v4_exit_target_uses_observed_shortfall_after_ten_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0.10")] * 10 + [Decimal("6.65")] * 5,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["sample_count"] == 15
    assert context["ready"] is True
    assert context["fully_mature"] is False
    assert context["raw_p80_bps"] == Decimal("6.65")
    assert context["calibration_weight"] == Decimal("1")
    assert context["stage_floor_bps"] == Decimal("1.50")
    assert context["applied_dynamic_bps"] == Decimal("3.00")
    assert runtime.live_inventory_basis_v4_exit_shortfall_reserve_bps() == Decimal(
        "3.00"
    )
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "4.00"
    )


def test_v4_exit_target_uses_early_ready_floor_for_low_shortfall() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0")] * 10,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["ready"] is True
    assert context["fully_mature"] is False
    assert context["stage_floor_bps"] == Decimal("1.50")
    assert context["applied_dynamic_bps"] == Decimal("1.50")
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "2.50"
    )


def test_v4_exit_target_caps_observed_shortfall_after_twenty_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0.10")] * 14 + [Decimal("6.65")] * 6,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["sample_count"] == 20
    assert context["ready"] is True
    assert context["fully_mature"] is True
    assert context["raw_p80_bps"] == Decimal("6.65")
    assert context["calibration_weight"] == Decimal("1")
    assert context["stage_floor_bps"] == Decimal("0.50")
    assert context["applied_dynamic_bps"] == Decimal("3.00")
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "4.00"
    )


def test_v4_exit_target_uses_mature_floor_for_low_shortfall() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0")] * 20,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["fully_mature"] is True
    assert context["stage_floor_bps"] == Decimal("0.50")
    assert context["applied_dynamic_bps"] == Decimal("0")
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "1.50"
    )


def test_v4_execution_reserve_loaders_ignore_other_strategies_and_assets(
    tmp_path,
) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.orders_file = Path(tmp_path) / "order_metrics.jsonl"
    runtime.logger = logging.getLogger("test_v4_execution_reserve_loader")
    runtime.live_inventory_execution_loss_bps_samples = deque(maxlen=20)
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(maxlen=20)
    rows = [
        {
            "event": "live_inventory_final_pnl",
            "strategy_version": "basis-v4-live-v3",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "entry_edge_capture_loss_bps": "-0.25",
        },
        {
            "event": "live_inventory_final_pnl",
            "strategy_version": "basis-v4-live-test-v1",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "entry_edge_capture_loss_bps": "3.19",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-test-v1",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "5.19",
            "actual_pnl_bps": "-1.46",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-test-v2",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "1.08",
            "actual_pnl_bps": "1.08",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-v3",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "2.50",
            "actual_pnl_bps": "0.50",
        },
        {
            "event": "live_inventory_final_pnl",
            "strategy_version": "execution-calibration-v1",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "entry_edge_capture_loss_bps": "99",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-v1",
            "asset": "BTC",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "99",
            "actual_pnl_bps": "0",
        },
    ]
    runtime.orders_file.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    runtime.load_recent_live_inventory_execution_loss_bps()
    runtime.load_recent_live_inventory_exit_shortfall_bps()

    assert list(runtime.live_inventory_execution_loss_bps_samples) == [
        Decimal("0"),
        Decimal("3.19"),
    ]
    assert list(runtime.live_inventory_exit_estimate_shortfall_bps_samples) == [
        Decimal("0"),
        Decimal("2.00"),
    ]


def test_v4_entry_threshold_accepts_36h_effective_7d_anchor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
        total_count=4320,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is not None
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_anchor_effective_seconds"] == 129600
    assert context["v4_anchor_min_effective_seconds"] == 129600
    assert context["v4_anchor_missing_effective_seconds"] == 0
    assert context["v4_anchor_progress_pct"] == "100.00"
    assert context["v4_anchor_projected_ready_seconds"] == 0


def test_v4_entry_threshold_keeps_7d_anchor_across_historical_gap() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 21_570 + index * 30, Decimal(index))
        for index in range(720)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is not None
    assert context["v4_mature_windows"] == [604800]
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_baseline_window_seconds"] == 604800
    assert Decimal(context["v4_anchor_max_sample_gap_seconds"]) > Decimal("60")
    assert context["v4_health_max_sample_gap_seconds"] == "30.000"
    assert threshold < Decimal("9999")


def test_v4_entry_threshold_rejects_recent_health_gap_without_dropping_anchor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    health_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(50)
    ] + [
        (now - 1400 + index * 30, Decimal(index + 50))
        for index in range(50)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        health_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is None
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is False
    assert Decimal(context["v4_health_max_sample_gap_seconds"]) > Decimal("60")
    assert len(runtime.live_inventory_basis_v4_history) == 5760


def test_v4_test_mode_can_bypass_recent_health_without_dropping_anchor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_test_skip_recent_health = True
    now = 1_000_000.0
    health_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(50)
    ] + [
        (now - 1400 + index * 30, Decimal(index + 50))
        for index in range(50)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        health_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(
        now=now
    )

    assert threshold is not None
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready_observed"] is False
    assert context["v4_health_gate_bypassed"] is True
    assert context["v4_health_ready"] is True


def test_v4_history_gap_preserves_rolling_anchor_before_recording() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_history = deque(
        [(100.0, Decimal("4.2"))]
    )
    runtime.live_inventory_basis_v4_next_history_sample_at = 130.0
    runtime.live_inventory_basis_v4_history_ready = True
    runtime.live_inventory_basis_v4_history_reason = "ready"

    recorded = runtime.record_live_inventory_basis_v4_edge(
        now=200.0,
        short_edge_bps=Decimal("5.1"),
    )

    assert list(runtime.live_inventory_basis_v4_history) == [
        (100.0, Decimal("4.2")),
        (200.0, Decimal("5.1"))
    ]
    assert recorded is True
    assert runtime.live_inventory_basis_v4_history_ready is True
    assert runtime.live_inventory_basis_v4_history_reason == "ready"


def test_v4_history_loader_requires_7d_anchor_and_recent_health(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    sample_rows = _v4_rolling_anchor_rows(
        now,
        [
            (now - 3001 + index * 30, Decimal(index))
            for index in range(101)
        ],
    )
    rows = [
        {
            "asset": "ETH",
            "logged_at": datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ).isoformat(),
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "short_edge_bps": str(edge_bps),
        }
        for timestamp, edge_bps in sample_rows
    ]
    (asset_dir / "2026-07-24.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["ready"] is True
    assert context["reason"] == "ready"
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_baseline_window_seconds"] == 604800
    assert context["v4_anchor_effective_seconds"] == 172800
    assert context["v4_entry_threshold_bps"] == "98.50"


def test_extension_disconnect_fuse_stops_flat_runtime_after_three_failures() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 0
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_extension_disconnect_fuse")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        for _ in range(3):
            await runtime.record_live_inventory_basis_quote_failure(
                asset="ETH",
                error="No extension command client connected.",
                failure_kind="command_rejected",
            )

        assert runtime.stop_flag is True
        assert runtime.shutdown_reason == "variational_extension_disconnected"
        assert runtime.live_inventory_extension_disconnect_failures == 3
        assert [event for event, _ in events].count(
            "live_inventory_runtime_fuse_triggered"
        ) == 1
        failures = [
            payload
            for event, payload in events
            if event == "live_inventory_basis_quote_failed"
        ]
        assert failures[-1]["extension_consecutive_failures"] == 3

    asyncio.run(run())


def test_non_extension_quote_failure_resets_disconnect_counter() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 2
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_extension_disconnect_reset")

        async def ignore(_event: str, _payload: dict) -> None:
            return None

        runtime.append_live_inventory_log = ignore
        await runtime.record_live_inventory_basis_quote_failure(
            asset="ETH",
            error="HTTP 503",
            failure_kind="command_rejected",
        )

        assert runtime.live_inventory_extension_disconnect_failures == 0
        assert runtime.stop_flag is False

    asyncio.run(run())


def test_html_quote_failure_is_normalized_and_fuses_flat_runtime() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(
            VariationalToLighterRuntime
        )
        runtime.live_inventory_extension_disconnect_failures = 0
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_last_fatal_quote_failure_kind = None
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_html_quote_fuse")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        html_error = (
            "<!doctype html><html><style>"
            + ("x" * 10_000)
            + "</style></html>"
        )
        for _ in range(3):
            await runtime.record_live_inventory_basis_quote_failure(
                asset="ETH",
                error=html_error,
                failure_kind="command_rejected",
            )

        assert runtime.stop_flag is True
        assert runtime.shutdown_reason == "variational_html_response"
        failures = [
            payload
            for event, payload in events
            if event == "live_inventory_basis_quote_failed"
        ]
        assert failures[-1]["error"] == "variational_html_response"
        assert failures[-1]["error_original_chars"] == len(html_error)
        assert failures[-1]["html_response"] is True
        assert [event for event, _ in events].count(
            "live_inventory_runtime_fuse_triggered"
        ) == 1

    asyncio.run(run())


def test_extension_disconnect_fuse_requires_review_when_position_is_open() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 2
        runtime.live_inventory_extension_failure_started_monotonic = (
            time.monotonic() - 61
        )
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = [{"lot_id": 1, "asset": "ETH"}]
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_open_extension_disconnect_fuse")
        events: list[tuple[str, dict]] = []
        reviews: list[dict] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        async def require_review(**kwargs) -> None:
            reviews.append(kwargs)
            runtime.stop_flag = True

        runtime.append_live_inventory_log = capture
        runtime.require_live_inventory_manual_review = require_review

        await runtime.record_live_inventory_basis_quote_failure(
            asset="ETH",
            error="No extension command client connected.",
            failure_kind="command_rejected",
        )

        assert runtime.stop_flag is True
        assert runtime.shutdown_reason == "variational_extension_disconnected"
        assert reviews[0]["reason"] == "variational_extension_disconnected"
        fuse = next(
            payload
            for event, payload in events
            if event == "live_inventory_runtime_fuse_triggered"
        )
        assert fuse["action"] == "manual_exchange_review_required"
        assert fuse["open_lots_total"] == 1

    asyncio.run(run())


def test_extension_disconnect_fuse_grants_open_position_recovery_window() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 2
        runtime.live_inventory_extension_failure_started_monotonic = time.monotonic()
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = [{"lot_id": 1, "asset": "ETH"}]
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_open_extension_disconnect_grace")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        await runtime.record_live_inventory_basis_quote_failure(
            asset="ETH",
            error="No extension command client connected.",
            failure_kind="command_rejected",
        )

        assert runtime.stop_flag is False
        assert runtime.shutdown_reason is None
        assert not any(
            event == "live_inventory_runtime_fuse_triggered"
            for event, _ in events
        )

    asyncio.run(run())


def test_runtime_disk_guard_stops_flat_live_below_three_gb(
    tmp_path,
    monkeypatch,
) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.output_dir = tmp_path
        runtime.live_inventory_last_disk_check_monotonic = 0.0
        runtime.live_inventory_last_disk_warning_monotonic = 0.0
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_runtime_disk_guard")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        monkeypatch.setattr(
            "main.shutil.disk_usage",
            lambda _path: SimpleNamespace(free=2 * 1024**3),
        )

        await runtime.maybe_enforce_live_disk_guard(asset="ETH")

        assert runtime.stop_flag is True
        assert runtime.shutdown_reason == "disk_free_below_stop_threshold"
        fuse = next(
            payload
            for event, payload in events
            if event == "live_inventory_runtime_fuse_triggered"
        )
        assert fuse["action"] == "auto_stop_flat"

    asyncio.run(run())


def test_v4_history_loader_does_not_authorize_recent_1h_without_7d_anchor(
    tmp_path,
) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    rows = [
        {
            "asset": "ETH",
            "logged_at": datetime.fromtimestamp(
                now - 3001 + index * 30, tz=timezone.utc
            ).isoformat(),
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "short_edge_bps": str(index),
        }
        for index in range(101)
    ]
    (asset_dir / "2026-07-24.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["ready"] is False
    assert context["v4_anchor_ready"] is False
    assert context["v4_health_ready"] is True
    assert "v4_entry_threshold_bps" not in context


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
    runtime.live_inventory_max_lighter_book_age_seconds = 0.0
    runtime.live_inventory_exit_blocked_log_throttle_seconds = 0.0
    runtime.live_inventory_lot_notional_usd = Decimal("10")
    runtime.live_inventory_max_total_notional_usd = Decimal("10")
    runtime.live_inventory_max_total_lots = 1
    runtime.live_inventory_min_hold_samples = 0
    runtime.live_inventory_max_hold_samples = 300
    runtime.live_inventory_max_unrealized_loss_bps = Decimal("25")
    runtime.live_inventory_basis_max_hold_action = "exit"
    runtime.live_inventory_basis_min_abs_entry_bps = Decimal("0")
    runtime.live_inventory_basis_exit_safety_buffer_bps = Decimal("0")
    runtime.live_inventory_basis_dynamic_exit_buffer = False
    runtime.live_inventory_basis_refresh_exit_quote_before_submit = False
    runtime.live_inventory_basis_max_var_quote_age_ms = 0.0
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(maxlen=20)
    runtime._last_live_inventory_exit_blocked_log = {}
    runtime.live_inventory_i_accept_basis_addon_diagnostic = False
    runtime.live_inventory_basis_addon_min_basis_improvement_bps = Decimal("1.5")
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


def test_entry_slippage_limit_forces_reduce_only_cleanup_after_both_fills(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_lighter_fill_timeout_seconds = 3.0
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("6")
        runtime.records = {}

        lighter_record = OrderLifecycle(
            trade_key="lighter-entry",
            trade_id="lighter-entry",
            side="sell",
            qty=Decimal("0.01040"),
            asset="ETH",
            mode="live",
            last_variational_status="submitted",
            lighter_fill_price=Decimal("1911.40"),
        )
        lighter_record.processing_stage = "lighter_filled"
        runtime.records[lighter_record.trade_key] = lighter_record
        var_record = OrderLifecycle(
            trade_key="var-entry",
            trade_id="var-entry",
            side="sell",
            qty=Decimal("0.01040"),
            asset="ETH",
            mode="live",
            last_variational_status="filled",
            var_fill_price=Decimal("1909.33"),
        )
        match = PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="sell",
            qty=Decimal("0.01040"),
            lot_id=1,
            role="live_inventory_entry_pending_var_fill",
            created_at_monotonic=time.monotonic(),
            context={
                "direction": "short_var_long_lighter",
                "var_side": "SELL",
                "lighter_price": "1910.07",
                "lighter_record_key": lighter_record.trade_key,
                "lighter_submitted_before_var_fill": True,
            },
        )
        cleanup_calls: list[dict] = []
        reviews: list[dict] = []

        async def fake_wait_for_lighter_final_fill(*_args, **_kwargs):
            return True

        async def fake_cleanup(**kwargs):
            cleanup_calls.append(kwargs)
            return {"enabled": True, "force": kwargs["force"]}

        async def fake_manual_review(**kwargs):
            reviews.append(kwargs)

        runtime.wait_for_lighter_final_fill = fake_wait_for_lighter_final_fill
        runtime.try_auto_close_unhedged_live_inventory_leg = fake_cleanup
        runtime.require_live_inventory_manual_review = fake_manual_review

        await runtime.complete_live_inventory_entry_after_var_fill(
            match=match,
            record=var_record,
            fill_payload={},
        )

        assert len(cleanup_calls) == 1
        assert cleanup_calls[0]["close_var"] is True
        assert cleanup_calls[0]["close_lighter"] is True
        assert cleanup_calls[0]["force"] is True
        assert reviews[0]["reason"] == (
            "basis_entry_lighter_actual_slippage_exceeds_limit"
        )
        assert reviews[0]["context"]["auto_close_unhedged"]["enabled"] is True
        assert runtime.live_inventory_open_lots == []

    asyncio.run(run())


def test_live_inventory_log_includes_run_id_and_config(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    telegram_events = []
    runtime.telegram_notifier = SimpleNamespace(
        enqueue=lambda event_type, payload: telegram_events.append(
            (event_type, payload)
        )
    )
    runtime.live_inventory_run_id = "test-run-id"
    runtime.live_inventory_signal_mode = "basis"
    runtime.live_inventory_max_lots = 1
    runtime.live_inventory_basis_z_entry = Decimal("3")
    runtime.live_inventory_basis_z_exit = Decimal("999")
    runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
    runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("3")
    runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0.5")

    asyncio.run(runtime.append_live_inventory_run_config())
    asyncio.run(runtime.append_live_inventory_log("live_inventory_test_event", {"asset": "ETH"}))

    rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

    assert rows[0]["event"] == "live_inventory_run_config"
    assert rows[0]["run_id"] == "test-run-id"
    assert rows[0]["config"]["live_inventory_signal_mode"] == "basis"
    assert rows[0]["config"]["live_inventory_max_total_notional_usd"] == "10"
    assert rows[1]["event"] == "live_inventory_test_event"
    assert rows[1]["run_id"] == "test-run-id"
    assert telegram_events[-1][0] == "live_inventory_test_event"
    assert telegram_events[-1][1]["run_id"] == "test-run-id"


def test_telegram_enqueue_failure_does_not_escape_live_log(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.telegram_notifier = SimpleNamespace(
        enqueue=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("telegram unavailable")
        )
    )

    asyncio.run(
        runtime.append_live_inventory_log(
            "live_inventory_entered",
            {"asset": "ETH", "lot_id": 1},
        )
    )

    rows = [
        json.loads(line)
        for line in runtime.orders_file.read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert rows[-1]["event"] == "live_inventory_entered"


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


def test_live_inventory_basis_real_entry_submits_var_and_lighter_concurrently(tmp_path) -> None:
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
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("-999")
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
            runtime.records[record.trade_key] = record
            return record, {"trade_key": "entry-1"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

        assert {call["venue"] for call in calls} == {"var", "lighter"}
        var_call = next(call for call in calls if call["venue"] == "var")
        assert var_call["confirm"] is True
        assert var_call["reuse_quote_id"] is None
        assert runtime.live_inventory_open_lots == []
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        assert runtime.pending_live_inventory_var_fill_matches[0].role == "live_inventory_entry_pending_var_fill"
        assert rows[-1]["event"] == "live_inventory_var_entry_submitted"
        assert rows[-1]["entry_confirmation_mode"] == "concurrent_var_and_lighter_pending_var_fill"

        await runtime.process_variational_trade_event(
            {
                "asset": "ETH",
                "side": "buy",
                "qty": var_call["amount"],
                "price": "1753.30",
                "status": "filled",
                "trade_id": "var-fill-1",
                "timestamp": "2999-06-16T03:25:21.000Z",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_open_lots[0]["entry_var_price_source"] == "final_fill"
        assert rows[-1]["event"] == "live_inventory_entered"
        assert rows[-1]["entry_confirmation_mode"] == "concurrent_var_and_lighter_then_var_fill_confirmed"

    asyncio.run(run())


def test_live_inventory_open_notional_uses_final_fill_and_conservative_legacy_fallback(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_lot_notional_usd = Decimal("10")
    runtime.live_inventory_open_lots = [
        {"qty": "0.0002", "entry_var_fill_price": "60000"},
        {"qty": "invalid", "entry_var_fill_price": "60000"},
    ]

    assert runtime.live_inventory_open_notional_usd() == Decimal("22")


def test_live_inventory_basis_abs_entry_threshold_blocks_thin_basis(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_dry_decisions = True
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.live_inventory_basis_min_abs_entry_bps = Decimal("12")
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

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "entry-quote",
                "bid": "1753.00",
                "ask": "1753.25",
                "quoteTimestamp": "2999-06-16T03:25:20.000Z",
            }, Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots == []
        assert any(
            row["event"] == "live_inventory_entry_blocked"
            and row["reason"] == "basis_abs_entry_threshold_not_met"
            for row in rows
        )

    asyncio.run(run())


def test_live_inventory_basis_collect_only_logs_state_without_touching_inventory_state(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_dry_decisions = True
        runtime.live_inventory_collect_only = True
        runtime.live_inventory_completed_cycles = runtime.live_inventory_max_cycles
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
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
        runtime.live_inventory_basis_var_spread_bps_samples = deque(maxlen=20)
        runtime.live_inventory_basis_lighter_spread_bps_samples = deque(maxlen=20)
        runtime.live_inventory_basis_sample_move_bps_samples = deque(maxlen=20)
        runtime.live_inventory_basis_size_ladder_notionals_usd = []
        runtime.live_inventory_basis_reversion_mode = False
        runtime.live_inventory_basis_reversion_min_deviation_bps = Decimal("0")
        runtime.live_inventory_basis_reversion_exit_deviation_bps = Decimal("0")
        runtime.live_inventory_basis_reversion_min_net_expected_pnl_bps = Decimal("0")
        runtime.live_inventory_basis_dynamic_sample_move_threshold_bps = lambda: (Decimal("3"), {})
        runtime.live_inventory_basis_reversion_medians = lambda **_kwargs: {
            300: None,
            1800: None,
            3600: None,
        }
        runtime.live_inventory_basis_reversion_execution_reserve_bps = lambda _direction: Decimal("0")
        runtime.live_inventory_record_stablecoin_basis_sample = lambda _context: None
        runtime.normalize_usdc_price_to_usdt = lambda _price, _context: None
        runtime.live_inventory_stablecoin_edge_context = lambda **_kwargs: (True, {})
        runtime.live_inventory_stablecoin_regime_context = lambda **_kwargs: (False, {})
        runtime.live_inventory_stablecoin_alignment = lambda **_kwargs: "unknown"
        runtime.live_inventory_dynamic_entry_quality_buffer_bps = lambda **_kwargs: Decimal("0")
        original_state = '{"status":"flat","completed_cycles":1}\n'
        runtime.live_inventory_state_file.write_text(original_state, encoding="utf-8")

        async def fake_fetch_live_inventory_stablecoin_context():
            return {}

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "collect-quote",
                "bid": "1753.00",
                "ask": "1753.25",
                "quoteTimestamp": "2999-06-16T03:25:20.000Z",
            }, Decimal("10")

        async def refuse_submit(**_kwargs):
            raise AssertionError("collect-only must not submit orders")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.fetch_live_inventory_stablecoin_context = fake_fetch_live_inventory_stablecoin_context
        runtime.send_variational_place_order = refuse_submit
        runtime.place_lighter_order_from_plan = refuse_submit

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert [row["event"] for row in rows] == ["live_inventory_basis_state"]
        assert rows[0]["basis_collect_only"] is True
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 1
        assert runtime.live_inventory_state_file.read_text(encoding="utf-8") == original_state

    asyncio.run(run())


def test_live_inventory_basis_var_quote_age_guard_blocks_entry(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_dry_decisions = True
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.live_inventory_basis_max_var_quote_age_ms = 1.0
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

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "entry-quote",
                "bid": "1753.00",
                "ask": "1753.25",
                "quoteTimestamp": "2000-01-01T00:00:00.000Z",
            }, Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots == []
        assert any(row["event"] == "live_inventory_entry_blocked" and row["reason"] == "basis_var_quote_too_old" for row in rows)

    asyncio.run(run())


def test_live_inventory_basis_real_entry_rejected_after_concurrent_lighter_requires_review(tmp_path) -> None:
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
            runtime.records[record.trade_key] = record
            return record, {"trade_key": "entry-1"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())
        var_call = next(call for call in calls if call["venue"] == "var")
        await runtime.process_variational_trade_event(
            {
                "asset": "ETH",
                "side": "buy",
                "qty": var_call["amount"],
                "price": "1753.30",
                "status": "rejected",
                "trade_id": "var-reject-1",
                "timestamp": "2999-06-16T03:25:21.000Z",
            }
        )

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert {call["venue"] for call in calls} == {"var", "lighter"}
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.stop_flag is True
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "variational_rejected:pending_live_inventory_entry_pending_var_fill"

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


def test_live_inventory_basis_addon_submits_when_basis_expands(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_i_accept_basis_addon_diagnostic = True
        runtime.live_inventory_max_total_lots = 2
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.live_inventory_basis_z_entry = Decimal("1.2")
        runtime.live_inventory_basis_z_exit = Decimal("0")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("0")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("5")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0")
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01158",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1726.62",
                "entry_lighter_fill_price": "1728.02",
                "entry_basis_bps": "-9.0",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -8.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_bid = Decimal("1726.00")
        snapshot.lighter_ask = Decimal("1726.10")
        snapshot.lighter_buy_price = Decimal("1726.10")
        snapshot.lighter_sell_price = Decimal("1726.00")
        snapshot.lighter_buy_fill_price = Decimal("1726.10")
        snapshot.lighter_sell_fill_price = Decimal("1726.00")
        runtime.lighter_order_book = {
            "bids": {Decimal("1726.00"): Decimal("1")},
            "asks": {Decimal("1726.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1726.00")
        runtime.lighter_best_ask = Decimal("1726.10")
        calls: list[dict] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "addon-quote",
                "bid": "1724.00",
                "ask": "1724.30",
                "quoteTimestamp": "2999-06-16T03:25:20.000Z",
            }, Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            calls.append(kwargs)
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**kwargs):
            record = OrderLifecycle(
                trade_key="addon-entry-2",
                trade_id="",
                side=str(kwargs["side"]).lower(),
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            runtime.records[record.trade_key] = record
            return record, {"trade_key": "addon-entry-2"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert len(calls) == 1
        assert calls[0]["side"] == "BUY"
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        assert runtime.pending_live_inventory_var_fill_matches[0].context["entry_kind"] == "basis_addon"
        assert rows[-1]["event"] == "live_inventory_var_entry_submitted"

    asyncio.run(run())


def test_live_inventory_basis_max_hold_warn_does_not_exit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_basis_z_exit = Decimal("0")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("999")
        runtime.live_inventory_basis_max_hold_action = "warn"
        runtime.live_inventory_max_hold_samples = 300
        runtime.live_inventory_max_unrealized_loss_bps = Decimal("999")
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
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01160",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1723.84",
                "entry_lighter_fill_price": "1725.05",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 300
        submit_calls: list[str] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {
                "quoteId": "exit-quote",
                "bid": "1723.01",
                "ask": "1723.43",
                "quoteTimestamp": "2999-06-16T03:25:20.000Z",
            }, Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert submit_calls == []
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_completed_cycles == 0
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["reason"] == "basis_max_hold_reached_waiting_for_reversion"

    asyncio.run(run())


def test_live_inventory_basis_exit_submits_var_before_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("-999")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 5
        calls: list[str] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"}, Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            calls.append("var")
            return {"ok": True, "result": {"quoteId": "exit-quote"}}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            calls.append("lighter")
            assert calls == ["var", "lighter"]

            class Record:
                processing_stage = "live_submit_sent"

            return Record(), {"trade_key": "lighter-exit"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        assert calls == ["var", "lighter"]
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 1

    asyncio.run(run())


def test_live_inventory_basis_exit_reconciles_var_no_position_before_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("-999")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 5
        calls: list[str] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"}, Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            calls.append("var")
            return {"ok": False, "error": {"error_message": ": No position exists for ETH to reduce"}}

        async def fake_fetch_variational_positions():
            calls.append("positions")
            return {"ok": True, "result": {"positions": []}}

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append("lighter")
            assert kwargs["reduce_only"] is True

            class Record:
                processing_stage = "live_submit_sent"

            return Record(), {"trade_key": "lighter-exit"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert calls == ["var", "positions", "lighter"]
        assert any(row["event"] == "live_inventory_var_exit_reconciled_flat" for row in rows)
        assert rows[-1]["event"] == "live_inventory_exited"
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 1

    asyncio.run(run())


def test_live_inventory_basis_exit_can_skip_blocked_first_lot(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("2")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1710",
                "entry_lighter_fill_price": "1710",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            },
            {
                "lot_id": 2,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            },
        ]
        runtime.live_inventory_sample_index = 5
        exited_lots: list[int] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"}, Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            return {"ok": True, "result": {"quoteId": "exit-quote", "amount": kwargs["amount"]}}

        async def fake_place_lighter_order_from_plan(**kwargs):
            exited_lots.append(kwargs["cycle_id"])

            class Record:
                processing_stage = "live_submit_sent"

            return Record(), {"trade_key": f"lighter-exit-{kwargs['cycle_id']}"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        exited = [row for row in rows if row["event"] == "live_inventory_exited"]
        assert exited_lots == [2]
        assert exited[-1]["lot_id"] == 2
        assert [lot["lot_id"] for lot in runtime.live_inventory_open_lots] == [1]
        assert runtime.live_inventory_completed_cycles == 1

    asyncio.run(run())


def test_live_inventory_basis_exit_safety_buffer_raises_effective_threshold(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0")
        runtime.live_inventory_basis_exit_safety_buffer_bps = Decimal("1")
        runtime.live_inventory_basis_max_hold_action = "warn"
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1710",
                "entry_lighter_fill_price": "1710",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 500

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"}, Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["pnl_bps"] == "0"
        assert rows[-1]["effective_min_exit_pnl_bps"] == "1"

    asyncio.run(run())


def test_live_inventory_basis_dynamic_exit_buffer_uses_recent_shortfall(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_basis_dynamic_exit_buffer = True
        runtime.live_inventory_exit_estimate_shortfall_bps_samples.extend([Decimal("2")])
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0")
        runtime.live_inventory_basis_max_hold_action = "warn"
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1710",
                "entry_lighter_fill_price": "1710",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 500

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"}, Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["dynamic_exit_buffer_bps"] == "2"
        assert rows[-1]["effective_min_exit_pnl_bps"] == "2"

    asyncio.run(run())


def test_live_inventory_basis_refresh_exit_quote_blocks_stale_profitable_exit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("1")
        runtime.live_inventory_basis_refresh_exit_quote_before_submit = True
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 5
        quotes = [
            {"quoteId": "signal-quote", "bid": "1710", "ask": "1710.5"},
            {"quoteId": "refresh-quote", "bid": "1700", "ask": "1700.5"},
        ]

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return quotes.pop(0), Decimal("10")

        async def fake_get_lighter_best_bid_ask():
            return Decimal("1719"), Decimal("1720")

        async def fake_send_variational_place_order(**_kwargs):
            raise AssertionError("should not submit Var exit after stale refresh")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.get_lighter_best_bid_ask = fake_get_lighter_best_bid_ask
        runtime.send_variational_place_order = fake_send_variational_place_order
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["reason"] == "basis_exit_refresh_pnl_below_threshold"

    asyncio.run(run())


def test_live_inventory_basis_refreshed_exit_context_uses_lighter_depth(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return {"quoteId": "refresh-1", "bid": "99.8", "ask": "99.9"}, Decimal(
                "12"
            )

        async def fake_get_lighter_best_bid_ask():
            return Decimal("100.2"), Decimal("100.3")

        async def fake_live_inventory_lighter_depth_context(**_kwargs):
            return {
                "estimated_fill_price": "100.1",
                "reference_price": "100.2",
                "slippage_bps": "1.0",
            }

        runtime.fetch_live_inventory_basis_quote = (
            fake_fetch_live_inventory_basis_quote
        )
        runtime.get_lighter_best_bid_ask = fake_get_lighter_best_bid_ask
        runtime.live_inventory_lighter_depth_context = (
            fake_live_inventory_lighter_depth_context
        )

        context = await runtime.live_inventory_basis_refreshed_exit_context(
            asset="ETH",
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
        )

        assert context["reason"] is None
        assert context["refreshed_pnl_bps"] == Decimal("30.000")
        assert context["executable_pnl_bps"] == Decimal("20.000")
        assert context["executable_lighter_exit_price"] == Decimal("100.1")
        assert context["refresh_quote_ms"] == Decimal("12")

    asyncio.run(run())


def test_v4_fast_refresh_requires_two_consecutive_executable_quotes(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        lot: dict[str, object] = {}
        contexts = [
            {
                "reason": None,
                "refresh_quote_ms": Decimal("10"),
                "refreshed_pnl_bps": Decimal("4.4"),
                "executable_pnl_bps": Decimal("4.3"),
                "exit_lighter_depth": {"slippage_bps": "0.2"},
            },
            {
                "reason": None,
                "refresh_quote_ms": Decimal("11"),
                "refreshed_pnl_bps": Decimal("5.0"),
                "executable_pnl_bps": Decimal("4.8"),
                "exit_lighter_depth": {"slippage_bps": "0.2"},
                "refreshed_var_exit_price": Decimal("99.9"),
                "executable_lighter_exit_price": Decimal("100.2"),
                "executable_pnl": Decimal("0.003"),
            },
            {
                "reason": None,
                "refresh_quote_ms": Decimal("9"),
                "refreshed_pnl_bps": Decimal("5.1"),
                "executable_pnl_bps": Decimal("4.9"),
                "exit_lighter_depth": {"slippage_bps": "0.2"},
                "refreshed_var_exit_price": Decimal("99.9"),
                "executable_lighter_exit_price": Decimal("100.2"),
                "executable_pnl": Decimal("0.0031"),
            },
        ]

        async def fake_refreshed_exit_context(**_kwargs):
            return contexts.pop(0)

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot=lot,
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("4.5"),
        )

        assert result["confirmed"] is True
        assert result["attempts"] == 3
        assert result["confirmation_count"] == 2
        assert result["observations"][0]["confirmation_count"] == 0
        assert result["observations"][1]["confirmation_count"] == 1
        assert result["observations"][2]["confirmation_count"] == 2
        assert result["selected_context"]["executable_pnl_bps"] == Decimal(
            "4.9"
        )
        assert lot["executable_exit_mfe_pnl_bps"] == "4.9"

    asyncio.run(run())


def test_v4_fast_refresh_exhausts_without_consecutive_confirmation(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        lot: dict[str, object] = {}
        executable_values = iter(
            (
                Decimal("4.8"),
                Decimal("4.4"),
                Decimal("4.9"),
                Decimal("4.4"),
                Decimal("4.8"),
                Decimal("4.4"),
            )
        )

        async def fake_refreshed_exit_context(**_kwargs):
            executable_pnl_bps = next(executable_values)
            return {
                "reason": None,
                "refresh_quote_ms": Decimal("10"),
                "refreshed_pnl_bps": Decimal("5.0"),
                "executable_pnl_bps": executable_pnl_bps,
                "exit_lighter_depth": {"slippage_bps": "0.2"},
            }

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot=lot,
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("4.5"),
        )

        assert result["confirmed"] is False
        assert result["attempts"] == 6
        assert result["confirmation_count"] == 0
        assert result["last_block_reason"] == (
            "basis_exit_lighter_depth_pnl_below_threshold"
        )
        assert result["max_executable_pnl_bps"] == Decimal("4.9")

    asyncio.run(run())


def test_v4_fast_refresh_does_not_retry_unavailable_quote(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        calls = 0

        async def fake_refreshed_exit_context(**_kwargs):
            nonlocal calls
            calls += 1
            return {
                "reason": "basis_exit_refresh_quote_unavailable",
                "refresh_quote_ms": None,
            }

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot={},
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("4.5"),
        )

        assert result["confirmed"] is False
        assert result["attempts"] == 1
        assert result["last_block_reason"] == (
            "basis_exit_refresh_quote_unavailable"
        )
        assert calls == 1

    asyncio.run(run())


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


def test_v4_pending_entry_reconciles_before_cycle_and_batch_gates(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 1
        runtime.auto_live_match_window_seconds = 30
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="sell",
                qty=Decimal("0.01"),
                lot_id=2,
                role="live_inventory_entry_pending_var_fill",
                created_at_monotonic=time.monotonic(),
            )
        ]
        calls: list[str] = []

        async def fake_reconcile_pending_entry(*, asset: str) -> bool:
            calls.append(asset)
            return True

        def refuse_batch_gate(**_kwargs):
            raise AssertionError(
                "batch gate ran before pending entry reconciliation"
            )

        runtime.maybe_timeout_pending_live_inventory_var_entry = (
            fake_reconcile_pending_entry
        )
        runtime.live_inventory_v4_batch_entry_gate = refuse_batch_gate

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        assert calls == ["ETH"]

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


def test_live_inventory_entry_blocks_below_lighter_min_base_after_quantize(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("10")
        runtime.lighter_min_base_amount = Decimal("0.05")
        runtime.live_inventory_lighter_submit_slippage_bps = Decimal("15")
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
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "hedge_below_lighter_min_base_amount"
        assert Decimal(state["last_blocked_context"]["lighter_min_base_amount"]) == Decimal("0.05")
        assert Decimal(state["last_blocked_context"]["qty"]) > Decimal("0")

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


def test_v4_entry_preflight_keeps_percentile_floor_without_immediate_arb_floor(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("5")
        runtime.lighter_order_book = {
            "bids": {Decimal("1755.00"): Decimal("1")},
            "asks": {Decimal("1755.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1755.00")
        runtime.lighter_best_ask = Decimal("1755.10")

        kwargs = {
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "var_side": "SELL",
            "qty": Decimal("0.01"),
            "var_price": Decimal("1753"),
            "lighter_price": Decimal("1755.10"),
            "edge_bps": Decimal("-2"),
            "var_spread_bps": Decimal("2"),
            "var_snapshot_timestamp": "2999-06-16T03:25:20.000Z",
            "min_entry_bps": Decimal("-6"),
            "dynamic_entry_buffer_bps": Decimal("5"),
        }

        standard_ok, standard_reason, standard_context = (
            await runtime.live_inventory_entry_preflight(**kwargs)
        )
        v4_ok, v4_reason, v4_context = (
            await runtime.live_inventory_entry_preflight(
                **kwargs,
                apply_dynamic_entry_floor=False,
            )
        )

        assert standard_ok is False
        assert standard_reason == "edge_bps_below_dynamic_live_inventory_entry"
        assert Decimal(standard_context["live_inventory_required_entry_bps"]) > 0
        assert standard_context["live_inventory_dynamic_entry_floor_applied"] is True
        assert v4_ok is True
        assert v4_reason == "ok"
        assert v4_context["live_inventory_required_entry_bps"] == "-6"
        assert v4_context["live_inventory_required_entry_margin_bps"] == "4"
        assert v4_context["live_inventory_dynamic_entry_floor_applied"] is False

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
    assert variational_api_amount_to_str(Decimal("0.2898975067364933127892633559"), asset="SOL") == "0.289"


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
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_execution_calibration = True
        runtime.live_inventory_calibration_max_cycle_loss_usd = Decimal("0.10")
        runtime.live_inventory_realized_pnl_usd = Decimal("0.02478067523956343718372446020")
        runtime.pending_live_inventory_actual_pnl["exit-1"] = {
            "asset": "ETH",
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
        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["asset"] == "ETH"
        assert state["realized_pnl_usd"] == "0.02509222000000000000000000000"
        assert state["reason"] == "actual_pnl_final_fill_update"
        assert "exit-1" not in runtime.pending_live_inventory_actual_pnl
        assert getattr(runtime, "live_inventory_calibration_halted_reason", None) is None
        final_rows = [
            row
            for row in rows
            if row["event"] == "live_inventory_final_pnl"
        ]
        assert len(final_rows) == 1
        assert final_rows[0]["final_pnl_source"] == (
            "actual_pnl_confirmed_fill_reconciliation"
        )
        assert final_rows[0]["final_pnl_usd"] == "0.02509222"

    asyncio.run(run())


def test_live_inventory_actual_pnl_uses_leg_specific_filled_qty(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_actual_pnl["exit-qty"] = {
            "asset": "ETH",
            "lot_id": 1,
            "direction": "short_var_long_lighter",
            "qty": "0.01075",
            "entry_var_final_fill_qty": "0.01075",
            "exit_var_final_fill_qty": "0.01075",
            "entry_lighter_final_fill_qty": "0.0107",
            "entry_var_price": "1859.33",
            "entry_lighter_price": "1861.33",
            "exit_var_price": "1914.82",
            "estimated_pnl_usd": "-0.003136",
            "estimated_pnl_bps": "-1.568957260048286961082131736",
        }

        await runtime.maybe_append_live_inventory_actual_pnl(
            {
                "trade_key": "exit-qty",
                "lighter_filled_price": "1916.531869158878504672897196",
                "lighter_filled_base_amount": "0.0107",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        row = rows[-1]
        assert row["event"] == "live_inventory_actual_pnl"
        assert row["actual_pnl_status"] == "lighter_final_fill_confirmed"
        assert row["actual_var_pnl_qty"] == "0.01075"
        assert row["actual_lighter_pnl_qty"] == "0.0107"
        assert row["cross_venue_entry_qty_delta"] == "0.00005"
        assert row["cross_venue_exit_qty_delta"] == "0.00005"
        assert row["actual_var_leg_pnl_usd"] == "-0.5965175"
        assert row["actual_lighter_leg_pnl_usd"] == "0.5906599999999999999999999972"
        assert row["actual_pnl_usd"] == "-0.0058575000000000000000000028"
        assert row["actual_pnl_bps"] == "-2.930537994493890584993170358"

    asyncio.run(run())


def test_execution_calibration_cycle_loss_triggers_fuse(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_execution_calibration = True
        runtime.live_inventory_calibration_max_cycle_loss_usd = Decimal("0.10")
        runtime.pending_live_inventory_actual_pnl["exit-loss"] = {
            "asset": "ETH",
            "lot_id": 1,
            "direction": "long_var_short_lighter",
            "qty": "1",
            "entry_var_price": "100",
            "entry_lighter_price": "100",
            "exit_var_price": "99.8",
            "estimated_pnl_usd": "-0.2",
            "estimated_pnl_bps": "-20",
        }

        await runtime.maybe_append_live_inventory_actual_pnl(
            {"trade_key": "exit-loss", "lighter_filled_price": "100"}
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_calibration_halted_reason == "calibration_max_cycle_loss_reached"
        assert any(row["event"] == "live_inventory_calibration_loss_fuse_triggered" for row in rows)
        assert rows[-1]["event"] == "live_inventory_actual_pnl"
        assert rows[-1]["actual_pnl_usd"] == "-0.2"

    asyncio.run(run())


def test_execution_calibration_can_lock_direction(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_execution_calibration = True
    runtime.live_inventory_calibration_direction = "short_var_long_lighter"

    runtime.live_inventory_completed_cycles = 0
    assert runtime.live_inventory_calibration_direction_for_cycle() == "short_var_long_lighter"
    runtime.live_inventory_completed_cycles = 1
    assert runtime.live_inventory_calibration_direction_for_cycle() == "short_var_long_lighter"


def test_execution_calibration_alternate_direction_remains_default(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_execution_calibration = True
    runtime.live_inventory_calibration_direction = "alternate"

    runtime.live_inventory_completed_cycles = 0
    assert runtime.live_inventory_calibration_direction_for_cycle() == "long_var_short_lighter"
    runtime.live_inventory_completed_cycles = 1
    assert runtime.live_inventory_calibration_direction_for_cycle() == "short_var_long_lighter"


def test_execution_calibration_weekday_gate_uses_utc(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_calibration_weekdays_only = True

    assert runtime.live_inventory_calibration_entry_time_allowed(
        datetime(2026, 7, 24, 23, 59, tzinfo=timezone.utc)
    )
    assert not runtime.live_inventory_calibration_entry_time_allowed(
        datetime(2026, 7, 25, 0, 0, tzinfo=timezone.utc)
    )


def test_register_actual_pnl_replays_fill_that_arrived_before_pending(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}

        class FilledRecord:
            lighter_fill_ts_iso = "2026-07-15T12:00:00+00:00"

            @staticmethod
            def to_payload() -> dict[str, str]:
                return {
                    "trade_key": "exit-race",
                    "lighter_filled_price": "100",
                    "lighter_filled_at": "2026-07-15T12:00:00+00:00",
                }

        finalized = await runtime.register_live_inventory_actual_pnl(
            trade_key="exit-race",
            pending={
                "asset": "ETH",
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "1",
                "entry_var_price": "100",
                "entry_lighter_price": "100",
                "exit_var_price": "100.1",
                "estimated_pnl_usd": "0.1",
                "estimated_pnl_bps": "10",
            },
            lighter_record=FilledRecord(),
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert finalized is True
        assert "exit-race" not in runtime.pending_live_inventory_actual_pnl
        assert rows[-1]["event"] == "live_inventory_actual_pnl"
        assert rows[-1]["actual_pnl_usd"] == "0.1"

    asyncio.run(run())


def test_live_inventory_state_asset_uses_allowed_single_asset(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}

        await runtime.persist_live_inventory_memory(reason="test")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["asset"] == "ETH"

    asyncio.run(run())


def test_v4_exit_pair_submits_both_legs_concurrently(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        var_started = asyncio.Event()
        lighter_started = asyncio.Event()

        async def fake_send_variational_place_order(**_kwargs):
            var_started.set()
            await asyncio.wait_for(lighter_started.wait(), timeout=1)
            return {"ok": True, "result": {"quoteId": "exit-var"}}

        async def fake_place_lighter_order_from_plan(**kwargs):
            assert kwargs["reduce_only"] is True
            lighter_started.set()
            await asyncio.wait_for(var_started.wait(), timeout=1)

            class Record:
                processing_stage = "live_submit_sent"

            return Record(), {"trade_key": "exit-lighter"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        original_append_live_inventory_log = runtime.append_live_inventory_log

        async def guarded_append_live_inventory_log(event_type, payload):
            if (
                event_type == "live_inventory_execution_ledger"
                and payload.get("execution_stage") == "submit_started"
            ):
                await asyncio.wait_for(var_started.wait(), timeout=1)
                await asyncio.wait_for(lighter_started.wait(), timeout=1)
            await original_append_live_inventory_log(event_type, payload)

        runtime.append_live_inventory_log = guarded_append_live_inventory_log

        result = await runtime.submit_live_inventory_exit_pair(
            asset="ETH",
            lot={"lot_id": 7, "basis_trace_id": "trace-7"},
            direction="short_var_long_lighter",
            exit_side="BUY",
            qty=Decimal("0.01"),
            var_amount="0.01",
            var_exit_price=Decimal("1900"),
            exit_lighter_depth={"estimated_fill_price": "1901"},
        )

        var_result, _, lighter_record, lighter_payload, _, var_exc, lighter_exc, context = result
        assert var_started.is_set()
        assert lighter_started.is_set()
        assert var_result["ok"] is True
        assert lighter_record is not None
        assert lighter_payload["trade_key"] == "exit-lighter"
        assert var_exc is None
        assert lighter_exc is None
        assert context["submit_mode"] == "concurrent"
        assert context["var_submit_ok"] is True
        assert context["lighter_submit_started"] is True

        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        ledger = [row for row in rows if row["event"] == "live_inventory_execution_ledger"]
        assert [row["execution_stage"] for row in ledger] == [
            "submit_started",
            "submit_returned",
        ]

    asyncio.run(run())


def test_v4_exit_pair_preserves_one_leg_exception_outcome(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True

        async def fake_send_variational_place_order(**_kwargs):
            raise RuntimeError("var timeout unknown")

        async def fake_place_lighter_order_from_plan(**_kwargs):
            class Record:
                processing_stage = "live_submit_sent"

            return Record(), {"trade_key": "exit-lighter"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        result = await runtime.submit_live_inventory_exit_pair(
            asset="ETH",
            lot={"lot_id": 8},
            direction="short_var_long_lighter",
            exit_side="BUY",
            qty=Decimal("0.01"),
            var_amount="0.01",
            var_exit_price=Decimal("1900"),
            exit_lighter_depth=None,
        )

        assert result[0] is None
        assert isinstance(result[5], RuntimeError)
        assert result[2] is not None
        assert result[7]["lighter_submit_started"] is True
        assert result[7]["var_submit_exception"] == "var timeout unknown"

    asyncio.run(run())


def test_v4_completed_cycle_emits_report_and_stops(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_cycle_report_emitted = False
        runtime.live_inventory_last_final_pnl_payload = None
        runtime.live_inventory_exit_events_logged = {"9.0"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 1
        runtime.stop_flag = False

        async def fake_persist_live_inventory_memory(**_kwargs):
            return None

        runtime.persist_live_inventory_memory = fake_persist_live_inventory_memory
        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is True
        assert runtime.stop_flag is True
        assert runtime.live_inventory_cycle_report_emitted is True
        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert rows[-1]["event"] == "live_inventory_cycle_report"
        assert rows[-1]["report_status"] == "completed"
        assert rows[-1]["auto_stop"] is True

    asyncio.run(run())


def test_v4_completed_cycle_waits_for_actual_pnl_queue(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_cycle_report_emitted = False
        runtime.live_inventory_last_final_pnl_payload = None
        runtime.live_inventory_exit_events_logged = {"9"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 1
        runtime.pending_live_inventory_actual_pnl = {"exit-9": {"lot_id": 9}}
        runtime.stop_flag = False

        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is False
        assert runtime.stop_flag is False
        assert runtime.live_inventory_cycle_report_emitted is False

    asyncio.run(run())


def test_v4_intermediate_cycle_checkpoints_without_stopping(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_cycle_report_emitted = False
        runtime.live_inventory_last_final_pnl_payload = None
        runtime.live_inventory_exit_events_logged = {"9"}
        runtime.live_inventory_v4_checkpointed_lot_ids = set()
        runtime.pending_live_inventory_final_pnl = {
            "ETH:1.0": {
                "asset": "eth",
                "lot_id": "9.0",
                "final_pnl_emitted": False,
            },
            "ETH:9": {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_emitted": True,
            },
        }
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 5
        runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
        runtime.live_inventory_realized_pnl_usd = Decimal("0.003")
        runtime.stop_flag = False

        async def fake_persist_live_inventory_memory(**_kwargs):
            return None

        runtime.persist_live_inventory_memory = fake_persist_live_inventory_memory
        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is False
        assert runtime.stop_flag is False
        assert runtime.live_inventory_cycle_report_emitted is False
        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert rows[-1]["event"] == "live_inventory_v4_cycle_checkpoint"
        assert rows[-1]["completed_cycles"] == 1
        assert rows[-1]["next_cycle"] == 2
        assert rows[-1]["cumulative_run_pnl_usd"] == "0.003"
        assert runtime.pending_live_inventory_final_pnl == {}

    asyncio.run(run())


def test_live_inventory_final_pnl_key_normalizes_numeric_lot_ids() -> None:
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        " eth ", 1
    ) == "ETH:1"
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        "ETH", "1.0"
    ) == "ETH:1"
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        "ETH", Decimal("1.00")
    ) == "ETH:1"
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        "ETH", "lot-a"
    ) == "ETH:lot-a"


def test_v4_checkpointed_lot_ignores_late_duplicate_fill(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_v4_checkpointed_lot_ids = {"9"}
        runtime.pending_live_inventory_final_pnl = {}

        await runtime.maybe_append_live_inventory_final_pnl_from_fill(
            {
                "auto_live_role": "live_inventory_exit",
                "auto_live_cycle_id": "9.0",
                "asset": "ETH",
                "qty": "0.01",
                "lighter_filled_price": "1900",
            }
        )

        assert runtime.pending_live_inventory_final_pnl == {}

    asyncio.run(run())


def test_v4_batch_gate_prunes_checkpointed_cycle_reconciliation(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 9
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_realized_pnl_usd = Decimal("-0.006")
    runtime.live_inventory_v4_checkpointed_lot_ids = {"1"}
    runtime.pending_live_inventory_actual_pnl = {
        "late-exit": {"asset": "ETH", "lot_id": "1.0"},
    }
    runtime.pending_live_inventory_final_pnl = {
        "ETH:1.0": {
            "asset": "ETH",
            "lot_id": 1,
            "final_pnl_emitted": False,
        },
    }
    runtime.pending_live_inventory_var_fill_matches = [
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="sell",
            qty=Decimal("0.01"),
            lot_id=1,
            role="live_inventory_entry_pending_var_fill",
            created_at_monotonic=time.monotonic(),
        ),
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.01"),
            lot_id=1,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic(),
        ),
    ]

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is True
    assert reason == "ready"
    assert context["pending_actual_pnl"] == 0
    assert context["unresolved_final_pnl"] == 0
    assert context["reconciliation_cleanup"] == {
        "pending_actual_pnl": 1,
        "pending_final_pnl": 1,
        "pending_var_fill_matches": 2,
    }
    assert runtime.pending_live_inventory_actual_pnl == {}
    assert runtime.pending_live_inventory_final_pnl == {}
    assert runtime.pending_live_inventory_var_fill_matches == []
    assert runtime.pending_live_inventory_actions_payload() == []


def test_v4_cycle_checkpoint_waits_for_exit_registration(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 9
        runtime.live_inventory_exit_events_logged = {"1"}
        runtime.live_inventory_v4_exit_reconciliation_lot_ids = {"1"}
        runtime.stop_flag = False

        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 1,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is False
        assert getattr(runtime, "live_inventory_v4_checkpointed_lot_ids", set()) == set()

    asyncio.run(run())


def test_v4_batch_entry_gate_waits_for_reconciliation_and_cooldown(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 5
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.05")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 180.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_realized_pnl_usd = Decimal("0.01")
    runtime.live_inventory_v4_last_exit_monotonic = 100.0
    runtime.pending_live_inventory_actual_pnl = {"trade": {}}

    ready, reason, _ = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=200.0
    )
    assert ready is False
    assert reason == "v4_batch_waiting_for_reconciliation"

    runtime.pending_live_inventory_actual_pnl = {}
    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=200.0
    )
    assert ready is False
    assert reason == "v4_batch_cycle_cooldown"
    assert context["cooldown_remaining_seconds"] == 80.0

    ready, reason, _ = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=281.0
    )
    assert ready is True
    assert reason == "ready"


def test_v4_batch_entry_gate_supports_optional_cumulative_loss_limit(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 5
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.05")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 180.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("1.00")
    runtime.live_inventory_realized_pnl_usd = Decimal("0.95")

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is False
    assert reason == "v4_batch_max_run_loss_reached"
    assert context["batch_run_pnl_usd"] == "-0.05"


def test_v4_stop_loss_halts_the_remaining_batch(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 3
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.025")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_realized_pnl_usd = Decimal("-0.005")
    runtime.live_inventory_v4_batch_halted_reason = None

    runtime.require_live_inventory_basis_v4_rearm(
        exit_reason="max_unrealized_loss_bps",
        entry_threshold_bps=Decimal("-7.5"),
    )
    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is False
    assert reason == "v4_batch_halted_after_stop_loss"
    assert context["v4_episode_state"] == "halted"


def test_v4_max_hold_exit_enforces_thirty_minute_cooldown(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 3
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.025")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_v4_last_exit_monotonic = 100.0
    runtime.live_inventory_v4_last_exit_reason = "v4_max_hold_timeout"

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=200.0
    )

    assert ready is False
    assert reason == "v4_batch_cycle_cooldown"
    assert context["cycle_cooldown_seconds"] == 1800.0
    assert context["cooldown_remaining_seconds"] == 1700.0


def test_v4_max_hold_cooldown_survives_process_restart(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 3
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.025")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_v4_last_exit_monotonic = 0.0
    runtime.live_inventory_v4_last_exit_reason = "v4_max_hold_timeout"
    runtime.live_inventory_v4_last_exit_at = datetime.now(timezone.utc).isoformat()

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate()

    assert ready is False
    assert reason == "v4_batch_cycle_cooldown"
    assert 1799.0 <= context["cooldown_remaining_seconds"] <= 1800.0


def test_v4_episode_rearms_only_after_three_confirmed_resets() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_open_lots = []
    runtime.live_inventory_v4_episode_id = "episode-1"
    runtime.live_inventory_v4_next_tranche_index = 2
    runtime.live_inventory_v4_batch_halted_reason = None
    runtime.require_live_inventory_basis_v4_rearm(
        exit_reason="v4_executable_net_target_reached",
        entry_threshold_bps=Decimal("-7.5"),
    )

    rearmed, _ = runtime.live_inventory_basis_v4_update_rearm(
        short_edge_bps=Decimal("-7.9"),
        entry_threshold_bps=Decimal("-7.5"),
    )
    assert rearmed is False
    assert runtime.live_inventory_v4_rearm_confirmation_count == 0

    for expected_count in (1, 2):
        rearmed, _ = runtime.live_inventory_basis_v4_update_rearm(
            short_edge_bps=Decimal("-8.1"),
            entry_threshold_bps=Decimal("-7.5"),
        )
        assert rearmed is False
        assert runtime.live_inventory_v4_rearm_confirmation_count == expected_count

    rearmed, context = runtime.live_inventory_basis_v4_update_rearm(
        short_edge_bps=Decimal("-8.2"),
        entry_threshold_bps=Decimal("-7.5"),
    )

    assert rearmed is True
    assert runtime.live_inventory_v4_rearm_required is False
    assert runtime.live_inventory_v4_episode_id is None
    assert runtime.live_inventory_v4_next_tranche_index == 1
    assert context["v4_episode_state"] == "armed"


def test_v4_episode_state_survives_restart_for_future_tranches() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.load_live_inventory_state = lambda: {
        "open_lots": [],
        "next_lot_id": 4,
        "realized_pnl_usd": "0.01",
        "completed_cycles": 2,
        "v4_episode_id": "episode-2",
        "v4_next_tranche_index": 3,
        "v4_rearm_required": True,
        "v4_rearm_confirmation_count": 2,
        "v4_rearm_reason": "v4_executable_net_target_reached",
        "v4_rearm_threshold_bps": "-7.5",
        "v4_last_exit_reason": "v4_executable_net_target_reached",
        "v4_last_exit_at": "2026-08-08T00:00:00+00:00",
    }

    runtime.sync_live_inventory_memory_from_state()

    assert runtime.live_inventory_v4_episode_id == "episode-2"
    assert runtime.live_inventory_v4_next_tranche_index == 3
    assert runtime.live_inventory_v4_rearm_required is True
    assert runtime.live_inventory_v4_rearm_confirmation_count == 2
    assert runtime.live_inventory_v4_rearm_threshold_bps == Decimal("-7.5")


def test_v4_batch_entry_gate_disables_cumulative_loss_limit_at_zero(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 9
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("1.00")
    runtime.live_inventory_realized_pnl_usd = Decimal("-10.00")

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is True
    assert reason == "ready"
    assert context["batch_max_run_loss_usd"] == "0"


def test_v4_basis_state_logging_is_adaptive_but_keeps_crossings(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_last_basis_state_log_monotonic = 0.0
    runtime.live_inventory_open_lots = []

    assert runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-10", "v4_entry_threshold_bps": "-8"}
    )
    assert not runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-10", "v4_entry_threshold_bps": "-8"}
    )
    assert runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-7", "v4_entry_threshold_bps": "-8"}
    )
    assert not runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-7", "v4_entry_threshold_bps": "-8"}
    )


def test_v4_negative_direction_shadow_logging_is_throttled(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_open_lots = []
    runtime.live_inventory_negative_direction_shadow_last_monotonic = 0.0

    assert runtime.should_log_live_inventory_negative_direction_shadow()
    assert not runtime.should_log_live_inventory_negative_direction_shadow()
    runtime.live_inventory_negative_direction_shadow_last_monotonic -= 301
    assert runtime.should_log_live_inventory_negative_direction_shadow()
