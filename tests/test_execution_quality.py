import asyncio
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from main import VariationalToLighterRuntime
from tools.lib.execution_quality import exchange_epoch_seconds, lighter_book_clock


@pytest.mark.parametrize("scale", [1, 1000, 1_000_000, 1_000_000_000])
def test_exchange_time_units(scale):
    assert exchange_epoch_seconds(1792500000 * scale) == 1792500000


@pytest.mark.parametrize("value", [None, True, False, 0, -1, "nan", "inf", "bad"])
def test_invalid_exchange_times_are_not_fresh(value):
    assert exchange_epoch_seconds(value) is None


def runtime_with_book(*, direction="long_var_short_lighter"):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.lighter_order_book_lock = asyncio.Lock()
    runtime.live_inventory_basis_max_var_quote_age_ms = 1500
    runtime.live_inventory_max_lighter_book_age_seconds = 2
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_basis_v4_bidirectional = True
    runtime.live_inventory_max_lighter_slippage_bps = Decimal("6")
    runtime.live_inventory_lighter_exit_submit_slippage_bps = Decimal("30")
    runtime.live_inventory_exit_shortfall_bps_samples_by_direction = {
        value: deque(maxlen=20) for value in ("long_var_short_lighter", "short_var_long_lighter")
    }
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(maxlen=20)
    runtime.lighter_order_book_offset = 42
    bid, ask = ("100", "100.01") if direction == "long_var_short_lighter" else ("100.10", "100.11")
    runtime.lighter_best_bid, runtime.lighter_best_ask = Decimal(bid), Decimal(ask)
    runtime.lighter_order_book = {
        "bids": {Decimal(bid): Decimal("1")}, "asks": {Decimal(ask): Decimal("1")},
    }
    runtime.record_lighter_order_book_clock({"timestamp": time.time() * 1000})
    runtime.live_inventory_order_limiter = lambda _venue: SimpleNamespace(
        snapshot=lambda: {"used": 0, "hard_limit": 36, "backoff_seconds": 0}
    )
    return runtime


def profit_context(qty=Decimal("0.2")):
    return {
        "quote_request_qty": str(qty), "quote_size_mode": "exact_base_qty_v1",
        "quote_timestamp": datetime.now(timezone.utc).isoformat(), "quote_asset": "ETH",
        "entry_var_price": Decimal("100"), "entry_lighter_price": Decimal("100"),
        "effective_min_exit_pnl_bps": Decimal("4.5"), "exit_reserve_bps": Decimal("3.5"),
    }


def check(runtime, *, direction="long_var_short_lighter", context=None, qty=Decimal("0.2")):
    return runtime.live_inventory_profit_exit_preflight(
        asset="ETH", direction=direction, qty=qty,
        var_exit_price=Decimal("100.1") if direction == "long_var_short_lighter" else Decimal("100"),
        reuse_quote_id="quote-1", context=context or profit_context(qty),
    )


def test_received_now_does_not_make_old_exchange_frame_fresh():
    runtime = runtime_with_book()
    runtime.record_lighter_order_book_clock({"timestamp": (time.time() - 7) * 1000})
    valid, age = runtime.live_inventory_lighter_book_age_ok()
    assert not valid and age >= 7
    assert runtime.lighter_order_book_freshness_context()["book_local_age_seconds"] < 0.1


def test_exchange_state_time_is_checked_even_with_new_frame():
    runtime = runtime_with_book()
    runtime.record_lighter_order_book_clock({
        "timestamp": time.time() * 1000,
        "order_book": {"last_updated_at": (time.time() - 10) * 1_000_000},
    })
    assert not runtime.live_inventory_lighter_book_age_ok()[0]


@pytest.mark.parametrize("message", [{}, {"timestamp": "nan"}, {"timestamp": 9999999999999}])
def test_unknown_or_future_exchange_time_blocks_profit_exit(message):
    runtime = runtime_with_book()
    runtime.record_lighter_order_book_clock(message)
    result = asyncio.run(check(runtime))
    assert result["reason"] == "profit_exit_lighter_book_stale_before_submit"


def test_frame_timestamp_can_be_iso():
    at = datetime.now(timezone.utc)
    clock = lighter_book_clock({"timestamp": at.isoformat()}, received_epoch=at.timestamp())
    assert clock["error"] is None


def test_local_monotonic_age_still_matters_when_exchange_time_looks_fresh():
    runtime = runtime_with_book()
    runtime.last_lighter_order_book_received_monotonic -= 5
    assert not runtime.live_inventory_lighter_book_age_ok()[0]


@pytest.mark.parametrize("direction", ["long_var_short_lighter", "short_var_long_lighter"])
def test_valid_profit_exit_rechecks_full_quantity_and_returns_quote(direction):
    runtime = runtime_with_book(direction=direction)
    result = asyncio.run(check(runtime, direction=direction))
    assert result["ok"]
    assert Decimal(result["executable_pnl_bps"]) >= Decimal(result["effective_min_exit_pnl_bps"])
    assert result["exit_lighter_depth"]["requested_qty"] == "0.2"
    assert result["exit_lighter_depth"]["book_offset"] == 42
    assert result["completion_slippage_budget_bps"] == "30"


def test_full_batch_depth_blocks_exit_although_top_price_is_profitable():
    runtime = runtime_with_book(direction="short_var_long_lighter")
    runtime.lighter_order_book["bids"] = {Decimal("100.1"): Decimal("0.01"), Decimal("99"): Decimal("1")}
    result = asyncio.run(check(runtime, direction="short_var_long_lighter"))
    assert result["reason"] == "profit_exit_pnl_below_threshold_before_submit"
    assert result["exit_lighter_depth"]["levels_used"] == 2


@pytest.mark.parametrize("direction", ["long_var_short_lighter", "short_var_long_lighter"])
def test_normal_exit_retains_anchored_completion_budget_when_one_leg_cancels(direction):
    async def run():
        runtime = runtime_with_book(direction=direction)
        submitted = []

        async def var_submit(**_kwargs):
            return {"ok": True}

        async def lighter_submit(**kwargs):
            submitted.append(kwargs)
            return SimpleNamespace(
                lighter_order_status="canceled-too-much-slippage", processing_stage="live_submit_failed",
                lighter_reduce_only=True, lighter_client_order_id=42,
            ), {"trade_key": "exit-1"}

        async def no_log(*_args):
            pass

        runtime.send_variational_place_order = var_submit
        runtime.place_lighter_order_from_plan = lighter_submit
        runtime.append_live_inventory_log = no_log
        result = await runtime.submit_live_inventory_exit_pair(
            asset="ETH", lot={"lot_id": 1}, direction=direction,
            exit_side="SELL" if direction == "long_var_short_lighter" else "BUY",
            qty=Decimal("0.2"), var_amount="0.2",
            var_exit_price=Decimal("100.1") if direction == "long_var_short_lighter" else Decimal("100"),
            exit_lighter_depth={}, reuse_quote_id="quote-1", profit_exit_context=profit_context(),
        )
        assert result[0]["ok"] is True
        assert result[2].lighter_order_status == "canceled-too-much-slippage"
        assert result[5:7] == (None, None)
        assert result[-1]["lighter_submit_started"] is True
        assert runtime.lighter_exit_submission_known(result[2])
        assert submitted[0]["reduce_only"] is True
        preflight = result[-1]["profit_preflight"]
        assert submitted[0]["retry_price_boundary"] == Decimal(preflight["completion_price_boundary"])
        top = runtime.lighter_best_ask if direction == "long_var_short_lighter" else runtime.lighter_best_bid
        multiplier = Decimal("1.003") if direction == "long_var_short_lighter" else Decimal("0.997")
        assert submitted[0]["retry_price_boundary"] == top * multiplier

    asyncio.run(run())


def test_execution_unknown_does_not_become_an_automatically_retryable_cancel():
    runtime = runtime_with_book()
    unknown = SimpleNamespace(
        processing_stage="live_submit_timed_out", lighter_reduce_only=True,
        lighter_client_order_id=42, lighter_order_status=None,
    )
    assert not runtime.lighter_exit_submission_known(unknown)


def test_tail_reserve_can_tighten_after_candidate_was_selected():
    runtime = runtime_with_book()
    runtime.observe_live_inventory_exit_shortfall("long_var_short_lighter", Decimal("10"))
    result = asyncio.run(check(runtime))
    assert result["reason"] == "profit_exit_pnl_below_threshold_before_submit"
    assert result["effective_min_exit_pnl_bps"] == "11.0"


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("failure", ["stale", "price_move", "expired_quote", "budget"])
def test_preflight_failure_starts_neither_venue_order(batch, failure):
    async def run():
        runtime = runtime_with_book()
        context = profit_context()
        if failure == "stale":
            runtime.record_lighter_order_book_clock({"timestamp": (time.time() - 0.4) * 1000})
        elif failure == "price_move":
            runtime.lighter_order_book["asks"] = {Decimal("101"): Decimal("1")}
        elif failure == "expired_quote":
            context["quote_timestamp"] = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        else:
            runtime.live_inventory_order_limiter = lambda _venue: SimpleNamespace(
                snapshot=lambda: {"used": 36, "hard_limit": 36, "backoff_seconds": 0}
            )

        async def no_submit(**_kwargs):
            raise AssertionError("No order may start when the preflight fails")

        runtime.send_variational_place_order = no_submit
        runtime.place_lighter_order_from_plan = no_submit
        result = await runtime.submit_live_inventory_exit_pair(
            asset="ETH", lot={"lot_id": 1, "portfolio_atomic_exit": batch},
            direction="long_var_short_lighter", exit_side="SELL", qty=Decimal("0.2"),
            var_amount="0.2", var_exit_price=Decimal("100.1"), exit_lighter_depth={},
            reuse_quote_id="quote-1", profit_exit_context=context,
        )
        assert result[-1]["preflight_blocked"]
        assert result[:7] == (None,) * 7

    asyncio.run(run())


def test_p95_detects_four_tail_losses_ignored_by_p80():
    runtime = runtime_with_book()
    samples = runtime.live_inventory_exit_shortfall_bps_samples_by_direction["long_var_short_lighter"]
    samples.extend([Decimal("0")] * 16 + [Decimal("4")] * 4)
    context = runtime.live_inventory_basis_v4_exit_calibration_context(direction="long_var_short_lighter")
    assert context["raw_p80_bps"] == 0
    assert context["raw_p95_bps"] == 4
    assert context["reserve_bps"] == 4
    assert context["cap_bps"] is None


def test_single_new_tail_loss_raises_reserve_immediately_then_decays_slowly():
    runtime = runtime_with_book()
    direction = "short_var_long_lighter"
    for _ in range(20):
        runtime.observe_live_inventory_exit_shortfall(direction, Decimal("0"))
    runtime.observe_live_inventory_exit_shortfall(direction, Decimal("6.65"))
    assert runtime.live_inventory_basis_v4_exit_calibration_context(direction=direction)["reserve_bps"] == Decimal("6.65")
    runtime.observe_live_inventory_exit_shortfall(direction, Decimal("0"))
    assert runtime.live_inventory_basis_v4_exit_calibration_context(direction=direction)["reserve_bps"] == Decimal("6.40")
    assert runtime.live_inventory_basis_v4_threshold_cache_by_direction == {}
    assert runtime.live_inventory_basis_v4_exit_calibration_context(direction="long_var_short_lighter")["reserve_bps"] == Decimal("3.50")


def test_reserve_envelope_survives_state_reload():
    runtime = runtime_with_book()
    runtime.load_live_inventory_state = lambda: {
        "open_lots": [], "v4_exit_shortfall_reserve_floor_by_direction": {
            "short_var_long_lighter": "5.75", "long_var_short_lighter": "NaN",
        },
    }
    runtime.sync_live_inventory_memory_from_state()
    assert runtime.live_inventory_basis_v4_exit_calibration_context(direction="short_var_long_lighter")["reserve_bps"] == Decimal("5.75")
    assert "long_var_short_lighter" not in runtime.live_inventory_exit_shortfall_reserve_floor_by_direction
    runtime.live_inventory_basis_v4_mode = True
    payload = runtime.live_inventory_v4_episode_payload()
    assert payload["v4_exit_shortfall_reserve_floor_by_direction"] == {"short_var_long_lighter": "5.75"}


def test_observed_exit_cost_increases_new_entry_threshold_without_waiting_for_cache():
    runtime = runtime_with_book()
    runtime.live_inventory_basis_v4_bidirectional = False
    now = 1_000_000.0
    runtime.live_inventory_basis_v4_history = deque([
        (now - 604700 + i * 30, Decimal(i % 100)) for i in range(5660)
    ] + [(now - 3000 + i * 30, Decimal(i)) for i in range(100)])
    before, _ = runtime.live_inventory_basis_v4_entry_threshold(now=now)
    runtime.observe_live_inventory_exit_shortfall("short_var_long_lighter", Decimal("6.65"))
    after, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)
    assert after - before == Decimal("3.15")
    assert context["v4_entry_exit_cost_increment_bps"] == "3.15"
