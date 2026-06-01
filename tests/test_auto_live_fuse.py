import logging
from decimal import Decimal

from main import AutoLivePositionState, VariationalToLighterRuntime


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
    runtime._last_auto_live_guard_log = None
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
