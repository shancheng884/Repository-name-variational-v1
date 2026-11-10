"""An empty window during the rolling rebuild must not flip health."""

import time
from collections import deque
from decimal import Decimal

from main import VariationalToLighterRuntime


def test_empty_window_during_rebuild_reuses_the_last_decision() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_last_threshold_by_direction = {
        "short_var_long_lighter": (
            Decimal("12.5"),
            {"v4_health_ready": True, "v4_health_count": 120},
            time.monotonic(),
        )
    }

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(
        now=1_000_000.0,
        direction="short_var_long_lighter",
    )

    assert threshold == Decimal("12.5")
    assert context["v4_health_ready"] is True


def test_empty_window_without_a_recent_decision_still_blocks() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_last_threshold_by_direction = {
        "short_var_long_lighter": (
            Decimal("12.5"),
            {"v4_health_ready": True},
            time.monotonic() - 600.0,
        )
    }

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(
        now=1_000_000.0,
        direction="short_var_long_lighter",
    )

    assert threshold is None
    assert context["v4_health_ready"] is False
