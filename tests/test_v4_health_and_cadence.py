"""Regression tests for v4 sample cadence, health output and block reasons."""

import asyncio
import json
import logging
import time
from collections import deque
from decimal import Decimal

from main import (
    LIVE_INVENTORY_BASIS_V4_HEALTH_WINDOW_SECONDS,
    LIVE_INVENTORY_BASIS_V4_MAX_SAMPLE_GAP_SECONDS,
    LIVE_INVENTORY_BASIS_V4_MIN_HISTORY_SAMPLES,
    LIVE_INVENTORY_BASIS_V4_MIN_WINDOW_COVERAGE,
    VariationalToLighterRuntime,
)


def _anchor_rows(now, recent_rows, total_count=5760):
    older_count = total_count - len(recent_rows)
    older_rows = [
        (now - 604_700 + index * 30, Decimal(index % 100))
        for index in range(older_count)
    ]
    return deque([*older_rows, *recent_rows])


def _edge_recorder():
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_bidirectional = False
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    return runtime


def test_v4_gate_limits_match_the_reviewed_source_values() -> None:
    """The VPS once carried a silent 0.30/30/300 relaxation; keep it visible."""

    assert LIVE_INVENTORY_BASIS_V4_MIN_WINDOW_COVERAGE == Decimal("0.80")
    assert LIVE_INVENTORY_BASIS_V4_MIN_HISTORY_SAMPLES == 100
    assert LIVE_INVENTORY_BASIS_V4_MAX_SAMPLE_GAP_SECONDS == 60


def test_v4_sample_cadence_is_anchored_to_wall_clock() -> None:
    runtime = _edge_recorder()
    base = 1_000_000.0

    assert (
        runtime.record_live_inventory_basis_v4_edge(
            now=base,
            direction="short_var_long_lighter",
            edge_bps=Decimal("1.0"),
        )
        is True
    )
    # 1_000_000 is 10s into a 30s bucket, so the next slot is the next bucket.
    assert runtime.live_inventory_basis_v4_next_history_sample_at == 1_000_020.0

    assert (
        runtime.record_live_inventory_basis_v4_edge(
            now=base + 19.9,
            direction="short_var_long_lighter",
            edge_bps=Decimal("1.1"),
        )
        is False
    )
    assert (
        runtime.record_live_inventory_basis_v4_edge(
            now=base + 20.0,
            direction="short_var_long_lighter",
            edge_bps=Decimal("1.2"),
        )
        is True
    )
    assert runtime.live_inventory_basis_v4_next_history_sample_at == 1_000_050.0


def test_v4_sample_cadence_does_not_drift_with_loop_latency() -> None:
    """Regression: per-sample lateness used to accumulate past the 60s gate."""

    runtime = _edge_recorder()
    now = 1_000_000.0
    recorded: list[float] = []
    for _ in range(400):
        # The decision loop ticks roughly every 1.9s.
        now += 1.9
        if runtime.record_live_inventory_basis_v4_edge(
            now=now,
            direction="short_var_long_lighter",
            edge_bps=Decimal("1.0"),
        ):
            recorded.append(now)

    gaps = [
        later - earlier for earlier, later in zip(recorded, recorded[1:])
    ]
    assert len(recorded) >= 20
    assert max(gaps) < 32.0
    # The very first slot is a one-off alignment to the wall clock; every
    # later interval stays within one loop tick of the 30s cadence.
    assert min(gaps[1:]) > 28.0
    # Timestamps stay the real quote times: no backfilled or shifted slots.
    assert all(value == value for value in recorded)
    assert runtime.live_inventory_basis_v4_run_sample_count == len(recorded)


def test_v4_health_reasons_name_the_failing_gate() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    health_rows = [
        (now - 3000 + index * 30, Decimal(index)) for index in range(50)
    ] + [
        (now - 1400 + index * 30, Decimal(index + 50)) for index in range(50)
    ]
    runtime.live_inventory_basis_v4_history = _anchor_rows(now, health_rows)

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)
    reasons = runtime.live_inventory_basis_v4_health_reasons(context, threshold)

    assert threshold is None
    assert "recent_1h_health_sample_gap_too_large" in reasons


def test_v4_health_reasons_are_empty_when_ready() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index)) for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _anchor_rows(now, recent_rows)

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)
    reasons = runtime.live_inventory_basis_v4_health_reasons(context, threshold)

    assert threshold is not None
    assert reasons == []


def test_entry_block_payload_expires_stale_blocks() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_last_entry_block = {
        "reason": "basis_entry_edge_below_threshold",
        "at": "2026-11-10T00:00:00+00:00",
        "monotonic": time.monotonic(),
        "direction": "short_var_long_lighter",
        "edge_bps": "4.2",
        "min_entry_edge_bps": "9.5",
    }

    fresh = runtime.live_inventory_entry_block_payload()
    assert fresh["entry_gate_block_reason"] == "basis_entry_edge_below_threshold"
    assert fresh["entry_gate_block"]["edge_bps"] == "4.2"

    runtime.live_inventory_last_entry_block["monotonic"] -= 400.0
    stale = runtime.live_inventory_entry_block_payload()
    assert stale["entry_gate_block_reason"] == "stale_entry_block_cleared"


def test_append_live_inventory_log_remembers_entry_block_reason() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.mode = "live"
    runtime.live_inventory_dry_decisions = True
    runtime.telegram_notifier = None
    runtime.logger = logging.getLogger("test_v4_health")
    written: list[tuple[str, dict]] = []

    async def _append(event_type, payload):
        written.append((event_type, payload))

    runtime.append_order_log = _append

    asyncio.run(
        runtime.append_live_inventory_log(
            "live_inventory_v4_entry_blocked",
            {
                "reason": "recent_1h_health_not_ready",
                "direction": "short_var_long_lighter",
            },
        )
    )
    asyncio.run(
        runtime.append_live_inventory_log(
            "live_inventory_basis_state",
            {"reason": "not_a_block"},
        )
    )

    assert [event for event, _ in written] == [
        "live_inventory_v4_entry_blocked",
        "live_inventory_basis_state",
    ]
    assert (
        runtime.live_inventory_last_entry_block["reason"]
        == "recent_1h_health_not_ready"
    )
    assert (
        runtime.live_inventory_last_entry_block["direction"]
        == "short_var_long_lighter"
    )


def test_v4_health_payload_publishes_gates_and_block() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_basis_v4_bidirectional = False
    runtime.live_inventory_basis_v4_last_threshold_by_direction = {
        "default": (
            Decimal("12.50"),
            {
                "v4_health_ready": True,
                "v4_health_ready_observed": True,
                "v4_health_count": 120,
                "v4_health_coverage_seconds": "3590.100",
                "v4_health_max_sample_gap_seconds": "30.400",
                "v4_anchor_ready": True,
                "v4_anchor_count": 11527,
                "v4_anchor_progress_pct": "100.00",
                "v4_fast_percentile": "95",
            },
            time.monotonic(),
        )
    }
    runtime.live_inventory_last_entry_block = {
        "reason": "basis_entry_edge_below_threshold",
        "at": "2026-11-10T00:00:00+00:00",
        "monotonic": time.monotonic(),
        "direction": "short_var_long_lighter",
        "edge_bps": "4.2",
        "min_entry_edge_bps": "9.5",
        "v4_health_ready": True,
    }

    payload = runtime.live_inventory_v4_health_payload()

    assert payload["v4_health_ready"] is True
    assert payload["v4_health_reasons"] == []
    assert payload["v4_entry_threshold_bps"] == "12.50"
    assert payload["v4_gate_limits"] == {
        "min_window_coverage": "0.80",
        "min_history_samples": 100,
        "max_sample_gap_seconds": 60,
        "health_window_seconds": LIVE_INVENTORY_BASIS_V4_HEALTH_WINDOW_SECONDS,
        "history_sample_seconds": 30,
    }
    assert payload["entry_gate_block_reason"] == "basis_entry_edge_below_threshold"
    # The payload must stay JSON serialisable: it is written to health.json.
    assert json.loads(json.dumps(payload))["v4_health_ready"] is True


def test_v4_health_payload_reports_missing_context() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_mode = True

    payload = runtime.live_inventory_v4_health_payload()

    assert payload["v4_health_ready"] is False
    assert payload["v4_health_reasons"] == ["v4_gate_context_not_initialised"]
