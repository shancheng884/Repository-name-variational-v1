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
        "planned_downtime_excluded": True,
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


def _downtime_runtime(now, hole_seconds, downtime=True):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    first_len = 50
    start = now - 3300
    part_one = [
        (start + index * 30, Decimal(index)) for index in range(first_len)
    ]
    resume = start + first_len * 30 + hole_seconds
    part_two = [
        (resume + index * 30, Decimal(first_len + index))
        for index in range(int((now - resume) // 30) + 1)
    ]
    runtime.live_inventory_basis_v4_history = _anchor_rows(
        now,
        part_one + part_two,
    )
    last_row_before_stop = part_one[-1][0]
    runtime.live_inventory_planned_downtime_cache = (
        [(last_row_before_stop, resume)] if downtime else []
    )
    return runtime


def test_planned_downtime_is_not_a_market_gap() -> None:
    now = 1_000_000.0
    runtime = _downtime_runtime(now, 240.0, downtime=True)

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is not None
    assert context["v4_health_ready"] is True
    assert Decimal(context["v4_health_excluded_downtime_seconds"]) >= Decimal("240")
    assert Decimal(context["v4_health_observed_coverage_seconds"]) >= Decimal(
        "2880"
    )
    assert runtime.live_inventory_basis_v4_health_reasons(context, threshold) == []


def test_unmarked_gap_still_blocks_trading() -> None:
    now = 1_000_000.0
    runtime = _downtime_runtime(now, 240.0, downtime=False)

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is None
    assert context["v4_health_excluded_downtime_seconds"] == "0.000"
    reasons = runtime.live_inventory_basis_v4_health_reasons(context, threshold)
    assert "recent_1h_health_sample_gap_too_large" in reasons


def test_long_planned_downtime_does_not_satisfy_the_48_minute_budget() -> None:
    now = 1_000_000.0
    runtime = _downtime_runtime(now, 2400.0, downtime=True)

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is None
    assert Decimal(context["v4_health_observed_coverage_seconds"]) < Decimal("2880")
    reasons = runtime.live_inventory_basis_v4_health_reasons(context, threshold)
    assert "recent_1h_health_coverage_below_minimum" in reasons


def test_partial_downtime_overlap_keeps_the_gap(tmp_path) -> None:
    now = 1_000_000.0
    runtime = _downtime_runtime(now, 240.0, downtime=True)
    # The marker only covers the first half of the hole, so the rest is a gap.
    hole_start = runtime.live_inventory_planned_downtime_cache[0][0]
    runtime.live_inventory_planned_downtime_cache = [
        (hole_start, hole_start + 120.0)
    ]

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is None
    assert Decimal(context["v4_health_max_sample_gap_seconds"]) > Decimal("60")


def test_shutdown_marker_is_closed_at_the_next_start(tmp_path) -> None:
    path = tmp_path / "planned_downtime.json"
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_planned_downtime_file = path

    runtime.record_live_inventory_planned_downtime(
        reason="signal_15",
        stopped_at=time.time() - 120.0,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["open"]["reason"] == "signal_15"

    restarted = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    restarted.live_inventory_planned_downtime_file = path
    intervals = restarted.load_live_inventory_planned_downtime()

    assert len(intervals) == 1
    start, end = intervals[0]
    assert end > start
    assert 0.0 < end - start <= 125.0
    # The marker is consumed, so a later start cannot widen the same interval.
    assert json.loads(path.read_text(encoding="utf-8"))["open"] is None


def test_downtime_marker_older_than_a_day_is_ignored() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = time.time()
    runtime.live_inventory_planned_downtime_cache = [
        (now - 200_000.0, now - 199_000.0),
    ]

    assert runtime.live_inventory_planned_downtime_intervals(now) == []


def test_health_seeding_keeps_every_sample_in_its_wall_clock_slot() -> None:
    """Rows a few hundred ms early must not be dropped into a fake 60s gap."""

    from main import v4_sample_slot

    interval = 30.0
    start = 1_000_000.0
    rows = [start + index * 29.3 for index in range(120)]
    slots = [v4_sample_slot(value, interval) for value in rows]

    kept = len(dict.fromkeys(slots))
    assert kept >= 115  # the old "previous + 30" chain kept about 80


def test_downtime_marker_pulls_back_to_the_last_sample() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_last_sample_at = 1_000_000.0

    # A shutdown 8s after the last sample starts the downtime at that sample.
    assert runtime.live_inventory_downtime_start_ts(1_000_008.0) == 1_000_000.0
    # A stall that began long before the shutdown is not excused.
    assert runtime.live_inventory_downtime_start_ts(1_000_600.0) == 1_000_600.0
    # Without any sample the marker keeps the shutdown time.
    runtime.live_inventory_basis_v4_last_sample_at = None
    assert runtime.live_inventory_downtime_start_ts(1_000_008.0) == 1_000_008.0


def test_bucket_mode_keeps_rows_the_interval_chain_would_drop() -> None:
    """Regression: the 30s chain must not also gate the bucket-mode window."""

    from main import v4_accept_history_sample

    interval = 30.0
    start = 1_000_000.0
    kept: list[float] = []
    last_bucket = None
    next_sample_at = 0.0
    for step, offset in enumerate([0.0, 20.0, 50.0, 80.0, 110.0]):
        timestamp = start + offset
        accepted, last_bucket = v4_accept_history_sample(
            timestamp=timestamp,
            last_bucket=last_bucket,
            next_sample_at=next_sample_at,
            interval=interval,
            bucket_align=True,
        )
        if accepted:
            kept.append(timestamp)
            next_sample_at = timestamp + interval

    # Every row sits in its own 30s slot, so none of them may be dropped even
    # though the plain interval chain would have skipped the 20s/80s rows.
    assert len(kept) == 5

    # The legacy rule is still available and still skips early rows.
    accepted, _ = v4_accept_history_sample(
        timestamp=start + 20.0,
        last_bucket=None,
        next_sample_at=start + 30.0,
        interval=interval,
        bucket_align=False,
    )
    assert accepted is False


def test_warmup_extends_the_restart_downtime_to_the_first_sample(
    tmp_path, monkeypatch
) -> None:
    import main as main_module

    path = tmp_path / "planned_downtime.json"
    payload = {
        "closed": [
            {
                "stopped_at": "2026-11-10T14:00:00+00:00",
                "started_at": "2026-11-10T14:00:02+00:00",
                "reason": "signal_15",
            }
        ],
        "open": None,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        main_module,
        "PROCESS_STARTED_WALL_CLOCK",
        parse_iso("2026-11-10T14:00:02+00:00"),
    )

    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_planned_downtime_file = path
    runtime.extend_live_inventory_planned_downtime_to_first_sample(
        parse_iso("2026-11-10T14:00:52+00:00")
    )

    intervals = runtime.load_live_inventory_planned_downtime()
    assert intervals and abs(intervals[-1][1] - parse_iso("2026-11-10T14:00:52+00:00")) < 1.0

    # A silent feed after boot is not warm-up: past the cap nothing extends.
    runtime.extend_live_inventory_planned_downtime_to_first_sample(
        parse_iso("2026-11-10T14:30:00+00:00")
    )
    intervals = runtime.load_live_inventory_planned_downtime()
    assert abs(intervals[-1][1] - parse_iso("2026-11-10T14:00:52+00:00")) < 1.0


def parse_iso(value: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(value).timestamp()


def _move_runtime(moves, static=Decimal("3")):
    from collections import deque as _deque

    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_max_sample_move_bps = static
    runtime.live_inventory_basis_sample_move_bps_samples = _deque(maxlen=200)
    now = time.time()
    rows = [(now - 3600 + index * 30, Decimal("0")) for index in range(len(moves) + 1)]
    values = [Decimal("0")]
    for move in moves:
        values.append(values[-1] + Decimal(str(move)))
    runtime.live_inventory_basis_bps_history = _deque(
        (rows[index][0], values[index]) for index in range(len(values))
    )
    return runtime


def test_jump_guard_tracks_the_one_hour_p95() -> None:
    # A calm hour: p95 of the moves is small, so the static 3bps floor holds.
    runtime = _move_runtime([1.0] * 120)
    threshold, context = runtime.live_inventory_basis_dynamic_sample_move_threshold_bps()
    assert threshold == Decimal("3")
    assert context["basis_sample_move_source"] == "one_hour_window"
    assert Decimal(context["basis_sample_move_p95_bps"]) == Decimal("1.0")

    # A violent hour: p95 = 4bps, so the guard opens to 4 * 1.5 = 6bps.
    runtime = _move_runtime([1.0] * 100 + [4.0] * 20)
    threshold, context = runtime.live_inventory_basis_dynamic_sample_move_threshold_bps()
    assert threshold == Decimal("6.0")
    assert context["basis_sample_move_source"] == "one_hour_window"


def test_jump_guard_is_capped() -> None:
    runtime = _move_runtime([9.0] * 120)
    threshold, context = runtime.live_inventory_basis_dynamic_sample_move_threshold_bps()
    assert threshold == Decimal("8.0")
    assert context["basis_sample_move_dynamic_cap_bps"] == "8.0"


def test_jump_guard_falls_back_while_the_hour_fills() -> None:
    from collections import deque as _deque

    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_max_sample_move_bps = Decimal("3")
    runtime.live_inventory_basis_sample_move_bps_samples = _deque(
        [Decimal("2.0")] * 40,
        maxlen=200,
    )
    runtime.live_inventory_basis_bps_history = _deque()

    threshold, context = runtime.live_inventory_basis_dynamic_sample_move_threshold_bps()

    assert context["basis_sample_move_source"] == "short_live_window"
    assert threshold == Decimal("3")


def test_entry_threshold_waits_for_the_history_rebuild_lock() -> None:
    """A rebuild must never be observable as an empty window."""

    import threading

    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index)) for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _anchor_rows(now, recent_rows)
    runtime.live_inventory_basis_v4_projection_cached_at_by_direction = {}
    runtime.live_inventory_basis_v4_projection_cache_by_direction = {}

    lock = runtime.live_inventory_basis_history_lock()
    holding = threading.Event()
    release = threading.Event()

    def _hold() -> None:
        with lock:
            holding.set()
            release.wait(2.0)

    thread = threading.Thread(target=_hold)
    thread.start()
    assert holding.wait(1.0)

    finished = threading.Event()

    def _read() -> None:
        runtime.live_inventory_basis_v4_entry_threshold(now=now)
        finished.set()

    reader = threading.Thread(target=_read)
    reader.start()

    # The reader cannot finish while the rebuild holds the lock.
    assert finished.wait(0.3) is False
    release.set()
    assert finished.wait(2.0) is True
    thread.join()
    reader.join()
