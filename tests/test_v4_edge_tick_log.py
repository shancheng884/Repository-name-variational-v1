import json
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import main as app


def _runtime(tmp_path: Path) -> app.VariationalToLighterRuntime:
    runtime = app.VariationalToLighterRuntime.__new__(app.VariationalToLighterRuntime)
    runtime.output_dir = tmp_path
    runtime.live_inventory_v4_edge_tick_log_enabled = True
    return runtime


def _write(runtime, **overrides):
    payload = dict(
        asset="ETH",
        long_edge_bps=Decimal("8.5"),
        short_edge_bps=Decimal("-12.25"),
        var_bid=Decimal("2050.10"),
        var_ask=Decimal("2050.30"),
        lighter_bid=Decimal("2052.00"),
        lighter_ask=Decimal("2052.10"),
        thresholds={
            app.DIRECTION_LONG_VAR_SHORT_LIGHTER: Decimal("16.0"),
            app.DIRECTION_SHORT_VAR_LONG_LIGHTER: Decimal("-16.4"),
        },
    )
    payload.update(overrides)
    return runtime.record_live_inventory_v4_edge_tick(**payload)


def test_edge_tick_appends_one_compact_line(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    assert _write(runtime) is True
    assert _write(runtime, short_edge_bps=Decimal("-13.5")) is True

    path = runtime.live_inventory_v4_edge_tick_path()
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["a"] == "ETH"
    assert first["S"] == "-12.25"
    assert first["tL"] == "16.0"
    assert first["tS"] == "-16.4"
    assert set(first) == {"t", "a", "L", "S", "vb", "va", "lb", "la", "tL", "tS"}


def test_edge_tick_can_be_switched_off(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    runtime.live_inventory_v4_edge_tick_log_enabled = False
    assert _write(runtime) is False
    assert not runtime.live_inventory_v4_edge_tick_path().exists()


def test_edge_tick_prunes_files_older_than_the_retention(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    directory = tmp_path / "edge_ticks"
    directory.mkdir(parents=True)
    stale = directory / "2026-01-01.jsonl"
    stale.write_text("{}\n", encoding="utf-8")
    old = time.time() - (app.LIVE_INVENTORY_V4_EDGE_TICK_KEEP_DAYS + 2) * 86400
    os.utime(stale, (old, old))

    assert _write(runtime) is True
    assert not stale.exists()
    assert runtime.live_inventory_v4_edge_tick_path().exists()


def test_edge_tick_failure_never_raises(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    runtime.output_dir = tmp_path / "not-a-dir"
    (tmp_path / "not-a-dir").write_text("x", encoding="utf-8")  # a file, not a dir
    assert _write(runtime) is False
