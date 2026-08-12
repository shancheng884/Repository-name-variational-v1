from __future__ import annotations

import gzip
import json
from pathlib import Path

from tools.compact_order_metrics import HIGH_VOLUME_RETENTION, compact


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_compact_preserves_critical_events_and_bounds_diagnostics(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    diagnostic_limit = HIGH_VOLUME_RETENTION[
        "live_inventory_negative_direction_shadow_candidate"
    ]
    rows = [
        {
            "event": "live_inventory_negative_direction_shadow_candidate",
            "sample_index": index,
        }
        for index in range(diagnostic_limit + 5)
    ]
    rows.extend(
        [
            {"event": "live_inventory_entered", "lot_id": 1},
            {"event": "live_inventory_actual_pnl", "actual_pnl_bps": "1.2"},
            {"event": "live_inventory_final_pnl", "final_pnl_bps": "1.1"},
        ]
    )
    write_rows(path, rows)

    archive, source_lines, retained_lines = compact(path)

    assert source_lines == diagnostic_limit + 8
    assert retained_lines == diagnostic_limit + 3
    compacted = [json.loads(line) for line in path.read_text().splitlines()]
    diagnostics = [
        row
        for row in compacted
        if row["event"] == "live_inventory_negative_direction_shadow_candidate"
    ]
    assert len(diagnostics) == diagnostic_limit
    assert diagnostics[0]["sample_index"] == 5
    assert [row["event"] for row in compacted[-3:]] == [
        "live_inventory_entered",
        "live_inventory_actual_pnl",
        "live_inventory_final_pnl",
    ]
    with gzip.open(archive, "rt", encoding="utf-8") as handle:
        archived = [json.loads(line) for line in handle]
    assert archived == rows
