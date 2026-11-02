import gzip
import json

from tools.lib.metrics_store import (
    dedupe,
    iter_rows,
    metric_files,
    row_identity,
)


def _row(event: str, lot: int, at: str, **extra):
    return {"event": event, "run_id": "run-1", "asset": "ETH", "lot_id": lot,
            "logged_at": at, **extra}


def _write(path, rows):
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )


def _write_gz(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_archives_load_before_the_live_file_in_chronological_order(tmp_path):
    _write_gz(tmp_path / "order_metrics.jsonl.full-20260926T043730Z.gz",
              [_row("live_inventory_exited", 1, "2026-09-26T04:00:00+00:00")])
    _write_gz(tmp_path / "order_metrics.jsonl.full-20261102T031917Z.gz",
              [_row("live_inventory_exited", 2, "2026-11-02T03:00:00+00:00")])
    _write(tmp_path / "order_metrics.jsonl",
           [_row("live_inventory_exited", 3, "2026-11-02T05:00:00+00:00")])

    names = [path.name for path in metric_files(tmp_path)]
    assert names == [
        "order_metrics.jsonl.full-20260926T043730Z.gz",
        "order_metrics.jsonl.full-20261102T031917Z.gz",
        "order_metrics.jsonl",
    ]

    lots = [row["lot_id"] for row in iter_rows(tmp_path)]
    assert lots == [1, 2, 3]


def test_numeric_rotation_keeps_the_live_file_last(tmp_path):
    _write(tmp_path / "order_metrics.jsonl", [])
    _write(tmp_path / "order_metrics.jsonl.1", [])
    _write(tmp_path / "order_metrics.jsonl.2", [])
    names = [path.name for path in metric_files(tmp_path)]
    assert names == [
        "order_metrics.jsonl.2",
        "order_metrics.jsonl.1",
        "order_metrics.jsonl",
    ]


def test_iter_rows_filters_by_event_and_asset(tmp_path):
    _write(
        tmp_path / "order_metrics.jsonl",
        [
            _row("live_inventory_exited", 1, "2026-11-02T05:00:00+00:00"),
            _row("live_inventory_exit_blocked", 1, "2026-11-02T05:00:01+00:00"),
            {**_row("live_inventory_exited", 2, "2026-11-02T05:00:02+00:00"),
             "asset": "WTI"},
        ],
    )
    rows = list(iter_rows(tmp_path, events={"live_inventory_exited"}, asset="ETH"))
    assert [row["lot_id"] for row in rows] == [1]


def test_include_archives_false_reads_only_the_live_file(tmp_path):
    _write_gz(tmp_path / "order_metrics.jsonl.full-20260926T043730Z.gz",
              [_row("live_inventory_exited", 1, "2026-09-26T04:00:00+00:00")])
    _write(tmp_path / "order_metrics.jsonl",
           [_row("live_inventory_exited", 2, "2026-11-02T05:00:00+00:00")])
    rows = list(iter_rows(tmp_path, include_archives=False))
    assert [row["lot_id"] for row in rows] == [2]


def test_dedupe_drops_a_cycle_seen_in_both_archive_and_live_file():
    row = _row("live_inventory_exited", 1, "2026-11-02T05:00:00+00:00")
    assert row_identity(row) == row_identity(dict(row))
    assert dedupe([row, dict(row)]) == [row]


def test_missing_log_directory_is_empty_not_an_error(tmp_path):
    assert metric_files(tmp_path / "missing") == []
    assert list(iter_rows(tmp_path / "missing")) == []