from datetime import datetime, timezone
from decimal import Decimal

from tools.lib.funding_ledger import (
    append_rows,
    beijing_day_bounds,
    daily_funding_usd,
    read_ledger,
    sum_funding_between,
)


def _row(fid, at, change, venue="lighter", asset="ETH"):
    return {
        "venue": venue,
        "asset": asset,
        "funding_id": fid,
        "funding_at": at,
        "change_usd": change,
        "source": "lighter_position_funding",
    }


def test_appending_is_idempotent_by_funding_id(tmp_path):
    path = tmp_path / "ledger.jsonl"
    rows = [_row(1, "2026-11-03T00:00:00+00:00", "0.000239")]

    assert append_rows(path, rows) == 1
    assert append_rows(path, rows) == 0
    assert len(read_ledger(path)) == 1


def test_a_failed_rerun_does_not_duplicate_rows(tmp_path):
    path = tmp_path / "ledger.jsonl"
    append_rows(path, [_row(1, "2026-11-03T00:00:00+00:00", "0.0002")])
    append_rows(
        path,
        [
            _row(1, "2026-11-03T00:00:00+00:00", "0.0002"),
            _row(2, "2026-11-03T01:00:00+00:00", "0.0003"),
        ],
    )
    assert len(read_ledger(path)) == 2


def test_summing_only_counts_the_requested_window_and_venue():
    rows = [
        _row(1, "2026-11-03T00:00:00+00:00", "0.0002"),
        _row(2, "2026-11-03T01:00:00+00:00", "0.0003"),
        _row(3, "2026-11-03T01:00:00+00:00", "9.9", venue="variational"),
        _row(4, "2026-11-03T20:00:00+00:00", "5.0"),
    ]
    start = datetime(2026, 11, 2, 16, tzinfo=timezone.utc)  # 11-03 00:00 Beijing
    end = datetime(2026, 11, 3, 16, tzinfo=timezone.utc)
    total = sum_funding_between(rows, start, end, venue="lighter", asset="ETH")
    assert total == Decimal("0.0005")


def test_daily_total_uses_the_beijing_day(tmp_path):
    path = tmp_path / "ledger.jsonl"
    append_rows(
        path,
        [
            # 2026-11-03 00:30 Beijing == 2026-11-02 16:30 UTC
            _row(1, "2026-11-02T16:30:00+00:00", "0.0004"),
            # 2026-11-02 23:30 Beijing == 2026-11-02 15:30 UTC (previous day)
            _row(2, "2026-11-02T15:30:00+00:00", "0.0009"),
        ],
    )
    assert daily_funding_usd(path, "2026-11-03") == Decimal("0.0004")
    assert daily_funding_usd(path, "2026-11-02") == Decimal("0.0009")


def test_a_missing_ledger_reads_as_empty(tmp_path):
    assert read_ledger(tmp_path / "nope.jsonl") == []
    assert daily_funding_usd(tmp_path / "nope.jsonl", "2026-11-03") == Decimal("0")


def test_beijing_day_bounds_are_half_open():
    start, end = beijing_day_bounds("2026-11-03")
    assert start.isoformat() == "2026-11-03T00:00:00+08:00"
    assert end.isoformat() == "2026-11-04T00:00:00+08:00"