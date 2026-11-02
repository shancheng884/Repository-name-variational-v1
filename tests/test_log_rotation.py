import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tools.lib.log_rotation import (
    archive_name,
    existing_archives,
    plan_rotation,
    prune_archives,
    rotate_jsonl,
)


def _write(path: Path, lines: int) -> None:
    rows = [json.dumps({"line": "x", "i": i}) + "\n" for i in range(lines)]
    path.write_text("".join(rows), encoding="utf-8")


FIXED = datetime(2026, 11, 2, 4, 30, tzinfo=timezone.utc)


def test_plan_skips_small_file(tmp_path: Path) -> None:
    log = tmp_path / "order_metrics.jsonl"
    _write(log, 5)

    plan = plan_rotation(log, min_bytes=1024)

    assert plan["rotate"] is False
    assert plan["reason"] == "below_threshold"


def test_plan_skips_missing_file(tmp_path: Path) -> None:
    plan = plan_rotation(tmp_path / "missing.jsonl", min_bytes=1)

    assert plan["rotate"] is False
    assert plan["reason"] == "missing"


def test_plan_uses_utc_stamp_in_archive_name(tmp_path: Path) -> None:
    log = tmp_path / "order_metrics.jsonl"

    name = archive_name(log, now=FIXED)

    assert name.name == "order_metrics.jsonl.full-20261102T043000Z.gz"


def test_rotation_archives_every_line_and_empties_live_file(tmp_path: Path) -> None:
    log = tmp_path / "order_metrics.jsonl"
    _write(log, 200)

    result = rotate_jsonl(log, min_bytes=1, keep=6, now=FIXED)

    assert result["rotated"] is True
    assert result["archived_lines"] == 200
    assert log.stat().st_size == 0
    archive = Path(result["archive"])
    assert archive.exists()
    with gzip.open(archive, "rt", encoding="utf-8") as handle:
        assert len(handle.readlines()) == 200


def test_rotation_keeps_writer_appends_after_truncate(tmp_path: Path) -> None:
    log = tmp_path / "order_metrics.jsonl"
    _write(log, 10)

    rotate_jsonl(log, min_bytes=1, keep=6, now=FIXED)
    with log.open("a", encoding="utf-8") as handle:
        handle.write("{\"line\": \"after\"}\n")

    assert log.read_text(encoding="utf-8") == "{\"line\": \"after\"}\n"


def test_rotation_prunes_old_archives(tmp_path: Path) -> None:
    log = tmp_path / "order_metrics.jsonl"
    _write(log, 10)
    for stamp in ("20260101T000000Z", "20260102T000000Z", "20260103T000000Z"):
        (tmp_path / f"order_metrics.jsonl.full-{stamp}.gz").write_bytes(b"")

    result = rotate_jsonl(log, min_bytes=1, keep=3, now=FIXED)

    assert result["rotated"] is True
    assert len(existing_archives(log)) == 3
    assert result["pruned"] == ["order_metrics.jsonl.full-20260101T000000Z.gz"]
    assert not (tmp_path / "order_metrics.jsonl.full-20260101T000000Z.gz").exists()
    assert (tmp_path / "order_metrics.jsonl.full-20261102T043000Z.gz").exists()


def test_rotation_leaves_file_untouched_when_compression_fails(tmp_path: Path, monkeypatch) -> None:
    log = tmp_path / "order_metrics.jsonl"
    _write(log, 10)
    before = log.read_bytes()

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr("tools.lib.log_rotation._stream_to_gzip", boom)
    result = rotate_jsonl(log, min_bytes=1, keep=6, now=FIXED)

    assert result["rotated"] is False
    assert "disk full" in result["error"]
    assert log.read_bytes() == before
    assert not list(tmp_path.glob("*.part"))


def test_prune_with_non_positive_keep_is_noop(tmp_path: Path) -> None:
    log = tmp_path / "order_metrics.jsonl"
    (tmp_path / "order_metrics.jsonl.full-20260101T000000Z.gz").write_bytes(b"")

    assert prune_archives(log, keep=0) == []
    assert len(existing_archives(log)) == 1
