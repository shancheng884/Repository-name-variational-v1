from datetime import datetime, timedelta, timezone
from pathlib import Path
import gzip

from tools.market_capture.storage import TapeWriter, gzip_cold_files, prune_old


def test_writer_rotates_hourly_and_flushes(tmp_path):
    writer = TapeWriter(tmp_path, flush_seconds=0.01).start()
    old = datetime(2026, 11, 12, 10, 59, tzinfo=timezone.utc)
    new = datetime(2026, 11, 12, 11, 0, tzinfo=timezone.utc)
    assert writer.append("quotes", "lighter_mainnet", "NVDA", {"n": 1}, at=old)
    assert writer.append("quotes", "lighter_mainnet", "NVDA", {"n": 2}, at=new)
    writer.close()
    files = sorted((tmp_path / "quotes" / "lighter_mainnet" / "NVDA").glob("*.jsonl"))
    assert [p.name for p in files] == ["20261112-10.jsonl", "20261112-11.jsonl"]
    assert '"n":1' in files[0].read_text()
    assert '"n":2' in files[1].read_text()


def test_writer_drops_when_queue_full(tmp_path):
    writer = TapeWriter(tmp_path, queue_size=1)
    assert writer.append("quotes", "a", "b", {"x": 1})
    assert not writer.append("quotes", "a", "b", {"x": 2})
    assert writer.stats()["dropped"] == {"quotes:a": 1}
    writer.close()


def test_gzip_only_closed_hours(tmp_path):
    target = tmp_path / "quotes" / "a" / "b"
    target.mkdir(parents=True)
    old = target / "20261112-10.jsonl"
    current = target / "20261112-11.jsonl"
    old.write_text("hello\n")
    current.write_text("now\n")
    past = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
    import os
    os.utime(old, (past.timestamp() - 7200, past.timestamp() - 7200))
    result = gzip_cold_files(tmp_path, older_than_hours=1, now=past)
    assert result["files"] == 1
    assert not old.exists() and current.exists()
    with gzip.open(old.with_suffix(".jsonl.gz"), "rt") as fh:
        assert fh.read() == "hello\n"


def test_prune_removes_old_files(tmp_path):
    path = tmp_path / "quotes" / "a" / "b" / "20260101-00.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("x")
    import os
    old = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
    os.utime(path, (old, old))
    result = prune_old(tmp_path, retention_days=1, now=datetime(2026, 11, 12, tzinfo=timezone.utc))
    assert result["files"] == 1 and not path.exists()
