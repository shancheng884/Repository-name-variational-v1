from datetime import datetime, timedelta, timezone
from pathlib import Path
import gzip
import os

from tools.archive_capture import gzip_file, iter_closed_jsonl, prune_verified, remote_key


def test_gzip_file_replaces_closed_jsonl(tmp_path):
    path = tmp_path / "quotes" / "a" / "20261112-10.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text('{"x":1}\n')
    target = gzip_file(path)
    assert not path.exists()
    assert target.exists()
    with gzip.open(target, "rt") as handle:
        assert handle.read() == '{"x":1}\n'


def test_iter_closed_jsonl_skips_current_hour(tmp_path):
    now = datetime(2026, 11, 13, 4, 30, tzinfo=timezone.utc)
    old = tmp_path / "20261113-02.jsonl"
    current = tmp_path / "20261113-04.jsonl"
    old.write_text("old\n")
    current.write_text("current\n")
    old_time = now.timestamp() - 2 * 3600
    os.utime(old, (old_time, old_time))
    files = [path.name for path, _ in iter_closed_jsonl(tmp_path, min_age_minutes=1, now=now)]
    assert files == ["20261113-02.jsonl"]


def test_remote_key_preserves_partitions(tmp_path):
    path = tmp_path / "quotes" / "lighter_mainnet" / "NVDA" / "20261113-01.jsonl.gz"
    assert remote_key(path, tmp_path, "market_capture/") == (
        "market_capture/quotes/lighter_mainnet/NVDA/20261113-01.jsonl.gz"
    )


def test_prune_only_verified_and_old(tmp_path):
    old = tmp_path / "quotes" / "a" / "20261110-00.jsonl.gz"
    new = tmp_path / "quotes" / "a" / "20261113-00.jsonl.gz"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    new.write_bytes(b"new")
    now = datetime(2026, 11, 13, 4, 0, tzinfo=timezone.utc)
    old_time = (now - timedelta(days=3)).timestamp()
    os.utime(old, (old_time, old_time))
    state = {"files": {
        "quotes/a/20261110-00.jsonl.gz": {"status": "verified"},
        "quotes/a/20261113-00.jsonl.gz": {"status": "verified"},
    }}
    removed = prune_verified(tmp_path, state, hot_days=3, now=now)
    assert removed == ["quotes/a/20261110-00.jsonl.gz"]
    assert not old.exists() and new.exists()
