from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import gzip
import json
import os
import queue
import re
import shutil
import threading
import time

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_component(value: str) -> str:
    text = _SAFE.sub("_", str(value)).strip("._-")
    return text[:80] or "unknown"


def disk_free_gb(path: Path) -> float:
    try:
        usage = shutil.disk_usage(path)
        return usage.free / (1024 ** 3)
    except OSError:
        return 0.0


class TapeWriter:
    """Append-only JSONL writer with hourly rotation.

    A dedicated thread keeps disk latency out of the event loop.  Rows are
    grouped per file and written in small batches; the queue is bounded and
    drops are counted rather than blocking market feeds.
    """

    def __init__(self, root: Path, *, queue_size: int = 50000, flush_seconds: float = 0.5) -> None:
        self.root = Path(root)
        self.queue: queue.Queue[tuple[str, str, str, dict[str, Any], datetime]] = queue.Queue(queue_size)
        self.flush_seconds = flush_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="market-capture-writer", daemon=True)
        self._stats_lock = threading.Lock()
        self.counts: Counter[tuple[str, str]] = Counter()
        self.dropped: Counter[tuple[str, str]] = Counter()
        self.bytes_written = 0
        self.last_write_at: datetime | None = None
        self._closed = False
        self._started = False

    def start(self) -> "TapeWriter":
        if not self._started:
            self._thread.start()
            self._started = True
        return self

    def path_for(self, stream: str, venue: str, symbol: str, at: datetime) -> Path:
        hour = at.astimezone(timezone.utc).strftime("%Y%m%d-%H")
        return (
            self.root
            / safe_component(stream)
            / safe_component(venue)
            / safe_component(symbol)
            / f"{hour}.jsonl"
        )

    def append(
        self,
        stream: str,
        venue: str,
        symbol: str,
        row: dict[str, Any],
        *,
        at: datetime | None = None,
    ) -> bool:
        record = (
            safe_component(stream),
            safe_component(venue),
            safe_component(symbol),
            row,
            (at or datetime.now(timezone.utc)).astimezone(timezone.utc),
        )
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            with self._stats_lock:
                self.dropped[(record[0], record[1])] += 1
            return False
        with self._stats_lock:
            self.counts[(record[0], record[1])] += 1
        return True

    def _run(self) -> None:
        pending: list[tuple[str, str, str, dict[str, Any], datetime]] = []
        last_flush = time.monotonic()
        while not self._stop.is_set() or not self.queue.empty() or pending:
            timeout = max(0.05, min(self.flush_seconds, 0.5))
            try:
                pending.append(self.queue.get(timeout=timeout))
            except queue.Empty:
                pass
            while len(pending) < 1000:
                try:
                    pending.append(self.queue.get_nowait())
                except queue.Empty:
                    break
            if pending and (
                len(pending) >= 500
                or time.monotonic() - last_flush >= self.flush_seconds
                or self.queue.empty()
            ):
                self._write(pending)
                pending.clear()
                last_flush = time.monotonic()
        if pending:
            self._write(pending)

    def _write(self, records: list[tuple[str, str, str, dict[str, Any], datetime]]) -> None:
        groups: dict[Path, list[str]] = defaultdict(list)
        for stream, venue, symbol, row, at in records:
            path = self.path_for(stream, venue, symbol, at)
            groups[path].append(json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str) + "\n")
        for path, lines in groups.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = "".join(lines).encode("utf-8")
            with path.open("ab") as handle:
                handle.write(payload)
            with self._stats_lock:
                self.bytes_written += len(payload)
                self.last_write_at = datetime.now(timezone.utc)

    def pending(self) -> int:
        return self.queue.qsize()

    def flush(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.queue.empty():
                time.sleep(self.flush_seconds + 0.05)
                if self.queue.empty():
                    return True
            time.sleep(0.02)
        return False

    def close(self, timeout: float = 10.0) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._started:
            self._thread.join(timeout)
        with self._stats_lock:
            self.last_write_at = datetime.now(timezone.utc)

    def stats(self) -> dict[str, Any]:
        with self._stats_lock:
            by_stream: dict[str, int] = Counter()
            by_venue: dict[str, int] = Counter()
            for (stream, venue), count in self.counts.items():
                by_stream[stream] += count
                by_venue[venue] += count
            dropped = {f"{k[0]}:{k[1]}": v for k, v in self.dropped.items()}
            return {
                "rows": dict(by_stream),
                "rows_by_venue": dict(by_venue),
                "dropped": dropped,
                "pending": self.queue.qsize(),
                "bytes_written": self.bytes_written,
                "last_write_at": self.last_write_at.isoformat() if self.last_write_at else None,
            }


def gzip_cold_files(
    root: Path,
    *,
    older_than_hours: float = 6.0,
    now: datetime | None = None,
    limit: int = 500,
) -> dict[str, int]:
    """Compress closed .jsonl files. Never touches the current hour file."""
    root = Path(root)
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    current_hour = now.strftime("%Y%m%d-%H")
    cutoff = now.timestamp() - older_than_hours * 3600
    compressed = 0
    saved = 0
    if not root.exists():
        return {"files": 0, "bytes_saved": 0}
    for path in sorted(root.rglob("*.jsonl")):
        if path.stem == current_hour or path.stem >= current_hour:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat.st_mtime > cutoff:
            continue
        target = path.with_suffix(path.suffix + ".gz")
        try:
            with path.open("rb") as source, gzip.open(target, "wb", compresslevel=6) as sink:
                shutil.copyfileobj(source, sink, length=1024 * 256)
            path.unlink()
        except OSError:
            if target.exists():
                target.unlink(missing_ok=True)
            continue
        compressed += 1
        try:
            saved += max(0, stat.st_size - target.stat().st_size)
        except OSError:
            pass
        if compressed >= limit:
            break
    return {"files": compressed, "bytes_saved": saved}


def prune_old(root: Path, *, retention_days: int, now: datetime | None = None) -> dict[str, int]:
    """Delete old capture files, including .gz, older than retention_days."""
    root = Path(root)
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cutoff = now.timestamp() - max(1, retention_days) * 86400
    removed = 0
    freed = 0
    if not root.exists():
        return {"files": 0, "bytes": 0}
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in {".jsonl", ".gz"}:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat.st_mtime >= cutoff:
            continue
        try:
            path.unlink()
        except OSError:
            continue
        removed += 1
        freed += stat.st_size
    return {"files": removed, "bytes": freed}
