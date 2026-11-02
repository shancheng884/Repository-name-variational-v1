"""Archive-aware reader for the live order-metrics stream.

Rotation moves the live file aside as ``order_metrics.jsonl.full-<UTC>.gz`` and
then truncates it in place. A reader that only opens ``order_metrics.jsonl``
therefore loses every cycle that happened before a rotation, which makes the
daily PnL report and every latency audit silently under-count history.

This module is the single reader for reports and audits: archives oldest
first, the live file last, one line stream over all of them.
"""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path
from typing import Any, Iterable, Iterator

ACTIVE_NAME = "order_metrics.jsonl"

ARCHIVE_RE = re.compile(
    r"^order_metrics\.jsonl"
    r"(?:\.full-(?P<stamp>\d{8}T\d{6}Z)|\.(?P<index>\d+))?"
    r"(?:\.gz)?$"
)


def is_metric_file(path: Path) -> bool:
    return bool(ARCHIVE_RE.match(Path(path).name))


def sort_key(path: Path) -> tuple[int, str, int]:
    """Oldest archive first, live file last."""

    name = Path(path).name
    if name == ACTIVE_NAME:
        return (2, "", 0)
    match = ARCHIVE_RE.match(name)
    if match is None:
        return (3, name, 0)
    stamp = match.group("stamp")
    if stamp:
        return (0, stamp, 0)
    # Legacy numeric rotation: the live file becomes ``.1``, so a larger
    # index is an older copy.
    return (1, "", -int(match.group("index") or 0))


def metric_files(log_dir: Path, *, include_archives: bool = True) -> list[Path]:
    root = Path(log_dir)
    if not root.exists():
        return []
    files = [item for item in root.iterdir() if item.is_file() and is_metric_file(item)]
    if not include_archives:
        files = [item for item in files if item.name == ACTIVE_NAME]
    return sorted(files, key=sort_key)


def _open(path: Path) -> Any:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return path.open("r", encoding="utf-8", errors="ignore")


def iter_lines(
    log_dir: Path, *, include_archives: bool = True
) -> Iterator[tuple[Path, int, str]]:
    for path in metric_files(Path(log_dir), include_archives=include_archives):
        try:
            handle = _open(path)
        except OSError:
            continue
        try:
            with handle:
                for number, line in enumerate(handle, 1):
                    yield path, number, line
        except OSError:
            continue


def iter_rows(
    log_dir: Path,
    *,
    include_archives: bool = True,
    events: Iterable[str] | None = None,
    asset: str | None = None,
) -> Iterator[dict[str, Any]]:
    wanted = {str(item) for item in events} if events else None
    needles = tuple(f'"event": "{item}"' for item in sorted(wanted or ()))
    wanted_asset = str(asset).upper() if asset else ""
    for _path, _number, line in iter_lines(
        Path(log_dir), include_archives=include_archives
    ):
        text = line.strip()
        if not text:
            continue
        if needles and not any(needle in line for needle in needles):
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if wanted is not None and str(row.get("event") or "") not in wanted:
            continue
        row_asset = str(row.get("asset") or "").upper()
        if wanted_asset and row_asset and row_asset != wanted_asset:
            continue
        yield row


def read_rows(
    log_dir: Path,
    *,
    include_archives: bool = True,
    events: Iterable[str] | None = None,
    asset: str | None = None,
) -> list[dict[str, Any]]:
    return list(
        iter_rows(
            Path(log_dir),
            include_archives=include_archives,
            events=events,
            asset=asset,
        )
    )


def row_identity(row: dict[str, Any]) -> tuple[str, ...]:
    """Stable identity used to drop the same row seen twice.

    Rotation copies bytes and only then truncates, so a crash in between can
    leave one cycle in both the archive and the live file.
    """

    lot = row.get("lot_id")
    if lot is None:
        lot = row.get("cycle_id")
    return (
        str(row.get("event") or ""),
        str(row.get("run_id") or ""),
        str(row.get("asset") or "").upper(),
        str(lot if lot is not None else ""),
        str(row.get("logged_at") or ""),
    )


def dedupe(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        key = row_identity(row)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result