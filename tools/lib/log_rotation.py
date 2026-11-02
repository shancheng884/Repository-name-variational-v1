from __future__ import annotations

import gzip
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARCHIVE_SEPARATOR = ".full-"
CHUNK_BYTES = 4 * 1024 * 1024
VERIFY_LINE_TOLERANCE = 2
DEFAULT_KEEP_ARCHIVES = 6
DEFAULT_MIN_BYTES = 200 * 1024 * 1024


def archive_name(path: Path, *, now: datetime | None = None) -> Path:
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return path.with_name(f"{path.name}{ARCHIVE_SEPARATOR}{stamp}.gz")


def existing_archives(path: Path) -> list[Path]:
    prefix = f"{path.name}{ARCHIVE_SEPARATOR}"
    if not path.parent.exists():
        return []
    return sorted(
        (
            item
            for item in path.parent.iterdir()
            if item.is_file()
            and item.name.startswith(prefix)
            and item.name.endswith(".gz")
        ),
        key=lambda item: item.name,
    )


def plan_rotation(
    path: Path,
    *,
    min_bytes: int = DEFAULT_MIN_BYTES,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not path.exists():
        return {"rotate": False, "reason": "missing", "size_bytes": 0}
    size = path.stat().st_size
    if size < max(1, min_bytes):
        return {"rotate": False, "reason": "below_threshold", "size_bytes": size}
    return {
        "rotate": True,
        "reason": "threshold_reached",
        "size_bytes": size,
        "archive": archive_name(path, now=now),
    }


def _stream_to_gzip(source: Path, target: Path, *, level: int) -> tuple[int, int]:
    copied = 0
    newlines = 0
    with source.open("rb") as reader, gzip.open(target, "wb", compresslevel=level) as writer:
        while True:
            chunk = reader.read(CHUNK_BYTES)
            if not chunk:
                break
            writer.write(chunk)
            copied += len(chunk)
            newlines += chunk.count(b"\n")
    return copied, newlines


def _count_archive_lines(path: Path) -> int:
    lines = 0
    with gzip.open(path, "rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            lines += chunk.count(b"\n")
    return lines


def truncate_live_file(path: Path) -> None:
    with path.open("r+b") as handle:
        handle.truncate(0)


def prune_archives(path: Path, *, keep: int) -> list[str]:
    if keep <= 0:
        return []
    archives = existing_archives(path)
    if len(archives) <= keep:
        return []
    removed: list[str] = []
    for item in archives[: len(archives) - keep]:
        try:
            item.unlink()
        except OSError:
            continue
        removed.append(item.name)
    return removed


def rotate_jsonl(
    path: Path,
    *,
    min_bytes: int = DEFAULT_MIN_BYTES,
    keep: int = DEFAULT_KEEP_ARCHIVES,
    level: int = 6,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Archive an append-only jsonl file, then truncate it in place.

    Safe while a writer holds the file open in append mode: the bytes are
    streamed into a gzip archive and verified first, and the live file is only
    truncated once that succeeded.
    """

    plan = plan_rotation(path, min_bytes=min_bytes, now=now)
    if not plan["rotate"]:
        return {**plan, "rotated": False}

    archive = plan["archive"]
    temporary = archive.with_suffix(archive.suffix + ".part")

    try:
        copied, expected_lines = _stream_to_gzip(path, temporary, level=level)
        if copied <= 0:
            temporary.unlink(missing_ok=True)
            return {**plan, "rotated": False, "error": "no_bytes_copied"}
        archived_lines = _count_archive_lines(temporary)
        if archived_lines < expected_lines - VERIFY_LINE_TOLERANCE:
            temporary.unlink(missing_ok=True)
            return {
                **plan,
                "rotated": False,
                "error": "archive_incomplete",
                "archived_lines": archived_lines,
                "expected_lines": expected_lines,
            }
        os.replace(temporary, archive)
    except Exception as exc:
        temporary.unlink(missing_ok=True)
        return {**plan, "rotated": False, "error": f"{type(exc).__name__}:{exc}"}

    truncate_live_file(path)
    pruned = prune_archives(path, keep=keep)
    return {
        **plan,
        "rotated": True,
        "archive": str(archive),
        "archived_bytes": copied,
        "archived_lines": archived_lines,
        "original_bytes": plan["size_bytes"],
        "pruned": pruned,
    }
