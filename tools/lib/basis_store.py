from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "3"


def _utc_day(value: str | None = None) -> str:
    if value:
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(text).astimezone(timezone.utc).date().isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).date().isoformat()


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def compress_jsonl(path: Path) -> Path:
    """Compress a closed JSONL file atomically, preserving the source on failure."""
    target = Path(str(path) + ".gz")
    temporary = Path(str(target) + ".tmp")
    with path.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as destination:
        while chunk := source.read(1024 * 1024):
            destination.write(chunk)
    os.replace(temporary, target)
    path.unlink()
    return target


@dataclass
class DayStats:
    rows: int = 0
    baseline_rows: int = 0
    burst_rows: int = 0
    first_at: str | None = None
    last_at: str | None = None
    sha256: Any = None

    def __post_init__(self) -> None:
        if self.sha256 is None:
            self.sha256 = hashlib.sha256()


class BasisSampleStore:
    """Append-only, per-asset daily storage with lossless gzip rotation."""

    def __init__(self, root: Path, *, config_hash: str, commit: str) -> None:
        self.root = root
        self.config_hash = config_hash
        self.commit = commit
        self.stats: dict[tuple[str, str], DayStats] = {}
        self.root.mkdir(parents=True, exist_ok=True)

    def append(self, row: dict[str, Any]) -> Path:
        asset = str(row.get("asset") or "UNKNOWN").upper()
        logged_at = str(row.get("logged_at") or datetime.now(timezone.utc).isoformat())
        day = _utc_day(logged_at)
        asset_dir = self.root / asset
        asset_dir.mkdir(parents=True, exist_ok=True)
        path = asset_dir / f"{day}.jsonl"
        payload = {
            "schema_version": SCHEMA_VERSION,
            "collector_config_hash": self.config_hash,
            "collector_commit": self.commit,
            **row,
        }
        line = json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
        key = (asset, day)
        stats = self.stats.setdefault(key, DayStats())
        stats.rows += 1
        sample_kind = str(payload.get("sample_kind") or "baseline")
        if sample_kind == "burst":
            stats.burst_rows += 1
        else:
            stats.baseline_rows += 1
        stats.first_at = stats.first_at or logged_at
        stats.last_at = logged_at
        stats.sha256.update(line.encode("utf-8"))
        return path

    def write_manifests(self) -> None:
        for (asset, day), stats in self.stats.items():
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "asset": asset,
                "utc_day": day,
                "rows_this_process": stats.rows,
                "baseline_rows_this_process": stats.baseline_rows,
                "burst_rows_this_process": stats.burst_rows,
                "first_at_this_process": stats.first_at,
                "last_at_this_process": stats.last_at,
                "process_rows_sha256": stats.sha256.hexdigest(),
                "collector_config_hash": self.config_hash,
                "collector_commit": self.commit,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_json(self.root / asset / f"{day}.manifest.json", manifest)

    def rotate_closed_days(self, *, current_day: str | None = None) -> list[Path]:
        today = current_day or _utc_day()
        compressed: list[Path] = []
        for path in sorted(self.root.glob("*/*.jsonl")):
            if path.stem >= today:
                continue
            compressed.append(compress_jsonl(path))
        return compressed


def basis_sample_paths(root: Path, asset_filter: str | None = None) -> list[Path]:
    if not root.exists():
        return []
    assets: Iterable[Path]
    if asset_filter:
        assets = [root / asset_filter.upper()]
    else:
        assets = [path for path in root.iterdir() if path.is_dir()]
    paths: list[Path] = []
    for asset_dir in assets:
        if not asset_dir.exists():
            continue
        paths.extend(asset_dir.glob("*.jsonl"))
        paths.extend(asset_dir.glob("*.jsonl.gz"))
    return sorted(paths, key=lambda path: (path.parent.name, path.name))


def read_basis_samples(
    root: Path,
    *,
    limit: int,
    asset_filter: str | None = None,
    sample_kind_filter: str | None = None,
    sample_quality_filter: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    paths_by_asset: dict[str, list[Path]] = {}
    for path in basis_sample_paths(root, asset_filter):
        paths_by_asset.setdefault(path.parent.name, []).append(path)
    for paths in paths_by_asset.values():
        asset_rows: deque[dict[str, Any]] = deque(maxlen=max(1, limit))
        for path in sorted(paths, key=lambda item: item.name):
            opener = gzip.open if path.suffix == ".gz" else open
            try:
                with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
                    for line in handle:
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(row, dict):
                            continue
                        if (
                            sample_kind_filter is not None
                            and str(row.get("sample_kind") or "")
                            != sample_kind_filter
                        ):
                            continue
                        if (
                            sample_quality_filter is not None
                            and str(row.get("sample_quality") or "")
                            != sample_quality_filter
                        ):
                            continue
                        asset_rows.append(row)
            except OSError:
                continue
        rows.extend(asset_rows)
    rows.sort(key=lambda row: str(row.get("logged_at") or ""))
    return rows[-max(1, limit) :]
