#!/usr/bin/env python3
"""Compress closed capture files and optionally archive them to R2.

This process is deliberately separate from the live collector.  It never
touches the current hour, and only removes local files after a verified upload.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = ROOT / "log" / "market_capture"
DEFAULT_STATE = ROOT / "log" / "capture_archive_state.json"
DEFAULT_LOCK = ROOT / "log" / "capture_archive.lock"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _hour_key(at: datetime) -> str:
    return at.astimezone(timezone.utc).strftime("%Y%m%d-%H")


def iter_closed_jsonl(root: Path, *, min_age_minutes: float, now: datetime | None = None):
    now = now or _now()
    current_hour = _hour_key(now)
    cutoff = now.timestamp() - max(0.0, min_age_minutes) * 60
    for path in sorted(root.rglob("*.jsonl")):
        if path.stem >= current_hour:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat.st_mtime <= cutoff:
            yield path, stat


def gzip_file(path: Path) -> Path:
    target = path.with_suffix(path.suffix + ".gz")
    if target.exists() and target.stat().st_size > 0:
        path.unlink(missing_ok=True)
        return target
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    with path.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as sink:
        shutil.copyfileobj(source, sink, length=1024 * 256)
    os.replace(temporary, target)
    try:
        stat = path.stat()
        os.utime(target, (stat.st_atime, stat.st_mtime))
    except OSError:
        pass
    path.unlink(missing_ok=True)
    return target


def remote_key(path: Path, root: Path, prefix: str) -> str:
    relative = path.relative_to(root).as_posix()
    prefix = prefix.strip("/")
    return f"{prefix}/{relative}" if prefix else relative


def load_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def rclone_upload(rclone: str, source: Path, destination: str, *, dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        return {"ok": True, "dry_run": True, "destination": destination}
    command = [rclone, "copyto", "--checksum", str(source), destination]
    result = subprocess.run(command, capture_output=True, text=True, timeout=1800)
    if result.returncode != 0:
        return {
            "ok": False, "returncode": result.returncode,
            "stderr": (result.stderr or "")[-500:],
        }
    size = subprocess.run(
        [rclone, "size", destination, "--json"],
        capture_output=True, text=True, timeout=120,
    )
    if size.returncode != 0:
        return {"ok": False, "returncode": size.returncode, "stderr": (size.stderr or "")[-500:]}
    try:
        remote = json.loads(size.stdout or "{}")
        remote_bytes = int(remote.get("bytes", -1))
    except (TypeError, ValueError):
        return {"ok": False, "error": "rclone_size_unparseable"}
    if remote_bytes != source.stat().st_size:
        return {"ok": False, "error": "remote_size_mismatch", "remote_bytes": remote_bytes,
                "local_bytes": source.stat().st_size}
    return {"ok": True, "bytes": remote_bytes, "destination": destination}


def prune_verified(
    root: Path,
    state: dict[str, Any],
    *,
    hot_days: float,
    now: datetime | None = None,
    dry_run: bool = False,
) -> list[str]:
    now = now or _now()
    cutoff = now.timestamp() - max(0.0, hot_days) * 86400
    removed = []
    files = state.setdefault("files", {})
    for path in sorted(root.rglob("*.jsonl.gz")):
        rel = path.relative_to(root).as_posix()
        entry = files.get(rel)
        if not entry or entry.get("status") != "verified":
            continue
        try:
            if path.stat().st_mtime > cutoff:
                continue
        except OSError:
            continue
        removed.append(rel)
        if not dry_run:
            path.unlink(missing_ok=True)
            files.pop(rel, None)
    return removed


def _acquire_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        import fcntl
    except ImportError:
        return handle
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError("capture_archive_already_running")
    return handle


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compress and optionally upload capture files to R2.")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--min-age-minutes", type=float, default=60.0)
    parser.add_argument("--hot-days", type=float, default=float(os.getenv("ARCHIVE_HOT_DAYS", "3")))
    parser.add_argument("--local-only", action="store_true")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--prune", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = Path(args.root)
    state_path = Path(args.state)
    lock = _acquire_lock(DEFAULT_LOCK)
    state = load_state(state_path)
    state.setdefault("files", {})
    summary: dict[str, Any] = {
        "ok": True, "root": str(root), "compressed": 0, "uploaded": 0,
        "upload_failed": 0, "pruned": 0, "dry_run": bool(args.dry_run),
    }
    try:
        for path, _stat in list(iter_closed_jsonl(root, min_age_minutes=args.min_age_minutes)):
            target = path.with_suffix(path.suffix + ".gz") if args.dry_run else gzip_file(path)
            summary["compressed"] += 1
            summary.setdefault("compressed_files", []).append(target.relative_to(root).as_posix())
        remote = os.getenv("R2_REMOTE", "r2").strip()
        bucket = os.getenv("R2_BUCKET", "").strip()
        prefix = os.getenv("R2_PREFIX", "market_capture").strip()
        do_upload = args.upload or (not args.local_only and bool(bucket))
        if do_upload and bucket:
            rclone = os.getenv("RCLONE_BIN", "rclone")
            for path in sorted(root.rglob("*.jsonl.gz")):
                rel = path.relative_to(root).as_posix()
                entry = state["files"].get(rel) or {}
                if entry.get("status") == "verified" and entry.get("bytes") == path.stat().st_size:
                    continue
                key = remote_key(path, root, prefix)
                destination = f"{remote}:{bucket}/{key}"
                result = rclone_upload(rclone, path, destination, dry_run=args.dry_run)
                if result.get("ok"):
                    state["files"][rel] = {
                        "status": "verified", "bytes": path.stat().st_size,
                        "mtime": path.stat().st_mtime, "remote": destination,
                        "uploaded_at": _now().isoformat(),
                    }
                    summary["uploaded"] += 1
                else:
                    summary["upload_failed"] += 1
                    summary.setdefault("failures", []).append({"file": rel, **result})
        state["updated_at"] = _now().isoformat()
        if not args.dry_run:
            save_state(state_path, state)
        if args.prune:
            removed = prune_verified(root, state, hot_days=args.hot_days, dry_run=args.dry_run)
            summary["pruned"] = len(removed)
            summary["pruned_files"] = removed
            if not args.dry_run:
                save_state(state_path, state)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if summary["upload_failed"] == 0 else 1
    finally:
        lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
