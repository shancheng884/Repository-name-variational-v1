#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import os
from datetime import datetime, timezone
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "log"
TARGETS = (
    "rest_events.jsonl",
    "ws_events.jsonl",
    "market_samples.jsonl",
    "runtime.log",
    "basis_collector.log",
)


def running_collectors() -> list[str]:
    try:
        result = subprocess.run(
            ["pgrep", "-af", "python.*(main.py|tools/basis_collector.py)"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return []
    return [line for line in result.stdout.splitlines() if line.strip() and "archive_legacy_logs.py" not in line]


def archive_file(path: Path, stamp: str) -> Path:
    target = path.with_name(f"{path.name}.legacy-{stamp}.gz")
    temporary = target.with_suffix(target.suffix + ".tmp")
    if target.exists() or temporary.exists():
        raise FileExistsError(f"archive target already exists: {target}")
    with path.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as destination:
        while chunk := source.read(1024 * 1024):
            destination.write(chunk)
    os.replace(temporary, target)
    path.unlink()
    path.touch()
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description="Losslessly gzip closed legacy high-volume logs.")
    parser.add_argument("--execute", action="store_true", help="Perform the archive. Without this flag, only print the plan.")
    args = parser.parse_args()
    processes = running_collectors()
    if processes:
        print("REFUSE_ARCHIVE reason=collector_or_main_process_running")
        for process in processes:
            print(process)
        return 2
    candidates = [
        LOG_DIR / name
        for name in TARGETS
        if (LOG_DIR / name).exists() and (LOG_DIR / name).stat().st_size > 0
    ]
    candidates.extend(
        path
        for path in sorted(LOG_DIR.glob("basis_collector.log.*"))
        if path.is_file()
        and path.suffix != ".gz"
        and ".legacy-" not in path.name
        and path.stat().st_size > 0
    )
    for path in candidates:
        print(f"archive_candidate path={path} bytes={path.stat().st_size}")
    print("order_metrics_action=use_tools/compact_order_metrics.py")
    if not args.execute:
        print("DRY_RUN add --execute only after process/state/venue checks and stopping the collector")
        return 0
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for path in candidates:
        target = archive_file(path, stamp)
        print(f"archived source={path} target={target} compressed_bytes={target.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
