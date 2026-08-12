#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import defaultdict, deque
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
ORDER_METRICS = ROOT / "log" / "order_metrics.jsonl"
HIGH_VOLUME_RETENTION = {
    "live_inventory_negative_direction_shadow_candidate": 1_000,
    "live_inventory_basis_state": 10_000,
    "live_inventory_size_ladder_shadow": 1_000,
    "live_inventory_exit_blocked": 5_000,
    "live_inventory_v4_entry_blocked": 5_000,
    "live_inventory_entry_blocked": 5_000,
    "live_inventory_cycle_cap_reached": 1_000,
    "live_inventory_v4_batch_waiting": 1_000,
}


def running_strategies() -> list[str]:
    result = subprocess.run(
        ["pgrep", "-af", "python.*(main.py|tools/basis_collector.py)"],
        capture_output=True,
        text=True,
        check=False,
    )
    return [
        line
        for line in result.stdout.splitlines()
        if line.strip() and "compact_order_metrics.py" not in line
    ]


def event_name(line: bytes) -> str:
    try:
        row = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return "__unparseable__"
    return str(row.get("event") or "__unknown__")


def retained_offsets(path: Path) -> tuple[set[int], dict[str, int], int]:
    recent: dict[str, deque[int]] = {
        event: deque(maxlen=limit)
        for event, limit in HIGH_VOLUME_RETENTION.items()
    }
    counts: dict[str, int] = defaultdict(int)
    total = 0
    with path.open("rb") as handle:
        while True:
            offset = handle.tell()
            line = handle.readline()
            if not line:
                break
            total += 1
            event = event_name(line)
            counts[event] += 1
            if event in recent:
                recent[event].append(offset)
    offsets = {offset for values in recent.values() for offset in values}
    return offsets, dict(counts), total


def compact(path: Path) -> tuple[Path, int, int]:
    offsets, counts, source_lines = retained_offsets(path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive = path.with_name(f"{path.name}.full-{stamp}.gz")
    archive_tmp = archive.with_suffix(archive.suffix + ".tmp")
    compact_tmp = path.with_suffix(path.suffix + ".compact.tmp")
    if archive.exists() or archive_tmp.exists() or compact_tmp.exists():
        raise FileExistsError("archive or temporary compact file already exists")

    retained_lines = 0
    source_mode = path.stat().st_mode
    try:
        with path.open("rb") as source, compact_tmp.open("wb") as destination:
            while True:
                offset = source.tell()
                line = source.readline()
                if not line:
                    break
                event = event_name(line)
                if event not in HIGH_VOLUME_RETENTION or offset in offsets:
                    destination.write(line)
                    retained_lines += 1
            destination.flush()
            os.fsync(destination.fileno())

        with path.open("rb") as source, gzip.open(
            archive_tmp,
            "wb",
            compresslevel=6,
        ) as destination:
            while chunk := source.read(1024 * 1024):
                destination.write(chunk)

        os.chmod(compact_tmp, source_mode)
        os.replace(archive_tmp, archive)
        os.replace(compact_tmp, path)
    except Exception:
        archive_tmp.unlink(missing_ok=True)
        compact_tmp.unlink(missing_ok=True)
        raise

    print(f"source_lines={source_lines}")
    print(f"retained_lines={retained_lines}")
    for event in HIGH_VOLUME_RETENTION:
        print(
            f"event={event} source={counts.get(event, 0)} "
            f"retained={min(counts.get(event, 0), HIGH_VOLUME_RETENTION[event])}"
        )
    return archive, source_lines, retained_lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Losslessly archive order_metrics.jsonl and retain all critical "
            "events plus bounded recent high-volume diagnostics."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform compaction. Without this flag, print a safe plan.",
    )
    args = parser.parse_args()

    processes = running_strategies()
    if processes:
        print("REFUSE_COMPACT reason=collector_or_main_process_running")
        for process in processes:
            print(process)
        return 2
    if not ORDER_METRICS.exists():
        print("REFUSE_COMPACT reason=order_metrics_missing")
        return 2

    print(f"source={ORDER_METRICS}")
    print(f"source_bytes={ORDER_METRICS.stat().st_size}")
    print("preserve=all_non_high_volume_events")
    for event, limit in HIGH_VOLUME_RETENTION.items():
        print(f"retain_recent event={event} limit={limit}")
    if not args.execute:
        print("DRY_RUN add --execute only while strategy is stopped and venues are flat")
        return 0

    archive, _, _ = compact(ORDER_METRICS)
    print(f"archive={archive}")
    print(f"archive_bytes={archive.stat().st_size}")
    print(f"compacted_bytes={ORDER_METRICS.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
