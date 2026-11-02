#!/usr/bin/env python3
"""Follow the order-metrics stream and log exit qualification-to-fill latency.

Read-only: it tails a JSONL file, writes its own log, state and health files,
and never touches the trading state or the order path.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.exit_latency_watch import (  # noqa: E402
    WatchState,
    format_event,
    load_state,
    save_state,
)

_STOP = False


def _handle_stop(_signum: int, _frame: object) -> None:
    global _STOP
    _STOP = True


def _write_json(path: Path, payload: dict) -> None:
    temporary = Path(path).with_suffix(Path(path).suffix + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _append(path: Path, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", type=Path, default=ROOT / "log" / "order_metrics.jsonl")
    parser.add_argument("--log", type=Path, default=ROOT / "log" / "exit_latency_watch.log")
    parser.add_argument("--state", type=Path, default=ROOT / "log" / "exit_latency_watch_state.json")
    parser.add_argument("--health", type=Path, default=ROOT / "log" / "exit_latency_watch_health.json")
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument(
        "--replay-from-start",
        action="store_true",
        help="Read the whole file instead of starting at its current end.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    metrics = Path(args.metrics)
    state = load_state(args.state)
    if args.replay_from_start:
        offset = 0
    else:
        size = metrics.stat().st_size if metrics.exists() else 0
        # Resume where the previous run stopped; a truncated file means the
        # saved offset belongs to a rotation that already happened.
        offset = state.offset if 0 < state.offset <= size else size
    started_at = datetime.now(timezone.utc).isoformat()
    _append(args.log, f"{started_at} WATCH_START offset={offset} pid={os.getpid()}")

    rows = 0
    last_health = 0.0
    last_event_at = None
    errors = 0
    dirty = False
    while not _STOP:
        try:
            if not metrics.exists():
                time.sleep(args.interval_seconds)
                continue
            size = metrics.stat().st_size
            if size < offset:  # rotated or truncated in place
                offset = 0
            if size > offset:
                with metrics.open("r", encoding="utf-8", errors="ignore") as handle:
                    handle.seek(offset)
                    chunk = handle.read()
                    offset = handle.tell()
                for line in chunk.splitlines():
                    text = line.strip()
                    if not text:
                        continue
                    rows += 1
                    try:
                        row = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(row, dict):
                        continue
                    event = state.observe(row)
                    if event is None:
                        continue
                    _append(args.log, f"{datetime.now(timezone.utc).isoformat()} {format_event(event)}")
                    last_event_at = datetime.now(timezone.utc).isoformat()
                    dirty = True
        except OSError as exc:
            errors += 1
            _append(args.log, f"{datetime.now(timezone.utc).isoformat()} ERROR {type(exc).__name__}:{exc}")
        if dirty or state.offset != offset:
            state.offset = offset
            try:
                save_state(args.state, state)
            except OSError:
                errors += 1
            dirty = False
        now = time.monotonic()
        if now - last_health >= 5:
            try:
                _write_json(
                    args.health,
                    {
                        "status": "stopped" if _STOP else "running",
                        "pid": os.getpid(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "started_at": started_at,
                        "offset": offset,
                        "rows_seen": rows,
                        "qualified_total": state.qualified_total,
                        "closed_total": state.closed_total,
                        "open_qualified": len(state.qualified),
                        "last_event_at": last_event_at,
                        "errors": errors,
                    },
                )
            except OSError:
                errors += 1
            last_health = now
        time.sleep(args.interval_seconds)
    try:
        state.offset = offset
        save_state(args.state, state)
        _write_json(
            args.health,
            {
                "status": "stopped",
                "pid": os.getpid(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "started_at": started_at,
                "offset": offset,
                "rows_seen": rows,
                "qualified_total": state.qualified_total,
                "closed_total": state.closed_total,
                "open_qualified": len(state.qualified),
                "last_event_at": last_event_at,
                "errors": errors,
            },
        )
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())