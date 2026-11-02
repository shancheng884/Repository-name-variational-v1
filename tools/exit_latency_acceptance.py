#!/usr/bin/env python3
"""Exit latency acceptance measured from the order-metrics stream."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.exit_latency_acceptance import (  # noqa: E402
    DEFAULT_MAX_LATENCY_SECONDS,
    QUALIFIED_EVENTS,
    measure,
    summary_lines,
)
from tools.lib.metrics_store import iter_rows  # noqa: E402

EXIT_EVENTS = set(QUALIFIED_EVENTS) | {
    "live_inventory_exited",
    "live_inventory_final_pnl",
    "live_inventory_actual_pnl",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure how long a qualified lot waited before closing."
    )
    parser.add_argument("--log-dir", type=Path, default=ROOT / "log")
    parser.add_argument("--asset", default="ETH")
    parser.add_argument(
        "--max-latency-seconds", type=float, default=DEFAULT_MAX_LATENCY_SECONDS
    )
    parser.add_argument("--no-archives", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rows = iter_rows(
        args.log_dir,
        include_archives=not args.no_archives,
        events=EXIT_EVENTS,
        asset=args.asset,
    )
    summary = measure(
        rows, asset=args.asset, max_latency_seconds=args.max_latency_seconds
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    else:
        print("== 平仓延迟验收 ==")
        for line in summary_lines(summary):
            print(line)
    return 0 if summary["slow"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())