#!/usr/bin/env python3
"""Per-signal, de-duplicated decision audit over the whole metrics stream."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.metrics_store import iter_rows  # noqa: E402
from tools.lib.signal_audit import (  # noqa: E402
    SIGNAL_EVENTS,
    audit_rows,
    summary_lines,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fold decision rows into one record per market signal."
    )
    parser.add_argument("--log-dir", type=Path, default=ROOT / "log")
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--no-archives", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rows = iter_rows(
        args.log_dir,
        include_archives=not args.no_archives,
        events=SIGNAL_EVENTS,
        asset=args.asset,
    )
    summary = audit_rows(rows, asset=args.asset, limit=args.limit)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        return 0
    print("== 信号去重审计 ==")
    for line in summary_lines(summary):
        print(line)
    for item in summary.get("signal_detail") or []:
        print(
            f"  信号 {item['signal']} 行={item['rows']} 重复={item['duplicate_rows']} "
            f"最好价差={item['best_pnl_bps']} 门槛={item['threshold_bps']} "
            f"越线={item['crossed']} 平仓={item['exited']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())