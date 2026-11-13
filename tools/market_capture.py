#!/usr/bin/env python3
"""Read-only multi-venue RWA market capture.

No order is ever submitted.  The command can run beside the live strategies;
it uses its own WebSocket sessions and writes under log/market_capture/.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.market_capture.config import ConfigError, load_config
from tools.market_capture.runner import run_capture
from tools.market_capture.safety import assert_no_order_submission_source


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only RWA market capture (no order submission).")
    parser.add_argument("--enable-capture", action="store_true", help="required unless MARKET_CAPTURE_ENABLED=true")
    parser.add_argument("--output-dir", default=str(ROOT / "log" / "market_capture"))
    parser.add_argument("--assets", default="OPENAI,ANTHROPIC,NVDA,SNDK,CRCL,AAPL,QQQ,DRAM,COIN")
    parser.add_argument("--notional-usd", type=float, default=20.0)
    parser.add_argument("--book-snapshot-seconds", type=float, default=5.0)
    parser.add_argument("--health-seconds", type=float, default=5.0)
    parser.add_argument("--latency-seconds", type=float, default=5.0)
    parser.add_argument("--system-seconds", type=float, default=60.0)
    parser.add_argument("--fill-poll-seconds", type=float, default=2.0)
    parser.add_argument("--edge-seconds", type=float, default=1.0)
    parser.add_argument("--var-quote-seconds", type=float, default=0.0,
                        help="reserved; Var RWA executable RFQ is not polled by this collector")
    parser.add_argument("--retention-days", type=int, default=14)
    parser.add_argument("--disk-floor-gb", type=float, default=2.0)
    parser.add_argument("--max-book-levels", type=int, default=10)
    parser.add_argument("--duration-seconds", type=float, default=0.0)
    parser.add_argument("--no-variational", action="store_true")
    parser.add_argument("--no-lighter-mainnet", action="store_true")
    parser.add_argument("--no-lighter-rh", action="store_true")
    parser.add_argument("--no-entropy", action="store_true")
    parser.add_argument("--print-config", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args(argv)
    try:
        config = load_config(args)
    except ConfigError as exc:
        print(json.dumps({"ok": False, "error": str(exc), "orders_attempted": False}), file=sys.stderr)
        return 2
    forbidden = assert_no_order_submission_source()
    if forbidden:
        print(json.dumps({"ok": False, "error": "forbidden_order_calls", "matches": forbidden}), file=sys.stderr)
        return 3
    if args.print_config:
        print(json.dumps(config.public_dict(), ensure_ascii=False, indent=2))
        return 0
    return asyncio.run(run_capture(config))


if __name__ == "__main__":
    raise SystemExit(main())
