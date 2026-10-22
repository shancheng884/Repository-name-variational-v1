#!/usr/bin/env python3
"""Replay a user-parameterized WTI hedge rule against stored quote samples."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import read_basis_samples  # noqa: E402
from tools.lib.wti_replay import ReplayConfig, replay_wti  # noqa: E402


def _positive_decimal(value: str) -> Decimal:
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("must be a decimal number") from exc
    if not result.is_finite() or result <= 0:
        raise argparse.ArgumentTypeError("must be finite and greater than zero")
    return result


def _nonnegative_decimal(value: str) -> Decimal:
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("must be a decimal number") from exc
    if not result.is_finite() or result < 0:
        raise argparse.ArgumentTypeError("must be finite and non-negative")
    return result


def _positive_seconds(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number of seconds") from exc
    if not (result > 0 and result < float("inf")):
        raise argparse.ArgumentTypeError("must be finite and greater than zero")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Indicative WTI quote-path replay. This is not a fill simulation or "
            "authorization to trade."
        )
    )
    parser.add_argument(
        "--sample-root", type=Path,
        default=ROOT / "log" / "oil_swap" / "basis_samples",
    )
    parser.add_argument("--entry-threshold-bps", required=True, type=_positive_decimal)
    parser.add_argument("--exit-target-bps", required=True, type=_nonnegative_decimal)
    parser.add_argument(
        "--roundtrip-cost-bps", required=True, type=_nonnegative_decimal,
        help="Explicit assumed total cost for both venues, including any operator-selected buffer.",
    )
    parser.add_argument("--max-hold-seconds", required=True, type=_positive_seconds)
    parser.add_argument("--max-sample-gap-seconds", type=_positive_seconds, default=60.0)
    parser.add_argument("--lighter-market-id", type=int)
    parser.add_argument("--lighter-multiplier", type=_positive_decimal)
    parser.add_argument("--limit", type=int, default=200000)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit <= 0:
        parser.error("--limit must be positive")
    if args.lighter_market_id is not None and args.lighter_market_id < 0:
        parser.error("--lighter-market-id must be non-negative")
    try:
        rows: list[dict[str, Any]] = read_basis_samples(
            args.sample_root,
            limit=args.limit,
            asset_filter="WTI",
        )
        report = replay_wti(
            rows,
            config=ReplayConfig(
                entry_threshold_bps=args.entry_threshold_bps,
                exit_target_bps=args.exit_target_bps,
                roundtrip_cost_bps=args.roundtrip_cost_bps,
                max_hold_seconds=args.max_hold_seconds,
                max_sample_gap_seconds=args.max_sample_gap_seconds,
            ),
            expected_market_id=args.lighter_market_id,
            expected_multiplier=args.lighter_multiplier,
        )
    except ValueError as exc:
        print(json.dumps({"replay_error": str(exc), "live_authorization": False}, indent=2))
        return 2
    payload = asdict(report)
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if report.sample_count >= 2 else 2


if __name__ == "__main__":
    raise SystemExit(main())
