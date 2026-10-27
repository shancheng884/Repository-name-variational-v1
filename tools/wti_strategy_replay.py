#!/usr/bin/env python3
"""Replay a user-parameterized WTI hedge rule against stored quote samples."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import sys
import tempfile
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import read_basis_samples  # noqa: E402
from tools.lib.oil_swap_readiness import assess_oil_readiness  # noqa: E402
from tools.lib.wti_replay import ReplayConfig, replay_wti, replay_wti_gradient  # noqa: E402


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


def _input_manifest(sample_root: Path) -> tuple[list[dict[str, Any]], str]:
    paths = [sample_root] if sample_root.is_file() else sorted(
        path for path in sample_root.rglob("*")
        if path.is_file() and (path.name.endswith(".jsonl") or path.name.endswith(".jsonl.gz"))
    ) if sample_root.exists() else []
    manifest: list[dict[str, Any]] = []
    aggregate = hashlib.sha256()
    for path in paths:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        file_hash = digest.hexdigest()
        try:
            relative = path.relative_to(sample_root).as_posix() if sample_root.is_dir() else path.name
        except ValueError:
            relative = path.name
        item = {"path": relative, "bytes": size, "sha256": file_hash}
        manifest.append(item)
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(str(size).encode("ascii"))
        aggregate.update(b"\0")
        aggregate.update(file_hash.encode("ascii"))
        aggregate.update(b"\n")
    return manifest, aggregate.hexdigest()


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=True, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


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
    parser.add_argument(
        "--max-sample-gap-seconds", type=_positive_seconds, default=120.0,
        help="Runtime-matched live entry-authorization gap; the value can be lowered for a stricter sensitivity replay.",
    )
    parser.add_argument("--max-slippage-bps", type=_positive_decimal, default=Decimal("8"))
    parser.add_argument("--max-order-notional-usd", type=_positive_decimal, default=Decimal("20"))
    parser.add_argument("--lighter-market-id", type=int)
    parser.add_argument("--lighter-multiplier", type=_positive_decimal)
    parser.add_argument("--compare-gradient", action="store_true")
    parser.add_argument(
        "--continuous-gradient", action="store_true",
        help="Replay sequential bounded gradient rounds; this is still indicative and never authorizes live trading.",
    )
    parser.add_argument(
        "--assume-verified-flat-session-restarts", action="store_true",
        help="Model supervisor restarts between sessions only under an explicit independent dual-account flat-reconciliation assumption.",
    )
    parser.add_argument("--gradient-second-tier-bps", type=_positive_decimal)
    parser.add_argument("--gradient-layer-notional-usd", type=_positive_decimal)
    parser.add_argument("--gradient-aggregate-max-notional-usd", type=_positive_decimal)
    parser.add_argument("--gradient-tier-count", type=int, default=5)
    parser.add_argument(
        "--gradient-equity-usd", type=_positive_decimal,
        help="Explicit symmetric equity scenario for the paired 1x-to-5x tier capacity; not inferred from historical data.",
    )
    parser.add_argument(
        "--gradient-elastic-capacity", action="store_true",
        help="Replay the optional single first-tier reserve child; disabled by default.",
    )
    parser.add_argument("--limit", type=int, default=200000)
    parser.add_argument("--evidence-output", type=Path, help="Write a hash-addressed replay/readiness evidence JSON file.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit <= 0:
        parser.error("--limit must be positive")
    if args.lighter_market_id is not None and args.lighter_market_id < 0:
        parser.error("--lighter-market-id must be non-negative")
    if not 2 <= args.gradient_tier_count <= 5:
        parser.error("--gradient-tier-count must be between 2 and 5")
    if args.compare_gradient and any(value is None for value in (
        args.gradient_second_tier_bps,
        args.gradient_layer_notional_usd,
        args.gradient_aggregate_max_notional_usd,
    )):
        parser.error("--compare-gradient requires all gradient tier and notional options")
    if args.continuous_gradient and not args.compare_gradient:
        parser.error("--continuous-gradient requires --compare-gradient")
    if (
        not args.compare_gradient and any(value is not None for value in (
            args.gradient_second_tier_bps,
            args.gradient_layer_notional_usd,
            args.gradient_aggregate_max_notional_usd,
            args.gradient_equity_usd,
        ))
    ) or (args.gradient_elastic_capacity and not args.compare_gradient):
        parser.error("gradient options require --compare-gradient")
    try:
        rows: list[dict[str, Any]] = read_basis_samples(
            args.sample_root,
            limit=args.limit,
            asset_filter="WTI",
        )
        config = ReplayConfig(
            entry_threshold_bps=args.entry_threshold_bps,
            exit_target_bps=args.exit_target_bps,
            roundtrip_cost_bps=args.roundtrip_cost_bps,
            max_hold_seconds=args.max_hold_seconds,
            max_sample_gap_seconds=args.max_sample_gap_seconds,
            max_slippage_bps=args.max_slippage_bps,
            max_order_notional_usd=args.max_order_notional_usd,
        )
        baseline = replay_wti(
            rows,
            config=config,
            expected_market_id=args.lighter_market_id,
            expected_multiplier=args.lighter_multiplier,
        )
        gradient = None
        if args.compare_gradient:
            gradient = replay_wti_gradient(
                rows,
                config=config,
                second_tier_threshold_bps=args.gradient_second_tier_bps,
                layer_notional_usd=args.gradient_layer_notional_usd,
                aggregate_max_notional_usd=args.gradient_aggregate_max_notional_usd,
                max_layers=args.gradient_tier_count,
                equity_usd=args.gradient_equity_usd,
                expected_market_id=args.lighter_market_id,
                expected_multiplier=args.lighter_multiplier,
                max_completed_roundtrips=None if args.continuous_gradient else 1,
                assume_verified_flat_session_restarts=args.assume_verified_flat_session_restarts,
                elastic_capacity_enabled=args.gradient_elastic_capacity,
            )
    except ValueError as exc:
        print(json.dumps({"replay_error": str(exc), "live_authorization": False}, indent=2))
        return 2
    if gradient is None:
        payload = asdict(baseline)
        report = baseline
    else:
        payload = {
            "live_authorization": False,
            "baseline": asdict(baseline),
            "gradient": asdict(gradient),
            "comparison": {
                "completed_trade_delta": gradient.completed_trades - baseline.completed_trades,
                "net_pnl_delta_usd": str(Decimal(gradient.net_pnl_usd) - Decimal(baseline.net_pnl_usd)),
                "all_gates_remain_hard": True,
                "profitability_claim": False,
            },
        }
        report = baseline
    if args.evidence_output is not None:
        manifest, input_sha256 = _input_manifest(args.sample_root)
        readiness = assess_oil_readiness(
            rows,
            now_utc=datetime.now(timezone.utc),
            minimum_sessions=3,
            minimum_valid_samples_per_session=1200,
            minimum_coverage_seconds=6 * 3600,
            maximum_gap_seconds=120,
            maximum_sample_age_seconds=120,
            minimum_confirmed_roundtrips=5,
            expected_lighter_market_id=args.lighter_market_id,
            expected_lighter_exposure_multiplier=args.lighter_multiplier,
        )
        evidence: dict[str, Any] = {
            "schema_version": 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "sample_root": str(args.sample_root.resolve()),
            "input_files": manifest,
            "input_sha256": input_sha256,
            "sample_count_loaded": len(rows),
            "market_identity": {
                "lighter_market_id": args.lighter_market_id,
                "lighter_multiplier": None if args.lighter_multiplier is None else str(args.lighter_multiplier),
            },
            "replay_config": {
                "entry_threshold_bps": str(config.entry_threshold_bps),
                "exit_target_bps": str(config.exit_target_bps),
                "roundtrip_cost_bps": str(config.roundtrip_cost_bps),
                "max_hold_seconds": config.max_hold_seconds,
                "max_sample_gap_seconds": config.max_sample_gap_seconds,
                "max_slippage_bps": str(config.max_slippage_bps),
                "entry_cutoff_seconds": config.entry_cutoff_seconds,
                "assume_verified_flat_session_restarts": args.assume_verified_flat_session_restarts,
            },
            "readiness": asdict(readiness),
            "replay": payload,
            "live_authorization": False,
            "profitability_claim": False,
        }
        canonical = json.dumps(evidence, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        evidence["evidence_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        _write_json_atomic(args.evidence_output, evidence)
        payload["evidence"] = {
            "path": str(args.evidence_output),
            "sha256": evidence["evidence_sha256"],
            "input_sha256": input_sha256,
            "production_ready": readiness.ready_for_production_rotation,
        }
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if report.sample_count >= 2 else 2


if __name__ == "__main__":
    raise SystemExit(main())
