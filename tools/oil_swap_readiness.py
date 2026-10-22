from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import read_basis_samples  # noqa: E402
from tools.lib.oil_swap_readiness import assess_oil_readiness  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Assess WTI data for operator review and production-rotation readiness."
    )
    parser.add_argument("--sample-root", type=Path, default=ROOT / "log" / "oil_swap" / "basis_samples")
    parser.add_argument("--minimum-sessions", type=int, default=3)
    parser.add_argument("--minimum-valid-samples-per-session", type=int, default=1200)
    parser.add_argument("--minimum-coverage-hours", type=float, default=6)
    parser.add_argument("--maximum-gap-seconds", type=float, default=120)
    parser.add_argument("--maximum-sample-age-seconds", type=float, default=120)
    parser.add_argument("--minimum-confirmed-roundtrips", type=int, default=5)
    parser.add_argument(
        "--lighter-market-id",
        type=int,
        help="Expected WTI market ID from current Lighter metadata; inferred only when history has one ID.",
    )
    parser.add_argument(
        "--lighter-exposure-multiplier",
        type=Decimal,
        help="Current WTI exposure multiplier from Lighter metadata; required for production promotion.",
    )
    args = parser.parse_args(argv)
    rows = read_basis_samples(args.sample_root, limit=200000, asset_filter="WTI")
    report = assess_oil_readiness(
        rows,
        minimum_sessions=args.minimum_sessions,
        minimum_valid_samples_per_session=args.minimum_valid_samples_per_session,
        minimum_coverage_seconds=args.minimum_coverage_hours * 3600,
        maximum_gap_seconds=args.maximum_gap_seconds,
        maximum_sample_age_seconds=args.maximum_sample_age_seconds,
        minimum_confirmed_roundtrips=args.minimum_confirmed_roundtrips,
        expected_lighter_market_id=args.lighter_market_id,
        expected_lighter_exposure_multiplier=args.lighter_exposure_multiplier,
    )
    print(json.dumps(asdict(report), ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if report.ready_for_bounded_live_review else 2


if __name__ == "__main__":
    raise SystemExit(main())
