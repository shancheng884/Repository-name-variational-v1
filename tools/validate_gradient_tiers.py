from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import read_basis_samples  # noqa: E402


DEFAULT_PERCENTILES = (
    Decimal("97.5"),
    Decimal("98.25"),
    Decimal("99"),
    Decimal("99.5"),
    Decimal("99.8"),
)

DIRECTION_LONG_VAR_SHORT_LIGHTER = "long_var_short_lighter"
DIRECTION_SHORT_VAR_LONG_LIGHTER = "short_var_long_lighter"
DIRECTION_EDGE_KEYS = {
    DIRECTION_LONG_VAR_SHORT_LIGHTER: "long_edge_bps",
    DIRECTION_SHORT_VAR_LONG_LIGHTER: "short_edge_bps",
}


def percentile(values: list[Decimal], value: Decimal) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires samples")
    position = (Decimal(len(ordered) - 1) * value) / Decimal("100")
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def parse_sample(
    row: dict[str, Any],
    *,
    direction: str = DIRECTION_SHORT_VAR_LONG_LIGHTER,
) -> tuple[datetime, Decimal] | None:
    edge_key = DIRECTION_EDGE_KEYS[direction]
    try:
        observed_at = datetime.fromisoformat(
            str(row["logged_at"]).replace("Z", "+00:00")
        )
        edge = Decimal(str(row[edge_key]))
    except (KeyError, ValueError, TypeError):
        return None
    return observed_at, edge


def confirmed_crossing_indexes(
    samples: list[tuple[datetime, Decimal]],
    *,
    threshold: Decimal,
) -> list[int]:
    """Return independent latest-and-2-of-3 threshold confirmations."""
    crossings: list[int] = []
    armed = True
    for index, (_, edge) in enumerate(samples):
        if edge < threshold:
            armed = True
        if index < 2 or edge < threshold:
            continue
        window = samples[index - 2 : index + 1]
        confirmed = sum(value >= threshold for _, value in window) >= 2
        if armed and confirmed:
            crossings.append(index)
            armed = False
    return crossings


def evaluate_tier_holdout(
    samples: list[tuple[datetime, Decimal]],
    *,
    direction: str = DIRECTION_SHORT_VAR_LONG_LIGHTER,
    train_ratio: Decimal = Decimal("0.70"),
    horizon_seconds: int = 6 * 3600,
    targets_bps: tuple[Decimal, ...] = (
        Decimal("1"),
        Decimal("2"),
        Decimal("4"),
    ),
) -> dict[str, Any]:
    if len(samples) < 100:
        raise ValueError("at least 100 chronological samples are required")
    split = max(1, min(len(samples) - 1, int(Decimal(len(samples)) * train_ratio)))
    train = samples[:split]
    test = samples[split:]
    train_edges = [edge for _, edge in train]
    tiers: list[dict[str, Any]] = []
    for tier_index, percentile_value in enumerate(DEFAULT_PERCENTILES, start=1):
        threshold = percentile(train_edges, percentile_value)
        crossings = confirmed_crossing_indexes(test, threshold=threshold)
        hits = {target: 0 for target in targets_bps}
        favorable_moves: list[Decimal] = []
        for index in crossings:
            entered_at, entry_edge = test[index]
            minimum_edge = entry_edge
            for future_at, future_edge in test[index + 1 :]:
                if (future_at - entered_at).total_seconds() > horizon_seconds:
                    break
                minimum_edge = min(minimum_edge, future_edge)
            favorable = entry_edge - minimum_edge
            favorable_moves.append(favorable)
            for target in targets_bps:
                if favorable >= target:
                    hits[target] += 1
        tiers.append(
            {
                "tier": tier_index,
                "percentile": str(percentile_value),
                "threshold_bps": str(threshold),
                "holdout_crossings": len(crossings),
                "median_favorable_reversion_bps": str(
                    percentile(favorable_moves, Decimal("50"))
                )
                if favorable_moves
                else None,
                "target_hit_rates": {
                    str(target): (
                        hits[target] / len(crossings) if crossings else None
                    )
                    for target in targets_bps
                },
            }
        )
    return {
        "direction": direction,
        "samples": len(samples),
        "train_samples": len(train),
        "holdout_samples": len(test),
        "train_start": train[0][0].isoformat(),
        "train_end": train[-1][0].isoformat(),
        "holdout_start": test[0][0].isoformat(),
        "holdout_end": test[-1][0].isoformat(),
        "horizon_seconds": horizon_seconds,
        "tiers": tiers,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Walk-forward validation for five V4 gradient tiers."
    )
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--limit", type=int, default=500000)
    parser.add_argument("--root", type=Path, default=Path("log/basis_samples"))
    parser.add_argument(
        "--direction",
        choices=("both", *DIRECTION_EDGE_KEYS),
        default="both",
        help="Direction to validate. Default: both.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    rows = read_basis_samples(
        args.root,
        limit=args.limit,
        asset_filter=args.asset,
        sample_kind_filter="baseline",
        sample_quality_filter="valid",
    )
    directions = (
        tuple(DIRECTION_EDGE_KEYS)
        if args.direction == "both"
        else (args.direction,)
    )
    results: dict[str, dict[str, Any]] = {}
    for direction in directions:
        parsed = [
            sample
            for row in rows
            if (sample := parse_sample(row, direction=direction)) is not None
        ]
        results[direction] = evaluate_tier_holdout(
            parsed,
            direction=direction,
        )
    if args.json:
        print(json.dumps({"asset": args.asset.upper(), "directions": results}, ensure_ascii=False, indent=2))
        return 0
    for direction, result in results.items():
        print(
            f"direction={direction} samples={result['samples']} "
            f"train={result['train_samples']} holdout={result['holdout_samples']} "
            f"horizon_hours={result['horizon_seconds'] / 3600:g}"
        )
        for tier in result["tiers"]:
            print(
                f"direction={direction} tier={tier['tier']} "
                f"p={tier['percentile']} threshold_bps={tier['threshold_bps']} "
                f"confirmed_crossings={tier['holdout_crossings']} "
                f"median_reversion_bps={tier['median_favorable_reversion_bps']} "
                f"hit_rates={tier['target_hit_rates']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
