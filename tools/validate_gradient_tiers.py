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


def percentile(values: list[Decimal], value: Decimal) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires samples")
    position = (Decimal(len(ordered) - 1) * value) / Decimal("100")
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def parse_sample(row: dict[str, Any]) -> tuple[datetime, Decimal] | None:
    try:
        observed_at = datetime.fromisoformat(
            str(row["logged_at"]).replace("Z", "+00:00")
        )
        edge = Decimal(str(row["short_edge_bps"]))
    except (KeyError, ValueError, TypeError):
        return None
    return observed_at, edge


def evaluate_tier_holdout(
    samples: list[tuple[datetime, Decimal]],
    *,
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
        crossings: list[int] = []
        previous = test[0][1]
        for index in range(1, len(test)):
            edge = test[index][1]
            if previous < threshold <= edge:
                crossings.append(index)
            previous = edge
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
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    parsed = [
        sample
        for row in read_basis_samples(
            args.root,
            limit=args.limit,
            asset_filter=args.asset,
        )
        if (sample := parse_sample(row)) is not None
    ]
    result = evaluate_tier_holdout(parsed)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    print(
        f"samples={result['samples']} train={result['train_samples']} "
        f"holdout={result['holdout_samples']} horizon_hours="
        f"{result['horizon_seconds'] / 3600:g}"
    )
    for tier in result["tiers"]:
        print(
            f"tier={tier['tier']} p={tier['percentile']} "
            f"threshold_bps={tier['threshold_bps']} "
            f"crossings={tier['holdout_crossings']} "
            f"median_reversion_bps={tier['median_favorable_reversion_bps']} "
            f"hit_rates={tier['target_hit_rates']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
