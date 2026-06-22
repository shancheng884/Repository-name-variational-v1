from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


DEFAULT_INPUT = Path("log/order_metrics.jsonl")


def dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def fmt(value: Decimal | None) -> str:
    return "n/a" if value is None else format(value, "f")


def percentile(values: list[Decimal], pct: int) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = ((len(ordered) * pct) + 99) // 100
    return ordered[max(0, min(len(ordered) - 1, rank - 1))]


def avg(values: list[Decimal]) -> Decimal | None:
    return None if not values else sum(values, Decimal("0")) / Decimal(len(values))


def summarize(path: Path, *, asset: str) -> dict[str, Any]:
    exits: dict[str, dict[str, Any]] = {}
    asset = asset.upper()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("asset") or "").upper() != asset:
                continue
            event = row.get("event")
            lot_id = str(row.get("lot_id"))
            if event == "live_inventory_exited":
                exits.setdefault(lot_id, {}).update(
                    {
                        "lot_id": lot_id,
                        "direction": row.get("direction"),
                        "entry_kind": row.get("entry_kind"),
                        "estimated_pnl_usd": dec(row.get("pnl_usd")),
                        "estimated_pnl_bps": dec(row.get("pnl_bps")),
                    }
                )
            elif event == "live_inventory_actual_pnl":
                exits.setdefault(lot_id, {}).update(
                    {
                        "lot_id": lot_id,
                        "direction": row.get("direction"),
                        "actual_pnl_usd": dec(row.get("actual_pnl_usd")),
                        "actual_pnl_bps": dec(row.get("actual_pnl_bps")),
                    }
                )
    matched = [row for row in exits.values() if row.get("estimated_pnl_bps") is not None and row.get("actual_pnl_bps") is not None]
    shortfalls = [row["estimated_pnl_bps"] - row["actual_pnl_bps"] for row in matched]
    positive_shortfalls = [value for value in shortfalls if value > 0]
    return {
        "matched_exits": len(matched),
        "positive_shortfalls": len(positive_shortfalls),
        "avg_shortfall_bps": avg(shortfalls),
        "avg_positive_shortfall_bps": avg(positive_shortfalls),
        "p80_positive_shortfall_bps": percentile(positive_shortfalls, 80),
        "p90_positive_shortfall_bps": percentile(positive_shortfalls, 90),
        "max_positive_shortfall_bps": max(positive_shortfalls) if positive_shortfalls else None,
        "rows": matched,
    }


def print_summary(summary: dict[str, Any], *, recent: int) -> None:
    for key in (
        "matched_exits",
        "positive_shortfalls",
        "avg_shortfall_bps",
        "avg_positive_shortfall_bps",
        "p80_positive_shortfall_bps",
        "p90_positive_shortfall_bps",
        "max_positive_shortfall_bps",
    ):
        value = summary[key]
        print(f"{key}: {fmt(value) if isinstance(value, Decimal) or value is None else value}")
    print("recent:")
    for row in summary["rows"][-recent:]:
        est = row.get("estimated_pnl_bps")
        actual = row.get("actual_pnl_bps")
        shortfall = est - actual if est is not None and actual is not None else None
        print(
            f"  lot={row.get('lot_id')} dir={row.get('direction')} est_bps={fmt(est)} actual_bps={fmt(actual)} shortfall_bps={fmt(shortfall)}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze live basis estimated-vs-actual exit slippage.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--recent", type=int, default=10)
    args = parser.parse_args()
    print_summary(summarize(args.input, asset=args.asset), recent=args.recent)


if __name__ == "__main__":
    main()
