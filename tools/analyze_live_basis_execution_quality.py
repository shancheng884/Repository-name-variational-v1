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


def avg(values: list[Decimal]) -> Decimal | None:
    return None if not values else sum(values, Decimal("0")) / Decimal(len(values))


def fmt(value: Decimal | None) -> str:
    return "n/a" if value is None else format(value, "f")


def summarize(path: Path, *, asset: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if '"event": "live_inventory_actual_pnl"' not in line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("asset") or "").upper() == asset.upper():
                rows.append(row)
    actual_bps = [value for row in rows if (value := dec(row.get("actual_pnl_bps"))) is not None]
    estimated_bps = [value for row in rows if (value := dec(row.get("estimated_pnl_bps"))) is not None]
    shortfalls = []
    lighter_exit_drifts = []
    var_legs = []
    lighter_legs = []
    for row in rows:
        est = dec(row.get("estimated_pnl_bps"))
        actual = dec(row.get("actual_pnl_bps"))
        if est is not None and actual is not None:
            shortfalls.append(est - actual)
        expected_lighter_exit = dec(row.get("exit_lighter_price"))
        actual_lighter_exit = dec(row.get("exit_lighter_final_fill_price"))
        entry_var = dec(row.get("entry_var_price"))
        if expected_lighter_exit is not None and actual_lighter_exit is not None and entry_var is not None and entry_var > 0:
            lighter_exit_drifts.append((actual_lighter_exit - expected_lighter_exit) / entry_var * Decimal("10000"))
        if (value := dec(row.get("actual_var_leg_pnl_usd"))) is not None:
            var_legs.append(value)
        if (value := dec(row.get("actual_lighter_leg_pnl_usd"))) is not None:
            lighter_legs.append(value)
    return {
        "actual_rows": len(rows),
        "avg_estimated_pnl_bps": avg(estimated_bps),
        "avg_actual_pnl_bps": avg(actual_bps),
        "avg_estimated_minus_actual_bps": avg(shortfalls),
        "avg_lighter_exit_fill_drift_bps": avg(lighter_exit_drifts),
        "avg_var_leg_pnl_usd": avg(var_legs),
        "avg_lighter_leg_pnl_usd": avg(lighter_legs),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze live basis execution quality by leg and estimate drift.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--asset", default="ETH")
    args = parser.parse_args()
    for key, value in summarize(args.input, asset=args.asset).items():
        print(f"{key}: {fmt(value) if isinstance(value, Decimal) or value is None else value}")


if __name__ == "__main__":
    main()
