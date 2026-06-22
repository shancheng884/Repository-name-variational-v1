#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def percentile(values: list[Decimal], pct: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (Decimal(len(ordered) - 1) * pct) / Decimal("100")
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - Decimal(low)
    return ordered[low] + (ordered[high] - ordered[low]) * fraction


def fmt(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format(value.quantize(Decimal("0.000001")), "f")


def summarize(name: str, values: list[Decimal]) -> None:
    print(
        f"{name}: n={len(values)} "
        f"p50={fmt(percentile(values, Decimal('50')))} "
        f"p90={fmt(percentile(values, Decimal('90')))} "
        f"p99={fmt(percentile(values, Decimal('99')))} "
        f"avg={fmt(sum(values) / Decimal(len(values)) if values else None)}"
    )


def avg(values: list[Decimal]) -> Decimal | None:
    return sum(values) / Decimal(len(values)) if values else None


def suggest_threshold(values: list[Decimal], *, minimum: Decimal, extra: Decimal = Decimal("0")) -> Decimal | None:
    p90 = percentile(values, Decimal("90"))
    if p90 is None:
        return None
    return max(minimum, p90 + extra)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze live inventory latency, slippage, and PnL quality.")
    parser.add_argument("--file", default="log/order_metrics.jsonl", help="Path to order_metrics.jsonl")
    parser.add_argument("--run-id", default=None, help="Only include one live inventory run_id")
    parser.add_argument("--tail", type=int, default=10000, help="Only inspect the last N lines")
    args = parser.parse_args()

    path = Path(args.file)
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-args.tail :]:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if args.run_id and row.get("run_id") != args.run_id:
            continue
        rows.append(row)

    entered = [r for r in rows if r.get("event") == "live_inventory_entered"]
    exited = [r for r in rows if r.get("event") == "live_inventory_exited"]
    actual = [r for r in rows if r.get("event") == "live_inventory_actual_pnl"]
    blocked = [r for r in rows if r.get("event") == "live_inventory_entry_blocked"]

    print(f"rows={len(rows)} entered={len(entered)} exited={len(exited)} actual_pnl={len(actual)} blocked={len(blocked)}")

    summarize("entry_lighter_slippage_bps", [v for r in entered if (v := to_decimal(r.get("entry_lighter_slippage_bps"))) is not None])
    summarize("exit_lighter_slippage_bps", [v for r in actual if (v := to_decimal(r.get("exit_lighter_slippage_bps"))) is not None])
    summarize("exit_var_fill_to_lighter_fill_ms", [v for r in actual if (v := to_decimal(r.get("exit_var_fill_to_lighter_fill_ms"))) is not None])
    summarize("estimated_vs_actual_pnl_shortfall_bps", [v for r in actual if (v := to_decimal(r.get("estimated_vs_actual_pnl_shortfall_bps"))) is not None])
    summarize("actual_pnl_bps", [v for r in actual if (v := to_decimal(r.get("actual_pnl_bps"))) is not None])
    summarize("actual_pnl_usd", [v for r in actual if (v := to_decimal(r.get("actual_pnl_usd"))) is not None])

    by_direction: dict[str, list[Decimal]] = defaultdict(list)
    entry_slippage_by_direction: dict[str, list[Decimal]] = defaultdict(list)
    for r in actual:
        direction = str(r.get("direction") or "unknown")
        pnl = to_decimal(r.get("actual_pnl_bps"))
        if pnl is not None:
            by_direction[direction].append(pnl)
    for r in entered:
        direction = str(r.get("direction") or "unknown")
        slippage = to_decimal(r.get("entry_lighter_slippage_bps"))
        if slippage is not None:
            entry_slippage_by_direction[direction].append(slippage)

    print("\nactual_pnl_bps_by_direction:")
    for direction, values in sorted(by_direction.items()):
        summarize(f"  {direction}", values)

    print("\nentry_lighter_slippage_bps_by_direction:")
    for direction, values in sorted(entry_slippage_by_direction.items()):
        summarize(f"  {direction}", values)

    by_exit_reason: dict[str, list[Decimal]] = defaultdict(list)
    exit_reason_by_key: dict[tuple[str, str], str] = {}
    for r in exited:
        exit_reason_by_key[(str(r.get("run_id")), str(r.get("lot_id")))] = str(r.get("exit_reason") or "unknown")
    for r in actual:
        key = (str(r.get("run_id")), str(r.get("lot_id")))
        pnl = to_decimal(r.get("actual_pnl_bps"))
        if pnl is not None:
            by_exit_reason[exit_reason_by_key.get(key, "unknown")].append(pnl)

    print("\nactual_pnl_bps_by_exit_reason:")
    for reason, values in sorted(by_exit_reason.items()):
        summarize(f"  {reason}", values)

    blocked_counts: dict[str, int] = defaultdict(int)
    for r in blocked:
        blocked_counts[str(r.get("reason") or "unknown")] += 1
    print("\nentry_blocked_reasons:")
    for reason, count in sorted(blocked_counts.items(), key=lambda item: item[1], reverse=True):
        print(f"  {count} {reason}")

    latency_values = [v for r in actual if (v := to_decimal(r.get("exit_var_fill_to_lighter_fill_ms"))) is not None]
    shortfall_values = [v for r in actual if (v := to_decimal(r.get("estimated_vs_actual_pnl_shortfall_bps"))) is not None and v > 0]
    quote_age_values = [v * Decimal("1000") for r in rows if (v := to_decimal(r.get("var_quote_age_seconds"))) is not None]
    sample_move_values = [v for r in rows if (v := to_decimal(r.get("basis_sample_move_bps"))) is not None]
    entry_slippage_values = [v for r in entered if (v := to_decimal(r.get("entry_lighter_slippage_bps"))) is not None and v > 0]

    print("\nparameter_suggestions:")
    suggested_entry_edge = suggest_threshold(shortfall_values + entry_slippage_values, minimum=Decimal("6"), extra=Decimal("1"))
    suggested_quote_age = percentile(quote_age_values, Decimal("90"))
    suggested_sample_move = percentile(sample_move_values, Decimal("95"))
    suggested_latency_trigger = percentile(latency_values, Decimal("90"))
    print(f"  live_inventory_basis_min_entry_edge_bps >= {fmt(suggested_entry_edge)}")
    print(f"  live_inventory_basis_max_var_quote_age_ms ~= {fmt(suggested_quote_age)}")
    print(f"  live_inventory_basis_max_sample_move_bps ~= {fmt(suggested_sample_move)}")
    print(f"  live_inventory_basis_latency_buffer_p90_ms ~= {fmt(suggested_latency_trigger)}")
    for direction, values in sorted(by_direction.items()):
        direction_avg = avg(values)
        if direction_avg is not None and len(values) >= 3 and direction_avg < 0:
            print(f"  consider pausing {direction}: n={len(values)} avg_actual_pnl_bps={fmt(direction_avg)}")
    if blocked_counts.get("basis_entry_confirmation_pending", 0) > max(3, len(entered) * 3):
        print("  entry_confirm_samples may be too high for current signal frequency")
    if blocked_counts.get("basis_entry_refreshed_edge_below_threshold", 0) > blocked_counts.get("edge_bps_below_dynamic_live_inventory_entry", 0):
        print("  refreshed VWAP check is filtering weak signals; keep refresh-entry enabled")


if __name__ == "__main__":
    main()
