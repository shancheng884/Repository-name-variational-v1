#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

LONG = "long_var_short_lighter"
SHORT = "short_var_long_lighter"


def dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def fmt(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format(value.quantize(Decimal("0.000001")), "f")


def pnl_bps(direction: str, entry: dict[str, Any], exit_row: dict[str, Any]) -> Decimal | None:
    qty = Decimal("1")
    if direction == LONG:
        entry_var = dec(entry.get("var_ask"))
        entry_lighter = dec(entry.get("lighter_sell_price"))
        exit_var = dec(exit_row.get("var_bid"))
        exit_lighter = dec(exit_row.get("lighter_buy_price"))
        if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
            return None
        pnl = (exit_var - entry_var) * qty + (entry_lighter - exit_lighter) * qty
        return pnl / entry_var * Decimal("10000")
    entry_var = dec(entry.get("var_bid"))
    entry_lighter = dec(entry.get("lighter_buy_price"))
    exit_var = dec(exit_row.get("var_ask"))
    exit_lighter = dec(exit_row.get("lighter_sell_price"))
    if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
        return None
    pnl = (entry_var - exit_var) * qty + (exit_lighter - entry_lighter) * qty
    return pnl / entry_var * Decimal("10000")


def direction_signal(direction: str, z: Decimal) -> Decimal:
    return -z if direction == LONG else z


def abs_ok(direction: str, basis: Decimal, threshold: Decimal) -> bool:
    if threshold <= 0:
        return True
    return basis <= -threshold if direction == LONG else basis >= threshold


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward-simulate live basis candidates from basis_state logs.")
    parser.add_argument("--file", default="log/order_metrics.jsonl")
    parser.add_argument("--tail", type=int, default=10000)
    parser.add_argument("--z-entry", type=Decimal, default=Decimal("1.5"))
    parser.add_argument("--min-entry-edge-bps", type=Decimal, default=Decimal("8"))
    parser.add_argument("--long-min-entry-edge-bps", type=Decimal, default=Decimal("0"))
    parser.add_argument("--short-min-entry-edge-bps", type=Decimal, default=Decimal("0"))
    parser.add_argument("--min-abs-entry-bps", type=Decimal, default=Decimal("10"))
    parser.add_argument("--long-min-abs-entry-bps", type=Decimal, default=Decimal("0"))
    parser.add_argument("--short-min-abs-entry-bps", type=Decimal, default=Decimal("0"))
    parser.add_argument("--max-entry-roundtrip-cost-bps", type=Decimal, default=Decimal("3"))
    parser.add_argument("--entry-confirm-samples", type=int, default=3)
    parser.add_argument("--max-sample-move-bps", type=Decimal, default=Decimal("3"))
    parser.add_argument("--horizons", default="30,60,120")
    args = parser.parse_args()

    lines = Path(args.file).read_text(encoding="utf-8", errors="replace").splitlines()[-args.tail :]
    rows: list[dict[str, Any]] = []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("event") == "live_inventory_basis_state" and row.get("warm") is True:
            rows.append(row)

    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]
    confirm = defaultdict(int)
    candidates: list[tuple[int, str]] = []
    last_basis: Decimal | None = None
    for idx, row in enumerate(rows):
        basis = dec(row.get("basis_bps"))
        z = dec(row.get("z"))
        if basis is None or z is None:
            continue
        move = abs(basis - last_basis) if last_basis is not None else None
        last_basis = basis
        if move is not None and args.max_sample_move_bps > 0 and move > args.max_sample_move_bps:
            confirm.clear()
            continue
        checks = [
            (LONG, dec(row.get("long_edge_bps")), dec(row.get("long_roundtrip_pnl_bps")), args.long_min_entry_edge_bps or args.min_entry_edge_bps, args.long_min_abs_entry_bps or args.min_abs_entry_bps),
            (SHORT, dec(row.get("short_edge_bps")), dec(row.get("short_roundtrip_pnl_bps")), args.short_min_entry_edge_bps or args.min_entry_edge_bps, args.short_min_abs_entry_bps or args.min_abs_entry_bps),
        ]
        for direction, edge, roundtrip, min_edge, min_abs in checks:
            if edge is None or roundtrip is None:
                continue
            if direction_signal(direction, z) < args.z_entry or not abs_ok(direction, basis, min_abs):
                confirm[direction] = 0
                continue
            if edge < min_edge or roundtrip < -args.max_entry_roundtrip_cost_bps:
                confirm[direction] = 0
                continue
            confirm[direction] += 1
            if confirm[direction] >= args.entry_confirm_samples:
                candidates.append((idx, direction))
                confirm.clear()
                break

    print(f"basis_rows={len(rows)} candidates={len(candidates)}")
    by_direction = defaultdict(int)
    results: dict[int, list[Decimal]] = {horizon: [] for horizon in horizons}
    direction_results: dict[tuple[str, int], list[Decimal]] = defaultdict(list)
    for idx, direction in candidates:
        by_direction[direction] += 1
        for horizon in horizons:
            exit_idx = idx + horizon
            if exit_idx >= len(rows):
                continue
            value = pnl_bps(direction, rows[idx], rows[exit_idx])
            if value is None:
                continue
            results[horizon].append(value)
            direction_results[(direction, horizon)].append(value)

    print("candidates_by_direction:")
    for direction, count in sorted(by_direction.items()):
        print(f"  {count} {direction}")
    print("forward_pnl_bps:")
    for horizon in horizons:
        values = results[horizon]
        if not values:
            print(f"  {horizon}: n=0 avg=- win_rate=- worst=-")
            continue
        wins = sum(1 for value in values if value > 0)
        print(f"  {horizon}: n={len(values)} avg={fmt(sum(values) / Decimal(len(values)))} win_rate={wins}/{len(values)} worst={fmt(min(values))}")
    print("forward_pnl_bps_by_direction:")
    for (direction, horizon), values in sorted(direction_results.items()):
        wins = sum(1 for value in values if value > 0)
        print(f"  {direction} {horizon}: n={len(values)} avg={fmt(sum(values) / Decimal(len(values)))} win_rate={wins}/{len(values)} worst={fmt(min(values))}")


if __name__ == "__main__":
    main()
