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


def bucket_label(value: Decimal, width: Decimal) -> str:
    if width <= 0:
        return "all"
    start = (value // width) * width
    end = start + width
    return f"{fmt(start)}..{fmt(end)}"


def pnl_bps(direction: str, entry: dict[str, Any], exit_row: dict[str, Any]) -> Decimal | None:
    qty = Decimal("1")
    if direction == LONG:
        entry_var = dec(entry.get("var_ask"))
        entry_lighter = dec(entry.get("lighter_sell_price"))
        exit_var = dec(exit_row.get("var_bid"))
        exit_lighter = dec(exit_row.get("lighter_buy_price"))
        if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
            return None
        return ((exit_var - entry_var) * qty + (entry_lighter - exit_lighter) * qty) / entry_var * Decimal("10000")
    entry_var = dec(entry.get("var_bid"))
    entry_lighter = dec(entry.get("lighter_buy_price"))
    exit_var = dec(exit_row.get("var_ask"))
    exit_lighter = dec(exit_row.get("lighter_sell_price"))
    if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
        return None
    return ((entry_var - exit_var) * qty + (exit_lighter - entry_lighter) * qty) / entry_var * Decimal("10000")


def edge_value(row: dict[str, Any], direction: str, edge_field: str) -> Decimal | None:
    prefix = "long" if direction == LONG else "short"
    if edge_field == "normalized":
        return dec(row.get(f"normalized_{prefix}_edge_bps"))
    return dec(row.get(f"{prefix}_edge_bps"))


def summarize(values: list[Decimal]) -> str:
    if not values:
        return "n=0 avg=- win_rate=- worst=- best=-"
    wins = sum(1 for value in values if value > 0)
    return (
        f"n={len(values)} avg={fmt(sum(values) / Decimal(len(values)))} "
        f"win_rate={wins}/{len(values)} worst={fmt(min(values))} best={fmt(max(values))}"
    )


def bucket_start(label: str) -> Decimal:
    return Decimal(label.split("..")[0]) if ".." in label else Decimal("0")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bucket live basis forward PnL by entry edge bps.")
    parser.add_argument("--file", default="log/order_metrics.jsonl")
    parser.add_argument("--tail", type=int, default=10000)
    parser.add_argument("--bucket-width-bps", type=Decimal, default=Decimal("1"))
    parser.add_argument("--edge-field", choices=("raw", "normalized"), default="raw")
    parser.add_argument("--min-edge-bps", type=Decimal, default=Decimal("0"))
    parser.add_argument("--max-sample-move-bps", type=Decimal, default=Decimal("0"), help="Skip rows whose basis_sample_move_bps exceeds this. 0 disables.")
    parser.add_argument("--horizons", default="30,60,120")
    parser.add_argument("--recommend-horizon", type=int, default=30)
    parser.add_argument("--recommend-min-samples", type=int, default=3)
    parser.add_argument("--recommend-min-avg-pnl-bps", type=Decimal, default=Decimal("0"))
    parser.add_argument("--recommend-min-win-rate", type=Decimal, default=Decimal("0.5"))
    args = parser.parse_args()

    basis_rows: list[dict[str, Any]] = []
    lines = Path(args.file).read_text(encoding="utf-8", errors="replace").splitlines()[-args.tail :]
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("event") == "live_inventory_basis_state" and row.get("warm") is True:
            basis_rows.append(row)

    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]
    buckets: dict[tuple[str, str, int], list[Decimal]] = defaultdict(list)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for idx, row in enumerate(basis_rows):
        move = dec(row.get("basis_sample_move_bps"))
        if args.max_sample_move_bps > 0 and move is not None and move > args.max_sample_move_bps:
            continue
        direction_edges = [
            (LONG, edge_value(row, LONG, args.edge_field)),
            (SHORT, edge_value(row, SHORT, args.edge_field)),
        ]
        for direction, edge in direction_edges:
            if edge is None or edge < args.min_edge_bps:
                continue
            label = bucket_label(edge, args.bucket_width_bps)
            counts[(direction, label)] += 1
            for horizon in horizons:
                exit_idx = idx + horizon
                if exit_idx >= len(basis_rows):
                    continue
                value = pnl_bps(direction, row, basis_rows[exit_idx])
                if value is not None:
                    buckets[(direction, label, horizon)].append(value)

    print(f"basis_rows={len(basis_rows)} edge_field={args.edge_field} bucket_width_bps={args.bucket_width_bps} min_edge_bps={args.min_edge_bps}")
    for direction in (LONG, SHORT):
        labels = sorted({label for dir_name, label in counts if dir_name == direction}, key=bucket_start)
        print(f"\n{direction}:")
        if not labels:
            print("  no buckets")
            continue
        for label in labels:
            count = counts[(direction, label)]
            print(f"  edge {label} candidates={count}")
            for horizon in horizons:
                print(f"    h={horizon} {summarize(buckets[(direction, label, horizon)])}")

    print("\nrecommendations:")
    for direction in (LONG, SHORT):
        labels = sorted({label for dir_name, label in counts if dir_name == direction}, key=bucket_start)
        recommendation = None
        for label in labels:
            values = buckets[(direction, label, args.recommend_horizon)]
            if len(values) < args.recommend_min_samples:
                continue
            avg = sum(values) / Decimal(len(values))
            win_rate = Decimal(sum(1 for value in values if value > 0)) / Decimal(len(values))
            if avg >= args.recommend_min_avg_pnl_bps and win_rate >= args.recommend_min_win_rate:
                recommendation = (label, avg, win_rate, len(values))
                break
        if recommendation is None:
            print(f"  {direction}: no bucket met criteria")
        else:
            label, avg, win_rate, count = recommendation
            print(
                f"  {direction}: min_edge_bps~{fmt(bucket_start(label))} "
                f"bucket={label} h={args.recommend_horizon} n={count} avg={fmt(avg)} win_rate={fmt(win_rate)}"
            )


if __name__ == "__main__":
    main()
