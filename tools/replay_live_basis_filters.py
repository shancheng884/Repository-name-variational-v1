#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
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


def direction_signal(direction: str, z: Decimal) -> Decimal:
    return -z if direction == LONG else z


def abs_entry_ok(direction: str, basis_bps: Decimal, threshold: Decimal) -> bool:
    if threshold <= 0:
        return True
    if direction == LONG:
        return basis_bps <= -threshold
    return basis_bps >= threshold


def load_rows(path: Path, tail: int | None, run_id: str | None) -> list[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if tail:
        lines = lines[-tail:]
    rows: list[dict[str, Any]] = []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if run_id and row.get("run_id") != run_id:
            continue
        rows.append(row)
    return rows


def actual_pnl_by_trace_or_lot(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for row in rows:
        if row.get("event") != "live_inventory_actual_pnl":
            continue
        pnl = dec(row.get("actual_pnl_bps"))
        if pnl is None:
            continue
        keys = [str(row.get("basis_trace_id") or ""), f"{row.get('run_id')}:{row.get('lot_id')}"]
        for key in keys:
            if key and key != "None:None":
                result[key] = pnl
    return result


def actual_pnls_in_order(rows: list[dict[str, Any]]) -> list[Decimal]:
    values: list[Decimal] = []
    for row in rows:
        if row.get("event") != "live_inventory_actual_pnl":
            continue
        pnl = dec(row.get("actual_pnl_bps"))
        if pnl is not None:
            values.append(pnl)
    return values


def prepare_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        if row.get("event") != "live_inventory_basis_state" or row.get("__basis_prepared"):
            continue
        row["__basis_prepared"] = True
        row["__basis_bps"] = dec(row.get("basis_bps"))
        row["__z"] = dec(row.get("z"))
        row["__long_edge_bps"] = dec(row.get("long_edge_bps"))
        row["__long_roundtrip_pnl_bps"] = dec(row.get("long_roundtrip_pnl_bps"))
        row["__short_edge_bps"] = dec(row.get("short_edge_bps"))
        row["__short_roundtrip_pnl_bps"] = dec(row.get("short_roundtrip_pnl_bps"))


def replay(rows: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    reasons: Counter[str] = Counter()
    selected: list[dict[str, Any]] = []
    prepare_rows(rows)
    pnl_lookup = actual_pnl_by_trace_or_lot(rows)
    ordered_actual_pnls = actual_pnls_in_order(rows)
    confirm_counts: dict[str, int] = defaultdict(int)
    last_basis: Decimal | None = None
    open_lot = False

    for row in rows:
        event = row.get("event")
        if event in {"live_inventory_exited", "live_inventory_actual_pnl"}:
            open_lot = False
        if event != "live_inventory_basis_state":
            continue
        if bool(row.get("warm")) is not True:
            reasons["not_warm"] += 1
            confirm_counts.clear()
            continue
        if open_lot and not args.allow_overlap:
            reasons["open_lot"] += 1
            continue
        basis = row.get("__basis_bps")
        z = row.get("__z")
        if basis is None or z is None:
            reasons["missing_basis_or_z"] += 1
            continue
        move = abs(basis - last_basis) if last_basis is not None else None
        last_basis = basis
        if move is not None and args.max_sample_move_bps > 0 and move > args.max_sample_move_bps:
            reasons["sample_move_too_large"] += 1
            confirm_counts.clear()
            continue

        candidates = [
            (LONG, row.get("__long_edge_bps"), row.get("__long_roundtrip_pnl_bps")),
            (SHORT, row.get("__short_edge_bps"), row.get("__short_roundtrip_pnl_bps")),
        ]
        accepted = False
        for direction, edge, roundtrip in candidates:
            if edge is None or roundtrip is None:
                reasons[f"{direction}:missing_edge"] += 1
                confirm_counts[direction] = 0
                continue
            if direction_signal(direction, z) < args.z_entry:
                confirm_counts[direction] = 0
                continue
            if not abs_entry_ok(direction, basis, args.min_abs_entry_bps):
                reasons[f"{direction}:abs_entry_not_met"] += 1
                confirm_counts[direction] = 0
                continue
            if edge < args.min_entry_edge_bps:
                reasons[f"{direction}:edge_too_low"] += 1
                confirm_counts[direction] = 0
                continue
            if roundtrip < -args.max_entry_roundtrip_cost_bps:
                reasons[f"{direction}:roundtrip_too_low"] += 1
                confirm_counts[direction] = 0
                continue
            confirm_counts[direction] += 1
            other = SHORT if direction == LONG else LONG
            confirm_counts[other] = 0
            if confirm_counts[direction] < args.entry_confirm_samples:
                reasons[f"{direction}:confirm_pending"] += 1
                continue
            selected.append({"row": row, "direction": direction, "edge_bps": edge, "roundtrip_bps": roundtrip})
            open_lot = True
            confirm_counts.clear()
            accepted = True
            break
        if not accepted:
            reasons["no_candidate"] += 1

    matched_pnls: list[Decimal] = []
    for item in selected:
        row = item["row"]
        keys = [str(row.get("basis_trace_id") or ""), f"{row.get('run_id')}:{row.get('lot_id')}"]
        for key in keys:
            if key in pnl_lookup:
                matched_pnls.append(pnl_lookup[key])
                break
    if not matched_pnls and ordered_actual_pnls:
        matched_pnls = ordered_actual_pnls[: len(selected)]
    return {"selected": selected, "matched_pnls": matched_pnls, "reasons": reasons}


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay live basis filter rules against order_metrics.jsonl.")
    parser.add_argument("--file", default="log/order_metrics.jsonl")
    parser.add_argument("--run-id")
    parser.add_argument("--tail", type=int, default=10000)
    parser.add_argument("--z-entry", type=Decimal, default=Decimal("1.5"))
    parser.add_argument("--min-entry-edge-bps", type=Decimal, default=Decimal("6"))
    parser.add_argument("--min-abs-entry-bps", type=Decimal, default=Decimal("7"))
    parser.add_argument("--max-entry-roundtrip-cost-bps", type=Decimal, default=Decimal("3"))
    parser.add_argument("--entry-confirm-samples", type=int, default=2)
    parser.add_argument("--max-sample-move-bps", type=Decimal, default=Decimal("5"))
    parser.add_argument("--allow-overlap", action="store_true")
    args = parser.parse_args()

    rows = load_rows(Path(args.file), args.tail, args.run_id)
    result = replay(rows, args)
    selected = result["selected"]
    pnls = result["matched_pnls"]
    print(f"rows={len(rows)} selected_entries={len(selected)} matched_actual_pnls={len(pnls)}")
    if pnls:
        wins = sum(1 for value in pnls if value > 0)
        print(f"matched_avg_pnl_bps={fmt(sum(pnls) / Decimal(len(pnls)))} win_rate={wins}/{len(pnls)} worst_pnl_bps={fmt(min(pnls))}")
    print("\nselected_by_direction:")
    counts = Counter(item["direction"] for item in selected)
    for direction, count in sorted(counts.items()):
        print(f"  {count} {direction}")
    print("\nblocked_or_skipped_reasons:")
    for reason, count in result["reasons"].most_common(25):
        print(f"  {count} {reason}")


if __name__ == "__main__":
    main()
