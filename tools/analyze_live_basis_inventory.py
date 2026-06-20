from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


DEFAULT_INPUT = Path("log/order_metrics.jsonl")


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def fmt(value: Decimal | None) -> str:
    return "n/a" if value is None else format(value, "f")


def avg(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


@dataclass(slots=True)
class Summary:
    entered: int
    exited: int
    open_lots: int
    winning_exits: int
    losing_exits: int
    actual_pnl_exits: int
    estimated_pnl_exits: int
    realized_pnl_usd: Decimal
    avg_pnl_bps: Decimal | None
    min_pnl_bps: Decimal | None
    max_pnl_bps: Decimal | None
    avg_entry_z: Decimal | None
    avg_entry_edge_bps: Decimal | None
    avg_entry_roundtrip_pnl_bps: Decimal | None
    min_entry_roundtrip_pnl_bps: Decimal | None
    max_entry_roundtrip_pnl_bps: Decimal | None
    exit_reasons: dict[str, int]
    latest_state: dict[str, Any] | None


def summarize(path: Path, *, asset: str, execution_mode: str | None) -> Summary:
    entered: dict[str, dict[str, Any]] = {}
    exits: dict[str, dict[str, Any]] = {}
    entry_z_values: list[Decimal] = []
    entry_edge_values: list[Decimal] = []
    entry_roundtrip_values: list[Decimal] = []
    exit_reasons: dict[str, int] = {}
    latest_state: dict[str, Any] | None = None
    asset = asset.upper()
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            if not raw.strip():
                continue
            row = json.loads(raw)
            if str(row.get("asset") or "").upper() != asset:
                continue
            event = row.get("event")
            if execution_mode and row.get("execution_mode") != execution_mode:
                # Older actual-PnL rows may not carry execution_mode. They are
                # live-only corrections for a preceding live_inventory_exited row.
                if not (
                    execution_mode == "live"
                    and event == "live_inventory_actual_pnl"
                    and row.get("execution_mode") is None
                ):
                    continue
            if event == "live_inventory_basis_state":
                latest_state = row
                continue
            if event in {"live_inventory_dry_entered", "live_inventory_entered"}:
                lot_id = str(row.get("lot_id"))
                entered[lot_id] = row
                if (value := parse_decimal(row.get("z"))) is not None:
                    entry_z_values.append(value)
                if (value := parse_decimal(row.get("edge_bps"))) is not None:
                    entry_edge_values.append(value)
                if (value := parse_decimal(row.get("roundtrip_pnl_bps"))) is not None:
                    entry_roundtrip_values.append(value)
                continue
            if event in {"live_inventory_dry_exited", "live_inventory_exited"}:
                lot_id = str(row.get("lot_id"))
                entered.pop(lot_id, None)
                reason = str(row.get("exit_reason") or "unknown")
                exits[lot_id] = {
                    "pnl_usd": parse_decimal(row.get("pnl_usd")) or Decimal("0"),
                    "pnl_bps": parse_decimal(row.get("pnl_bps")),
                    "exit_reason": reason,
                    "pnl_source": "estimated" if event == "live_inventory_exited" else "reported",
                }
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
                continue
            if event == "live_inventory_actual_pnl":
                lot_id = str(row.get("lot_id"))
                if lot_id in exits:
                    exits[lot_id]["pnl_usd"] = parse_decimal(row.get("actual_pnl_usd")) or Decimal("0")
                    exits[lot_id]["pnl_bps"] = parse_decimal(row.get("actual_pnl_bps"))
                    exits[lot_id]["pnl_source"] = "actual"

    realized = sum((exit["pnl_usd"] for exit in exits.values()), Decimal("0"))
    pnl_bps_values = [exit["pnl_bps"] for exit in exits.values() if exit["pnl_bps"] is not None]
    winning = sum(1 for exit in exits.values() if exit["pnl_usd"] > 0)
    losing = sum(1 for exit in exits.values() if exit["pnl_usd"] < 0)
    actual_pnl_exits = sum(1 for exit in exits.values() if exit["pnl_source"] == "actual")
    estimated_pnl_exits = sum(1 for exit in exits.values() if exit["pnl_source"] == "estimated")
    return Summary(
        entered=len(entry_z_values),
        exited=len(exits),
        open_lots=len(entered),
        winning_exits=winning,
        losing_exits=losing,
        actual_pnl_exits=actual_pnl_exits,
        estimated_pnl_exits=estimated_pnl_exits,
        realized_pnl_usd=realized,
        avg_pnl_bps=avg(pnl_bps_values),
        min_pnl_bps=min(pnl_bps_values) if pnl_bps_values else None,
        max_pnl_bps=max(pnl_bps_values) if pnl_bps_values else None,
        avg_entry_z=avg(entry_z_values),
        avg_entry_edge_bps=avg(entry_edge_values),
        avg_entry_roundtrip_pnl_bps=avg(entry_roundtrip_values),
        min_entry_roundtrip_pnl_bps=min(entry_roundtrip_values) if entry_roundtrip_values else None,
        max_entry_roundtrip_pnl_bps=max(entry_roundtrip_values) if entry_roundtrip_values else None,
        exit_reasons=exit_reasons,
        latest_state=latest_state,
    )


def print_summary(summary: Summary) -> None:
    print(f"entered: {summary.entered}")
    print(f"exited: {summary.exited}")
    print(f"open_lots: {summary.open_lots}")
    print(f"winning_exits: {summary.winning_exits}")
    print(f"losing_exits: {summary.losing_exits}")
    print(f"actual_pnl_exits: {summary.actual_pnl_exits}")
    print(f"estimated_pnl_exits: {summary.estimated_pnl_exits}")
    print(f"realized_pnl_usd: {fmt(summary.realized_pnl_usd)}")
    print(f"avg_pnl_bps: {fmt(summary.avg_pnl_bps)}")
    print(f"min_pnl_bps: {fmt(summary.min_pnl_bps)}")
    print(f"max_pnl_bps: {fmt(summary.max_pnl_bps)}")
    print(f"avg_entry_z: {fmt(summary.avg_entry_z)}")
    print(f"avg_entry_edge_bps: {fmt(summary.avg_entry_edge_bps)}")
    print(f"avg_entry_roundtrip_pnl_bps: {fmt(summary.avg_entry_roundtrip_pnl_bps)}")
    print(f"min_entry_roundtrip_pnl_bps: {fmt(summary.min_entry_roundtrip_pnl_bps)}")
    print(f"max_entry_roundtrip_pnl_bps: {fmt(summary.max_entry_roundtrip_pnl_bps)}")
    print("exit_reasons:")
    for reason, count in sorted(summary.exit_reasons.items()):
        print(f"  {reason}: {count}")
    if summary.latest_state:
        print("latest_state:")
        for key in ("sample_index", "open_lots_total", "realized_pnl_usd", "completed_cycles", "z", "basis_bps"):
            print(f"  {key}: {summary.latest_state.get(key)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize live inventory basis dry-decision or live results.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--execution-mode", default="dry_decision")
    args = parser.parse_args()
    print_summary(summarize(args.input, asset=args.asset, execution_mode=args.execution_mode))


if __name__ == "__main__":
    main()
