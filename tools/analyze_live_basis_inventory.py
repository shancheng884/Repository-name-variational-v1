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
    exited = 0
    winning = 0
    losing = 0
    realized = Decimal("0")
    pnl_bps_values: list[Decimal] = []
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
            if execution_mode and row.get("execution_mode") != execution_mode:
                continue
            event = row.get("event")
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
                exited += 1
                lot_id = str(row.get("lot_id"))
                entered.pop(lot_id, None)
                pnl_usd = parse_decimal(row.get("pnl_usd")) or Decimal("0")
                realized += pnl_usd
                if pnl_usd > 0:
                    winning += 1
                elif pnl_usd < 0:
                    losing += 1
                if (value := parse_decimal(row.get("pnl_bps"))) is not None:
                    pnl_bps_values.append(value)
                reason = str(row.get("exit_reason") or "unknown")
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    return Summary(
        entered=len(entry_z_values),
        exited=exited,
        open_lots=len(entered),
        winning_exits=winning,
        losing_exits=losing,
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
