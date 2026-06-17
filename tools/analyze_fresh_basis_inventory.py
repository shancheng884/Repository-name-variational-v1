from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class FreshBasisInventorySummary:
    rows: int
    entered: int
    exited: int
    open_lots: int
    realized_pnl_usd: Decimal
    winning_exits: int
    losing_exits: int
    avg_pnl_bps: Decimal | None
    min_pnl_bps: Decimal | None
    max_pnl_bps: Decimal | None
    avg_entry_z: Decimal | None
    avg_entry_edge_bps: Decimal | None
    avg_entry_roundtrip_pnl_bps: Decimal | None
    min_entry_roundtrip_pnl_bps: Decimal | None
    max_entry_roundtrip_pnl_bps: Decimal | None
    exit_reasons: Counter[str]
    latest_row: dict[str, Any]


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def average(values: list[Decimal]) -> Decimal | None:
    return sum(values) / len(values) if values else None


def summarize_rows(rows: list[dict[str, Any]]) -> FreshBasisInventorySummary:
    entered = 0
    exited = 0
    winning_exits = 0
    losing_exits = 0
    pnl_bps_values: list[Decimal] = []
    entry_z_values: list[Decimal] = []
    entry_edge_values: list[Decimal] = []
    entry_roundtrip_values: list[Decimal] = []
    exit_reasons: Counter[str] = Counter()

    for row in rows:
        actions = row.get("actions") if isinstance(row.get("actions"), list) else []
        for action in actions:
            if not isinstance(action, dict):
                continue
            event = action.get("event")
            if event == "inventory_paper_entered":
                entered += 1
                if (z := parse_decimal(row.get("z"))) is not None:
                    entry_z_values.append(z)
                if (edge := parse_decimal(action.get("edge_bps"))) is not None:
                    entry_edge_values.append(edge)
                direction = action.get("direction")
                key = "long_roundtrip_pnl_bps" if direction == "long_var_short_lighter" else "short_roundtrip_pnl_bps"
                if (roundtrip := parse_decimal(row.get(key))) is not None:
                    entry_roundtrip_values.append(roundtrip)
            elif event == "inventory_paper_exited":
                exited += 1
                reason = str(action.get("exit_reason") or "unknown")
                exit_reasons[reason] += 1
                pnl_usd = parse_decimal(action.get("pnl_usd")) or Decimal("0")
                if pnl_usd > 0:
                    winning_exits += 1
                elif pnl_usd < 0:
                    losing_exits += 1
                if (pnl_bps := parse_decimal(action.get("pnl_bps"))) is not None:
                    pnl_bps_values.append(pnl_bps)

    latest = rows[-1] if rows else {}
    return FreshBasisInventorySummary(
        rows=len(rows),
        entered=entered,
        exited=exited,
        open_lots=int(latest.get("open_lots") or 0),
        realized_pnl_usd=parse_decimal(latest.get("realized_pnl_usd")) or Decimal("0"),
        winning_exits=winning_exits,
        losing_exits=losing_exits,
        avg_pnl_bps=average(pnl_bps_values),
        min_pnl_bps=min(pnl_bps_values) if pnl_bps_values else None,
        max_pnl_bps=max(pnl_bps_values) if pnl_bps_values else None,
        avg_entry_z=average(entry_z_values),
        avg_entry_edge_bps=average(entry_edge_values),
        avg_entry_roundtrip_pnl_bps=average(entry_roundtrip_values),
        min_entry_roundtrip_pnl_bps=min(entry_roundtrip_values) if entry_roundtrip_values else None,
        max_entry_roundtrip_pnl_bps=max(entry_roundtrip_values) if entry_roundtrip_values else None,
        exit_reasons=exit_reasons,
        latest_row=latest,
    )


def text(value: Decimal | None) -> str:
    return "None" if value is None else str(value)


def print_summary(summary: FreshBasisInventorySummary) -> None:
    print(f"rows: {summary.rows}")
    print(f"entered: {summary.entered}")
    print(f"exited: {summary.exited}")
    print(f"open_lots: {summary.open_lots}")
    print(f"realized_pnl_usd: {summary.realized_pnl_usd}")
    print(f"winning_exits: {summary.winning_exits}")
    print(f"losing_exits: {summary.losing_exits}")
    print(f"avg_pnl_bps: {text(summary.avg_pnl_bps)}")
    print(f"min_pnl_bps: {text(summary.min_pnl_bps)}")
    print(f"max_pnl_bps: {text(summary.max_pnl_bps)}")
    print(f"avg_entry_z: {text(summary.avg_entry_z)}")
    print(f"avg_entry_edge_bps: {text(summary.avg_entry_edge_bps)}")
    print(f"avg_entry_roundtrip_pnl_bps: {text(summary.avg_entry_roundtrip_pnl_bps)}")
    print(f"min_entry_roundtrip_pnl_bps: {text(summary.min_entry_roundtrip_pnl_bps)}")
    print(f"max_entry_roundtrip_pnl_bps: {text(summary.max_entry_roundtrip_pnl_bps)}")
    print("exit_reasons:")
    for reason, count in sorted(summary.exit_reasons.items()):
        print(f"  {reason}: {count}")
    if summary.latest_row:
        print("latest:")
        print(f"  sample_index: {summary.latest_row.get('sample_index')}")
        print(f"  z: {summary.latest_row.get('z')}")
        print(f"  long_edge_bps: {summary.latest_row.get('long_edge_bps')}")
        print(f"  long_roundtrip_pnl_bps: {summary.latest_row.get('long_roundtrip_pnl_bps')}")
        print(f"  short_edge_bps: {summary.latest_row.get('short_edge_bps')}")
        print(f"  short_roundtrip_pnl_bps: {summary.latest_row.get('short_roundtrip_pnl_bps')}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze fresh quote basis inventory paper JSONL results.")
    parser.add_argument("file", type=Path)
    args = parser.parse_args()
    if not args.file.exists():
        parser.error(f"{args.file} not found")
    print(f"file: {args.file}")
    print_summary(summarize_rows(load_rows(args.file)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
