import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


@dataclass
class DirectionSummary:
    exits: int
    total_pnl_usd: Decimal
    avg_pnl_bps: Decimal | None
    min_pnl_bps: Decimal | None
    max_pnl_bps: Decimal | None
    min_holding_samples: int | None
    max_holding_samples: int | None


@dataclass
class InventoryPaperSummary:
    events: int
    entered: int
    exited: int
    latest_event: str
    latest_open_lots_total: int
    latest_realized_pnl_usd: Decimal
    max_open_lots_total: int
    losing_exits: list[dict[str, Any]]
    by_direction: dict[str, DirectionSummary]
    latest_events: list[dict[str, Any]]


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def load_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events


def summarize_events(events: list[dict[str, Any]], latest_limit: int = 10) -> InventoryPaperSummary:
    entered = [event for event in events if event.get("event") == "inventory_paper_entered"]
    exited = [event for event in events if event.get("event") == "inventory_paper_exited"]
    latest = events[-1] if events else {}
    by_direction_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in exited:
        by_direction_rows[str(event.get("direction") or "unknown")].append(event)

    by_direction: dict[str, DirectionSummary] = {}
    for direction, rows in by_direction_rows.items():
        pnl = sum((parse_decimal(row.get("pnl_usd")) or Decimal("0")) for row in rows)
        bps_values = [value for row in rows if (value := parse_decimal(row.get("pnl_bps"))) is not None]
        holding_values = [parse_int(row.get("holding_samples")) for row in rows if row.get("holding_samples") is not None]
        by_direction[direction] = DirectionSummary(
            exits=len(rows),
            total_pnl_usd=pnl,
            avg_pnl_bps=sum(bps_values) / len(bps_values) if bps_values else None,
            min_pnl_bps=min(bps_values) if bps_values else None,
            max_pnl_bps=max(bps_values) if bps_values else None,
            min_holding_samples=min(holding_values) if holding_values else None,
            max_holding_samples=max(holding_values) if holding_values else None,
        )

    losing_exits = [
        event
        for event in exited
        if (parse_decimal(event.get("pnl_usd")) or Decimal("0")) < 0
    ]

    return InventoryPaperSummary(
        events=len(events),
        entered=len(entered),
        exited=len(exited),
        latest_event=str(latest.get("event") or ""),
        latest_open_lots_total=parse_int(latest.get("open_lots_total")),
        latest_realized_pnl_usd=parse_decimal(latest.get("realized_pnl_usd")) or Decimal("0"),
        max_open_lots_total=max((parse_int(event.get("open_lots_total")) for event in events), default=0),
        losing_exits=losing_exits,
        by_direction=dict(sorted(by_direction.items())),
        latest_events=events[-latest_limit:] if latest_limit > 0 else [],
    )


def decimal_text(value: Decimal | None) -> str:
    return "None" if value is None else str(value)


def print_summary(summary: InventoryPaperSummary) -> None:
    print(f"events: {summary.events}")
    print(f"entered: {summary.entered}")
    print(f"exited: {summary.exited}")
    print(f"latest_event: {summary.latest_event}")
    print(f"latest_open_lots_total: {summary.latest_open_lots_total}")
    print(f"latest_realized_pnl_usd: {summary.latest_realized_pnl_usd}")
    print(f"max_open_lots_total: {summary.max_open_lots_total}")
    print()
    print("by_direction_exits:")
    for direction, row in summary.by_direction.items():
        print(f"direction: {direction}")
        print(f"  exits: {row.exits}")
        print(f"  total_pnl_usd: {row.total_pnl_usd}")
        print(f"  avg_pnl_bps: {decimal_text(row.avg_pnl_bps)}")
        print(f"  min_pnl_bps: {decimal_text(row.min_pnl_bps)}")
        print(f"  max_pnl_bps: {decimal_text(row.max_pnl_bps)}")
        print(f"  min_holding_samples: {row.min_holding_samples}")
        print(f"  max_holding_samples: {row.max_holding_samples}")
    print()
    print(f"losing_exits: {len(summary.losing_exits)}")
    for event in summary.losing_exits[-10:]:
        print(
            "loser:",
            {
                "lot_id": event.get("lot_id"),
                "direction": event.get("direction"),
                "pnl_usd": event.get("pnl_usd"),
                "pnl_bps": event.get("pnl_bps"),
                "holding_samples": event.get("holding_samples"),
                "edge_bps": event.get("edge_bps"),
            },
        )
    print()
    print("latest_events:")
    for event in summary.latest_events:
        print(
            {
                "event": event.get("event"),
                "direction": event.get("direction"),
                "lot_id": event.get("lot_id"),
                "edge_bps": event.get("edge_bps"),
                "pnl_usd": event.get("pnl_usd"),
                "pnl_bps": event.get("pnl_bps"),
                "holding_samples": event.get("holding_samples"),
                "open_lots_total": event.get("open_lots_total"),
                "realized_pnl_usd": event.get("realized_pnl_usd"),
            }
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze log/inventory_paper.jsonl paper inventory results.")
    parser.add_argument("--file", type=Path, default=Path("log/inventory_paper.jsonl"))
    parser.add_argument("--latest", type=int, default=10, help="Number of latest events to print.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.file.exists():
        raise SystemExit(f"{args.file} not found")
    events = load_events(args.file)
    print(f"file: {args.file}")
    print_summary(summarize_events(events, latest_limit=args.latest))


if __name__ == "__main__":
    main()
