from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_STATE = Path("log/live_inventory_state.json")
DEFAULT_METRICS = Path("log/order_metrics.jsonl")


EVENTS = {
    "live_inventory_entered",
    "live_inventory_exited",
    "live_inventory_actual_pnl",
    "live_inventory_manual_review_required",
    "manual_review_required",
}


def load_state(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def latest_events(path: Path, *, asset: str, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("event") in EVENTS and str(row.get("asset") or "").upper() == asset.upper():
                rows.append(row)
    return rows[-limit:]


def suggest_action(state: dict[str, Any], events: list[dict[str, Any]]) -> str:
    status = state.get("status")
    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    if status == "manual_review_required" or any(row.get("event") in {"manual_review_required", "live_inventory_manual_review_required"} for row in events[-3:]):
        return "manual_review: stop live, manually confirm both venues flat or matching state"
    if status == "open" or open_lots:
        return "wait_or_resume: position is open; do not start a new flat-start round"
    if status == "flat":
        return "flat: eligible for next test after manually confirming both venues flat"
    return "unknown: inspect state and venues manually"


def print_summary(state: dict[str, Any], events: list[dict[str, Any]]) -> None:
    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    print(f"status: {state.get('status')}")
    print(f"completed_cycles: {state.get('completed_cycles')}")
    print(f"open_lots: {len(open_lots)}")
    print(f"realized_pnl_usd: {state.get('realized_pnl_usd')}")
    print(f"reason: {state.get('reason')}")
    print(f"manual_review_reason: {state.get('manual_review_reason')}")
    print(f"suggested_action: {suggest_action(state, events)}")
    print("open_lots_detail:")
    for lot in open_lots:
        print(f"  lot={lot.get('lot_id')} kind={lot.get('entry_kind')} qty={lot.get('qty')} entry_basis={lot.get('entry_basis_bps')}")
    print("recent_events:")
    for row in events:
        print(
            "  event={event} lot={lot_id} kind={entry_kind} qty={qty} est_bps={pnl_bps} actual_bps={actual_pnl_bps} reason={reason}".format(
                event=row.get("event"),
                lot_id=row.get("lot_id"),
                entry_kind=row.get("entry_kind"),
                qty=row.get("qty"),
                pnl_bps=row.get("pnl_bps"),
                actual_pnl_bps=row.get("actual_pnl_bps"),
                reason=row.get("reason") or row.get("exit_reason"),
            )
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize the latest live ETH basis round and next action.")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--limit", type=int, default=12)
    args = parser.parse_args()
    events = latest_events(args.metrics, asset=args.asset, limit=args.limit)
    print_summary(load_state(args.state), events)


if __name__ == "__main__":
    main()
