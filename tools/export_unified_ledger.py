import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "none":
        return ""
    return text


def first_text(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = clean_text(row.get(key, ""))
        if value:
            return value
    return ""


def infer_hedge_completion_status(row: dict[str, Any]) -> str:
    explicit = clean_text(row.get("hedge_completion_status", ""))
    if explicit:
        return explicit

    var_filled_at = clean_text(row.get("variational_filled_at", ""))
    lighter_filled_at = clean_text(row.get("lighter_filled_at", ""))
    processing_stage = clean_text(row.get("processing_stage", ""))
    failure_stage = clean_text(row.get("failure_stage", ""))

    if not var_filled_at:
        return "no_variational_fill"
    if lighter_filled_at or processing_stage == "lighter_filled":
        return "hedged"
    if processing_stage in {"live_submit_started", "live_submit_sent"}:
        return "hedge_pending"
    if processing_stage in {"live_submit_failed", "live_submit_timed_out"}:
        return "naked_variational_leg"
    if failure_stage == "hedge_plan":
        return "hedge_blocked_before_submit"
    if processing_stage in {"dry_run_pending", "dry_run_planned"}:
        return "dry_run_only"
    if processing_stage == "blocked_by_mode":
        return "not_live_mode"
    return "open"


def infer_rollback_action(row: dict[str, Any]) -> str:
    explicit = clean_text(row.get("rollback_action", ""))
    if explicit:
        return explicit
    if infer_hedge_completion_status(row) == "naked_variational_leg":
        return "manual_review_required"
    return "none"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def latest_execution_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_by_trade_key: dict[str, dict[str, Any]] = {}
    for event in events:
        trade_key = clean_text(event.get("trade_key", ""))
        if not trade_key:
            continue
        latest_by_trade_key[trade_key] = event
    return list(latest_by_trade_key.values())


def execution_ledger_row(event: dict[str, Any]) -> dict[str, Any]:
    status = clean_text(event.get("processing_stage", "")) or clean_text(event.get("event", ""))
    side = clean_text(event.get("side", ""))
    return {
        "record_kind": "execution_lifecycle",
        "source_event": clean_text(event.get("event", "")),
        "record_id": first_text(event, "trade_key", "trade_id"),
        "asset": clean_text(event.get("asset", "")).upper(),
        "direction": side,
        "status": status,
        "mode": clean_text(event.get("mode", "")),
        "entry_time": clean_text(event.get("variational_filled_at", "")),
        "exit_time": clean_text(event.get("lighter_filled_at", "")),
        "notional_usd": first_text(event, "live_notional_usd", "variational_notional"),
        "qty": clean_text(event.get("qty", "")),
        "gross_pnl_usd": "",
        "net_pnl_usd": "",
        "fees_usd": "",
        "latency_ms": clean_text(event.get("live_fill_latency_ms", "")),
        "hedge_completion_status": infer_hedge_completion_status(event),
        "rollback_action": infer_rollback_action(event),
        "failure_stage": clean_text(event.get("failure_stage", "")),
        "failure_reason": clean_text(event.get("failure_reason", "")),
        "logged_at": clean_text(event.get("logged_at", "")),
    }


def paper_ledger_row(event: dict[str, Any]) -> dict[str, Any]:
    event_name = clean_text(event.get("event", ""))
    status = clean_text(event.get("status", "")) or event_name
    return {
        "record_kind": "paper_opportunity",
        "source_event": event_name,
        "record_id": clean_text(event.get("opportunity_id", "")),
        "asset": clean_text(event.get("asset", "")).upper(),
        "direction": clean_text(event.get("direction", "")),
        "status": status,
        "mode": clean_text(event.get("execution_mode", "paper")) or "paper",
        "entry_time": clean_text(event.get("entry_time", "")),
        "exit_time": clean_text(event.get("exit_time", "")),
        "notional_usd": clean_text(event.get("planned_notional_usd", "")),
        "qty": clean_text(event.get("planned_qty", "")),
        "gross_pnl_usd": first_text(event, "gross_pair_pnl_usd", "entry_spread_pnl_usd"),
        "net_pnl_usd": clean_text(event.get("net_pnl_conservative_usd", "")),
        "fees_usd": clean_text(event.get("fees_usd", "")),
        "latency_ms": "",
        "hedge_completion_status": "paper_closed" if event_name == "paper_closed" else "paper_open",
        "rollback_action": "none",
        "failure_stage": "",
        "failure_reason": "",
        "logged_at": clean_text(event.get("logged_at", "")),
    }


def build_ledger(order_events: list[dict[str, Any]], opportunity_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [execution_ledger_row(event) for event in latest_execution_events(order_events)]
    rows.extend(paper_ledger_row(event) for event in opportunity_events)
    rows.sort(key=lambda row: (row.get("logged_at", ""), row.get("record_kind", ""), row.get("record_id", "")))
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "record_kind",
        "source_event",
        "record_id",
        "asset",
        "direction",
        "status",
        "mode",
        "entry_time",
        "exit_time",
        "notional_usd",
        "qty",
        "gross_pnl_usd",
        "net_pnl_usd",
        "fees_usd",
        "latency_ms",
        "hedge_completion_status",
        "rollback_action",
        "failure_stage",
        "failure_reason",
        "logged_at",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a unified ledger view from execution and paper logs.")
    parser.add_argument("--orders", default="log/order_metrics.jsonl", help="Path to order_metrics.jsonl")
    parser.add_argument("--opportunities", default="log/opportunities.jsonl", help="Path to opportunities.jsonl")
    parser.add_argument("--output", default="log/unified_ledger.csv", help="Output CSV path")
    args = parser.parse_args()

    order_events = read_jsonl(Path(args.orders))
    opportunity_events = read_jsonl(Path(args.opportunities))
    rows = build_ledger(order_events, opportunity_events)
    write_csv(Path(args.output), rows)
    print(f"wrote {len(rows)} rows to {args.output}")
    by_kind = Counter(row["record_kind"] for row in rows)
    if by_kind:
        print("record_kind: " + ", ".join(f"{key}={value}" for key, value in sorted(by_kind.items())))
    by_status = Counter(row["hedge_completion_status"] for row in rows)
    if by_status:
        print("status: " + ", ".join(f"{key}={value}" for key, value in sorted(by_status.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
