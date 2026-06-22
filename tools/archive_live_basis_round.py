from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from tools.analyze_live_basis_execution_quality import summarize as summarize_execution_quality
from tools.analyze_live_basis_slippage import summarize as summarize_slippage
from tools.grid_replay_live_basis_params import parse_decimals, parse_ints
from tools.recommend_live_basis_params import recommend
from tools.summarize_latest_live_basis_round import latest_events, load_state, suggest_action


DEFAULT_STATE = Path("log/live_inventory_state.json")
DEFAULT_METRICS = Path("log/order_metrics.jsonl")
DEFAULT_ARCHIVE_DIR = Path("log/archive")


def json_safe(value):
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    return value


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def latest_run_id(rows: list[dict[str, Any]], *, asset: str) -> str | None:
    asset = asset.upper()
    for row in reversed(rows):
        if row.get("event") != "live_inventory_run_config":
            continue
        if row.get("asset") and str(row.get("asset")).upper() != asset:
            continue
        run_id = row.get("run_id")
        if run_id:
            return str(run_id)
    return None


def latest_run_start(rows: list[dict[str, Any]], *, asset: str) -> tuple[str | None, int | None]:
    asset = asset.upper()
    for idx in range(len(rows) - 1, -1, -1):
        row = rows[idx]
        if row.get("event") != "live_inventory_run_config":
            continue
        if row.get("asset") and str(row.get("asset")).upper() != asset:
            continue
        run_id = row.get("run_id")
        return (str(run_id) if run_id else None), idx
    return None, None


def filter_rows_for_run(rows: list[dict[str, Any]], *, run_id: str | None, start_index: int | None) -> list[dict[str, Any]]:
    if start_index is not None:
        return rows[start_index:]
    if not run_id:
        return rows
    return [row for row in rows if row.get("run_id") == run_id]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=True) + "\n" for row in rows), encoding="utf-8")


def dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def actual_pnl_summary(rows: list[dict[str, Any]], *, asset: str) -> dict[str, Any]:
    actual_rows = [
        row
        for row in rows
        if row.get("event") == "live_inventory_actual_pnl" and str(row.get("asset") or "").upper() == asset.upper()
    ]
    pnl_usd = [value for row in actual_rows if (value := dec(row.get("actual_pnl_usd"))) is not None]
    pnl_bps = [value for row in actual_rows if (value := dec(row.get("actual_pnl_bps"))) is not None]
    winning = sum(1 for value in pnl_usd if value > 0)
    losing = sum(1 for value in pnl_usd if value < 0)
    flat = sum(1 for value in pnl_usd if value == 0)
    total_usd = sum(pnl_usd, Decimal("0"))
    avg_bps = None if not pnl_bps else sum(pnl_bps, Decimal("0")) / Decimal(len(pnl_bps))
    return {
        "actual_exit_count": len(actual_rows),
        "actual_pnl_total_usd": total_usd,
        "actual_pnl_avg_bps": avg_bps,
        "winning_exits": winning,
        "losing_exits": losing,
        "flat_exits": flat,
    }


def next_step_commands(summary: dict[str, Any]) -> list[str]:
    state = summary["state"]
    status = state.get("status")
    events = summary.get("recent_events") or []
    if status == "manual_review_required" or any(row.get("event") in {"manual_review_required", "live_inventory_manual_review_required"} for row in events[-3:]):
        return [
            "python3 tools/variational_api_command.py positions",
            "python3 tools/inspect_live_basis_state.py",
        ]
    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    if status == "open" or open_lots:
        return ["python3 tools/inspect_live_basis_state.py"]
    if status == "flat":
        return [
            "python3 tools/preflight_live_basis.py",
            "python3 tools/recommend_live_basis_params.py --input log/order_metrics.jsonl --asset ETH",
        ]
    return ["python3 tools/inspect_live_basis_state.py"]


def render_summary_text(summary: dict[str, Any]) -> str:
    state = summary["state"]
    recommendation = summary["recommendation"]
    best = recommendation.get("best")
    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    lines = [
        f"run_id: {summary.get('run_id') or 'n/a'}",
        f"run_id_filter_applied: {summary['run_id_filter_applied']}",
        f"since_run_start_applied: {summary['since_run_start_applied']}",
        f"archived_events: {summary['archived_events']}",
        f"status: {state.get('status')}",
        f"completed_cycles: {state.get('completed_cycles')}",
        f"open_lots: {len(open_lots)}",
        f"realized_pnl_usd: {state.get('realized_pnl_usd')}",
        f"manual_review_reason: {state.get('manual_review_reason')}",
        f"suggested_action: {summary['suggested_action']}",
        f"actual_exit_count: {summary['actual_pnl']['actual_exit_count']}",
        f"actual_pnl_total_usd: {summary['actual_pnl']['actual_pnl_total_usd']}",
        f"actual_pnl_avg_bps: {summary['actual_pnl']['actual_pnl_avg_bps']}",
        f"winning_exits: {summary['actual_pnl']['winning_exits']}",
        f"losing_exits: {summary['actual_pnl']['losing_exits']}",
        f"matched_actual_exits: {recommendation['matched_actual_exits']}",
        f"candidate_count: {recommendation['candidate_count']}",
        f"recommendation_warnings: {','.join(recommendation['warnings']) if recommendation['warnings'] else 'none'}",
        f"recommendation_action: {recommendation['suggested_action']}",
        "next_step_commands:",
    ]
    lines.extend(f"  {command}" for command in summary["next_step_commands"])
    if best:
        lines.extend(
            [
                "best:",
                f"  z_entry: {best['z_entry']}",
                f"  min_abs: {best['min_abs']}",
                f"  min_edge: {best['min_edge']}",
                f"  max_cost: {best['max_cost']}",
                f"  addon: {best['addon']}",
                f"  min_exit: {best['min_exit']}",
                f"  max_total: {best['max_total']}",
                f"  adjusted_pnl_usd: {best.get('adjusted_pnl_usd')}",
                f"suggested_flags_one_line: {recommendation['suggested_flags_one_line']}",
                "suggested_flags:",
            ]
        )
        lines.extend(f"  {flag}" for flag in recommendation["suggested_flags"])
    else:
        lines.append("best: n/a")
    if summary["warnings"]:
        lines.append(f"archive_warnings: {','.join(summary['warnings'])}")
    return "\n".join(lines) + "\n"


def archive(args: argparse.Namespace) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = args.archive_dir / f"live_basis_round_{ts}"
    target.mkdir(parents=True, exist_ok=False)
    if args.state.exists():
        shutil.copy2(args.state, target / args.state.name)
    if args.metrics.exists():
        shutil.copy2(args.metrics, target / args.metrics.name)
    rows = load_jsonl(args.metrics)
    run_start_id, run_start_index = latest_run_start(rows, asset=args.asset)
    selected_run_id = getattr(args, "run_id", None) or run_start_id or latest_run_id(rows, asset=args.asset)
    round_rows = filter_rows_for_run(rows, run_id=selected_run_id, start_index=None if getattr(args, "run_id", None) else run_start_index)
    warnings = []
    if selected_run_id or run_start_index is not None:
        round_metrics = target / "round_order_metrics.jsonl"
        write_jsonl(round_metrics, round_rows)
    else:
        round_metrics = args.metrics
        warnings.append("run_id_not_found_using_full_metrics")
    state = load_state(args.state)
    events = latest_events(round_metrics, asset=args.asset, limit=args.limit)
    args.input = round_metrics
    recommendation = recommend(args)
    summary = {
        "run_id": selected_run_id,
        "run_start_index": run_start_index,
        "run_id_filter_applied": bool(selected_run_id),
        "since_run_start_applied": run_start_index is not None and getattr(args, "run_id", None) is None,
        "archived_events": len(round_rows) if selected_run_id or run_start_index is not None else len(rows),
        "warnings": warnings,
        "state": state,
        "suggested_action": suggest_action(state, events),
        "next_step_commands": [],
        "recent_events": events,
        "actual_pnl": actual_pnl_summary(round_rows if selected_run_id or run_start_index is not None else rows, asset=args.asset),
        "slippage": {k: v for k, v in summarize_slippage(round_metrics, asset=args.asset).items() if k != "rows"},
        "execution_quality": summarize_execution_quality(round_metrics, asset=args.asset),
        "recommendation": recommendation,
    }
    summary["next_step_commands"] = next_step_commands(summary)
    (target / "summary.json").write_text(json.dumps(json_safe(summary), indent=2, ensure_ascii=True), encoding="utf-8")
    (target / "summary.txt").write_text(render_summary_text(summary), encoding="utf-8")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive current live basis round state, metrics, and reports.")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS)
    parser.add_argument("--input", type=Path, default=DEFAULT_METRICS, help="Alias used by recommendation tooling.")
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--run-id", default=None, help="Archive a specific run_id. Default: latest live_inventory_run_config run_id.")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--lot-notional-usd", type=Decimal, default=Decimal("20"))
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--z-entry", type=parse_decimals, default=parse_decimals("3,4,5"))
    parser.add_argument("--z-exit", type=Decimal, default=Decimal("999"))
    parser.add_argument("--min-entry-edge-bps", type=parse_decimals, default=parse_decimals("7,8,10"))
    parser.add_argument("--min-abs-entry-bps", type=parse_decimals, default=parse_decimals("10,12,14,16"))
    parser.add_argument("--max-entry-roundtrip-cost-bps", type=parse_decimals, default=parse_decimals("3,4"))
    parser.add_argument("--addon-min-basis-improvement-bps", type=parse_decimals, default=parse_decimals("3,4,5"))
    parser.add_argument("--min-exit-pnl-bps", type=parse_decimals, default=parse_decimals("0.5,1,1.5,2"))
    parser.add_argument("--max-total-lots", type=parse_ints, default=parse_ints("1,2"))
    parser.add_argument("--min-hold-samples", type=int, default=0)
    parser.add_argument("--adjust-shortfall-bps", type=Decimal, default=None)
    parser.add_argument("--min-actual-exits", type=int, default=3)
    args = parser.parse_args()
    target = archive(args)
    print(f"archive_dir: {target}")
    print(f"summary: {target / 'summary.txt'}")


if __name__ == "__main__":
    main()
