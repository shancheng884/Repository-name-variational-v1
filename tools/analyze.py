#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import (  # noqa: E402
    LIVE_STATE,
    LOG_DIR,
    ORDER_METRICS,
    RUNTIME_LOG,
    avg,
    fmt_decimal,
    human_bytes,
    parse_time,
    percentile,
    read_json,
    tail_jsonl,
    tail_text,
    to_decimal,
)


def running_main_processes() -> list[str]:
    try:
        result = subprocess.run(["pgrep", "-af", "python.*main.py"], check=False, capture_output=True, text=True)
    except FileNotFoundError:
        return []
    return [line for line in result.stdout.splitlines() if line.strip() and "tools/analyze.py" not in line]


def latest_run_filter(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_run_id = ""
    for row in reversed(rows):
        run_id = str(row.get("run_id") or "")
        if run_id:
            latest_run_id = run_id
            break
    if not latest_run_id:
        return rows
    return [row for row in rows if str(row.get("run_id") or "") == latest_run_id]


def depth_slippage(row: dict[str, Any], key: str) -> Decimal | None:
    value = row.get(key)
    if not isinstance(value, dict):
        return None
    return to_decimal(value.get("slippage_bps"))


def best_entry_score(row: dict[str, Any], shortfall_buffer: Decimal, sample_move_penalty: Decimal) -> tuple[Decimal | None, str]:
    long_norm = to_decimal(row.get("normalized_long_edge_bps"))
    short_norm = to_decimal(row.get("normalized_short_edge_bps"))
    long_raw = to_decimal(row.get("long_edge_bps"))
    short_raw = to_decimal(row.get("short_edge_bps"))
    long_score = long_norm if long_norm is not None else long_raw
    short_score = short_norm if short_norm is not None else short_raw
    if long_score is None and short_score is None:
        return None, "-"
    if long_score is not None and (short_score is None or long_score >= short_score):
        direction = "long_var_short_lighter"
        edge = long_score
        roundtrip = to_decimal(row.get("long_roundtrip_pnl_bps")) or Decimal("0")
    else:
        direction = "short_var_long_lighter"
        edge = short_score or Decimal("0")
        roundtrip = to_decimal(row.get("short_roundtrip_pnl_bps")) or Decimal("0")
    sample_move = abs(to_decimal(row.get("basis_sample_move_bps")) or Decimal("0"))
    return edge + min(roundtrip, Decimal("0")) - shortfall_buffer - (sample_move * sample_move_penalty), direction


def file_size(path: Path) -> str:
    try:
        return human_bytes(path.stat().st_size)
    except FileNotFoundError:
        return "missing"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze real live trading logs.")
    parser.add_argument("--tail", type=int, default=50000, help="JSONL rows to inspect from order_metrics.jsonl. Default: 50000.")
    parser.add_argument("--all-runs", action="store_true", help="Analyze all tailed rows instead of only the latest run_id.")
    parser.add_argument("--top", type=int, default=5, help="Number of blocked candidates/reasons to print. Default: 5.")
    args = parser.parse_args()
    if args.tail <= 0:
        parser.error("--tail must be > 0")
    if args.top <= 0:
        parser.error("--top must be > 0")

    raw_rows = tail_jsonl(ORDER_METRICS, args.tail)
    rows = raw_rows if args.all_runs else latest_run_filter(raw_rows)
    state = read_json(LIVE_STATE)
    processes = running_main_processes()

    events = Counter(str(row.get("event") or "-") for row in rows)
    assets = Counter(str(row.get("asset") or "-").upper() for row in rows if row.get("asset"))
    blocked_reasons = Counter(str(row.get("reason") or "unknown") for row in rows if row.get("event") == "live_inventory_entry_blocked")

    actual_pnl_bps: list[Decimal] = []
    actual_pnl_usd: list[Decimal] = []
    shortfalls: list[Decimal] = []
    entry_slippage: list[Decimal] = []
    exit_slippage: list[Decimal] = []
    sample_moves: list[Decimal] = []
    normalized_edges: list[Decimal] = []
    blocked_scores: list[tuple[Decimal, str, str, str]] = []
    access_restricted = 0

    for row in rows:
        text = str(row)
        if "Access Restricted" in text:
            access_restricted += 1
        event = row.get("event")
        if event == "live_inventory_actual_pnl":
            if (value := to_decimal(row.get("actual_pnl_bps"))) is not None:
                actual_pnl_bps.append(value)
            if (value := to_decimal(row.get("actual_pnl_usd"))) is not None:
                actual_pnl_usd.append(value)
            if (value := to_decimal(row.get("estimated_vs_actual_pnl_shortfall_bps"))) is not None:
                shortfalls.append(value)
            if (value := to_decimal(row.get("entry_lighter_slippage_bps"))) is not None:
                entry_slippage.append(value)
            if (value := to_decimal(row.get("exit_lighter_slippage_bps"))) is not None:
                exit_slippage.append(value)
        if event == "live_inventory_entry_blocked":
            if (move := to_decimal(row.get("basis_sample_move_bps"))) is not None:
                sample_moves.append(abs(move))
            for key in ("normalized_long_edge_bps", "normalized_short_edge_bps"):
                if (value := to_decimal(row.get(key))) is not None:
                    normalized_edges.append(value)
            score, direction = best_entry_score(row, Decimal("5.5"), Decimal("0.5"))
            if score is not None:
                blocked_scores.append((score, str(row.get("asset") or "-").upper(), direction, str(row.get("reason") or "unknown")))

    latest_at = "-"
    latest_dt = None
    for row in reversed(rows):
        latest_dt = parse_time(row.get("logged_at"))
        if latest_dt is not None:
            latest_at = latest_dt.isoformat()
            break
    age = "-"
    if latest_dt is not None:
        age = f"{(datetime.now(timezone.utc) - latest_dt).total_seconds():.0f}s"

    status = str(state.get("status") or ("missing" if not state else "unknown"))
    state_asset = str(state.get("asset") or "-").upper()
    open_lots = state.get("open_lots") or []
    pending_actions = state.get("pending_actions") or []

    print("== live ==")
    print(f"process={'YES' if processes else 'NO'}")
    for process in processes:
        print(f"process_detail={process}")
    print(f"state={status} asset={state_asset} open_lots={len(open_lots)} pending_actions={len(pending_actions)}")
    print(f"logs order_metrics={file_size(ORDER_METRICS)} runtime={file_size(RUNTIME_LOG)} log_dir={human_bytes(sum(p.stat().st_size for p in LOG_DIR.rglob('*') if p.is_file())) if LOG_DIR.exists() else 'missing'}")
    print(f"rows={len(rows)}/{len(raw_rows)} latest_at={latest_at} latest_age={age}")

    print("== events ==")
    print(
        " ".join(
            f"{name}={events[name]}"
            for name in [
                "live_inventory_entered",
                "live_inventory_exited",
                "live_inventory_actual_pnl",
                "live_inventory_entry_blocked",
                "live_inventory_manual_review_required",
                "live_inventory_basis_quote_failed",
                "lighter_filled",
                "variational_filled",
            ]
        )
    )
    print(f"assets={dict(assets.most_common())}")
    print(f"blocked_reasons={dict(blocked_reasons.most_common(args.top))}")
    print(f"access_restricted_rows={access_restricted}")

    print("== pnl ==")
    print(
        f"actual_n={len(actual_pnl_bps)} avg_bps={fmt_decimal(avg(actual_pnl_bps))} "
        f"p50_bps={fmt_decimal(percentile(actual_pnl_bps, Decimal('50')))} "
        f"sum_usd={fmt_decimal(sum(actual_pnl_usd) if actual_pnl_usd else None, '0.0001')}"
    )
    print(
        f"shortfall_n={len(shortfalls)} avg={fmt_decimal(avg(shortfalls))} "
        f"p80={fmt_decimal(percentile(shortfalls, Decimal('80')))} "
        f"entry_slip_p80={fmt_decimal(percentile(entry_slippage, Decimal('80')))} "
        f"exit_slip_p80={fmt_decimal(percentile(exit_slippage, Decimal('80')))}"
    )

    print("== signal_quality ==")
    print(
        f"sample_move_p50={fmt_decimal(percentile(sample_moves, Decimal('50')))} "
        f"sample_move_p80={fmt_decimal(percentile(sample_moves, Decimal('80')))} "
        f"normalized_edge_p80={fmt_decimal(percentile(normalized_edges, Decimal('80')))}"
    )
    for index, (score, asset, direction, reason) in enumerate(sorted(blocked_scores, reverse=True)[: args.top], start=1):
        print(f"blocked_candidate_{index}=asset={asset} dir={direction} score={fmt_decimal(score)} reason={reason}")

    recommendation = "no_change"
    if events["live_inventory_manual_review_required"] or status == "manual_review_required":
        recommendation = "manual_review_required_check_both_exchanges"
    elif status == "missing":
        recommendation = "state_missing_manual_exchange_flat_confirmation_required_before_start"
    elif status != "flat" or open_lots or pending_actions:
        recommendation = "do_not_start_new_live_state_not_flat"
    elif access_restricted:
        recommendation = "do_not_run_probe_access_restricted_detected"
    elif not processes:
        recommendation = "ok_to_start_live_after_manual_exchange_flat_confirmation"
    print(f"recommendation={recommendation}")

    runtime_tail = tail_text(RUNTIME_LOG, 5)
    if runtime_tail:
        print("== runtime_tail ==")
        for line in runtime_tail:
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
