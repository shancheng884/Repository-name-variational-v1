#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def fmt(value: Decimal | None, places: str = "0.01") -> str:
    if value is None:
        return "-"
    return format(value.quantize(Decimal(places)), "f")


def percentile(values: list[Decimal], pct: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int((Decimal(len(ordered) - 1) * pct / Decimal("100")).to_integral_value(rounding="ROUND_HALF_UP"))
    return ordered[max(0, min(index, len(ordered) - 1))]


def avg(values: list[Decimal]) -> Decimal | None:
    return sum(values) / Decimal(len(values)) if values else None


def tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return []
    if limit > 0:
        return rows[-limit:]
    return rows


def read_state(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def filter_rows(rows: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    filtered = rows
    if args.since_minutes is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=args.since_minutes)
        filtered = [row for row in filtered if (parse_time(row.get("logged_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff]
    if args.latest_run_only:
        latest_run_id = ""
        for row in reversed(filtered):
            run_id = str(row.get("run_id") or "")
            if run_id:
                latest_run_id = run_id
                break
        if latest_run_id:
            filtered = [row for row in filtered if str(row.get("run_id") or "") == latest_run_id]
    return filtered


def depth_slippage(row: dict[str, Any], key: str) -> Decimal | None:
    value = row.get(key)
    if not isinstance(value, dict):
        return None
    return to_decimal(value.get("slippage_bps"))


@dataclass
class MissedCandidate:
    score: Decimal
    asset: str
    direction: str
    reason: str
    logged_at: str
    raw_edge: Decimal | None
    normalized_edge: Decimal | None
    sample_move: Decimal | None
    roundtrip: Decimal | None
    entry_slippage: Decimal | None
    exit_slippage: Decimal | None


def best_direction(row: dict[str, Any]) -> tuple[str, Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    long_norm = to_decimal(row.get("normalized_long_edge_bps"))
    short_norm = to_decimal(row.get("normalized_short_edge_bps"))
    long_raw = to_decimal(row.get("long_edge_bps"))
    short_raw = to_decimal(row.get("short_edge_bps"))
    long_score = long_norm if long_norm is not None else long_raw
    short_score = short_norm if short_norm is not None else short_raw
    if long_score is not None and (short_score is None or long_score >= short_score):
        return (
            "long_var_short_lighter",
            long_raw,
            long_norm,
            to_decimal(row.get("long_roundtrip_pnl_bps")),
            depth_slippage(row, "long_entry_lighter_depth"),
            depth_slippage(row, "long_exit_lighter_depth"),
        )
    return (
        "short_var_long_lighter",
        short_raw,
        short_norm,
        to_decimal(row.get("short_roundtrip_pnl_bps")),
        depth_slippage(row, "short_entry_lighter_depth"),
        depth_slippage(row, "short_exit_lighter_depth"),
    )


def candidate_score(row: dict[str, Any], shortfall_buffer: Decimal, sample_move_penalty: Decimal) -> MissedCandidate | None:
    direction, raw_edge, normalized_edge, roundtrip, entry_slippage, exit_slippage = best_direction(row)
    edge = normalized_edge if normalized_edge is not None else raw_edge
    if edge is None:
        return None
    sample_move = to_decimal(row.get("basis_sample_move_bps"))
    move_abs = abs(sample_move) if sample_move is not None else Decimal("0")
    score = edge + min(roundtrip or Decimal("0"), Decimal("0")) - shortfall_buffer - (move_abs * sample_move_penalty)
    return MissedCandidate(
        score=score,
        asset=str(row.get("asset") or "-").upper(),
        direction=direction,
        reason=str(row.get("reason") or "-"),
        logged_at=str(row.get("logged_at") or "-"),
        raw_edge=raw_edge,
        normalized_edge=normalized_edge,
        sample_move=move_abs,
        roundtrip=roundtrip,
        entry_slippage=entry_slippage,
        exit_slippage=exit_slippage,
    )


def summarize(args: argparse.Namespace) -> int:
    raw_rows = tail_jsonl(args.file, args.tail)
    rows = filter_rows(raw_rows, args)
    state = read_state(args.state_file)

    events = Counter(str(row.get("event") or "-") for row in rows)
    blocked_reasons: Counter[str] = Counter()
    assets: Counter[str] = Counter()
    actual_pnl_bps: list[Decimal] = []
    actual_pnl_usd: list[Decimal] = []
    shortfalls: list[Decimal] = []
    exit_slippage: list[Decimal] = []
    entry_slippage: list[Decimal] = []
    sample_moves: list[Decimal] = []
    normalized_edges: list[Decimal] = []
    candidates: list[MissedCandidate] = []

    shortfall_buffer = Decimal(str(args.shortfall_buffer_bps))
    sample_move_penalty = Decimal(str(args.sample_move_penalty))

    for row in rows:
        asset = str(row.get("asset") or "-").upper()
        if asset != "-":
            assets[asset] += 1
        event = row.get("event")
        if event == "live_inventory_entry_blocked":
            blocked_reasons[str(row.get("reason") or "unknown")] += 1
            candidate = candidate_score(row, shortfall_buffer, sample_move_penalty)
            if candidate is not None:
                candidates.append(candidate)
            move = to_decimal(row.get("basis_sample_move_bps"))
            if move is not None:
                sample_moves.append(abs(move))
            for key in ("normalized_long_edge_bps", "normalized_short_edge_bps"):
                value = to_decimal(row.get(key))
                if value is not None:
                    normalized_edges.append(value)
        elif event == "live_inventory_actual_pnl":
            value = to_decimal(row.get("actual_pnl_bps"))
            if value is not None:
                actual_pnl_bps.append(value)
            value = to_decimal(row.get("actual_pnl_usd"))
            if value is not None:
                actual_pnl_usd.append(value)
            value = to_decimal(row.get("estimated_vs_actual_pnl_shortfall_bps"))
            if value is not None:
                shortfalls.append(value)
            value = to_decimal(row.get("exit_lighter_slippage_bps"))
            if value is not None:
                exit_slippage.append(value)
            value = to_decimal(row.get("entry_lighter_slippage_bps"))
            if value is not None:
                entry_slippage.append(value)

    top_candidates = sorted(candidates, key=lambda item: item.score, reverse=True)[: args.top_missed]
    best = top_candidates[0] if top_candidates else None
    state_status = state.get("status") or "unknown"
    state_asset = state.get("asset") or "-"
    completed_cycles = state.get("completed_cycles") if state else "-"
    open_lots = state.get("open_lots") if state else "-"

    latest_run_id = "-"
    for row in reversed(rows):
        if row.get("run_id"):
            latest_run_id = str(row.get("run_id"))
            break
    dominant_asset = assets.most_common(1)[0][0] if assets else "-"
    stale_warning = "-"
    if state_status == "flat" and state_asset not in {"-", dominant_asset} and dominant_asset != "-":
        stale_warning = "state_asset_stale_or_flat_reset_artifact"

    print(
        f"state status={state_status} asset={state_asset} completed_cycles={completed_cycles} open_lots={open_lots} "
        f"latest_run_id={latest_run_id} rows={len(rows)}/{len(raw_rows)} warning={stale_warning}"
    )
    print(
        f"events entered={events['live_inventory_entered']} exited={events['live_inventory_exited']} "
        f"actual_pnl={events['live_inventory_actual_pnl']} manual_review={events['live_inventory_manual_review_required']} "
        f"blocked={events['live_inventory_entry_blocked']}"
    )
    print(f"assets={dict(assets.most_common())}")
    print(f"blocked_reasons={dict(blocked_reasons.most_common(8))}")
    print(
        "actual_pnl "
        f"n={len(actual_pnl_bps)} avg_bps={fmt(avg(actual_pnl_bps))} p50_bps={fmt(percentile(actual_pnl_bps, Decimal('50')))} "
        f"sum_usd={fmt(sum(actual_pnl_usd) if actual_pnl_usd else None, '0.0001')}"
    )
    print(
        "shortfall "
        f"n={len(shortfalls)} avg={fmt(avg(shortfalls))} p80={fmt(percentile(shortfalls, Decimal('80')))} "
        f"exit_slip_p80={fmt(percentile(exit_slippage, Decimal('80')))} entry_slip_p80={fmt(percentile(entry_slippage, Decimal('80')))}"
    )
    print(
        "blocked_quality "
        f"sample_move_p50={fmt(percentile(sample_moves, Decimal('50')))} sample_move_p80={fmt(percentile(sample_moves, Decimal('80')))} "
        f"norm_edge_p80={fmt(percentile(normalized_edges, Decimal('80')))}"
    )
    if best is None:
        print("best_missed=-")
    else:
        print(
            "best_missed "
            f"asset={best.asset} dir={best.direction} score={fmt(best.score)} reason={best.reason} "
            f"norm={fmt(best.normalized_edge)} raw={fmt(best.raw_edge)} roundtrip={fmt(best.roundtrip)} "
            f"move={fmt(best.sample_move)} entry_slip={fmt(best.entry_slippage)} exit_slip={fmt(best.exit_slippage)} "
            f"at={best.logged_at}"
        )
        print("top_missed:")
        for index, item in enumerate(top_candidates, start=1):
            print(
                f"  {index}. asset={item.asset} dir={item.direction} score={fmt(item.score)} reason={item.reason} "
                f"norm={fmt(item.normalized_edge)} raw={fmt(item.raw_edge)} roundtrip={fmt(item.roundtrip)} "
                f"move={fmt(item.sample_move)} at={item.logged_at}"
            )

    recommendation = "no_change"
    if events["live_inventory_manual_review_required"]:
        recommendation = "manual_review_required_check_exchange_positions"
    elif state_status != "flat":
        recommendation = "do_not_restart_state_not_flat"
    elif events["live_inventory_entered"] == 0 and events["live_inventory_entry_blocked"] >= args.min_blocked_for_stop:
        sample_move_p80 = percentile(sample_moves, Decimal("80"))
        norm_edge_p80 = percentile(normalized_edges, Decimal("80"))
        if sample_move_p80 is not None and sample_move_p80 > Decimal("5"):
            recommendation = "stop_or_wait_better_market_do_not_relax_sample_move"
        elif norm_edge_p80 is not None and Decimal("0.5") <= norm_edge_p80 < Decimal("1.0"):
            recommendation = "consider_lower_normalized_edge_to_0.5_next_run_only"
        else:
            recommendation = "keep_waiting_or_stop_no_clean_entry"
    elif actual_pnl_bps and (avg(actual_pnl_bps) or Decimal("0")) < Decimal("0"):
        recommendation = "stop_review_actual_pnl_negative"
    print(f"recommendation={recommendation}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize recent live inventory logs.")
    parser.add_argument("--file", type=Path, default=Path("log/order_metrics.jsonl"))
    parser.add_argument("--state-file", type=Path, default=Path("log/live_inventory_state.json"))
    parser.add_argument("--tail", type=int, default=12000)
    parser.add_argument("--latest-run-only", action="store_true")
    parser.add_argument("--since-minutes", type=float, default=None)
    parser.add_argument("--shortfall-buffer-bps", type=float, default=5.5)
    parser.add_argument("--sample-move-penalty", type=float, default=0.5)
    parser.add_argument("--min-blocked-for-stop", type=int, default=20)
    parser.add_argument("--top-missed", type=int, default=3)
    args = parser.parse_args()
    if args.tail <= 0:
        parser.error("--tail must be > 0")
    if args.since_minutes is not None and args.since_minutes <= 0:
        parser.error("--since-minutes must be > 0")
    if args.top_missed <= 0:
        parser.error("--top-missed must be > 0")
    return summarize(args)


if __name__ == "__main__":
    raise SystemExit(main())
