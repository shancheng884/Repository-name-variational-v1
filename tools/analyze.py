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


def best_direction_metrics(row: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for direction, edge_key, normalized_key, roundtrip_key, stablecoin_key in (
        ("long_var_short_lighter", "long_edge_bps", "normalized_long_edge_bps", "long_roundtrip_pnl_bps", "long_stablecoin_filter_ok"),
        ("short_var_long_lighter", "short_edge_bps", "normalized_short_edge_bps", "short_roundtrip_pnl_bps", "short_stablecoin_filter_ok"),
    ):
        edge = to_decimal(row.get(edge_key))
        if edge is None:
            continue
        normalized = to_decimal(row.get(normalized_key))
        roundtrip = to_decimal(row.get(roundtrip_key))
        candidates.append(
            {
                "direction": direction,
                "edge": edge,
                "normalized_edge": normalized,
                "roundtrip": roundtrip,
                "stablecoin_ok": row.get(stablecoin_key),
            }
        )
    if not candidates:
        return None
    return max(candidates, key=lambda item: item["edge"])


def what_if_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("event") != "live_inventory_entry_blocked":
            continue
        metrics = best_direction_metrics(row)
        sample_move = abs(to_decimal(row.get("basis_sample_move_bps")) or Decimal("0"))
        if metrics is None:
            continue
        out.append(
            {
                **metrics,
                "asset": str(row.get("asset") or "-").upper(),
                "reason": str(row.get("reason") or "unknown"),
                "sample_move": sample_move,
                "basis_seen": row.get("basis_seen"),
            }
        )
    return out


def print_what_if(rows: list[dict[str, Any]]) -> None:
    samples = what_if_rows(rows)
    print("== what_if ==")
    print(f"blocked_samples={len(samples)}")
    if not samples:
        return

    min_abs_grid = [Decimal("13"), Decimal("12"), Decimal("11.5"), Decimal("11"), Decimal("10.5"), Decimal("10")]
    sample_move_grid = [Decimal("5"), Decimal("5.5"), Decimal("6"), Decimal("7")]
    min_norm = Decimal("1.0")
    min_roundtrip = Decimal("-3.0")

    for min_abs in min_abs_grid:
        for max_move in sample_move_grid:
            passed = [
                item
                for item in samples
                if abs(item["edge"]) >= min_abs
                and item["sample_move"] <= max_move
                and (item["normalized_edge"] is None or item["normalized_edge"] >= min_norm)
                and (item["roundtrip"] is None or item["roundtrip"] >= min_roundtrip)
                and item["stablecoin_ok"] is not False
            ]
            if not passed:
                print(f"candidate min_abs={min_abs} max_move={max_move} count=0")
                continue
            edges = [item["edge"] for item in passed]
            moves = [item["sample_move"] for item in passed]
            norms = [item["normalized_edge"] for item in passed if item["normalized_edge"] is not None]
            reasons = Counter(item["reason"] for item in passed)
            dirs = Counter(item["direction"] for item in passed)
            print(
                f"candidate min_abs={min_abs} max_move={max_move} count={len(passed)} "
                f"edge_p50={fmt_decimal(percentile(edges, Decimal('50')))} "
                f"edge_p80={fmt_decimal(percentile(edges, Decimal('80')))} "
                f"move_p80={fmt_decimal(percentile(moves, Decimal('80')))} "
                f"norm_p80={fmt_decimal(percentile(norms, Decimal('80')))} "
                f"dirs={dict(dirs.most_common())} reasons={dict(reasons.most_common(3))}"
            )

    best = sorted(
        samples,
        key=lambda item: (
            item["edge"] - (item["sample_move"] * Decimal("0.5")) + min(item["roundtrip"] or Decimal("0"), Decimal("0"))
        ),
        reverse=True,
    )[:5]
    for index, item in enumerate(best, start=1):
        print(
            f"what_if_top_{index}=asset={item['asset']} dir={item['direction']} "
            f"edge={fmt_decimal(item['edge'])} norm={fmt_decimal(item['normalized_edge'])} "
            f"roundtrip={fmt_decimal(item['roundtrip'])} move={fmt_decimal(item['sample_move'])} "
            f"stablecoin_ok={item['stablecoin_ok']} reason={item['reason']}"
        )


def _collect_decimal(rows: list[dict[str, Any]], key: str) -> list[Decimal]:
    values: list[Decimal] = []
    for row in rows:
        value = to_decimal(row.get(key))
        if value is not None:
            values.append(value)
    return values


def _best_edges(rows: list[dict[str, Any]], *, normalized: bool) -> list[Decimal]:
    values: list[Decimal] = []
    keys = (
        ("normalized_long_edge_bps", "normalized_short_edge_bps")
        if normalized
        else ("long_edge_bps", "short_edge_bps")
    )
    for row in rows:
        candidates = [to_decimal(row.get(key)) for key in keys]
        candidates = [value for value in candidates if value is not None]
        if candidates:
            values.append(max(candidates))
    return values


def _abs_values(values: list[Decimal]) -> list[Decimal]:
    return [abs(value) for value in values]


def print_basis_regime(rows: list[dict[str, Any]]) -> None:
    regimes = build_basis_regimes(rows)
    signal_rows = [
        row
        for row in rows
        if row.get("event") in {"live_inventory_basis_state", "live_inventory_entry_blocked"}
        and row.get("asset")
    ]
    print("== basis_regime ==")
    print(f"signal_rows={len(signal_rows)}")
    if not regimes:
        return

    for asset, item in sorted(regimes.items()):
        print(
            f"asset={asset} rows={item['rows']} blocked={item['blocked_count']} "
            f"latest_basis={fmt_decimal(item['latest_basis'])} latest_norm_basis={fmt_decimal(item['latest_norm_basis'])} "
            f"latest_best_raw={fmt_decimal(item['latest_best_raw'])} latest_best_norm={fmt_decimal(item['latest_best_norm'])} "
            f"latest_move={fmt_decimal(item['latest_move'])}"
        )
        print(
            f"asset={asset} basis_p10={fmt_decimal(item['basis_p10'])} "
            f"basis_p50={fmt_decimal(item['basis_p50'])} "
            f"basis_p90={fmt_decimal(item['basis_p90'])} "
            f"abs_basis_p90={fmt_decimal(item['abs_basis_p90'])} "
            f"norm_basis_p90={fmt_decimal(item['norm_basis_p90'])}"
        )
        print(
            f"asset={asset} raw_edge_p80={fmt_decimal(item['raw_edge_p80'])} "
            f"raw_edge_p95={fmt_decimal(item['raw_edge_p95'])} "
            f"norm_edge_p80={fmt_decimal(item['norm_edge_p80'])} "
            f"norm_edge_p95={fmt_decimal(item['norm_edge_p95'])} "
            f"sample_move_p80={fmt_decimal(item['sample_move_p80'])} "
            f"z_abs_p95={fmt_decimal(item['z_abs_p95'])}"
        )
        print(f"asset={asset} blocked_reasons={dict(item['blocked'].most_common(5))}")
        print(f"asset={asset} strategy_suggestion={item['suggestion']}")


def build_basis_regimes(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    signal_rows = [
        row
        for row in rows
        if row.get("event") in {"live_inventory_basis_state", "live_inventory_entry_blocked"}
        and row.get("asset")
    ]
    by_asset: dict[str, list[dict[str, Any]]] = {}
    for row in signal_rows:
        by_asset.setdefault(str(row.get("asset") or "-").upper(), []).append(row)

    regimes: dict[str, dict[str, Any]] = {}
    for asset, asset_rows in by_asset.items():
        basis = _collect_decimal(asset_rows, "basis_bps")
        norm_basis = _collect_decimal(asset_rows, "normalized_basis_bps")
        best_raw = _best_edges(asset_rows, normalized=False)
        best_norm = _best_edges(asset_rows, normalized=True)
        sample_moves = _abs_values(_collect_decimal(asset_rows, "basis_sample_move_bps"))
        z_values = _collect_decimal(asset_rows, "z")
        blocked = Counter(str(row.get("reason") or "-") for row in asset_rows if row.get("event") == "live_inventory_entry_blocked")
        latest = asset_rows[-1]
        latest_best_raw = max(
            [value for value in [to_decimal(latest.get("long_edge_bps")), to_decimal(latest.get("short_edge_bps"))] if value is not None],
            default=None,
        )
        latest_best_norm = max(
            [value for value in [to_decimal(latest.get("normalized_long_edge_bps")), to_decimal(latest.get("normalized_short_edge_bps"))] if value is not None],
            default=None,
        )
        raw_p95 = percentile(best_raw, Decimal("95"))
        norm_p80 = percentile(best_norm, Decimal("80"))
        norm_p95 = percentile(best_norm, Decimal("95"))
        move_p80 = percentile(sample_moves, Decimal("80"))
        if raw_p95 is None or len(asset_rows) < 1000:
            suggestion = "collect_more_data"
        elif raw_p95 < Decimal("10"):
            suggestion = "do_not_lower_threshold_market_edge_too_low"
        elif norm_p95 is not None and norm_p95 < Decimal("0.75"):
            suggestion = "wait_or_switch_asset_normalized_edge_weak"
        elif move_p80 is not None and move_p80 > Decimal("6"):
            suggestion = "avoid_looser_entries_sample_move_high"
        elif raw_p95 < Decimal("12"):
            suggestion = "test_small_only_min_abs_11_no_size_increase"
        else:
            suggestion = "current_threshold_reasonable_or_test_min_abs_12"
        regimes[asset] = {
            "rows": len(asset_rows),
            "blocked_count": sum(blocked.values()),
            "blocked": blocked,
            "latest_basis": to_decimal(latest.get("basis_bps")),
            "latest_norm_basis": to_decimal(latest.get("normalized_basis_bps")),
            "latest_best_raw": latest_best_raw,
            "latest_best_norm": latest_best_norm,
            "latest_move": abs(to_decimal(latest.get("basis_sample_move_bps")) or Decimal("0")),
            "basis_p10": percentile(basis, Decimal("10")),
            "basis_p50": percentile(basis, Decimal("50")),
            "basis_p90": percentile(basis, Decimal("90")),
            "abs_basis_p90": percentile(_abs_values(basis), Decimal("90")),
            "norm_basis_p90": percentile(norm_basis, Decimal("90")),
            "raw_edge_p80": percentile(best_raw, Decimal("80")),
            "raw_edge_p95": raw_p95,
            "norm_edge_p80": norm_p80,
            "norm_edge_p95": norm_p95,
            "sample_move_p80": move_p80,
            "z_abs_p95": percentile(_abs_values(z_values), Decimal("95")),
            "suggestion": suggestion,
        }
    return regimes


def print_strategy(rows: list[dict[str, Any]], events: Counter[str], status: str, open_lots: list[Any], pending_actions: list[Any], access_restricted: int) -> None:
    print("== strategy ==")
    regimes = build_basis_regimes(rows)
    if events["live_inventory_manual_review_required"] or status == "manual_review_required":
        print("STRATEGY action=STOP reason=manual_review_required_check_both_exchanges")
        return
    if status == "missing":
        print("STRATEGY action=WAIT reason=state_missing_confirm_exchanges_before_live")
        return
    if status != "flat" or open_lots or pending_actions:
        print("STRATEGY action=HOLD reason=state_not_flat_do_not_start_new_live")
        return
    if access_restricted:
        print("STRATEGY action=STOP reason=access_restricted_detected")
        return
    if not regimes:
        print("STRATEGY action=WAIT reason=collect_more_data")
        return

    ranked: list[tuple[Decimal, str, dict[str, Any]]] = []
    for asset, item in regimes.items():
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        score = raw + (norm * Decimal("2")) - max(move - Decimal("4"), Decimal("0"))
        ranked.append((score, asset, item))
    ranked.sort(reverse=True, key=lambda row: row[0])
    score, asset, item = ranked[0]
    print(
        f"STRATEGY best_asset={asset} score={fmt_decimal(score)} suggestion={item['suggestion']} "
        f"raw_p95={fmt_decimal(item['raw_edge_p95'])} norm_p95={fmt_decimal(item['norm_edge_p95'])} "
        f"move_p80={fmt_decimal(item['sample_move_p80'])} blocked={item['blocked_count']} rows={item['rows']}"
    )
    if item["suggestion"] == "current_threshold_reasonable_or_test_min_abs_12":
        print(f"STRATEGY action=CONTINUE_OR_TEST asset={asset} min_abs=12 size=20u reason=good_recent_edge_quality")
    elif item["suggestion"] == "test_small_only_min_abs_11_no_size_increase":
        print(f"STRATEGY action=TEST_SMALL asset={asset} min_abs=11 size=20u reason=edge_exists_but_not_enough_for_size_increase")
    elif item["suggestion"] == "avoid_looser_entries_sample_move_high":
        print(f"STRATEGY action=WAIT_OR_SWITCH asset={asset} reason=sample_move_high_do_not_loosen")
    elif item["suggestion"] == "wait_or_switch_asset_normalized_edge_weak":
        print(f"STRATEGY action=WAIT_OR_SWITCH asset={asset} reason=normalized_edge_weak_do_not_lower_threshold")
    elif item["suggestion"] == "do_not_lower_threshold_market_edge_too_low":
        print(f"STRATEGY action=WAIT_OR_SWITCH asset={asset} reason=raw_edge_too_low")
    else:
        print(f"STRATEGY action=WAIT asset={asset} reason=need_more_data")


def print_config_advice(rows: list[dict[str, Any]]) -> None:
    print("== config_advice ==")
    regimes = build_basis_regimes(rows)
    if not regimes:
        print("CONFIG_ADVICE action=NONE reason=collect_more_data")
        return

    ranked: list[tuple[Decimal, str, dict[str, Any]]] = []
    for asset, item in regimes.items():
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        score = raw + (norm * Decimal("2")) - max(move - Decimal("4"), Decimal("0"))
        ranked.append((score, asset, item))
    ranked.sort(reverse=True, key=lambda row: row[0])
    _, asset, item = ranked[0]

    raw_p95 = item["raw_edge_p95"] or Decimal("0")
    norm_p95 = item["norm_edge_p95"] or Decimal("0")
    move_p80 = item["sample_move_p80"] or Decimal("99")
    suggestion = item["suggestion"]

    if suggestion in {"wait_or_switch_asset_normalized_edge_weak", "do_not_lower_threshold_market_edge_too_low"}:
        print(
            f"CONFIG_ADVICE action=KEEP asset={asset} min_abs=current size=current "
            f"reason={suggestion} raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)} move_p80={fmt_decimal(move_p80)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=edge_quality_not_size_limited")
        return
    if suggestion == "avoid_looser_entries_sample_move_high":
        print(
            f"CONFIG_ADVICE action=KEEP_OR_SWITCH asset={asset} min_abs=current size=current "
            f"reason=sample_move_high move_p80={fmt_decimal(move_p80)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=sample_move_high")
        return
    if suggestion == "test_small_only_min_abs_11_no_size_increase":
        print(
            f"CONFIG_ADVICE action=SET asset={asset} min_entry_edge_bps=11 min_abs_entry_bps=11 "
            f"entry_confirm_samples=1 max_sample_move_bps=5 lot_notional_usd=20 live_max_notional_usd=25 "
            f"reason=some_edge_but_not_enough_for_larger_size raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=needs_successful_actual_pnl_first")
        return
    if suggestion == "current_threshold_reasonable_or_test_min_abs_12":
        if norm_p95 >= Decimal("1.5") and move_p80 <= Decimal("4") and raw_p95 >= Decimal("13"):
            print(
                f"CONFIG_ADVICE action=OPTIONAL_SIZE_TEST asset={asset} min_entry_edge_bps=12 min_abs_entry_bps=12 "
                f"entry_confirm_samples=1 lot_notional_usd=30 live_max_notional_usd=35 "
                f"reason=strong_edge_quality raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)} move_p80={fmt_decimal(move_p80)}"
            )
            return
        print(
            f"CONFIG_ADVICE action=SET asset={asset} min_entry_edge_bps=12 min_abs_entry_bps=12 "
            f"entry_confirm_samples=1 lot_notional_usd=20 live_max_notional_usd=25 "
            f"reason=reasonable_edge_but_size_not_justified raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)} move_p80={fmt_decimal(move_p80)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=need_actual_pnl_before_30u")
        return
    print(f"CONFIG_ADVICE action=NONE asset={asset} reason={suggestion}")


def print_asset_scores(rows: list[dict[str, Any]]) -> None:
    print("== asset_scores ==")
    regimes = build_basis_regimes(rows)
    if not regimes:
        print("ASSET_SCORE action=WAIT reason=collect_more_data")
        return

    scored: list[tuple[Decimal, str, dict[str, Any], str]] = []
    for asset, item in regimes.items():
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        rows_count = Decimal(str(item["rows"]))
        data_penalty = Decimal("0") if rows_count >= Decimal("1000") else Decimal("5")
        move_penalty = max(move - Decimal("4"), Decimal("0"))
        score = raw + (norm * Decimal("2")) - move_penalty - data_penalty
        if item["suggestion"] in {"wait_or_switch_asset_normalized_edge_weak", "do_not_lower_threshold_market_edge_too_low"}:
            action = "WAIT"
        elif item["suggestion"] == "avoid_looser_entries_sample_move_high":
            action = "AVOID_LOOSENING"
        elif item["suggestion"] == "test_small_only_min_abs_11_no_size_increase":
            action = "TEST_SMALL"
        elif item["suggestion"] == "current_threshold_reasonable_or_test_min_abs_12":
            action = "TEST_OR_CONTINUE"
        else:
            action = "COLLECT_MORE"
        scored.append((score, asset, item, action))

    scored.sort(reverse=True, key=lambda row: row[0])
    for rank, (score, asset, item, action) in enumerate(scored, start=1):
        print(
            f"ASSET_SCORE rank={rank} asset={asset} score={fmt_decimal(score)} action={action} "
            f"raw_p95={fmt_decimal(item['raw_edge_p95'])} norm_p95={fmt_decimal(item['norm_edge_p95'])} "
            f"move_p80={fmt_decimal(item['sample_move_p80'])} rows={item['rows']} blocked={item['blocked_count']} "
            f"suggestion={item['suggestion']}"
        )

    best_score, best_asset, best_item, best_action = scored[0]
    if best_action in {"TEST_SMALL", "TEST_OR_CONTINUE"}:
        print(
            f"ASSET_RECOMMENDATION asset={best_asset} action={best_action} size=20u "
            f"reason=best_current_quality score={fmt_decimal(best_score)}"
        )
    else:
        print(
            f"ASSET_RECOMMENDATION asset={best_asset} action=WAIT_OR_ROTATE "
            f"reason={best_item['suggestion']} score={fmt_decimal(best_score)}"
        )
    if len(scored) == 1:
        print("ASSET_RECOMMENDATION note=single_asset_log_use_all_runs_after_rotating_assets_for_comparison")


def print_run_scores(rows: list[dict[str, Any]]) -> None:
    print("== run_scores ==")
    by_run: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        run_id = str(row.get("run_id") or "")
        if not run_id:
            continue
        by_run.setdefault(run_id, []).append(row)
    scored: list[tuple[Decimal, str, str, dict[str, Any], str]] = []
    for run_id, run_rows in by_run.items():
        regimes = build_basis_regimes(run_rows)
        if not regimes:
            continue
        asset, item = max(regimes.items(), key=lambda pair: pair[1]["rows"])
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        rows_count = Decimal(str(item["rows"]))
        data_penalty = Decimal("0") if rows_count >= Decimal("1000") else Decimal("5")
        score = raw + (norm * Decimal("2")) - max(move - Decimal("4"), Decimal("0")) - data_penalty
        suggestion = str(item["suggestion"])
        scored.append((score, run_id, asset, item, suggestion))
    if not scored:
        print("RUN_SCORE action=WAIT reason=no_run_data")
        return
    scored.sort(reverse=True, key=lambda row: row[0])
    for rank, (score, run_id, asset, item, suggestion) in enumerate(scored, start=1):
        print(
            f"RUN_SCORE rank={rank} run_id={run_id} asset={asset} score={fmt_decimal(score)} "
            f"suggestion={suggestion} raw_p95={fmt_decimal(item['raw_edge_p95'])} "
            f"norm_p95={fmt_decimal(item['norm_edge_p95'])} move_p80={fmt_decimal(item['sample_move_p80'])} "
            f"rows={item['rows']} blocked={item['blocked_count']}"
        )
    best_score, best_run_id, best_asset, best_item, best_suggestion = scored[0]
    if best_suggestion in {"current_threshold_reasonable_or_test_min_abs_12", "test_small_only_min_abs_11_no_size_increase"}:
        action = "TEST_SMALL"
    else:
        action = "WAIT_OR_ROTATE"
    print(
        f"RUN_RECOMMENDATION asset={best_asset} run_id={best_run_id} action={action} "
        f"reason={best_suggestion} score={fmt_decimal(best_score)}"
    )


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
    parser.add_argument("--what-if", action="store_true", help="Replay blocked entries against looser threshold grids.")
    parser.add_argument("--basis-regime", action="store_true", help="Summarize recent basis/edge percentiles for strategy tuning.")
    parser.add_argument("--strategy", action="store_true", help="Print a concise live strategy recommendation.")
    parser.add_argument("--config-advice", action="store_true", help="Print recommended live_config.json changes, if any.")
    parser.add_argument("--asset-scores", action="store_true", help="Score assets seen in the selected log rows for rotation decisions.")
    parser.add_argument("--run-scores", action="store_true", help="Score each run_id separately for asset rotation decisions.")
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
    if args.what_if:
        print_what_if(rows)
    if args.basis_regime:
        print_basis_regime(rows)
    if args.strategy:
        print_strategy(rows, events, status, open_lots, pending_actions, access_restricted)
    if args.config_advice:
        print_config_advice(rows)
    if args.asset_scores:
        print_asset_scores(rows)
    if args.run_scores:
        print_run_scores(rows)

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
