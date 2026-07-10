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
    rotated_jsonl_paths,
    tail_jsonl,
    tail_jsonl_many,
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


def basis_state_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    states = [row for row in rows if row.get("event") == "live_inventory_basis_state" and row.get("asset")]
    if states:
        return states
    # Compatibility fallback for older logs that did not emit a state row per sample.
    return [row for row in rows if row.get("event") == "live_inventory_entry_blocked" and row.get("asset")]


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


def _entry_semantics_values(row: dict[str, Any], direction: str) -> tuple[Decimal | None, Decimal | None, Decimal | None, bool]:
    if direction == "long_var_short_lighter":
        raw_edge = to_decimal(row.get("long_edge_bps"))
        normalized_edge = to_decimal(row.get("normalized_long_edge_bps"))
        stablecoin_ok = row.get("long_stablecoin_filter_ok") is not False
    else:
        raw_edge = to_decimal(row.get("short_edge_bps"))
        normalized_edge = to_decimal(row.get("normalized_short_edge_bps"))
        stablecoin_ok = row.get("short_stablecoin_filter_ok") is not False
    return to_decimal(row.get("basis_bps")), raw_edge, normalized_edge, stablecoin_ok


def _entry_semantics_gate(
    row: dict[str, Any],
    *,
    direction: str,
    primary: str,
    primary_threshold: Decimal,
    min_abs_entry: Decimal,
    min_normalized_filter: Decimal,
) -> bool:
    basis, raw_edge, normalized_edge, stablecoin_ok = _entry_semantics_values(row, direction)
    if basis is None or raw_edge is None or normalized_edge is None:
        return False
    if row.get("basis_sample_move_ok") is False or not stablecoin_ok:
        return False
    if not _direction_abs_entry_ok(direction, basis, min_abs_entry):
        return False
    if normalized_edge < min_normalized_filter:
        return False
    edge = normalized_edge if primary == "normalized" else raw_edge
    return edge >= primary_threshold


def _entry_semantics_forward_pnl_bps(entry: dict[str, Any], future: dict[str, Any], direction: str) -> Decimal | None:
    if direction == "long_var_short_lighter":
        entry_var = to_decimal(entry.get("var_ask"))
        entry_lighter = to_decimal(entry.get("lighter_sell_price"))
        exit_var = to_decimal(future.get("var_bid"))
        exit_lighter = to_decimal(future.get("lighter_buy_price"))
        if entry_var is None or entry_lighter is None or exit_var is None or exit_lighter is None or entry_var <= 0:
            return None
        pnl_per_unit = (exit_var - entry_var) + (entry_lighter - exit_lighter)
    else:
        entry_var = to_decimal(entry.get("var_bid"))
        entry_lighter = to_decimal(entry.get("lighter_buy_price"))
        exit_var = to_decimal(future.get("var_ask"))
        exit_lighter = to_decimal(future.get("lighter_sell_price"))
        if entry_var is None or entry_lighter is None or exit_var is None or exit_lighter is None or entry_var <= 0:
            return None
        pnl_per_unit = (entry_var - exit_var) + (exit_lighter - entry_lighter)
    return pnl_per_unit / entry_var * Decimal("10000")


def build_entry_semantics(
    rows: list[dict[str, Any]],
    *,
    primary_threshold: Decimal,
    min_abs_entry: Decimal,
    min_normalized_filter: Decimal,
    horizons: tuple[int, ...] = (1, 3, 5, 10),
) -> dict[str, dict[str, Any]]:
    by_run_asset: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in basis_state_rows(rows):
        asset = str(row.get("asset") or "-").upper()
        run_id = str(row.get("run_id") or "legacy")
        by_run_asset.setdefault((run_id, asset), []).append(row)

    results: dict[str, dict[str, Any]] = {}
    for (_run_id, asset), run_rows in by_run_asset.items():
        run_rows.sort(key=lambda row: str(row.get("logged_at") or ""))
        item = results.setdefault(
            asset,
            {
                "rows": 0,
                "normalized_primary_candidates": 0,
                "raw_primary_candidates": 0,
                "forward": {horizon: {"attempts": 0, "retained": 0, "raw_edge_deltas": [], "pnl_bps": []} for horizon in horizons},
            },
        )
        item["rows"] += len(run_rows)
        for index, row in enumerate(run_rows):
            for direction in ("long_var_short_lighter", "short_var_long_lighter"):
                normalized_candidate = _entry_semantics_gate(
                    row,
                    direction=direction,
                    primary="normalized",
                    primary_threshold=primary_threshold,
                    min_abs_entry=min_abs_entry,
                    min_normalized_filter=min_normalized_filter,
                )
                raw_candidate = _entry_semantics_gate(
                    row,
                    direction=direction,
                    primary="raw",
                    primary_threshold=primary_threshold,
                    min_abs_entry=min_abs_entry,
                    min_normalized_filter=min_normalized_filter,
                )
                if normalized_candidate:
                    item["normalized_primary_candidates"] += 1
                if not raw_candidate:
                    continue
                item["raw_primary_candidates"] += 1
                _, entry_raw_edge, _, _ = _entry_semantics_values(row, direction)
                for horizon in horizons:
                    future_index = index + horizon
                    if future_index >= len(run_rows):
                        continue
                    future = run_rows[future_index]
                    forward = item["forward"][horizon]
                    forward["attempts"] += 1
                    if _entry_semantics_gate(
                        future,
                        direction=direction,
                        primary="raw",
                        primary_threshold=primary_threshold,
                        min_abs_entry=min_abs_entry,
                        min_normalized_filter=min_normalized_filter,
                    ):
                        forward["retained"] += 1
                    _, future_raw_edge, _, _ = _entry_semantics_values(future, direction)
                    if entry_raw_edge is not None and future_raw_edge is not None:
                        forward["raw_edge_deltas"].append(future_raw_edge - entry_raw_edge)
                    if (pnl_bps := _entry_semantics_forward_pnl_bps(row, future, direction)) is not None:
                        forward["pnl_bps"].append(pnl_bps)
    return results


def print_entry_semantics(
    rows: list[dict[str, Any]],
    *,
    primary_threshold: Decimal,
    min_abs_entry: Decimal,
    min_normalized_filter: Decimal,
) -> None:
    print("== entry_semantics ==")
    print(
        "model=static_proxy_forward_pnl_uses_logged_executable_prices_excludes_latency_fees_and_actual_fills "
        f"primary_threshold={fmt_decimal(primary_threshold)} min_abs_entry={fmt_decimal(min_abs_entry)} "
        f"min_normalized_filter={fmt_decimal(min_normalized_filter)}"
    )
    results = build_entry_semantics(
        rows,
        primary_threshold=primary_threshold,
        min_abs_entry=min_abs_entry,
        min_normalized_filter=min_normalized_filter,
    )
    if not results:
        print("ENTRY_SEMANTICS action=WAIT reason=no_basis_state_rows")
        return
    for asset, item in sorted(results.items()):
        print(
            f"asset={asset} rows={item['rows']} normalized_primary_candidates={item['normalized_primary_candidates']} "
            f"raw_primary_normalized_filter_candidates={item['raw_primary_candidates']}"
        )
        for horizon, forward in item["forward"].items():
            attempts = forward["attempts"]
            retained = forward["retained"]
            retained_pct = Decimal(retained) / Decimal(attempts) * Decimal("100") if attempts else None
            pnl_values = forward["pnl_bps"]
            positive_pct = (
                Decimal(sum(value > 0 for value in pnl_values)) / Decimal(len(pnl_values)) * Decimal("100")
                if pnl_values
                else None
            )
            print(
                f"asset={asset} raw_primary_forward_samples={horizon} attempts={attempts} retained={retained} "
                f"retained_pct={fmt_decimal(retained_pct)} raw_edge_delta_p50={fmt_decimal(percentile(forward['raw_edge_deltas'], Decimal('50')))} "
                f"pnl_proxy_p50={fmt_decimal(percentile(pnl_values, Decimal('50')))} "
                f"pnl_proxy_p80={fmt_decimal(percentile(pnl_values, Decimal('80')))} positive_pct={fmt_decimal(positive_pct)}"
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


def _spread_bps_from_bid_ask(row: dict[str, Any], bid_key: str, ask_key: str) -> Decimal | None:
    bid = to_decimal(row.get(bid_key))
    ask = to_decimal(row.get(ask_key))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    if ask < bid:
        bid, ask = ask, bid
    mid = (bid + ask) / Decimal("2")
    if mid <= 0:
        return None
    return (ask - bid) / mid * Decimal("10000")


def _spread_regime(current: Decimal | None, values: list[Decimal]) -> tuple[str, Decimal]:
    if current is None or not values:
        return "unknown", Decimal("0")
    median = percentile(values, Decimal("50"))
    p80 = percentile(values, Decimal("80"))
    p95 = percentile(values, Decimal("95"))
    if median is None or p80 is None or p95 is None:
        return "unknown", Decimal("0")
    if current > p95:
        return "extreme_filter", current - median
    if current > p80:
        return "wide_add_buffer", current - median
    return "normal", Decimal("0")


def _direction_abs_entry_ok(direction: str, basis: Decimal, threshold: Decimal) -> bool:
    if direction == "long_var_short_lighter":
        return basis <= -threshold
    if direction == "short_var_long_lighter":
        return basis >= threshold
    return False


def _direction_basis_pnl_bps(direction: str, entry_basis: Decimal, current_basis: Decimal) -> Decimal:
    if direction == "long_var_short_lighter":
        return current_basis - entry_basis
    return entry_basis - current_basis


def dynamic_cost_summary(rows: list[dict[str, Any]], *, asset_filter: str | None = None) -> dict[str, Any]:
    signal_rows = [
        row
        for row in basis_state_rows(rows)
        if row.get("asset")
        and (asset_filter is None or str(row.get("asset") or "").upper() == asset_filter.upper())
    ]
    var_spreads: list[Decimal] = []
    lighter_spreads: list[Decimal] = []
    latest: dict[str, Any] | None = None
    latest_var_spread = None
    latest_lighter_spread = None
    for row in signal_rows:
        var_spread = _spread_bps_from_bid_ask(row, "var_bid", "var_ask")
        lighter_spread = _spread_bps_from_bid_ask(row, "lighter_bid", "lighter_ask")
        if var_spread is not None:
            var_spreads.append(var_spread)
        if lighter_spread is not None:
            lighter_spreads.append(lighter_spread)
        if var_spread is not None or lighter_spread is not None:
            latest = row
            latest_var_spread = var_spread
            latest_lighter_spread = lighter_spread

    slippage_rows = [row for row in rows if row.get("event") == "live_inventory_actual_pnl"]
    entry_slippage = _collect_decimal(slippage_rows, "entry_lighter_slippage_bps")
    exit_slippage = _collect_decimal(slippage_rows, "exit_lighter_slippage_bps")
    slippage_pool = [value for value in entry_slippage + exit_slippage if value >= 0]
    expected_slippage = percentile(slippage_pool, Decimal("80")) or Decimal("0")
    var_regime, var_penalty = _spread_regime(latest_var_spread, var_spreads)
    lighter_regime, lighter_penalty = _spread_regime(latest_lighter_spread, lighter_spreads)
    current_var_cost = latest_var_spread or Decimal("0")
    current_lighter_cost = latest_lighter_spread or Decimal("0")
    dynamic_cost = current_var_cost + current_lighter_cost + expected_slippage + var_penalty + lighter_penalty
    return {
        "rows": len(signal_rows),
        "asset": str(latest.get("asset") or asset_filter or "-").upper() if latest else (asset_filter or "-").upper(),
        "latest_var_spread": latest_var_spread,
        "latest_lighter_spread": latest_lighter_spread,
        "var_spread_p50": percentile(var_spreads, Decimal("50")),
        "var_spread_p80": percentile(var_spreads, Decimal("80")),
        "var_spread_p95": percentile(var_spreads, Decimal("95")),
        "lighter_spread_p50": percentile(lighter_spreads, Decimal("50")),
        "lighter_spread_p80": percentile(lighter_spreads, Decimal("80")),
        "lighter_spread_p95": percentile(lighter_spreads, Decimal("95")),
        "expected_slippage": expected_slippage,
        "var_regime": var_regime,
        "lighter_regime": lighter_regime,
        "spread_regime_penalty": var_penalty + lighter_penalty,
        "dynamic_roundtrip_cost": dynamic_cost,
    }


def print_dynamic_cost(rows: list[dict[str, Any]], *, asset: str | None = None) -> None:
    item = dynamic_cost_summary(rows, asset_filter=asset)
    print("== dynamic_cost ==")
    print(
        f"asset={item['asset']} rows={item['rows']} "
        f"current_var_spread={fmt_decimal(item['latest_var_spread'])} "
        f"current_lighter_spread={fmt_decimal(item['latest_lighter_spread'])} "
        f"expected_slippage_p80={fmt_decimal(item['expected_slippage'])} "
        f"spread_penalty={fmt_decimal(item['spread_regime_penalty'])} "
        f"dynamic_roundtrip_cost={fmt_decimal(item['dynamic_roundtrip_cost'])}"
    )
    print(
        f"var_spread p50={fmt_decimal(item['var_spread_p50'])} p80={fmt_decimal(item['var_spread_p80'])} p95={fmt_decimal(item['var_spread_p95'])} "
        f"regime={item['var_regime']}"
    )
    print(
        f"lighter_spread p50={fmt_decimal(item['lighter_spread_p50'])} p80={fmt_decimal(item['lighter_spread_p80'])} p95={fmt_decimal(item['lighter_spread_p95'])} "
        f"regime={item['lighter_regime']}"
    )


def print_ladder_what_if(
    rows: list[dict[str, Any]],
    *,
    asset: str | None,
    lot_notional: Decimal,
    max_lots: int,
    addon_step: Decimal,
    min_entry_edge: Decimal,
    min_abs_entry: Decimal,
    min_norm_entry: Decimal,
    min_exit_pnl: Decimal,
    profit_take_pnl: Decimal,
) -> None:
    print("== ladder_what_if ==")
    print("model=rough_latest_cost_not_execution_replay")
    cost = dynamic_cost_summary(rows, asset_filter=asset)
    dynamic_cost = cost["dynamic_roundtrip_cost"] or Decimal("0")
    signal_rows = [
        row
        for row in basis_state_rows(rows)
        if row.get("asset")
        and (asset is None or str(row.get("asset") or "").upper() == asset.upper())
    ]
    open_lots: list[dict[str, Any]] = []
    entries = 0
    exits = 0
    blocked_cost = 0
    portfolio_pnls: list[Decimal] = []
    for row in signal_rows:
        basis = to_decimal(row.get("basis_bps"))
        if basis is None:
            continue
        if open_lots:
            direction = str(open_lots[0]["direction"])
            lot_pnls = [_direction_basis_pnl_bps(direction, lot["entry_basis"], basis) for lot in open_lots]
            gross_portfolio_pnl = sum(lot_pnls) / Decimal(len(lot_pnls))
            executable_pnl = gross_portfolio_pnl - dynamic_cost
            portfolio_pnls.append(executable_pnl)
            exit_target = profit_take_pnl if executable_pnl >= profit_take_pnl else min_exit_pnl
            if executable_pnl >= exit_target:
                exits += len(open_lots)
                open_lots.clear()
                continue

        metrics = best_direction_metrics(row)
        if metrics is None:
            continue
        direction = str(metrics["direction"])
        edge = metrics["edge"]
        normalized = metrics["normalized_edge"]
        roundtrip = metrics["roundtrip"]
        stablecoin_ok = metrics["stablecoin_ok"]
        if open_lots and direction != open_lots[0]["direction"]:
            continue
        if not _direction_abs_entry_ok(direction, basis, min_abs_entry):
            continue
        dynamic_entry_threshold = max(min_entry_edge, dynamic_cost + min_exit_pnl + Decimal("2.0"))
        if edge < dynamic_entry_threshold:
            continue
        if normalized is not None and normalized < min_norm_entry:
            continue
        if stablecoin_ok is False:
            continue
        if roundtrip is not None and roundtrip < Decimal("0"):
            blocked_cost += 1
            continue
        if edge < dynamic_cost:
            blocked_cost += 1
            continue
        if not open_lots:
            open_lots.append({"direction": direction, "entry_basis": basis, "notional": lot_notional})
            entries += 1
            continue
        if len(open_lots) >= max_lots:
            continue
        entry_bases = [lot["entry_basis"] for lot in open_lots]
        if direction == "long_var_short_lighter":
            addon_ok = basis <= min(entry_bases) - addon_step
        else:
            addon_ok = basis >= max(entry_bases) + addon_step
        if addon_ok:
            open_lots.append({"direction": direction, "entry_basis": basis, "notional": lot_notional})
            entries += 1

    print(
        f"asset={cost['asset']} rows={len(signal_rows)} lot_notional={fmt_decimal(lot_notional)} max_lots={max_lots} "
        f"addon_step_bps={fmt_decimal(addon_step)} dynamic_cost={fmt_decimal(dynamic_cost)} "
        f"dynamic_entry_threshold={fmt_decimal(max(min_entry_edge, dynamic_cost + min_exit_pnl + Decimal('2.0')))}"
    )
    print(
        f"entries={entries} exits={exits} open_lots={len(open_lots)} blocked_by_cost={blocked_cost} "
        f"portfolio_pnl_p50={fmt_decimal(percentile(portfolio_pnls, Decimal('50')))} "
        f"portfolio_pnl_p80={fmt_decimal(percentile(portfolio_pnls, Decimal('80')))} "
        f"portfolio_pnl_p95={fmt_decimal(percentile(portfolio_pnls, Decimal('95')))}"
    )
    if open_lots:
        basis = to_decimal(signal_rows[-1].get("basis_bps")) if signal_rows else None
        if basis is not None:
            direction = str(open_lots[0]["direction"])
            gross = sum(_direction_basis_pnl_bps(direction, lot["entry_basis"], basis) for lot in open_lots) / Decimal(len(open_lots))
            print(
                f"open_direction={direction} open_entry_basis={[fmt_decimal(lot['entry_basis']) for lot in open_lots]} "
                f"latest_basis={fmt_decimal(basis)} executable_pnl={fmt_decimal(gross - dynamic_cost)}"
            )


def print_basis_regime(rows: list[dict[str, Any]]) -> None:
    regimes = build_basis_regimes(rows)
    signal_rows = basis_state_rows(rows)
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
    signal_rows = basis_state_rows(rows)
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
        blocked = Counter(
            str(row.get("reason") or "-")
            for row in rows
            if row.get("event") == "live_inventory_entry_blocked"
            and str(row.get("asset") or "-").upper() == asset
        )
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
    parser.add_argument("--include-rotated", action="store_true", help="Include rotated order_metrics.jsonl.N and .gz files.")
    parser.add_argument("--all-runs", action="store_true", help="Analyze all tailed rows instead of only the latest run_id.")
    parser.add_argument("--top", type=int, default=5, help="Number of blocked candidates/reasons to print. Default: 5.")
    parser.add_argument("--what-if", action="store_true", help="Replay blocked entries against looser threshold grids.")
    parser.add_argument("--basis-regime", action="store_true", help="Summarize recent basis/edge percentiles for strategy tuning.")
    parser.add_argument("--strategy", action="store_true", help="Print a concise live strategy recommendation.")
    parser.add_argument("--config-advice", action="store_true", help="Print recommended live_config.json changes, if any.")
    parser.add_argument("--asset-scores", action="store_true", help="Score assets seen in the selected log rows for rotation decisions.")
    parser.add_argument("--run-scores", action="store_true", help="Score each run_id separately for asset rotation decisions.")
    parser.add_argument("--dynamic-cost", action="store_true", help="Summarize current spread regime and dynamic roundtrip cost.")
    parser.add_argument("--ladder-what-if", action="store_true", help="Simulate the SOL 20U x 3 add-on strategy over selected rows.")
    parser.add_argument("--entry-semantics", action="store_true", help="Compare normalized-primary and raw-primary-plus-normalized-filter entry proxies.")
    parser.add_argument("--semantics-primary-threshold", default="7", help="Static primary edge threshold for --entry-semantics. Default: 7.")
    parser.add_argument("--semantics-min-abs-entry-bps", default="7", help="Static absolute basis threshold for --entry-semantics. Default: 7.")
    parser.add_argument("--semantics-min-normalized-filter-bps", default="1", help="Normalized filter threshold for --entry-semantics. Default: 1.")
    parser.add_argument("--asset", help="Optional asset filter for dynamic-cost and ladder what-if sections.")
    parser.add_argument("--ladder-lot-notional-usd", default="20", help="What-if lot size. Default: 20.")
    parser.add_argument("--ladder-max-lots", type=int, default=3, help="What-if max open lots. Default: 3.")
    parser.add_argument("--ladder-addon-step-bps", default="2.0", help="Basis improvement required for each add-on. Default: 2.0.")
    parser.add_argument("--ladder-min-entry-edge-bps", default="7", help="Static floor for what-if entry edge. Default: 7.")
    parser.add_argument("--ladder-min-abs-entry-bps", default="7", help="Static floor for what-if absolute basis. Default: 7.")
    parser.add_argument("--ladder-min-normalized-entry-edge-bps", default="1.0", help="Minimum normalized edge for what-if. Default: 1.0.")
    parser.add_argument("--ladder-min-exit-pnl-bps", default="3.0", help="Minimum executable portfolio exit PnL. Default: 3.0.")
    parser.add_argument("--ladder-profit-take-pnl-bps", default="5.0", help="Portfolio profit-take PnL. Default: 5.0.")
    args = parser.parse_args()
    if args.tail <= 0:
        parser.error("--tail must be > 0")
    if args.top <= 0:
        parser.error("--top must be > 0")
    if args.ladder_max_lots <= 0:
        parser.error("--ladder-max-lots must be > 0")
    try:
        ladder_lot_notional = Decimal(str(args.ladder_lot_notional_usd))
        ladder_addon_step = Decimal(str(args.ladder_addon_step_bps))
        ladder_min_entry = Decimal(str(args.ladder_min_entry_edge_bps))
        ladder_min_abs_entry = Decimal(str(args.ladder_min_abs_entry_bps))
        ladder_min_norm_entry = Decimal(str(args.ladder_min_normalized_entry_edge_bps))
        ladder_min_exit = Decimal(str(args.ladder_min_exit_pnl_bps))
        ladder_profit_take = Decimal(str(args.ladder_profit_take_pnl_bps))
        semantics_primary_threshold = Decimal(str(args.semantics_primary_threshold))
        semantics_min_abs_entry = Decimal(str(args.semantics_min_abs_entry_bps))
        semantics_min_normalized_filter = Decimal(str(args.semantics_min_normalized_filter_bps))
    except Exception as exc:
        parser.error(f"invalid ladder decimal option: {exc}")
    if ladder_lot_notional <= 0 or ladder_addon_step <= 0:
        parser.error("ladder lot notional and addon step must be > 0")
    if semantics_primary_threshold <= 0 or semantics_min_abs_entry <= 0 or semantics_min_normalized_filter < 0:
        parser.error("entry semantics thresholds must be positive, except normalized filter may be 0")

    source_paths = rotated_jsonl_paths(ORDER_METRICS) if args.include_rotated else [ORDER_METRICS]
    raw_rows = tail_jsonl_many(source_paths, args.tail) if args.include_rotated else tail_jsonl(ORDER_METRICS, args.tail)
    rows = raw_rows if args.all_runs else latest_run_filter(raw_rows)
    current_run_rows = latest_run_filter(raw_rows)
    state = read_json(LIVE_STATE)
    processes = running_main_processes()

    events = Counter(str(row.get("event") or "-") for row in rows)
    assets = Counter(str(row.get("asset") or "-").upper() for row in rows if row.get("asset"))
    blocked_reasons = Counter(str(row.get("reason") or "unknown") for row in rows if row.get("event") == "live_inventory_entry_blocked")
    state_rows = basis_state_rows(rows)
    pre_gate_large_moves = sum(row.get("basis_sample_move_ok") is False for row in state_rows)
    qualified_large_move_blocks = sum(
        row.get("event") == "live_inventory_entry_blocked"
        and row.get("reason") == "basis_sample_move_too_large"
        and row.get("candidate_qualified") is True
        for row in rows
    )

    actual_pnl_bps: list[Decimal] = []
    actual_pnl_usd: list[Decimal] = []
    shortfalls: list[Decimal] = []
    entry_slippage: list[Decimal] = []
    exit_slippage: list[Decimal] = []
    all_sample_moves = _abs_values(_collect_decimal(basis_state_rows(rows), "basis_sample_move_bps"))
    blocked_sample_moves: list[Decimal] = []
    normalized_edges = [
        value
        for row in basis_state_rows(rows)
        for key in ("normalized_long_edge_bps", "normalized_short_edge_bps")
        if (value := to_decimal(row.get(key))) is not None
    ]
    blocked_scores: list[tuple[Decimal, str, str, str]] = []
    access_restricted = 0
    current_run_access_restricted = 0

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
                blocked_sample_moves.append(abs(move))
            score, direction = best_entry_score(row, Decimal("5.5"), Decimal("0.5"))
            if score is not None:
                blocked_scores.append((score, str(row.get("asset") or "-").upper(), direction, str(row.get("reason") or "unknown")))
    for row in current_run_rows:
        if "Access Restricted" in str(row):
            current_run_access_restricted += 1
    strategy_access_restricted = current_run_access_restricted if args.all_runs else access_restricted

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
    print(
        f"large_move_pre_gate_samples={pre_gate_large_moves} "
        f"qualified_large_move_blocks={qualified_large_move_blocks}"
    )
    print(f"access_restricted_rows={access_restricted}")
    if args.all_runs:
        print(f"access_restricted_rows_current_run={current_run_access_restricted}")

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
    latest_signal_row = next((row for row in reversed(rows) if row.get("event") == "live_inventory_basis_state"), None)
    print(
        f"all_sample_move_p50={fmt_decimal(percentile(all_sample_moves, Decimal('50')))} "
        f"all_sample_move_p80={fmt_decimal(percentile(all_sample_moves, Decimal('80')))} "
        f"blocked_event_move_p80={fmt_decimal(percentile(blocked_sample_moves, Decimal('80')))} "
        f"normalized_edge_p80={fmt_decimal(percentile(normalized_edges, Decimal('80')))} "
        f"latest_max_sample_move={fmt_decimal(to_decimal(latest_signal_row.get('basis_max_sample_move_bps')) if latest_signal_row else None)} "
        f"latest_sample_move_p80={fmt_decimal(to_decimal(latest_signal_row.get('basis_sample_move_p80_bps')) if latest_signal_row else None)}"
    )
    for index, (score, asset, direction, reason) in enumerate(sorted(blocked_scores, reverse=True)[: args.top], start=1):
        print(f"blocked_observation_{index}=asset={asset} dir={direction} score={fmt_decimal(score)} reason={reason}")
    if args.what_if:
        print_what_if(rows)
    if args.basis_regime:
        print_basis_regime(rows)
    if args.strategy:
        print_strategy(rows, events, status, open_lots, pending_actions, strategy_access_restricted)
    if args.config_advice:
        print_config_advice(rows)
    if args.asset_scores:
        print_asset_scores(rows)
    if args.run_scores:
        print_run_scores(rows)
    if args.dynamic_cost:
        print_dynamic_cost(rows, asset=args.asset)
    if args.ladder_what_if:
        print_ladder_what_if(
            rows,
            asset=args.asset,
            lot_notional=ladder_lot_notional,
            max_lots=args.ladder_max_lots,
            addon_step=ladder_addon_step,
            min_entry_edge=ladder_min_entry,
            min_abs_entry=ladder_min_abs_entry,
            min_norm_entry=ladder_min_norm_entry,
            min_exit_pnl=ladder_min_exit,
            profit_take_pnl=ladder_profit_take,
        )
    if args.entry_semantics:
        print_entry_semantics(
            rows,
            primary_threshold=semantics_primary_threshold,
            min_abs_entry=semantics_min_abs_entry,
            min_normalized_filter=semantics_min_normalized_filter,
        )

    operational_readiness = "unknown"
    if events["live_inventory_manual_review_required"] or status == "manual_review_required":
        operational_readiness = "manual_review_required_check_both_exchanges"
    elif status == "missing":
        operational_readiness = "state_missing_manual_exchange_flat_confirmation_required_before_start"
    elif status != "flat" or open_lots or pending_actions:
        operational_readiness = "not_ready_state_not_flat"
    elif strategy_access_restricted:
        operational_readiness = "not_ready_access_restricted_detected"
    elif not processes:
        operational_readiness = "flat_manual_exchange_confirmation_required"
    else:
        operational_readiness = "live_process_running"
    print(f"operational_readiness={operational_readiness}")

    runtime_tail = tail_text(RUNTIME_LOG, 5)
    if runtime_tail:
        print("== runtime_tail ==")
        for line in runtime_tail:
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
