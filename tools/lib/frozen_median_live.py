"""Integration with the existing reconciled, concurrent inventory executor."""
from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal as D
from pathlib import Path

from tools.lib.frozen_median import (
    FrozenMedianGradient, LONG, SHORT, MULTIPLIERS, Observation,
    executable_profit_bps,
)

LEVELS = tuple(map(D, ("5.6", "6.6", "7.6", "8.6", "9.6")))
ROUNDTRIP_BUDGET = D("4.6")
NET_TARGET = D(1)
STRATEGY = "frozen_median_56_96_cost46_net1_v2"


def cost_residual(*, direction, var_entry, lighter_entry, var_entry_mid,
                  lighter_entry_mid, var_exit, lighter_exit, var_exit_mid,
                  lighter_exit_mid):
    prices = tuple(map(D, (var_entry, lighter_entry, var_entry_mid, lighter_entry_mid,
                          var_exit, lighter_exit, var_exit_mid, lighter_exit_mid)))
    if not all(x.is_finite() and x > 0 for x in prices):
        raise ValueError("median_cost_reference_invalid")
    ve,le,vm,lm,vx,lx,vxm,lxm = prices
    sign = D(1) if direction == LONG else D(-1)
    embedded = sign*((ve-vm)+(lm-le)+(vxm-vx)+(lx-lxm))/((ve+le)/2)*10000
    return max(D(0), ROUNDTRIP_BUDGET-max(D(0),embedded))


def exit_reserve(lot, *, var_exit, lighter_exit, var_mid=None, lighter_mid=None):
    # Missing evidence receives no cost credit, never a guessed zero budget.
    if not all(lot.get(k) is not None for k in ("median_var_entry_mid", "median_lighter_entry_mid")):
        return ROUNDTRIP_BUDGET
    return cost_residual(direction=lot["direction"],
        var_entry=D(lot["entry_var_fill_price"]), lighter_entry=D(lot["entry_lighter_fill_price"]),
        var_entry_mid=D(lot["median_var_entry_mid"]), lighter_entry_mid=D(lot["median_lighter_entry_mid"]),
        var_exit=var_exit,lighter_exit=lighter_exit,
        var_exit_mid=var_mid if var_mid is not None else var_exit,
        lighter_exit_mid=lighter_mid if lighter_mid is not None else lighter_exit)


def enabled(runtime) -> bool:
    return bool(getattr(runtime, "live_inventory_frozen_median", False))


def pair_edge(var_price: D, lighter_price: D) -> D:
    return (lighter_price - var_price) / ((lighter_price + var_price) / 2) * 10000


def initialize(runtime, state: dict) -> None:
    if not enabled(runtime):
        if "frozen_median" in state and (state.get("open_lots") or state.get("pending_actions")):
            raise ValueError("frozen_median_positions_require_frozen_median_mode")
        return
    saved = state.get("frozen_median")
    if not saved and (state.get("open_lots") or state.get("pending_actions")):
        raise ValueError("cannot_adopt_legacy_inventory_into_median_strategy")
    runtime.frozen_median_model = (
        FrozenMedianGradient.restore(saved) if saved else FrozenMedianGradient(deviation_bps=LEVELS[0], distances=LEVELS)
    )
    runtime.frozen_median_last_row = None
    runtime.frozen_median_rfq_context = {}
    model = runtime.frozen_median_model
    wti = getattr(runtime, "live_allowed_assets", set()) == {"WTI"}
    if wti and model.signature and not model.signature[0].startswith("WTI:USDC:"):
        raise ValueError("wti_median_saved_instrument_mismatch")
    if model.distances != LEVELS:
        if model.anchor is not None or state.get("open_lots") or state.get("pending_actions") or any(o["state"] != "closed" for o in model.orders.values()):
            raise ValueError("median_policy_upgrade_requires_verified_flat_state")
        model.deviation, model.distances = LEVELS[0], LEVELS
    if model.window != 3600:
        raise ValueError("persisted_median_window_mismatch")
    # Only the qualified journal is eligible for restart warm-up, not old ticks.
    root = Path(runtime.output_dir) / "median_samples"
    cutoff = time.time() - model.window
    for path in sorted(root.glob("*.jsonl"))[-2:]:
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                    if item.get("schema") != 1 or item["at"] < cutoff:
                        continue
                    if wti and not str(item.get("instrument", "")).startswith("WTI:USDC:"):
                        continue
                    row = Observation(**{
                        k: D(v) if k in ("var_bid", "var_ask", "lighter_bid", "lighter_ask") else v
                        for k, v in item.items() if k != "schema"
                    })
                    model.observe(row, var_max_age=5,
                                  lighter_max_age=runtime.live_inventory_max_lighter_book_age_seconds)
                except (ValueError, TypeError, KeyError):
                    continue
    model.confirmations.clear()


def observe(runtime, *, asset, quote, snapshot, var_bid, var_ask,
            lighter_age, now):
    if not enabled(runtime):
        return
    model = runtime.frozen_median_model
    runtime.frozen_median_last_row = None
    raw = quote.get("raw") or {}
    semantics = quote.get("quote_semantics")
    source_time = quote.get("source_quote_timestamp") or quote.get("quoteTimestamp")
    try:
        source_at = datetime.fromisoformat(str(source_time).replace("Z", "+00:00")).timestamp()
    except ValueError:
        source_at = -1
    size_key = raw.get("__indicative_quote_size")
    # Never label the requested 20U RFQ size as the passive page's actual size.
    size_key = str(size_key or raw.get("__source_url") or semantics or "unknown")
    row = Observation(
        at=now, source_at=source_at, event_id=str(source_time or ""),
        instrument=f"{asset}:USDC:{semantics}", size_key=size_key,
        var_bid=var_bid, var_ask=var_ask,
        lighter_bid=snapshot.lighter_bid, lighter_ask=snapshot.lighter_ask,
        lighter_at=now - (float(lighter_age) if lighter_age is not None else 99999),
    )
    if semantics != "passive_indicative_bid_ask":
        model.confirmations.clear()
        return
    before = model.history[-1][0] if model.history else None
    if not model.observe(row, var_max_age=5,
                         lighter_max_age=runtime.live_inventory_max_lighter_book_age_seconds):
        return
    runtime.frozen_median_last_row = row
    if before != int(now):
        path = Path(runtime.output_dir) / "median_samples" / (
            datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d") + ".jsonl"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"schema": 1, **asdict(row)}, default=str) + "\n")


def checkpoint(runtime) -> dict:
    if not enabled(runtime):
        return {}
    m = runtime.frozen_median_model
    for lot in runtime.live_inventory_open_lots:
        key = lot.get("median_order_id") or lot.get("basis_trace_id")
        if key in m.orders and m.orders[key]["state"] == "pending":
            if runtime.live_inventory_entry_cost_confirmed(lot):
                m.confirm_open(key, matched_qty=True)
    rolling = m.rolling_median(now=time.time())
    return {"frozen_median": m.dump(),
            "median_strategy": STRATEGY,
            "median_roundtrip_cost_budget_bps": str(ROUNDTRIP_BUDGET),
            "median_net_target_bps": str(NET_TARGET),
            "median_rolling_bps": str(rolling) if rolling is not None else None,
            "median_anchor_bps": str(m.anchor) if m.anchor is not None else None,
            "median_current_basis_bps": str(m.last_basis) if m.last_basis is not None else None,
            "median_history_count": len(m.history),
            "median_review_reason": m.review_reason,
            "median_ready": rolling is not None and not m.review_reason,
            "median_levels_bps": [str(k) for k in m.distances]}


def thresholds(runtime, *, now, direction):
    m = runtime.frozen_median_model
    rolling = m.rolling_median(now=now)
    anchor = m.anchor if m.anchor is not None else rolling
    ready = anchor is not None and not m.review_reason
    sign = D(1) if direction == LONG else D(-1)
    levels = [anchor + sign * k for k in m.distances] if ready else []
    context = {
        "median_strategy": STRATEGY,
        "median_anchor_bps": str(m.anchor) if m.anchor is not None else None,
        "median_rolling_bps": str(rolling) if rolling is not None else None,
        "median_deviation_bps": str(m.deviation), "median_window_seconds": m.window,
        "median_history_count": len(m.history),
        "median_review_reason": m.review_reason,
        "v4_anchor_ready": ready, "v4_health_ready": ready,
        "v4_baseline_window_seconds": m.window,
        "v4_entry_threshold_bps": str(levels[0]) if levels else None,
        "v4_real_gradient_tier_thresholds_bps": [str(v) for v in levels],
        "v4_entry_execution_reserve_bps": "0",  # charged only in executable forecast
        "v4_entry_exit_cost_increment_bps": "0",
        "v4_raw_threshold_driver": "frozen_median_not_percentile",
    }
    return (levels[0] if levels else None), context


def active_tier(runtime, *, direction, now):
    m = runtime.frozen_median_model
    if (m.episode_id and m.orders and not runtime.live_inventory_open_lots
            and all(o["state"] == "closed" for o in m.orders.values())):
        return 0  # Wait for the independent account/order flat check.
    return m.candidate_tier(direction=direction, now=now)


def rfq_confirmation(runtime, *, direction, var_bid, var_ask,
                     lighter_entry, lighter_exit, now):
    """Map the frozen reference midpoint to executable exit prices once.

    The midpoint offset maps the forecast, not an extra fee. Quoted spread and
    depth replace part of the 4.6bps total budget; only its residual is charged.
    """
    m = runtime.frozen_median_model
    row = runtime.frozen_median_last_row
    anchor = m.anchor if m.anchor is not None else m.rolling_median(now=now)
    if (anchor is None or row is None or not 0 <= now - row.at <= 5
            or not 0 <= now - row.source_at <= 5
            or not 0 <= now - row.lighter_at <= runtime.live_inventory_max_lighter_book_age_seconds
            or m.review_reason):
        raise ValueError("median_reference_not_ready_for_rfq")
    reference_v = (row.var_bid + row.var_ask) / 2
    reference_l = (row.lighter_bid + row.lighter_ask) / 2
    common = (reference_v + reference_l) / 2
    target_v = common * (1 - anchor / 20000)
    target_l = common * (1 + anchor / 20000)
    midpoint_offset = (var_bid + var_ask) / 2 - reference_v
    halfspread = (var_ask - var_bid) / 2
    var_entry = var_ask if direction == LONG else var_bid
    var_exit = target_v + midpoint_offset + (-halfspread if direction == LONG else halfspread)
    projected_l_exit = target_l + (lighter_exit - reference_l)
    # Do not reuse legacy signal-to-fill calibration here: it includes RFQ
    # repricing, which the executable prices have already incorporated.
    remaining = cost_residual(direction=direction,var_entry=var_entry,lighter_entry=lighter_entry,
        var_entry_mid=(var_bid+var_ask)/2,lighter_entry_mid=reference_l,
        var_exit=var_exit,lighter_exit=projected_l_exit,
        var_exit_mid=target_v+midpoint_offset,lighter_exit_mid=target_l)
    profit = executable_profit_bps(
        direction=direction, var_entry=var_entry, lighter_entry=lighter_entry,
        var_exit=var_exit, lighter_exit=projected_l_exit,
        remaining_reserve_bps=remaining,
    )
    target = NET_TARGET
    edge = pair_edge(var_entry, lighter_entry)
    # Express the profit test in the same signed-edge units as the executor.
    sign = D(1) if direction == LONG else D(-1)
    required = edge + sign * (target - profit)
    levels = [required + sign * (k-m.distances[0]) for k in m.distances]
    result = {
        "median_anchor_bps": str(anchor), "median_expected_net_bps": str(profit),
        "median_remaining_unpriced_cost_bps": str(remaining),
        "median_roundtrip_cost_budget_bps": str(ROUNDTRIP_BUDGET),
        "median_var_entry_mid": str((var_bid+var_ask)/2),
        "median_lighter_entry_mid": str(reference_l),
        "median_rfq_midpoint_offset": str(midpoint_offset),
        "median_projected_var_exit": str(var_exit),
        "median_projected_lighter_exit": str(projected_l_exit),
        "median_profit_target_bps": str(target),
        "median_cost_semantics": "bid_ask_prices_plus_unpriced_execution_only",
    }
    runtime.frozen_median_rfq_context = result
    return edge, required, levels, result


def reserve(runtime, *, order_id, lot_id, direction, tier, capacity, now):
    m = runtime.frozen_median_model
    was_flat = m.anchor is None
    m.freeze(episode_id=runtime.live_inventory_v4_episode_id, direction=direction, now=now)
    if was_flat:
        m.anchor = D(runtime.frozen_median_rfq_context["median_anchor_bps"])
    if not m.reserve(order_id=order_id, tier=tier, capacity=capacity,
                     existing_slots=len(runtime.live_inventory_open_lots), now=now):
        raise ValueError("median_duplicate_intent")
    m.orders[order_id]["lot_id"] = lot_id
    return {"median_order_id": order_id, "median_anchor_bps": str(m.anchor),
        "median_entry_strategy": STRATEGY,
            **runtime.frozen_median_rfq_context}


def close_started(runtime, lot):
    if not enabled(runtime):
        return True
    m = runtime.frozen_median_model
    ids = {str(v) for v in (lot.get("portfolio_component_lot_ids") or [lot.get("lot_id")])}
    keys = [key for key, order in m.orders.items() if str(order.get("lot_id")) in ids]
    if len(keys) != len(ids):
        m.review_reason = "median_close_order_identity_missing"
        return False
    for key in keys:
        if m.orders[key]["state"] == "pending" and runtime.live_inventory_entry_cost_confirmed(lot):
            m.confirm_open(key, matched_qty=True)
    if any(m.orders[key]["state"] != "open" for key in keys):
        return False
    for key in keys:
        m.start_close(key)
        m.orders[key]["close_group_id"] = lot.get("lot_id")
    return True


def close_not_submitted(runtime, lot):
    if not enabled(runtime):
        return
    m = runtime.frozen_median_model
    for order in m.orders.values():
        if not (str(order.get("close_group_id", order.get("lot_id"))) == str(lot.get("lot_id"))
                and order["state"] == "closing"):
            continue
        order["state"] = "open"
        tier = order["tier"]
        if not any(o["tier"] == tier and o["state"] == "closing" for o in m.orders.values()):
            m.tiers[str(tier)]["armed"] = True


def confirm_exit_signal(runtime, lot, eligible, now):
    row = runtime.frozen_median_last_row
    if row is None:
        return False
    window = [item for item in lot.get("median_exit_signals", []) if 0 <= now - item[1] <= 3]
    if not any(item[0] == row.event_id for item in window):
        window.append([row.event_id, now, bool(eligible)])
    window = window[-3:]
    lot["median_exit_signals"] = window
    return bool(eligible and window and window[-1][2] and sum(x[2] for x in window) >= 2)


def final_closed(runtime, *, lot_id, quantities_match):
    if not enabled(runtime) or not quantities_match:
        return
    m = runtime.frozen_median_model
    for key, order in m.orders.items():
        if str(order.get("close_group_id", order.get("lot_id"))) == str(lot_id) and order["state"] == "closing":
            m.confirm_closed(key, both_legs_final=True, remaining_qty=D(0), active_orders=0)


async def entry_rollback_verified(runtime, *, asset, lot_id):
    """Called only after the executor confirms rollback to its remaining lots."""
    if not enabled(runtime):
        return True
    m = runtime.frozen_median_model
    matches = [o for o in m.orders.values() if str(o.get("lot_id")) == str(lot_id)
               and o["state"] == "pending"]
    if not matches:
        return True
    try:
        if await runtime.fetch_all_variational_pending_orders(asset=asset):
            return False
        if await runtime.fetch_lighter_active_orders():
            return False
    except Exception:
        return False
    for order in matches:
        order["state"] = "closed"
        order["resolution"] = "executor_verified_rollback_no_active_orders"
        m.tiers[str(order["tier"])].update(armed=False, reset_seen=False)
    return True


def reversion_ready(runtime, lot, basis):
    m = runtime.frozen_median_model
    anchor = D(lot["median_anchor_bps"])
    tier = int(lot["entry_gradient_tier"])
    target_distance = D(0) if tier == 1 else m.distances[tier - 2]
    return m.signed_distance(basis, anchor, lot["direction"]) <= target_distance


async def maybe_finish(runtime, *, asset, now):
    m = runtime.frozen_median_model
    if (m.anchor is None or m.review_reason or runtime.live_inventory_open_lots
            or runtime.pending_live_inventory_var_fill_matches
            or runtime.live_inventory_v4_exit_reconciliation_lot_ids
            or any(o["state"] != "closed" for o in m.orders.values())
            or m.last_basis is None
            or m.signed_distance(m.last_basis, m.anchor, m.direction)
            > m.deviation - m.reset_buffer):
        return
    last = getattr(runtime, "median_last_flat_check", 0)
    if now - last < 15:
        return
    runtime.median_last_flat_check = now
    try:
        var = await runtime.fetch_variational_positions()
        lighter = await runtime.fetch_lighter_account()
        vq = runtime.extract_all_variational_position_qtys(var, asset=asset)
        lq = runtime.extract_all_lighter_position_qtys(lighter, asset=asset)
        vo = await runtime.fetch_all_variational_pending_orders(asset=asset)
        lo = await runtime.fetch_lighter_active_orders()
        if (vq is None or lq is None or any(q != 0 for q in (*vq, *lq)) or vo or lo):
            return
        m.finish_episode(both_venues_flat=True, active_orders=0, now=time.time())
    except (ValueError, RuntimeError, OSError):
        return
    runtime.live_inventory_v4_episode_id = None
    runtime.live_inventory_v4_next_tranche_index = 1
    await runtime.persist_live_inventory_memory(reason="median_episode_reconciled_flat")
