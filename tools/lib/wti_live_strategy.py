"""Live WTI decisions aligned with the no-lookahead replay model."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from tools.lib.hedge_instruments import WTI_HEDGE, paired_realized_pnl
from tools.lib.market_sessions import session_observation_for_sample
from tools.lib.wti_sizing import size_paired_layer
from tools.lib.wti_replay import ReplayConfig, _decimal


def _valid_live_row(row: dict[str, Any]) -> bool:
    stamp = None
    try:
        stamp = datetime.fromisoformat(str(row.get("logged_at")).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        pass
    now = datetime.now(timezone.utc)
    age = None if stamp is None or stamp.tzinfo is None else (now - stamp.astimezone(timezone.utc)).total_seconds()
    quote_age = _decimal(row.get("var_quote_age_seconds"))
    book_age = _decimal(row.get("lighter_book_age_seconds"))
    return (
        row.get("asset") == "WTI"
        and row.get("sample_quality") == "valid"
        and row.get("market_availability") == "both_feeds_live"
        and session_observation_for_sample(row) is not None
        and age is not None and -5 <= age <= 20
        and quote_age is not None and -1 <= quote_age + Decimal(str(max(0, age))) <= 2
        and book_age is not None and 0 <= book_age + Decimal(str(max(0, age))) <= 2
    )


def decide_wti_action(
    row: dict[str, Any],
    position: dict[str, Any] | None,
    *,
    config: ReplayConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Choose one entry/exit action; costs stay conservative and fills are separate."""
    if (
        config.entry_threshold_bps <= 0
        or config.exit_target_bps < 0
        or config.roundtrip_cost_bps < 0
        or config.max_hold_seconds <= 0
        or config.max_sample_gap_seconds <= 0
        or config.entry_cutoff_seconds < 0
    ):
        raise ValueError("invalid WTI live strategy configuration")
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if not _valid_live_row(row):
        return {"action": "hold", "reason": "wti_market_sample_or_session_unverified"}
    stamp = datetime.fromisoformat(str(row["logged_at"]).replace("Z", "+00:00"))
    session = session_observation_for_sample(row)
    assert session is not None
    entry_cutoff = session.closing_at.timestamp() - config.entry_cutoff_seconds
    in_entry_cutoff = stamp.timestamp() >= entry_cutoff

    if position is not None:
        try:
            opened = datetime.fromisoformat(str(position["opened_at"]).replace("Z", "+00:00"))
            if opened.tzinfo is None or opened.utcoffset() is None:
                raise ValueError
            age = (stamp - opened.astimezone(timezone.utc)).total_seconds()
            direction = str(position["direction"])
            entry_session_id = str(position.get("session_id") or "").strip()
            if not entry_session_id or session.session_id != entry_session_id:
                return {
                    "action": "close",
                    "reason": "entry_session_unverified" if not entry_session_id else "session_boundary",
                    "position_age_seconds": age,
                }
            var_qty = _decimal(position.get("var_qty"))
            lighter_qty = _decimal(position.get("lighter_qty"))
            multiplier = _decimal(position.get("multiplier"))
            var_entry = _decimal(position.get("var_entry_price"))
            lighter_entry = _decimal(position.get("lighter_entry_price"))
            if (
                age < 0 or None in (var_qty, lighter_qty, multiplier, var_entry, lighter_entry)
                or min(var_qty, lighter_qty, multiplier, var_entry, lighter_entry) <= 0
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            return {"action": "hold", "reason": "wti_open_position_metadata_invalid"}
        instrument = replace(
            WTI_HEDGE,
            lighter=replace(WTI_HEDGE.lighter, exposure_multiplier=multiplier),
        )
        if direction == "long_var_short_lighter":
            var_exit = _decimal(row.get("var_bid"))
            lighter_exit = _decimal(row.get("lighter_buy_price"))
        elif direction == "short_var_long_lighter":
            var_exit = _decimal(row.get("var_ask"))
            lighter_exit = _decimal(row.get("lighter_sell_price"))
        else:
            return {"action": "hold", "reason": "wti_open_position_direction_invalid"}
        if var_exit is None or lighter_exit is None:
            return {"action": "hold", "reason": "wti_exit_quote_unavailable"}
        gross = paired_realized_pnl(
            direction=direction,
            var_entry=var_entry,
            var_exit=var_exit,
            var_qty=var_qty,
            lighter_entry=lighter_entry,
            lighter_exit=lighter_exit,
            lighter_qty=lighter_qty,
            instrument=instrument,
        )
        denominator = (var_entry * var_qty + lighter_entry * lighter_qty * multiplier) / 2
        assumed_cost = denominator * config.roundtrip_cost_bps / Decimal("10000")
        net_bps = (gross - assumed_cost) / denominator * Decimal("10000")
        reason = None
        if net_bps >= config.exit_target_bps:
            reason = "net_target_reached"
        elif age >= config.max_hold_seconds:
            reason = "max_hold"
        elif in_entry_cutoff:
            reason = "session_entry_cutoff"
        if reason:
            return {
                "action": "close", "reason": reason,
                "estimated_gross_pnl_usd": str(gross),
                "assumed_cost_usd": str(assumed_cost),
                "estimated_net_pnl_bps": str(net_bps),
                "position_age_seconds": age,
            }
        return {
            "action": "hold", "reason": "exit_target_not_reached",
            "estimated_net_pnl_bps": str(net_bps),
            "position_age_seconds": age,
        }

    if in_entry_cutoff:
        return {"action": "hold", "reason": "session_entry_cutoff"}
    edges = {
        "long_var_short_lighter": _decimal(row.get("long_edge_bps")),
        "short_var_long_lighter": _decimal(row.get("short_edge_bps")),
    }
    qualified = [(direction, edge) for direction, edge in edges.items()
                 if edge is not None and edge >= config.entry_threshold_bps]
    if not qualified:
        return {"action": "hold", "reason": "entry_edge_below_threshold"}
    direction, edge = max(qualified, key=lambda item: item[1])
    return {"action": "open", "direction": direction, "edge_bps": str(edge)}


def decide_wti_gradient_action(
    row: dict[str, Any],
    position: dict[str, Any] | None,
    pilot: dict[str, Any],
    *,
    config: ReplayConfig,
    now: datetime | None = None,
    thresholds_by_direction: Mapping[str, Sequence[Decimal]] | None = None,
) -> dict[str, Any]:
    """Apply strictly increasing, same-direction tiers within a durable pilot."""
    decision = decide_wti_action(row, position, config=config, now=now)
    pilot_id = pilot.get("pilot_id")
    tiers = pilot.get("config", {}).get("tier_thresholds_bps")
    layer_notional = pilot.get("config", {}).get("layer_notional_usd")
    if (
        not pilot_id or not isinstance(tiers, list) or len(tiers) != pilot.get("max_layers")
        or not tiers or layer_notional is None
    ):
        return {"action": "hold", "reason": "gradient_pilot_configuration_invalid"}
    try:
        thresholds = [Decimal(str(value)) for value in tiers]
        layer_notional = Decimal(str(layer_notional))
    except Exception:
        return {"action": "hold", "reason": "gradient_pilot_configuration_invalid"}
    if (
        any(not value.is_finite() or value <= 0 for value in thresholds)
        or any(left >= right for left, right in zip(thresholds, thresholds[1:]))
        or thresholds[0] != config.entry_threshold_bps
        or not layer_notional.is_finite() or layer_notional <= 0
    ):
        return {"action": "hold", "reason": "gradient_pilot_configuration_invalid"}
    if decision.get("action") == "close":
        return decision

    effective_thresholds: dict[str, list[Decimal]] | None = None
    if thresholds_by_direction is not None:
        effective_thresholds = {}
        try:
            for direction in ("long_var_short_lighter", "short_var_long_lighter"):
                values = [Decimal(str(value)) for value in thresholds_by_direction[direction]]
                if (
                    len(values) != len(thresholds)
                    or any(not value.is_finite() for value in values)
                    or any(value < floor for value, floor in zip(values, thresholds))
                    or any(left >= right for left, right in zip(values, values[1:]))
                ):
                    raise ValueError("invalid adaptive threshold profile")
                effective_thresholds[direction] = values
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return {"action": "hold", "reason": "gradient_threshold_profile_invalid"}

    if position is None:
        if pilot.get("status") != "available":
            return decision
        if pilot.get("layers_opened") or pilot.get("completed_roundtrips") != 0:
            return {"action": "hold", "reason": "gradient_pilot_entry_budget_unavailable"}
        if effective_thresholds is not None:
            edges = {
                "long_var_short_lighter": _decimal(row.get("long_edge_bps")),
                "short_var_long_lighter": _decimal(row.get("short_edge_bps")),
            }
            qualified = [
                (direction, edge)
                for direction, edge in edges.items()
                if edge is not None and edge >= effective_thresholds[direction][0]
            ]
            if not qualified:
                return {
                    "action": "hold",
                    "reason": "gradient_entry_below_adaptive_threshold",
                    "effective_tier_thresholds_bps": {
                        direction: [str(value) for value in values]
                        for direction, values in effective_thresholds.items()
                    },
                }
            direction, edge = max(qualified, key=lambda item: item[1])
            decision = {
                "action": "open", "direction": direction, "edge_bps": str(edge),
            }
        elif decision.get("action") != "open":
            return decision
        return {
            **decision,
            "gradient_tier": 1,
            "layer_notional_usd": str(layer_notional),
            **({
                "effective_tier_thresholds_bps": {
                    direction: [str(value) for value in values]
                    for direction, values in effective_thresholds.items()
                }
            } if effective_thresholds is not None else {}),
        }

    if decision.get("action") != "hold" or decision.get("reason") != "exit_target_not_reached":
        return decision
    if (
        pilot.get("status") != "in_position"
        or position.get("gradient_pilot_id") != pilot_id
        or pilot.get("completed_roundtrips") != 0
    ):
        return decision
    layers = pilot.get("layers_opened")
    if not isinstance(layers, list):
        return {"action": "hold", "reason": "gradient_pilot_state_invalid"}
    tier = len(layers) + 1
    if (
        tier > len(thresholds)
        or tier > pilot.get("max_layers", 0)
        or tier in pilot.get("exhausted_tiers", [])
    ):
        return decision
    direction = position.get("direction")
    edge_field = (
        "long_edge_bps" if direction == "long_var_short_lighter"
        else "short_edge_bps" if direction == "short_var_long_lighter" else None
    )
    edge = _decimal(row.get(edge_field)) if edge_field else None
    active_thresholds = (
        effective_thresholds.get(str(direction))
        if effective_thresholds is not None else thresholds
    )
    if active_thresholds is None:
        return {"action": "hold", "reason": "gradient_threshold_profile_invalid"}
    if edge is None or edge < active_thresholds[tier - 1]:
        if effective_thresholds is not None and edge is not None:
            return {
                "action": "hold",
                "reason": "gradient_add_below_adaptive_threshold",
                "edge_bps": str(edge),
                "gradient_threshold_bps": str(active_thresholds[tier - 1]),
                "effective_tier_thresholds_bps": {
                    key: [str(value) for value in values]
                    for key, values in effective_thresholds.items()
                },
            }
        return decision
    try:
        cap = Decimal(str(pilot["config"]["max_notional_usd"]))
        multiplier = Decimal(str(position["multiplier"]))
        var_qty = Decimal(str(position["var_qty"]))
        lighter_qty = Decimal(str(position["lighter_qty"]))
        max_slippage = Decimal(str(pilot["config"]["max_slippage_bps"]))
        if not max_slippage.is_finite() or max_slippage <= 0:
            raise ValueError("invalid max slippage")
        markup = Decimal("1") + max_slippage / Decimal("10000")
        var_order_price = Decimal(str(
            row["var_ask"] if direction == "long_var_short_lighter" else row["var_bid"]
        ))
        lighter_order_price = Decimal(str(
            row["lighter_sell_price"] if direction == "long_var_short_lighter"
            else row["lighter_buy_price"]
        ))
        var_mark = max(Decimal(str(row["var_bid"])), Decimal(str(row["var_ask"])))
        lighter_mark = max(
            Decimal(str(row["lighter_bid"])), Decimal(str(row["lighter_ask"]))
        )
        decimals = int(row["lighter_supported_size_decimals"])
        min_base = Decimal(str(row.get("lighter_min_base_amount") or "0"))
        min_quote = Decimal(str(row.get("lighter_min_quote_amount") or "0"))
        step = Decimal("1").scaleb(-decimals)
        qty = size_paired_layer(
            target_notional_usd=layer_notional,
            max_notional_usd=cap,
            multiplier=multiplier,
            qty_step=step,
            min_base_qty=min_base,
            min_quote_usd=min_quote,
            var_order_price=max(var_mark, var_order_price) * markup,
            lighter_order_price=max(lighter_mark, lighter_order_price) * markup,
            lighter_sizing_price=lighter_order_price,
            lighter_min_quote_price=lighter_order_price * (
                Decimal("1") - max_slippage / Decimal("10000")
            ),
            allow_reduce_to_cap=False,
            existing_var_qty=var_qty,
            existing_lighter_qty=lighter_qty,
            existing_var_mark_price=var_mark,
            existing_lighter_mark_price=lighter_mark,
        )
        if qty is None:
            return {"action": "hold", "reason": "gradient_aggregate_notional_cap_would_be_exceeded"}
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return {"action": "hold", "reason": "gradient_aggregate_notional_unverifiable"}
    return {
        "action": "add",
        "direction": direction,
        "edge_bps": str(edge),
        "reason": "gradient_tier_threshold_reached",
        "gradient_tier": tier,
        "layer_notional_usd": str(layer_notional),
        **({
            "effective_tier_thresholds_bps": {
                key: [str(value) for value in values]
                for key, values in effective_thresholds.items()
            }
        } if effective_thresholds is not None else {}),
    }
