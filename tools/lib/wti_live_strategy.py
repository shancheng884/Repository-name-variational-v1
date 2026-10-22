"""Live WTI decisions aligned with the no-lookahead replay model."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from tools.lib.hedge_instruments import WTI_HEDGE, paired_realized_pnl
from tools.lib.market_sessions import session_observation_for_sample
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
