"""Deterministic, no-lookahead WTI hedge replay over recorded quote samples.

Replay results are indicative only. Historical samples do not prove fills,
exchange session status, funding, roll cashflows, or live execution latency.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from tools.lib.hedge_instruments import (
    WTI_HEDGE,
    paired_exposure_matches,
    paired_realized_pnl,
)
from tools.lib.market_sessions import session_observation_for_sample


DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    entry_threshold_bps: Decimal
    exit_target_bps: Decimal
    roundtrip_cost_bps: Decimal
    max_hold_seconds: float
    max_sample_gap_seconds: float
    entry_cutoff_seconds: float = 1800


@dataclass(frozen=True, slots=True)
class ReplayTrade:
    direction: str
    entry_at: str
    exit_at: str
    duration_seconds: float
    entry_edge_bps: str
    gross_pnl_usd: str
    assumed_cost_usd: str
    net_pnl_usd: str
    net_pnl_bps: str
    exit_reason: str


@dataclass(frozen=True, slots=True)
class ReplayReport:
    replay_kind: str
    live_authorization: bool
    session_gated: bool
    sample_count: int
    rejected_sample_count: int
    duplicate_sample_count: int
    conflicting_sample_id_count: int
    sample_gap_count: int
    stopped_reason: str | None
    completed_trades: int
    unresolved_position: bool
    gross_pnl_usd: str
    assumed_cost_usd: str
    net_pnl_usd: str
    win_rate: str | None
    exit_reasons: dict[str, int]
    config: dict[str, Any]
    cost_caveat: str
    trades: tuple[ReplayTrade, ...]


def _decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _timestamp(value: Any) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if result.tzinfo is None or result.utcoffset() is None:
        return None
    return result.astimezone(timezone.utc)


def _valid_sample(
    row: dict[str, Any],
    *,
    expected_market_id: int | None,
    expected_multiplier: Decimal | None,
) -> bool:
    if (
        str(row.get("asset") or "").upper() != "WTI"
        or row.get("record_kind") != "basis_market_sample"
        or row.get("sample_kind", "baseline") != "baseline"
        or row.get("sample_quality") != "valid"
        or row.get("market_availability") != "both_feeds_live"
        or row.get("variational_underlying") != "USOILP"
        or row.get("variational_instrument_type") != "swap"
        or row.get("variational_instrument_kind") != "commodity"
        or _decimal(row.get("variational_funding_interval_s")) != 0
        or str(row.get("lighter_symbol") or "").upper() != "WTI"
        or str(row.get("lighter_market_type") or "").lower() != "perp"
        or not str(row.get("sample_id") or "").strip()
        or _timestamp(row.get("logged_at")) is None
        or session_observation_for_sample(row) is None
    ):
        return False
    market_id = _decimal(row.get("lighter_market_id"))
    multiplier = _decimal(row.get("lighter_multiplier"))
    if (
        market_id is None or market_id < 0 or market_id != market_id.to_integral_value()
        or multiplier is None or multiplier <= 0
        or (expected_market_id is not None and int(market_id) != expected_market_id)
        or (expected_multiplier is not None and multiplier != expected_multiplier)
        or str(row.get("lighter_market_status") or "").lower() != "active"
    ):
        return False
    for key in ("var_bid", "var_ask", "lighter_bid", "lighter_ask",
                "lighter_buy_price", "lighter_sell_price", "variational_qty", "lighter_qty"):
        value = _decimal(row.get(key))
        if value is None or value <= 0:
            return False
    var_bid, var_ask = _decimal(row["var_bid"]), _decimal(row["var_ask"])
    lighter_bid, lighter_ask = _decimal(row["lighter_bid"]), _decimal(row["lighter_ask"])
    var_age, book_age = _decimal(row.get("var_quote_age_seconds")), _decimal(row.get("lighter_book_age_seconds"))
    if (
        var_bid > var_ask or lighter_bid > lighter_ask
        or var_age is None or not Decimal("-1") <= var_age <= Decimal("2")
        or book_age is None or not Decimal("0") <= book_age <= Decimal("2")
    ):
        return False
    instrument = replace(
        WTI_HEDGE,
        lighter=replace(WTI_HEDGE.lighter, exposure_multiplier=multiplier),
    )
    return paired_exposure_matches(
        var_qty=_decimal(row["variational_qty"]),
        lighter_qty=_decimal(row["lighter_qty"]),
        instrument=instrument,
        tolerance=Decimal("0.00000001"),
    )


def _canonical_rows(
    rows: list[dict[str, Any]],
    *,
    expected_market_id: int | None,
    expected_multiplier: Decimal | None,
) -> tuple[list[dict[str, Any]], int, int, int]:
    candidates: dict[str, dict[str, Any]] = {}
    conflicting: set[str] = set()
    rejected = 0
    duplicates = 0
    for row in rows:
        if not isinstance(row, dict) or not _valid_sample(
            row,
            expected_market_id=expected_market_id,
            expected_multiplier=expected_multiplier,
        ):
            rejected += 1
            continue
        sample_id = str(row["sample_id"]).strip()
        old = candidates.get(sample_id)
        if old is None:
            candidates[sample_id] = row
        elif old == row:
            duplicates += 1
        else:
            conflicting.add(sample_id)
    for sample_id in conflicting:
        candidates.pop(sample_id, None)
    session_close_values: dict[str, set[datetime]] = {}
    for row in candidates.values():
        observation = session_observation_for_sample(row)
        if observation is not None:
            session_close_values.setdefault(observation.session_id, set()).add(
                observation.closing_at
            )
    conflicting_sessions = {
        session_id for session_id, closes in session_close_values.items()
        if len(closes) != 1
    }
    for sample_id, row in list(candidates.items()):
        observation = session_observation_for_sample(row)
        if observation is not None and observation.session_id in conflicting_sessions:
            candidates.pop(sample_id)
            rejected += 1
    profiles = {
        (int(_decimal(row["lighter_market_id"])), _decimal(row["lighter_multiplier"]))
        for row in candidates.values()
    }
    if len(profiles) > 1:
        raise ValueError("historical_wti_market_profile_changed_split_replay_by_contract")
    ordered = sorted(candidates.values(), key=lambda row: _timestamp(row["logged_at"]))
    return ordered, rejected, duplicates, len(conflicting)


def _trade_pnl(
    position: dict[str, Any],
    row: dict[str, Any],
    *,
    roundtrip_cost_bps: Decimal,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    direction = position["direction"]
    if direction == "long_var_short_lighter":
        var_exit = _decimal(row["var_bid"])
        lighter_exit = _decimal(row["lighter_buy_price"])
    else:
        var_exit = _decimal(row["var_ask"])
        lighter_exit = _decimal(row["lighter_sell_price"])
    gross = paired_realized_pnl(
        direction=direction,
        var_entry=position["var_entry"],
        var_exit=var_exit,
        var_qty=position["var_qty"],
        lighter_entry=position["lighter_entry"],
        lighter_exit=lighter_exit,
        lighter_qty=position["lighter_qty"],
        instrument=position["instrument"],
    )
    var_notional = position["var_entry"] * position["var_qty"]
    lighter_notional = (
        position["lighter_entry"] * position["lighter_qty"]
        * position["instrument"].lighter.exposure_multiplier
    )
    denominator = (var_notional + lighter_notional) / Decimal("2")
    if denominator <= 0:
        raise ValueError("replay_notional_invalid")
    assumed_cost = denominator * roundtrip_cost_bps / Decimal("10000")
    net = gross - assumed_cost
    net_bps = net / denominator * Decimal("10000")
    return gross, assumed_cost, net, net_bps


def replay_wti(
    rows: list[dict[str, Any]],
    *,
    config: ReplayConfig,
    expected_market_id: int | None = None,
    expected_multiplier: Decimal | None = None,
) -> ReplayReport:
    if config.entry_threshold_bps <= 0 or config.exit_target_bps < 0:
        raise ValueError("entry threshold must be positive and exit target non-negative")
    if config.roundtrip_cost_bps < 0:
        raise ValueError("roundtrip cost cannot be negative")
    if (
        config.max_hold_seconds <= 0
        or config.max_sample_gap_seconds <= 0
        or config.entry_cutoff_seconds < 0
    ):
        raise ValueError("hold and sample-gap limits must be positive; entry cutoff non-negative")
    samples, rejected, duplicates, conflicts = _canonical_rows(
        rows,
        expected_market_id=expected_market_id,
        expected_multiplier=expected_multiplier,
    )
    trades: list[ReplayTrade] = []
    exit_reasons: Counter[str] = Counter()
    current: dict[str, Any] | None = None
    gaps = 0
    stopped_reason: str | None = None
    previous_at: datetime | None = None
    previous_row: dict[str, Any] | None = None

    for row in samples:
        at = _timestamp(row["logged_at"])
        observation = session_observation_for_sample(row)
        session_changed = (
            previous_row is not None
            and row.get("session_id") != previous_row.get("session_id")
        )
        if previous_at is not None:
            gap = (at - previous_at).total_seconds()
            if gap < 0:
                stopped_reason = "sample_order_invalid"
                break
            if session_changed:
                previous_observation = session_observation_for_sample(previous_row)
                if current is not None:
                    stopped_reason = "position_crossed_session_boundary"
                    break
                if (
                    previous_observation is None
                    or previous_observation.closing_at < previous_at
                    or previous_observation.closing_at > at
                ):
                    stopped_reason = "session_transition_unverified"
                    break
            elif gap > config.max_sample_gap_seconds:
                gaps += 1
                stopped_reason = "sample_gap_exceeded"
                break
        previous_at = at
        previous_row = row
        cutoff_at = observation.closing_at - timedelta(seconds=config.entry_cutoff_seconds)
        in_entry_cutoff = at >= cutoff_at

        if current is not None:
            if at <= current["at"]:
                continue
            if (
                _decimal(row.get("variational_qty")) < current["var_qty"]
                or _decimal(row.get("lighter_qty")) < current["lighter_qty"]
            ):
                exit_reasons["exit_quote_depth_below_position_size"] += 1
                continue
            gross, cost, net, net_bps = _trade_pnl(
                current, row, roundtrip_cost_bps=config.roundtrip_cost_bps
            )
            duration = (at - current["at"]).total_seconds()
            reason = "target_reached" if net_bps >= config.exit_target_bps else None
            if reason is None and duration >= config.max_hold_seconds:
                reason = "max_hold"
            if reason is None and in_entry_cutoff:
                reason = "session_entry_cutoff"
            if reason is not None:
                denominator = (
                    current["var_entry"] * current["var_qty"]
                    + current["lighter_entry"] * current["lighter_qty"]
                    * current["instrument"].lighter.exposure_multiplier
                ) / Decimal("2")
                trades.append(ReplayTrade(
                    direction=current["direction"],
                    entry_at=current["at"].isoformat(),
                    exit_at=at.isoformat(),
                    duration_seconds=round(duration, 3),
                    entry_edge_bps=str(current["edge"]),
                    gross_pnl_usd=str(gross),
                    assumed_cost_usd=str(cost),
                    net_pnl_usd=str(net),
                    net_pnl_bps=str(net_bps),
                    exit_reason=reason,
                ))
                exit_reasons[reason] += 1
                current = None
            continue

        if in_entry_cutoff:
            continue

        edges = {
            "long_var_short_lighter": _decimal(row.get("long_edge_bps")),
            "short_var_long_lighter": _decimal(row.get("short_edge_bps")),
        }
        qualified = [(direction, edge) for direction, edge in edges.items()
                     if edge is not None and edge >= config.entry_threshold_bps]
        if not qualified:
            continue
        direction, edge = max(qualified, key=lambda item: item[1])
        var_entry = _decimal(row["var_ask"] if direction == "long_var_short_lighter" else row["var_bid"])
        lighter_entry = _decimal(row["lighter_sell_price"] if direction == "long_var_short_lighter" else row["lighter_buy_price"])
        multiplier = _decimal(row["lighter_multiplier"])
        var_qty, lighter_qty = _decimal(row["variational_qty"]), _decimal(row["lighter_qty"])
        current = {
            "direction": direction,
            "at": at,
            "edge": edge,
            "var_entry": var_entry,
            "lighter_entry": lighter_entry,
            "var_qty": var_qty,
            "lighter_qty": lighter_qty,
            "instrument": replace(
                WTI_HEDGE,
                lighter=replace(WTI_HEDGE.lighter, exposure_multiplier=multiplier),
            ),
        }

    gross_total = sum((Decimal(t.gross_pnl_usd) for t in trades), Decimal("0"))
    costs_total = sum((Decimal(t.assumed_cost_usd) for t in trades), Decimal("0"))
    net_total = sum((Decimal(t.net_pnl_usd) for t in trades), Decimal("0"))
    wins = sum(Decimal(t.net_pnl_usd) > 0 for t in trades)
    return ReplayReport(
        replay_kind="indicative_quote_path_no_lookahead_session_gated",
        live_authorization=False,
        session_gated=True,
        sample_count=len(samples),
        rejected_sample_count=rejected,
        duplicate_sample_count=duplicates,
        conflicting_sample_id_count=conflicts,
        sample_gap_count=gaps,
        stopped_reason=stopped_reason,
        completed_trades=len(trades),
        unresolved_position=current is not None,
        gross_pnl_usd=str(gross_total),
        assumed_cost_usd=str(costs_total),
        net_pnl_usd=str(net_total),
        win_rate=None if not trades else str(Decimal(wins) / Decimal(len(trades))),
        exit_reasons=dict(exit_reasons),
        config={
            "entry_threshold_bps": str(config.entry_threshold_bps),
            "exit_target_bps": str(config.exit_target_bps),
            "roundtrip_cost_bps": str(config.roundtrip_cost_bps),
            "max_hold_seconds": config.max_hold_seconds,
            "max_sample_gap_seconds": config.max_sample_gap_seconds,
            "entry_cutoff_seconds": config.entry_cutoff_seconds,
            "expected_market_id": expected_market_id,
            "expected_multiplier": None if expected_multiplier is None else str(expected_multiplier),
        },
        cost_caveat=(
            "Roundtrip cost is an explicit user assumption; funding, swaps/rolls, real fills, and latency are not inferred. "
            "Only fresh explicit exchange session observations qualify; entries are blocked in the configured pre-close cutoff."
        ),
        trades=tuple(trades),
    )
