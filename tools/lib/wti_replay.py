"""Deterministic, no-lookahead WTI hedge replay over recorded quote samples.

Replay results are indicative only. Historical samples do not prove fills,
exchange session status, funding, roll cashflows, or live execution latency.
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from tools.lib.hedge_instruments import (
    WTI_HEDGE,
    paired_exposure_matches,
    paired_realized_pnl,
)
from tools.lib.gradient_entry_confirmation import (
    confirm_gradient_entry_with_execution_refresh,
    elastic_addon_confirmation_allowed,
    update_gradient_entry_confirmation,
)
from tools.lib.market_sessions import session_observation_for_sample
from tools.lib.wti_sizing import equity_gradient_slot_caps, size_paired_layer
from tools.lib.wti_thresholds import (
    THRESHOLD_REFRESH_SECONDS as WTI_THRESHOLD_REFRESH_SECONDS,
    WTIGradientThresholdProfile,
    build_wti_gradient_threshold_profiles,
)


DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    entry_threshold_bps: Decimal
    exit_target_bps: Decimal
    roundtrip_cost_bps: Decimal
    max_hold_seconds: float
    max_sample_gap_seconds: float
    entry_cutoff_seconds: float = 1800
    max_slippage_bps: Decimal = Decimal("8")
    max_order_notional_usd: Decimal = Decimal("20")


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
    layer_count: int = 1
    layer_entry_edges_bps: tuple[str, ...] = ()


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


def _linked_execution_refreshes(
    rows: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]],
    *,
    expected_market_id: int | None,
    expected_multiplier: Decimal | None,
) -> tuple[dict[str, dict[str, Any]], int]:
    """Index valid, unique burst quotes linked to a baseline signal within 2s."""
    signals = {str(row.get("sample_id")): row for row in baseline_rows}
    candidates: dict[str, dict[str, Any]] = {}
    conflicted_signals: set[str] = set()
    seen_refresh_ids: dict[str, dict[str, Any]] = {}
    valid_count = 0
    for row in rows:
        if not isinstance(row, dict) or row.get("sample_kind") != "burst":
            continue
        refresh_id = str(row.get("sample_id") or "").strip()
        signal_id = str(row.get("signal_sample_id") or "").strip()
        signal = signals.get(signal_id)
        if not refresh_id or not signal_id or signal is None:
            continue
        old = seen_refresh_ids.get(refresh_id)
        if old is not None:
            if old != row:
                conflicted_signals.add(str(old.get("signal_sample_id") or ""))
                conflicted_signals.add(signal_id)
            continue
        seen_refresh_ids[refresh_id] = row
        valid_as_baseline = {**row, "sample_kind": "baseline"}
        if not _valid_sample(
            valid_as_baseline,
            expected_market_id=expected_market_id,
            expected_multiplier=expected_multiplier,
        ):
            continue
        signal_at = _timestamp(signal.get("logged_at"))
        refresh_at = _timestamp(row.get("logged_at"))
        if (
            signal_at is None or refresh_at is None
            or row.get("session_id") != signal.get("session_id")
            or refresh_at < signal_at
            or (refresh_at - signal_at).total_seconds() > 2
        ):
            continue
        valid_count += 1
        if signal_id in candidates:
            conflicted_signals.add(signal_id)
        else:
            candidates[signal_id] = row
    for signal_id in conflicted_signals:
        candidates.pop(signal_id, None)
    return candidates, valid_count


def _baseline_confirmation_invalidations(
    rows: list[dict[str, Any]],
    *,
    expected_market_id: int | None,
    expected_multiplier: Decimal | None,
) -> tuple[list[tuple[datetime, str]], set[str]]:
    """Mirror live confirmation resets for first-seen, same-contract bad baselines."""
    first_seen: set[str] = set()
    invalidations: list[tuple[datetime, str]] = []
    for row in rows:
        if (
            not isinstance(row, dict)
            or str(row.get("asset") or "").upper() != "WTI"
            or row.get("record_kind") != "basis_market_sample"
            or row.get("sample_kind", "baseline") != "baseline"
        ):
            continue
        sample_id = str(row.get("sample_id") or "").strip()
        at = _timestamp(row.get("logged_at"))
        if not sample_id or at is None or sample_id in first_seen:
            continue
        market_id = _decimal(row.get("lighter_market_id"))
        multiplier = _decimal(row.get("lighter_multiplier"))
        if (
            (expected_market_id is not None and market_id is not None
             and market_id != expected_market_id)
            or (expected_multiplier is not None and multiplier is not None
                and multiplier != expected_multiplier)
            or (row.get("variational_underlying") not in (None, "USOILP"))
            or (row.get("lighter_symbol") not in (None, "WTI"))
        ):
            continue
        first_seen.add(sample_id)
        if not _valid_sample(
            row,
            expected_market_id=expected_market_id,
            expected_multiplier=expected_multiplier,
        ):
            invalidations.append((at, sample_id))
    invalidations.sort(key=lambda event: event[0])
    return invalidations, {sample_id for _, sample_id in invalidations}


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
    entry_revoked_for_session = False
    position_gap_exit_pending = False

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
                entry_revoked_for_session = False
            elif gap > config.max_sample_gap_seconds:
                gaps += 1
                if current is None:
                    entry_revoked_for_session = True
                else:
                    position_gap_exit_pending = True
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
            if reason is None and position_gap_exit_pending:
                reason = "sample_gap_requires_position_drain"
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
                position_gap_exit_pending = False
            continue

        if in_entry_cutoff or entry_revoked_for_session:
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


def replay_wti_gradient(
    rows: list[dict[str, Any]],
    *,
    config: ReplayConfig,
    second_tier_threshold_bps: Decimal,
    layer_notional_usd: Decimal,
    aggregate_max_notional_usd: Decimal,
    max_layers: int = 2,
    equity_usd: Decimal | None = None,
    expected_market_id: int | None = None,
    expected_multiplier: Decimal | None = None,
    max_completed_roundtrips: int | None = 1,
    assume_verified_flat_session_restarts: bool = False,
    adaptive_threshold_refresh_seconds: int = WTI_THRESHOLD_REFRESH_SECONDS,
    elastic_capacity_enabled: bool = False,
) -> ReplayReport:
    """Replay conservative multi-tier WTI rounds; equity is an explicit scenario input."""
    if second_tier_threshold_bps <= config.entry_threshold_bps:
        raise ValueError("gradient_second_tier_must_exceed_entry_threshold")
    if layer_notional_usd <= 0 or aggregate_max_notional_usd <= 0:
        raise ValueError("gradient_notionals_must_be_positive")
    if not config.max_slippage_bps.is_finite() or config.max_slippage_bps <= 0:
        raise ValueError("gradient_max_slippage_must_be_positive")
    if not isinstance(max_layers, int) or isinstance(max_layers, bool) or not 2 <= max_layers <= 5:
        raise ValueError("gradient_layer_count_must_be_between_two_and_five")
    if equity_usd is not None and (not equity_usd.is_finite() or equity_usd <= 0):
        raise ValueError("gradient_equity_scenario_must_be_positive")
    if equity_usd is None and max_layers == 2 and layer_notional_usd * 2 > aggregate_max_notional_usd:
        raise ValueError("gradient_layers_exceed_aggregate_notional_cap")
    if adaptive_threshold_refresh_seconds <= 0:
        raise ValueError("gradient_threshold_refresh_seconds_must_be_positive")
    if max_completed_roundtrips is not None and (
        not isinstance(max_completed_roundtrips, int)
        or isinstance(max_completed_roundtrips, bool)
        or max_completed_roundtrips < 1
    ):
        raise ValueError("gradient_max_completed_roundtrips_must_be_positive_or_none")
    baseline_inputs = [
        row for row in rows
        if not isinstance(row, dict) or row.get("sample_kind", "baseline") != "burst"
    ]
    samples, rejected, duplicates, conflicts = _canonical_rows(
        baseline_inputs,
        expected_market_id=expected_market_id,
        expected_multiplier=expected_multiplier,
    )
    confirmation_invalidations, suppressed_sample_ids = _baseline_confirmation_invalidations(
        baseline_inputs,
        expected_market_id=expected_market_id,
        expected_multiplier=expected_multiplier,
    )
    suppressed_count = sum(
        str(row.get("sample_id") or "") in suppressed_sample_ids for row in samples
    )
    if suppressed_sample_ids:
        samples = [
            row for row in samples
            if str(row.get("sample_id") or "") not in suppressed_sample_ids
        ]
        rejected += suppressed_count
    execution_refreshes, execution_refresh_count = _linked_execution_refreshes(
        rows,
        samples,
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
    entry_authorization_revoked = False
    position_gap_exit_pending = False
    rejected_depth = 0
    capped_additions = 0
    execution_refreshes_used = 0
    unmatched_strong_probe_signals = 0
    strong_probe_refresh_rejections: Counter[str] = Counter()
    threshold_history: deque[dict[str, Any]] = deque()
    threshold_profiles: dict[str, WTIGradientThresholdProfile] = {}
    threshold_refreshed_at: datetime | None = None
    confirmation_history: dict[str, list[tuple[str, int]]] = {
        "long_var_short_lighter": [],
        "short_var_long_lighter": [],
    }
    hard_floors = (
        config.entry_threshold_bps,
        second_tier_threshold_bps,
        second_tier_threshold_bps + Decimal("0.25"),
        second_tier_threshold_bps + Decimal("0.50"),
        second_tier_threshold_bps + Decimal("0.75"),
    )
    capacity_slots = None if equity_usd is None else equity_gradient_slot_caps(
        variational_equity_usd=equity_usd,
        lighter_equity_usd=equity_usd,
        child_notional_usd=layer_notional_usd,
        tier_count=max_layers,
    )
    tier_caps = tuple(
        min(aggregate_max_notional_usd, Decimal(slots) * layer_notional_usd)
        for slots in capacity_slots
    ) if capacity_slots is not None else (aggregate_max_notional_usd,) * max_layers

    def refresh_thresholds(at: datetime, current_row: dict[str, Any]) -> None:
        nonlocal threshold_profiles, threshold_refreshed_at
        if (
            threshold_refreshed_at is None
            or (at - threshold_refreshed_at).total_seconds()
            >= adaptive_threshold_refresh_seconds
        ):
            threshold_profiles = build_wti_gradient_threshold_profiles(
                list(threshold_history),
                as_of=at,
                market_id=expected_market_id if expected_market_id is not None else int(
                    _decimal(current_row.get("lighter_market_id")) or Decimal("0")
                ),
                multiplier=expected_multiplier if expected_multiplier is not None else (
                    _decimal(current_row.get("lighter_multiplier")) or Decimal("1")
                ),
                first_floor_bps=config.entry_threshold_bps,
                second_floor_bps=second_tier_threshold_bps,
                tier_floors_bps=hard_floors,
                roundtrip_cost_bps=config.roundtrip_cost_bps,
                exit_target_bps=config.exit_target_bps,
            )
            threshold_refreshed_at = at

    def make_layer(
        row: dict[str, Any], direction: str, edge: Decimal, tier: int,
        existing: dict[str, Any] | None = None,
        aggregate_cap: Decimal | None = None,
    ) -> dict[str, Any] | None:
        multiplier = _decimal(row.get("lighter_multiplier"))
        lighter_entry = _decimal(
            row["lighter_sell_price"] if direction == "long_var_short_lighter"
            else row["lighter_buy_price"]
        )
        var_entry = _decimal(
            row["var_ask"] if direction == "long_var_short_lighter" else row["var_bid"]
        )
        decimals_value = _decimal(row.get("lighter_supported_size_decimals"))
        if None in (multiplier, lighter_entry, var_entry, decimals_value):
            return None
        if decimals_value != decimals_value.to_integral_value() or not 0 <= decimals_value <= 12:
            return None
        step = Decimal("1").scaleb(-int(decimals_value))
        min_base = _decimal(row.get("lighter_min_base_amount")) or Decimal("0")
        min_quote = _decimal(row.get("lighter_min_quote_amount")) or Decimal("0")
        var_order_price = _decimal(
            row["var_ask"] if direction == "long_var_short_lighter" else row["var_bid"]
        )
        lighter_order_price = _decimal(
            row["lighter_sell_price"] if direction == "long_var_short_lighter"
            else row["lighter_buy_price"]
        )
        var_mark = max(_decimal(row["var_bid"]), _decimal(row["var_ask"]))
        lighter_mark = max(_decimal(row["lighter_bid"]), _decimal(row["lighter_ask"]))
        if any(value is None for value in (
            var_order_price, lighter_order_price, var_mark, lighter_mark,
        )):
            return None
        markup = Decimal("1") + config.max_slippage_bps / Decimal("10000")
        lighter_qty = size_paired_layer(
            target_notional_usd=layer_notional_usd,
            max_child_notional_usd=config.max_order_notional_usd,
            max_notional_usd=(
                aggregate_max_notional_usd if aggregate_cap is None else aggregate_cap
            ),
            multiplier=multiplier,
            qty_step=step,
            min_base_qty=min_base,
            min_quote_usd=min_quote,
            var_order_price=max(var_order_price, var_mark) * markup,
            lighter_order_price=max(lighter_order_price, lighter_mark) * markup,
            lighter_sizing_price=lighter_entry,
            lighter_min_quote_price=lighter_entry * (
                Decimal("1") - config.max_slippage_bps / Decimal("10000")
            ),
            allow_reduce_to_cap=existing is None,
            existing_var_qty=existing["var_qty"] if existing else Decimal("0"),
            existing_lighter_qty=existing["lighter_qty"] if existing else Decimal("0"),
            existing_var_mark_price=var_mark,
            existing_lighter_mark_price=lighter_mark,
        )
        if lighter_qty is None:
            return None
        var_qty = lighter_qty * multiplier
        if (
            lighter_qty <= 0
            or lighter_qty < min_base
            or lighter_qty * lighter_entry < min_quote
            or _decimal(row.get("variational_qty")) < var_qty
            or _decimal(row.get("lighter_qty")) < lighter_qty
        ):
            return None
        return {
            "tier": tier,
            "direction": direction,
            "edge": edge,
            "at": _timestamp(row["logged_at"]),
            "var_entry": var_entry,
            "lighter_entry": lighter_entry,
            "var_qty": var_qty,
            "lighter_qty": lighter_qty,
            "session_id": row.get("session_id"),
        }

    def refresh_aggregate(position: dict[str, Any]) -> dict[str, Any]:
        layers = position["layers"]
        var_qty = sum((layer["var_qty"] for layer in layers), Decimal("0"))
        lighter_qty = sum((layer["lighter_qty"] for layer in layers), Decimal("0"))
        var_entry = sum(
            (layer["var_entry"] * layer["var_qty"] for layer in layers), Decimal("0")
        ) / var_qty
        lighter_entry = sum(
            (layer["lighter_entry"] * layer["lighter_qty"] for layer in layers), Decimal("0")
        ) / lighter_qty
        position.update({
            "var_qty": var_qty,
            "lighter_qty": lighter_qty,
            "var_entry": var_entry,
            "lighter_entry": lighter_entry,
        })
        return position

    invalidation_index = 0
    for row in samples:
        at = _timestamp(row["logged_at"])
        assert at is not None
        while (
            invalidation_index < len(confirmation_invalidations)
            and confirmation_invalidations[invalidation_index][0] <= at
        ):
            confirmation_history = {
                "long_var_short_lighter": [],
                "short_var_long_lighter": [],
            }
            invalidation_index += 1
        refresh_thresholds(at, row)
        threshold_cutoff = at - timedelta(days=7)
        while threshold_history:
            oldest_at = _timestamp(threshold_history[0].get("logged_at"))
            if oldest_at is not None and oldest_at > threshold_cutoff:
                break
            threshold_history.popleft()
        # Refresh happens before append, so the current row never influences its own decision.
        threshold_history.append(row)
        observation = session_observation_for_sample(row)
        session_changed = (
            previous_row is not None and row.get("session_id") != previous_row.get("session_id")
        )
        if previous_at is not None:
            gap = (at - previous_at).total_seconds()
            if gap < 0:
                stopped_reason = "sample_order_invalid"
                break
            if gap > config.max_sample_gap_seconds:
                gaps += 1
                entry_authorization_revoked = True
                confirmation_history = {
                    "long_var_short_lighter": [],
                    "short_var_long_lighter": [],
                }
                if current is not None:
                    position_gap_exit_pending = True
            if session_changed:
                confirmation_history = {
                    "long_var_short_lighter": [],
                    "short_var_long_lighter": [],
                }
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
                if assume_verified_flat_session_restarts:
                    # This explicitly models a supervisor restart after its independent
                    # dual-account flat/order reconciliation; quotes alone do not prove it.
                    entry_authorization_revoked = False
        previous_at, previous_row = at, row
        cutoff = observation.closing_at - timedelta(seconds=config.entry_cutoff_seconds)
        in_cutoff = at >= cutoff
        confirmation_thresholds = {
            direction: threshold_profiles[direction].thresholds_bps
            for direction in DIRECTIONS
        }
        update_gradient_entry_confirmation(
            row,
            confirmation_thresholds,
            confirmation_history,
            direction=None,
            candidate_tier=1,
        )

        if current is not None:
            if at <= current["at"]:
                continue
            if (
                _decimal(row.get("variational_qty")) < current["var_qty"]
                or _decimal(row.get("lighter_qty")) < current["lighter_qty"]
            ):
                exit_reasons["exit_quote_depth_below_position_size"] += 1
                continue
            instrument = replace(
                WTI_HEDGE,
                lighter=replace(
                    WTI_HEDGE.lighter,
                    exposure_multiplier=current["instrument"].lighter.exposure_multiplier,
                ),
            )
            gross, cost, net, net_bps = _trade_pnl(
                current, row, roundtrip_cost_bps=config.roundtrip_cost_bps
            )
            duration = (at - current["at"]).total_seconds()
            reason = "target_reached" if net_bps >= config.exit_target_bps else None
            if reason is None and position_gap_exit_pending:
                reason = "sample_gap_requires_position_drain"
            if reason is None and duration >= config.max_hold_seconds:
                reason = "max_hold"
            if reason is None and in_cutoff:
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
                    entry_edge_bps=str(current["layers"][0]["edge"]),
                    gross_pnl_usd=str(gross),
                    assumed_cost_usd=str(cost),
                    net_pnl_usd=str(net),
                    net_pnl_bps=str(net_bps),
                    exit_reason=reason,
                    layer_count=len(current["layers"]),
                    layer_entry_edges_bps=tuple(str(layer["edge"]) for layer in current["layers"]),
                ))
                exit_reasons[reason] += 1
                current = None
                position_gap_exit_pending = False
                confirmation_history = {
                    "long_var_short_lighter": [],
                    "short_var_long_lighter": [],
                }
                if (
                    max_completed_roundtrips is not None
                    and len(trades) >= max_completed_roundtrips
                ):
                    break
                continue
            # In the equity-capacity model max_layers counts risk tiers, not
            # child orders. Preserve the one-child-per-tier legacy replay.
            if not in_cutoff and (capacity_slots is not None or len(current["layers"]) < max_layers):
                edge = _decimal(row.get(
                    "long_edge_bps" if current["direction"] == "long_var_short_lighter"
                    else "short_edge_bps"
                ))
                direction_thresholds = threshold_profiles[current["direction"]].thresholds_bps
                opened_tiers = [layer["tier"] for layer in current["layers"]]
                active_tier = max(opened_tiers)
                elastic_capacity_addon = False
                current_notional: Decimal | None = None
                if capacity_slots is None:
                    # Preserve the legacy one-child-per-tier replay mode.
                    tier = len(current["layers"]) + 1
                else:
                    tier = active_tier
                    mark_var = max(_decimal(row.get("var_bid")) or Decimal("0"),
                                   _decimal(row.get("var_ask")) or Decimal("0"))
                    mark_lighter = max(_decimal(row.get("lighter_bid")) or Decimal("0"),
                                       _decimal(row.get("lighter_ask")) or Decimal("0"))
                    current_notional = max(
                        current["var_qty"] * mark_var,
                        current["lighter_qty"] * mark_lighter
                        * current["instrument"].lighter.exposure_multiplier,
                    )
                    if current_notional + layer_notional_usd >= tier_caps[tier - 1]:
                        elastic_used = any(
                            layer.get("elastic_capacity_addon") is True
                            for layer in current["layers"]
                        )
                        elastic_threshold = direction_thresholds[0]
                        if (
                            elastic_capacity_enabled
                            and tier == 1
                            and not elastic_used
                            and len(tier_caps) > 1
                            and tier_caps[1] > current_notional + layer_notional_usd
                            and elastic_threshold is not None
                            and edge is not None
                            and edge >= elastic_threshold + Decimal("0.25")
                        ):
                            elastic_capacity_addon = True
                        else:
                            tier += 1
                tier_threshold = (
                    direction_thresholds[tier - 1]
                    if 1 <= tier <= len(direction_thresholds) else None
                )
                if elastic_capacity_addon and tier_threshold is not None:
                    tier_threshold += Decimal("0.25")
                if (
                    tier <= max_layers
                    and tier_threshold is not None
                    and edge is not None and edge >= tier_threshold
                ):
                    strong_probe_used = any(
                        layer.get("confirmation_mode") == "strong_single_probe"
                        for layer in current["layers"]
                    )
                    confirmation = update_gradient_entry_confirmation(
                        row,
                        confirmation_thresholds,
                        confirmation_history,
                        direction=current["direction"],
                        candidate_tier=tier,
                        allow_strong_single_probe=not strong_probe_used,
                    )
                    layer_row = row
                    if confirmation.get("confirmation_mode") == "strong_single_probe":
                        refreshed = execution_refreshes.get(str(row.get("sample_id") or ""))
                        if refreshed is None:
                            unmatched_strong_probe_signals += 1
                            strong_probe_refresh_rejections["linked_execution_refresh_missing"] += 1
                            continue
                        confirmation = confirm_gradient_entry_with_execution_refresh(
                            row,
                            refreshed,
                            confirmation_thresholds,
                            confirmation_history,
                            direction=current["direction"],
                            candidate_tier=tier,
                            allow_strong_single_probe=not strong_probe_used,
                        )
                        if not confirmation.get("confirmed"):
                            strong_probe_refresh_rejections[str(
                                confirmation.get("reason") or "linked_execution_refresh_rejected"
                            )] += 1
                            continue
                        layer_row = refreshed
                        edge = _decimal(refreshed.get(
                            "long_edge_bps" if current["direction"] == "long_var_short_lighter"
                            else "short_edge_bps"
                        ))
                        if edge is None or edge < tier_threshold:
                            strong_probe_refresh_rejections["execution_edge_below_tier_threshold"] += 1
                            continue
                        execution_refreshes_used += 1
                    if not confirmation.get("confirmed"):
                        continue
                    if not elastic_addon_confirmation_allowed(
                        elastic_capacity_addon,
                        confirmation.get("confirmation_mode"),
                    ):
                        continue
                    layer = make_layer(
                        layer_row, current["direction"], edge, tier, current,
                        aggregate_cap=(tier_caps[tier] if elastic_capacity_addon else tier_caps[tier - 1]),
                    )
                    if layer is None:
                        capped_additions += 1
                    else:
                        layer["confirmation_mode"] = confirmation["confirmation_mode"]
                        if elastic_capacity_addon:
                            layer["elastic_capacity_addon"] = True
                        current["layers"].append(layer)
                        refresh_aggregate(current)
            continue

        if in_cutoff or entry_authorization_revoked:
            continue
        edges = {
            "long_var_short_lighter": _decimal(row.get("long_edge_bps")),
            "short_var_long_lighter": _decimal(row.get("short_edge_bps")),
        }
        qualified = [
            (direction, edge)
            for direction, edge in edges.items()
            if edge is not None
            and threshold_profiles[direction].thresholds_bps[0] is not None
            and edge >= threshold_profiles[direction].thresholds_bps[0]
        ]
        if not qualified:
            continue
        direction, edge = max(qualified, key=lambda item: item[1])
        confirmation = update_gradient_entry_confirmation(
            row,
            confirmation_thresholds,
            confirmation_history,
            direction=direction,
            candidate_tier=1,
            allow_strong_single_probe=True,
        )
        if not confirmation.get("confirmed"):
            continue
        entry_row = row
        if confirmation.get("confirmation_mode") == "strong_single_probe":
            refreshed = execution_refreshes.get(str(row.get("sample_id") or ""))
            if refreshed is None:
                unmatched_strong_probe_signals += 1
                strong_probe_refresh_rejections["linked_execution_refresh_missing"] += 1
                continue
            confirmation = confirm_gradient_entry_with_execution_refresh(
                row,
                refreshed,
                confirmation_thresholds,
                confirmation_history,
                direction=direction,
                candidate_tier=1,
                allow_strong_single_probe=True,
            )
            if not confirmation.get("confirmed"):
                strong_probe_refresh_rejections[str(
                    confirmation.get("reason") or "linked_execution_refresh_rejected"
                )] += 1
                continue
            edge = _decimal(refreshed.get(
                "long_edge_bps" if direction == "long_var_short_lighter" else "short_edge_bps"
            ))
            if edge is None:
                strong_probe_refresh_rejections["execution_edge_unavailable"] += 1
                continue
            entry_row = refreshed
            execution_refreshes_used += 1
        minimum_edge = _decimal(confirmation.get("required_edge_bps"))
        if minimum_edge is None or edge < minimum_edge:
            continue
        multiplier = _decimal(row.get("lighter_multiplier"))
        current = {
            "direction": direction,
            "at": _timestamp(entry_row.get("logged_at")) or at,
            "edge": edge,
            "instrument": replace(
                WTI_HEDGE,
                lighter=replace(WTI_HEDGE.lighter, exposure_multiplier=multiplier),
            ),
            "layers": [],
        }
        layer = make_layer(
            entry_row, direction, edge, 1, aggregate_cap=tier_caps[0],
        )
        if layer is None:
            current = None
            rejected_depth += 1
            continue
        layer["confirmation_mode"] = confirmation["confirmation_mode"]
        current["layers"].append(layer)
        refresh_aggregate(current)

    gross_total = sum((Decimal(trade.gross_pnl_usd) for trade in trades), Decimal("0"))
    costs_total = sum((Decimal(trade.assumed_cost_usd) for trade in trades), Decimal("0"))
    net_total = sum((Decimal(trade.net_pnl_usd) for trade in trades), Decimal("0"))
    wins = sum(Decimal(trade.net_pnl_usd) > 0 for trade in trades)
    return ReplayReport(
        replay_kind=(
            f"indicative_continuous_{max_layers}_tier_gradient_no_lookahead"
            if max_completed_roundtrips is None
            else f"indicative_bounded_{max_layers}_tier_gradient_no_lookahead"
        ),
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
        exit_reasons={**dict(exit_reasons),
                      **({"gradient_add_rejected_depth": rejected_depth} if rejected_depth else {}),
                      **({"gradient_add_aggregate_cap": capped_additions} if capped_additions else {})},
        config={
            "entry_threshold_bps": str(config.entry_threshold_bps),
            "second_tier_threshold_bps": str(second_tier_threshold_bps),
            "exit_target_bps": str(config.exit_target_bps),
            "roundtrip_cost_bps": str(config.roundtrip_cost_bps),
            "layer_notional_usd": str(layer_notional_usd),
            "max_order_notional_usd": str(config.max_order_notional_usd),
            "aggregate_max_notional_usd": str(aggregate_max_notional_usd),
            "max_layers": max_layers,
            "equity_scenario_usd": None if equity_usd is None else str(equity_usd),
            "max_equity_multiple": "5" if equity_usd is not None else None,
            "tier_capacity_notional_usd": [str(value) for value in tier_caps],
            "max_completed_roundtrips": max_completed_roundtrips,
            "assume_verified_flat_session_restarts": assume_verified_flat_session_restarts,
            "max_hold_seconds": config.max_hold_seconds,
            "max_sample_gap_seconds": config.max_sample_gap_seconds,
            "max_slippage_bps": str(config.max_slippage_bps),
            "aggregate_notional_basis": "paired_mean_with_per_leg_cap",
            "entry_cutoff_seconds": config.entry_cutoff_seconds,
            "expected_market_id": expected_market_id,
            "expected_multiplier": None if expected_multiplier is None else str(expected_multiplier),
            "gradient_threshold_mode": "eth_style_directional_7d_percentiles_with_wti_floors",
            "gradient_threshold_percentiles": ["97.5", "98.25", "99", "99.5", "99.8"][:max_layers],
            "gradient_threshold_refresh_seconds": adaptive_threshold_refresh_seconds,
            "gradient_threshold_sample_floor": 100,
            "gradient_threshold_effective_seconds_floor": 129600,
            "entry_confirmation": "two_of_three_or_single_tier_three_probe_with_linked_2s_execution_refresh",
            "invalid_baseline_confirmation_resets": len(confirmation_invalidations),
            "execution_refresh_replayed": execution_refreshes_used > 0,
            "execution_refresh_samples_available": execution_refresh_count,
            "execution_refresh_samples_used": execution_refreshes_used,
            "unmatched_strong_probe_signals": unmatched_strong_probe_signals,
            "strong_probe_refresh_rejections": dict(strong_probe_refresh_rejections),
            "elastic_capacity_enabled": elastic_capacity_enabled,
        },
        cost_caveat=(
            f"Sequential {max_layers}-tier rounds with the configured completed-round limit. Quote-path only; no historical fills, fees, funding, latency, or market impact are inferred. "
            "Sample-size/depth, exchange-session, process-latched sample-gap, cutoff, hold, slippage, paired-notional, and per-leg caps remain active. Invalid same-contract baseline rows reset the confirmation window as in live processing. Execution-time refreshes and queue delay cannot be reconstructed from historical samples and are not counted as signal confirmations; the live two-second gate remains independently enforced. Session re-entry is modeled only when the explicit supervisor-restart assumption is enabled; historical quotes do not prove the external flat-account reconciliation."
        ),
        trades=tuple(trades),
    )
