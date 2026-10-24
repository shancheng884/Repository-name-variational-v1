"""Evidence gates for WTI data review and production-rotation promotion."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from tools.lib.hedge_instruments import WTI_HEDGE, HedgeInstrument, paired_exposure_matches, paired_realized_pnl
from tools.lib.market_sessions import session_observation_for_sample


NEW_YORK = ZoneInfo("America/New_York")
EXPECTED_VAR_IDENTITY = "USOILP:swap:USDC:commodity:0"


@dataclass(frozen=True, slots=True)
class SessionEvidence:
    session_date: str
    session_id: str
    valid_samples: int
    coverage_seconds: float
    max_gap_seconds: float
    holdout: bool = False


@dataclass(frozen=True, slots=True)
class OilReadiness:
    ready_for_bounded_live_review: bool
    historical_promotion_evidence_ready: bool
    ready_for_production_rotation: bool
    market_data_ready: bool
    data_fresh: bool
    latest_sample_age_seconds: float | None
    lighter_market_id: int | None
    lighter_market_id_explicitly_configured: bool
    lighter_exposure_multiplier: str | None
    lighter_exposure_multiplier_explicitly_configured: bool
    reasons: tuple[str, ...]
    valid_sample_count: int
    unverified_session_sample_count: int
    session_count: int
    train_session_count: int
    holdout_session: str | None
    sessions: tuple[SessionEvidence, ...]
    confirmed_roundtrips: int
    rejected_roundtrips: int
    profitable_roundtrips: int
    validation_session_count: int
    validation_holdout_session: str | None
    validation_holdout_net_pnl_usd: str | None
    confirmed_net_pnl_usd: str


def _parse_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _finite_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _positive_decimal(row: dict[str, Any], key: str) -> Decimal | None:
    value = _finite_decimal(row.get(key))
    return value if value is not None and value > 0 else None


def _whole_nonnegative_int(value: Any) -> int | None:
    parsed = _finite_decimal(value)
    if parsed is None or parsed < 0 or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _valid_market_sample(
    row: dict[str, Any],
    *,
    max_quote_age_seconds: float,
    expected_lighter_market_id: int | None = None,
    expected_lighter_exposure_multiplier: Decimal | None = None,
) -> bool:
    if (
        not str(row.get("sample_id") or "").strip()
        or row.get("variational_underlying") != "USOILP"
        or row.get("variational_instrument_type") != "swap"
        or row.get("variational_instrument_kind") != "commodity"
        or _whole_nonnegative_int(row.get("variational_funding_interval_s")) != 0
        or str(row.get("lighter_symbol") or "").upper() != "WTI"
        or _whole_nonnegative_int(row.get("lighter_market_id")) is None
        or (
            expected_lighter_market_id is not None
            and _whole_nonnegative_int(row.get("lighter_market_id"))
            != expected_lighter_market_id
        )
        or (
            expected_lighter_exposure_multiplier is not None
            and _positive_decimal(row, "lighter_multiplier")
            != expected_lighter_exposure_multiplier
        )
        or row.get("market_availability") != "both_feeds_live"
    ):
        return False
    if _positive_decimal(row, "lighter_multiplier") is None:
        return False
    var_age = _finite_decimal(row.get("var_quote_age_seconds"))
    book_age = _finite_decimal(row.get("lighter_book_age_seconds"))
    if var_age is None or book_age is None:
        return False
    if not (Decimal("-1") <= var_age <= Decimal(str(max_quote_age_seconds))):
        return False
    if not (Decimal("0") <= book_age <= Decimal(str(max_quote_age_seconds))):
        return False
    for key in ("var_bid", "var_ask", "lighter_bid", "lighter_ask"):
        if _positive_decimal(row, key) is None:
            return False
    var_bid = _positive_decimal(row, "var_bid")
    var_ask = _positive_decimal(row, "var_ask")
    lighter_bid = _positive_decimal(row, "lighter_bid")
    lighter_ask = _positive_decimal(row, "lighter_ask")
    var_qty = _positive_decimal(row, "variational_qty")
    lighter_qty = _positive_decimal(row, "lighter_qty")
    if var_bid > var_ask or lighter_bid > lighter_ask or var_qty is None or lighter_qty is None:
        return False
    instrument = replace(
        WTI_HEDGE,
        var=replace(WTI_HEDGE.var, exposure_multiplier=Decimal("1")),
        lighter=replace(
            WTI_HEDGE.lighter,
            exposure_multiplier=_positive_decimal(row, "lighter_multiplier"),
        ),
    )
    return paired_exposure_matches(
        var_qty=var_qty,
        lighter_qty=lighter_qty,
        instrument=instrument,
        tolerance=Decimal("0.00000001"),
    )


def _roundtrip_pnl(
    row: dict[str, Any],
    *,
    expected_lighter_market_id: int,
    expected_var_exposure_multiplier: Decimal,
    expected_lighter_exposure_multiplier: Decimal,
    now_utc: datetime,
    paired_exposure_tolerance: Decimal,
    pnl_reconciliation_tolerance_usd: Decimal,
) -> tuple[datetime, Decimal, tuple[tuple[str, str], ...]] | None:
    if row.get("record_kind") != "oil_swap_roundtrip_validation":
        return None
    if any(row.get(key) is not True for key in (
        "fills_confirmed", "costs_account_confirmed", "paired_exposure_confirmed", "position_reconciled",
    )):
        return None
    if row.get("var_instrument_identity") != EXPECTED_VAR_IDENTITY:
        return None
    if str(row.get("lighter_symbol") or "").upper() != "WTI":
        return None
    if _whole_nonnegative_int(row.get("lighter_market_id")) != expected_lighter_market_id:
        return None

    stamp = _parse_timestamp(row.get("closed_at") or row.get("completed_at"))
    if stamp is None or stamp > now_utc:
        return None
    trade_ids: list[tuple[str, str]] = []
    for venue, key in (
        ("variational", "var_entry_trade_id"),
        ("lighter", "lighter_entry_trade_id"),
        ("variational", "var_exit_trade_id"),
        ("lighter", "lighter_exit_trade_id"),
    ):
        value = str(row.get(key) or "").strip()
        if not value:
            return None
        trade_ids.append((venue, value))
    if len(set(trade_ids)) != len(trade_ids):
        return None
    direction = str(row.get("direction") or "")
    side_tuple = tuple(
        str(row.get(key) or "").strip().lower()
        for key in ("var_entry_side", "var_exit_side", "lighter_entry_side", "lighter_exit_side")
    )
    expected_sides = {
        "long_var_short_lighter": ("buy", "sell", "sell", "buy"),
        "short_var_long_lighter": ("sell", "buy", "buy", "sell"),
    }.get(direction)
    if expected_sides is None or side_tuple != expected_sides:
        return None

    var_entry_qty = _positive_decimal(row, "var_entry_filled_qty")
    var_exit_qty = _positive_decimal(row, "var_exit_filled_qty")
    lighter_entry_qty = _positive_decimal(row, "lighter_entry_filled_qty")
    lighter_exit_qty = _positive_decimal(row, "lighter_exit_filled_qty")
    var_multiplier = _positive_decimal(row, "var_exposure_multiplier")
    lighter_multiplier = _positive_decimal(row, "lighter_exposure_multiplier")
    if None in (
        var_entry_qty,
        var_exit_qty,
        lighter_entry_qty,
        lighter_exit_qty,
        var_multiplier,
        lighter_multiplier,
    ):
        return None
    if (
        var_multiplier != expected_var_exposure_multiplier
        or lighter_multiplier != expected_lighter_exposure_multiplier
    ):
        return None
    instrument = HedgeInstrument(
        key=WTI_HEDGE.key,
        var=replace(WTI_HEDGE.var, exposure_multiplier=expected_var_exposure_multiplier),
        lighter=replace(WTI_HEDGE.lighter, exposure_multiplier=expected_lighter_exposure_multiplier),
    )
    # A verified roundtrip must return both venue positions to their entry baseline.
    if var_entry_qty != var_exit_qty or lighter_entry_qty != lighter_exit_qty:
        return None
    if not paired_exposure_matches(
        var_qty=var_entry_qty,
        lighter_qty=lighter_entry_qty,
        instrument=instrument,
        tolerance=paired_exposure_tolerance,
    ) or not paired_exposure_matches(
        var_qty=var_exit_qty,
        lighter_qty=lighter_exit_qty,
        instrument=instrument,
        tolerance=paired_exposure_tolerance,
    ):
        return None

    var_fee = _finite_decimal(row.get("var_actual_fee_usd"))
    lighter_fee = _finite_decimal(row.get("lighter_actual_fee_usd"))
    var_funding = _finite_decimal(row.get("var_funding_cashflow_usd"))
    lighter_funding = _finite_decimal(row.get("lighter_funding_cashflow_usd"))
    declared_pnl = _finite_decimal(row.get("actual_net_pnl_usd"))
    if None in (var_fee, lighter_fee, var_funding, lighter_funding, declared_pnl):
        return None
    if var_fee < 0 or lighter_fee < 0:
        return None

    try:
        pnl = paired_realized_pnl(
            direction=direction,
            var_entry=_positive_decimal(row, "var_entry_avg_price"),
            var_exit=_positive_decimal(row, "var_exit_avg_price"),
            var_qty=var_entry_qty,
            lighter_entry=_positive_decimal(row, "lighter_entry_avg_price"),
            lighter_exit=_positive_decimal(row, "lighter_exit_avg_price"),
            lighter_qty=lighter_entry_qty,
            instrument=instrument,
            var_fees=var_fee,
            lighter_fees=lighter_fee,
            var_funding=var_funding,
            lighter_funding=lighter_funding,
        )
    except (TypeError, ValueError):
        return None
    if declared_pnl * pnl < 0:
        return None
    if abs(declared_pnl - pnl) > pnl_reconciliation_tolerance_usd:
        return None
    return stamp, min(declared_pnl, pnl), tuple(trade_ids)


def assess_oil_readiness(
    rows: list[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
    minimum_sessions: int = 3,
    minimum_valid_samples_per_session: int = 1200,
    minimum_coverage_seconds: float = 6 * 3600,
    maximum_gap_seconds: float = 120,
    maximum_sample_age_seconds: float = 120,
    maximum_quote_age_seconds: float = 2,
    maximum_future_skew_seconds: float = 5,
    expected_lighter_market_id: int | None = None,
    expected_lighter_exposure_multiplier: Decimal | None = None,
    minimum_confirmed_roundtrips: int = 5,
    paired_exposure_tolerance: Decimal = Decimal("0.00000001"),
    pnl_reconciliation_tolerance_usd: Decimal = Decimal("0.01"),
) -> OilReadiness:
    """Separate historical data/review readiness from promotion evidence."""
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now_utc must include a timezone")
    if minimum_sessions < 3:
        raise ValueError("minimum_sessions must be at least three")
    if minimum_confirmed_roundtrips < 5:
        raise ValueError("minimum_confirmed_roundtrips must be at least five")
    if maximum_sample_age_seconds <= 0 or maximum_quote_age_seconds <= 0 or maximum_future_skew_seconds < 0:
        raise ValueError("sample age limits must be positive and future skew non-negative")
    if paired_exposure_tolerance < 0 or pnl_reconciliation_tolerance_usd < 0:
        raise ValueError("reconciliation tolerances must be non-negative")
    configured_market_id = (
        None
        if expected_lighter_market_id is None
        else _whole_nonnegative_int(expected_lighter_market_id)
    )
    if expected_lighter_market_id is not None and configured_market_id is None:
        raise ValueError("expected_lighter_market_id must be a non-negative integer")
    configured_lighter_multiplier = (
        None
        if expected_lighter_exposure_multiplier is None
        else _finite_decimal(expected_lighter_exposure_multiplier)
    )
    if expected_lighter_exposure_multiplier is not None and (
        configured_lighter_multiplier is None or configured_lighter_multiplier <= 0
    ):
        raise ValueError("expected_lighter_exposure_multiplier must be finite and positive")

    now = now.astimezone(timezone.utc)
    grouped: dict[str, list[datetime]] = defaultdict(list)
    grouped_date: dict[str, str] = {}
    valid_rows: list[dict[str, Any]] = []
    latest_sample: datetime | None = None
    seen_sample_ids: set[str] = set()
    sample_candidates: list[tuple[datetime, dict[str, Any], int, Decimal]] = []
    unverified_session_sample_count = 0
    sample_profiles: set[tuple[int, Decimal]] = set()
    roundtrips_by_id: dict[str, tuple[datetime, Decimal, tuple[tuple[str, str], ...]]] = {}
    rejected_roundtrips = 0
    for row in rows:
        if str(row.get("asset") or "").upper() != "WTI":
            continue
        if row.get("record_kind") == "oil_swap_roundtrip_validation":
            continue
        if str(row.get("sample_kind") or "baseline") != "baseline":
            continue
        if str(row.get("sample_quality") or "") != "valid":
            continue
        stamp = _parse_timestamp(row.get("logged_at"))
        if stamp is None or (stamp - now).total_seconds() > maximum_future_skew_seconds:
            continue
        if not _valid_market_sample(row, max_quote_age_seconds=maximum_quote_age_seconds):
            continue
        market_id = _whole_nonnegative_int(row.get("lighter_market_id"))
        lighter_multiplier = _positive_decimal(row, "lighter_multiplier")
        if market_id is None or lighter_multiplier is None:
            continue
        sample_candidates.append((stamp, row, market_id, lighter_multiplier))
        sample_profiles.add((market_id, lighter_multiplier))

    sample_market_ids = {market_id for market_id, _ in sample_profiles}
    market_id_inconsistent = configured_market_id is None and len(sample_market_ids) != 1
    active_market_id = configured_market_id
    if active_market_id is None and len(sample_market_ids) == 1:
        active_market_id = next(iter(sample_market_ids))
    market_profile_multipliers = {
        multiplier for market_id, multiplier in sample_profiles
        if market_id == active_market_id
    }
    multiplier_inconsistent = (
        configured_lighter_multiplier is None and len(market_profile_multipliers) != 1
    )
    active_lighter_multiplier = configured_lighter_multiplier
    if active_lighter_multiplier is None and len(market_profile_multipliers) == 1:
        active_lighter_multiplier = next(iter(market_profile_multipliers))
    market_identity_inconsistent = market_id_inconsistent or multiplier_inconsistent

    session_candidates: list[tuple[datetime, dict[str, Any], str]] = []
    session_closes: dict[str, set[datetime]] = defaultdict(set)
    for stamp, row, market_id, lighter_multiplier in sample_candidates:
        if (
            active_market_id is None
            or active_lighter_multiplier is None
            or market_id != active_market_id
            or lighter_multiplier != active_lighter_multiplier
        ):
            continue
        observation = session_observation_for_sample(
            row,
            maximum_future_skew_seconds=maximum_future_skew_seconds,
        )
        if observation is None:
            unverified_session_sample_count += 1
            continue
        session_candidates.append((stamp, row, observation.session_id))
        session_closes[observation.session_id].add(observation.closing_at)

    conflicting_session_ids = {
        session_id for session_id, closes in session_closes.items() if len(closes) != 1
    }
    for stamp, row, session_id in session_candidates:
        if session_id in conflicting_session_ids:
            unverified_session_sample_count += 1
            continue
        sample_id = str(row["sample_id"]).strip()
        if sample_id in seen_sample_ids:
            continue
        if not _valid_market_sample(
            row,
            max_quote_age_seconds=maximum_quote_age_seconds,
            expected_lighter_market_id=active_market_id,
            expected_lighter_exposure_multiplier=active_lighter_multiplier,
        ):
            continue
        seen_sample_ids.add(sample_id)
        valid_rows.append(row)
        latest_sample = max(latest_sample, stamp) if latest_sample is not None else stamp
        grouped[session_id].append(stamp)
        grouped_date.setdefault(
            session_id, stamp.astimezone(NEW_YORK).date().isoformat()
        )

    roundtrip_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    roundtrips_without_id: list[dict[str, Any]] = []
    for row in rows:
        if (
            str(row.get("asset") or "").upper() != "WTI"
            or row.get("record_kind") != "oil_swap_roundtrip_validation"
        ):
            continue
        roundtrip_id = str(row.get("roundtrip_id") or "").strip()
        if not roundtrip_id:
            roundtrips_without_id.append(row)
        else:
            roundtrip_groups[roundtrip_id].append(row)

    rejected_roundtrips = len(roundtrips_without_id)
    conflicting_roundtrip_ids: set[str] = set()
    roundtrip_representatives: dict[str, dict[str, Any]] = {}
    for roundtrip_id, copies in roundtrip_groups.items():
        first = copies[0]
        if any(row != first for row in copies[1:]):
            conflicting_roundtrip_ids.add(roundtrip_id)
        else:
            roundtrip_representatives[roundtrip_id] = first

    trade_id_owners: dict[tuple[str, str], set[str]] = defaultdict(set)
    for roundtrip_id, copies in roundtrip_groups.items():
        for row in copies:
            for venue, key in (
                ("variational", "var_entry_trade_id"),
                ("lighter", "lighter_entry_trade_id"),
                ("variational", "var_exit_trade_id"),
                ("lighter", "lighter_exit_trade_id"),
            ):
                trade_id = str(row.get(key) or "").strip()
                if trade_id:
                    trade_id_owners[(venue, trade_id)].add(roundtrip_id)
    for index, row in enumerate(roundtrips_without_id):
        anonymous_owner = f"__missing_roundtrip_id_{index}"
        for venue, key in (
            ("variational", "var_entry_trade_id"),
            ("lighter", "lighter_entry_trade_id"),
            ("variational", "var_exit_trade_id"),
            ("lighter", "lighter_exit_trade_id"),
        ):
            trade_id = str(row.get(key) or "").strip()
            if trade_id:
                trade_id_owners[(venue, trade_id)].add(anonymous_owner)

    reused_trade_ids = {
        owner
        for owners in trade_id_owners.values()
        if len(owners) > 1
        for owner in owners
    }
    conflicting_roundtrip_ids.update(
        owner for owner in reused_trade_ids if owner in roundtrip_groups
    )
    for roundtrip_id, copies in roundtrip_groups.items():
        rejected_roundtrips += (
            len(copies)
            if roundtrip_id in conflicting_roundtrip_ids
            else len(copies) - 1
        )

    roundtrip_conflicts_found = bool(conflicting_roundtrip_ids)
    trade_id_conflicts_found = any(len(owners) > 1 for owners in trade_id_owners.values())
    for roundtrip_id in sorted(roundtrip_representatives):
        if roundtrip_id in conflicting_roundtrip_ids:
            continue
        row = roundtrip_representatives[roundtrip_id]
        result = (
            None
            if active_market_id is None or active_lighter_multiplier is None
            else _roundtrip_pnl(
                row,
                expected_lighter_market_id=active_market_id,
                expected_var_exposure_multiplier=WTI_HEDGE.var.exposure_multiplier,
                expected_lighter_exposure_multiplier=active_lighter_multiplier,
                now_utc=now,
                paired_exposure_tolerance=paired_exposure_tolerance,
                pnl_reconciliation_tolerance_usd=pnl_reconciliation_tolerance_usd,
            )
        )
        if result is None:
            rejected_roundtrips += 1
            continue
        stamp, pnl, trade_ids = result
        roundtrips_by_id[roundtrip_id] = (stamp, pnl, trade_ids)

    sessions: list[SessionEvidence] = []
    for session_id, stamps in grouped.items():
        stamps.sort()
        gaps = [(right - left).total_seconds() for left, right in zip(stamps, stamps[1:])]
        sessions.append(
            SessionEvidence(
                session_date=grouped_date[session_id],
                session_id=session_id,
                valid_samples=len(stamps),
                coverage_seconds=(stamps[-1] - stamps[0]).total_seconds() if len(stamps) > 1 else 0.0,
                max_gap_seconds=max(gaps, default=0.0),
            )
        )

    sessions.sort(key=lambda item: grouped[item.session_id][-1])
    holdout_id = sessions[-1].session_id if sessions else None
    holdout_date = sessions[-1].session_date if sessions else None
    if holdout_date:
        sessions = [
            SessionEvidence(**{**asdict(item), "holdout": item.session_id == holdout_id})
            for item in sessions
        ]
    train_sessions = [item for item in sessions if not item.holdout]
    qualifying = [
        item for item in sessions
        if item.valid_samples >= minimum_valid_samples_per_session
        and item.coverage_seconds >= minimum_coverage_seconds
        and item.max_gap_seconds <= maximum_gap_seconds
    ]
    qualifying_ids = {item.session_id for item in qualifying}
    market_data_ready = (
        active_market_id is not None
        and not market_identity_inconsistent
        and len(qualifying_ids) >= minimum_sessions
        and sum(item.session_id in qualifying_ids for item in train_sessions) >= 2
        and holdout_id in qualifying_ids
    )
    latest_sample_age = None if latest_sample is None else (now - latest_sample).total_seconds()
    data_fresh = (
        latest_sample_age is not None
        and latest_sample_age >= -maximum_future_skew_seconds
        and latest_sample_age <= maximum_sample_age_seconds
    )
    review_ready = market_data_ready and data_fresh

    roundtrips = list(roundtrips_by_id.values())
    profitable = sum(pnl > 0 for _, pnl, _ in roundtrips)
    validation_dates: dict[str, Decimal] = defaultdict(Decimal)
    for stamp, pnl, _ in roundtrips:
        validation_dates[stamp.astimezone(NEW_YORK).date().isoformat()] += pnl
    validation_holdout = max(validation_dates) if validation_dates else None
    holdout_pnl = validation_dates.get(validation_holdout) if validation_holdout else None
    total_pnl = sum((pnl for _, pnl, _ in roundtrips), Decimal("0"))
    reasons: list[str] = []
    if not market_data_ready:
        reasons.append("insufficient_qualified_sessions_or_holdout")
    if unverified_session_sample_count:
        reasons.append("exchange_session_status_unverified_or_inconsistent")
    if active_market_id is None or market_id_inconsistent:
        reasons.append("lighter_market_id_unavailable_or_inconsistent")
    if configured_market_id is None:
        reasons.append("expected_lighter_market_id_not_configured")
    if active_lighter_multiplier is None or multiplier_inconsistent:
        reasons.append("lighter_exposure_multiplier_unavailable_or_inconsistent")
    if configured_lighter_multiplier is None:
        reasons.append("expected_lighter_exposure_multiplier_not_configured")
    if latest_sample is None:
        reasons.append("no_verified_exchange_session_samples")
    elif not data_fresh:
        reasons.append("latest_valid_sample_stale_or_future")
    if len(roundtrips) < minimum_confirmed_roundtrips:
        reasons.append("insufficient_confirmed_roundtrip_cost_evidence")
    if len(validation_dates) < 2:
        reasons.append("insufficient_independent_validation_sessions")
    if roundtrip_conflicts_found:
        reasons.append("conflicting_roundtrip_id_evidence")
    if trade_id_conflicts_found:
        reasons.append("trade_id_reused_across_roundtrips")
    if len(roundtrips) >= minimum_confirmed_roundtrips and total_pnl <= 0:
        reasons.append("confirmed_roundtrip_net_pnl_not_positive")
    if (
        len(roundtrips) >= minimum_confirmed_roundtrips
        and holdout_pnl is not None
        and holdout_pnl <= 0
    ):
        reasons.append("latest_validation_session_net_pnl_not_positive")
    historical_evidence_ready = (
        configured_market_id is not None
        and review_ready
        and not any(
            reason
            not in (
                "insufficient_qualified_sessions_or_holdout",
                "latest_valid_sample_stale_or_future",
            )
            for reason in reasons
        )
    )
    return OilReadiness(
        ready_for_bounded_live_review=review_ready,
        historical_promotion_evidence_ready=historical_evidence_ready,
        # This field is the strict historical promotion gate. The live runtime
        # separately verifies the current session, account, orders, and risk.
        ready_for_production_rotation=historical_evidence_ready,
        market_data_ready=market_data_ready,
        data_fresh=data_fresh,
        latest_sample_age_seconds=None if latest_sample_age is None else round(latest_sample_age, 3),
        lighter_market_id=active_market_id,
        lighter_market_id_explicitly_configured=configured_market_id is not None,
        lighter_exposure_multiplier=(
            None if active_lighter_multiplier is None else format(active_lighter_multiplier, "f")
        ),
        lighter_exposure_multiplier_explicitly_configured=configured_lighter_multiplier is not None,
        reasons=tuple(reasons),
        valid_sample_count=len(valid_rows),
        unverified_session_sample_count=unverified_session_sample_count,
        session_count=len(qualifying_ids),
        train_session_count=sum(item.session_id in qualifying_ids for item in train_sessions),
        holdout_session=holdout_date,
        sessions=tuple(sessions),
        confirmed_roundtrips=len(roundtrips),
        rejected_roundtrips=rejected_roundtrips,
        profitable_roundtrips=profitable,
        validation_session_count=len(validation_dates),
        validation_holdout_session=validation_holdout,
        validation_holdout_net_pnl_usd=None if holdout_pnl is None else format(holdout_pnl, "f"),
        confirmed_net_pnl_usd=format(total_pnl, "f"),
    )
