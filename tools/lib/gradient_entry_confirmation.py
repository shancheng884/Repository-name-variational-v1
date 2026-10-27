"""Shared live/replay confirmation gates for gradient entries."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence


DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")
CONFIRM_SAMPLES = 2
CONFIRM_WINDOW = 3
STRONG_SINGLE_MIN_TIER = 3


def _decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def strictest_entry_threshold(*values: Any) -> Decimal | None:
    """Keep the strongest strategy and confirmation threshold for fresh execution."""
    parsed: list[Decimal] = []
    for value in values:
        if value is None:
            continue
        threshold = _decimal(value)
        if threshold is None or threshold <= 0:
            return None
        parsed.append(threshold)
    return max(parsed) if parsed else None


def elastic_addon_confirmation_allowed(
    elastic_capacity_addon: bool, confirmation_mode: Any
) -> bool:
    return not elastic_capacity_addon or confirmation_mode == "two_of_three"


def confirm_gradient_entry_with_execution_refresh(
    signal_row: dict[str, Any] | None,
    execution_row: dict[str, Any],
    thresholds_by_direction: Mapping[str, Sequence[Decimal | None]],
    history_by_direction: dict[str, list[tuple[str, int]]],
    *,
    direction: str,
    candidate_tier: int,
    allow_strong_single_probe: bool = False,
    max_refresh_age_seconds: Decimal = Decimal("2"),
) -> dict[str, Any]:
    """Confirm a baseline signal against an independent, fresh execution quote.

    Burst quotes never enter the rolling confirmation history. A strong single
    probe must independently clear its tier-3 floor on both the signal and the
    execution refresh.
    """
    if direction not in DIRECTIONS or candidate_tier < 1:
        return {"confirmed": False, "reason": "gradient_entry_confirmation_parameters_invalid"}
    if execution_row.get("sample_kind") != "burst":
        return {"confirmed": False, "reason": "gradient_entry_execution_refresh_required"}
    if execution_row.get("sample_quality") != "valid" or execution_row.get("market_availability") != "both_feeds_live":
        return {"confirmed": False, "reason": "gradient_entry_execution_refresh_invalid"}
    if execution_row.get("session_status") != "open" or execution_row.get("session_status_feed_connected") is not True:
        return {"confirmed": False, "reason": "gradient_entry_execution_session_unverified"}

    execution_id = str(execution_row.get("sample_id") or "")
    execution_at = _aware_timestamp(execution_row.get("logged_at"))
    if not execution_id or execution_at is None:
        return {"confirmed": False, "reason": "gradient_entry_execution_refresh_identity_invalid"}
    if signal_row is not None:
        signal_id = str(signal_row.get("sample_id") or "")
        signal_at = _aware_timestamp(signal_row.get("logged_at"))
        if (
            signal_row.get("sample_kind", "baseline") != "baseline"
            or signal_row.get("sample_quality") != "valid"
            or signal_row.get("market_availability") != "both_feeds_live"
            or not signal_id or signal_at is None
            or signal_id == execution_id
            or signal_row.get("session_id") != execution_row.get("session_id")
        ):
            return {"confirmed": False, "reason": "gradient_entry_signal_refresh_pair_invalid"}
        elapsed = Decimal(str((execution_at - signal_at).total_seconds()))
        if elapsed < 0 or elapsed > max_refresh_age_seconds:
            return {"confirmed": False, "reason": "gradient_entry_execution_refresh_outside_window"}
        linked_signal_id = execution_row.get("signal_sample_id")
        if linked_signal_id not in (None, "", signal_id):
            return {"confirmed": False, "reason": "gradient_entry_execution_refresh_signal_mismatch"}
        baseline = update_gradient_entry_confirmation(
            signal_row,
            thresholds_by_direction,
            history_by_direction,
            direction=direction,
            candidate_tier=candidate_tier,
            allow_strong_single_probe=allow_strong_single_probe,
        )
    else:
        baseline = {"confirmed": False, "reason": "gradient_entry_confirmation_pending"}

    refreshed = update_gradient_entry_confirmation(
        execution_row,
        thresholds_by_direction,
        history_by_direction,
        direction=direction,
        candidate_tier=candidate_tier,
    )
    # A live two-of-three signal still needs the refreshed edge to retain the
    # candidate tier; the burst is only an execution check, never a vote.
    if refreshed.get("raw_tier", 0) < candidate_tier:
        return {"confirmed": False, "reason": "gradient_entry_execution_edge_fell_below_tier",
                "raw_tier": refreshed.get("raw_tier")}
    if refreshed.get("confirmed"):
        return {**refreshed, "execution_refresh_sample_id": execution_id}

    if baseline.get("confirmation_mode") == "strong_single_probe":
        minimum_tier = max(candidate_tier, STRONG_SINGLE_MIN_TIER)
        if refreshed.get("raw_tier", 0) < minimum_tier:
            return {"confirmed": False, "reason": "gradient_strong_signal_refresh_below_required_tier",
                    "raw_tier": refreshed.get("raw_tier"),
                    "required_tier": minimum_tier}
        values = thresholds_by_direction.get(direction, ())
        required = _decimal(values[minimum_tier - 1]) if len(values) >= minimum_tier else None
        if required is None:
            return {"confirmed": False, "reason": "gradient_threshold_profile_invalid"}
        return {
            "confirmed": True,
            "direction": direction,
            "confirmation_mode": "strong_single_probe",
            "confirm_count": 1,
            "required_edge_bps": str(required),
            "raw_tier": refreshed["raw_tier"],
            "signal_sample_id": str(signal_row["sample_id"]),
            "execution_refresh_sample_id": execution_id,
        }
    return refreshed


def _aware_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def update_gradient_entry_confirmation(
    row: dict[str, Any],
    thresholds_by_direction: Mapping[str, Sequence[Decimal | None]],
    history_by_direction: dict[str, list[tuple[str, int]]],
    *,
    direction: str | None,
    candidate_tier: int,
    allow_strong_single_probe: bool = False,
    confirm_samples: int = CONFIRM_SAMPLES,
    confirm_window: int = CONFIRM_WINDOW,
    strong_single_min_tier: int = STRONG_SINGLE_MIN_TIER,
) -> dict[str, Any]:
    """Count unique baseline samples; permit one separately bounded tier-3 probe."""
    if (
        direction not in {*DIRECTIONS, None}
        or candidate_tier < 1
        or confirm_samples < 1
        or confirm_window < confirm_samples
        or strong_single_min_tier < 1
    ):
        return {"confirmed": False, "reason": "gradient_entry_confirmation_parameters_invalid"}
    sample_id = str(row.get("sample_id") or "")
    if not sample_id:
        return {"confirmed": False, "reason": "gradient_entry_confirmation_sample_id_missing"}

    raw_tiers: dict[str, int] = {}
    normalized: dict[str, list[Decimal | None]] = {}
    for key in DIRECTIONS:
        try:
            values = list(thresholds_by_direction[key])
        except (KeyError, TypeError):
            return {"confirmed": False, "reason": "gradient_threshold_profile_invalid"}
        saw_unavailable = False
        previous: Decimal | None = None
        for value in values:
            if value is None:
                saw_unavailable = True
                continue
            parsed = _decimal(value)
            if parsed is None or saw_unavailable or (previous is not None and parsed <= previous):
                return {"confirmed": False, "reason": "gradient_threshold_profile_invalid"}
            previous = parsed
        normalized[key] = values
        edge = _decimal(row.get("long_edge_bps" if key == DIRECTIONS[0] else "short_edge_bps"))
        tier = 0
        if edge is not None:
            for value in values:
                parsed = _decimal(value) if value is not None else None
                if parsed is None or edge < parsed:
                    break
                tier += 1
        raw_tiers[key] = tier

    is_baseline = row.get("sample_kind", "baseline") == "baseline"
    if is_baseline:
        for key in DIRECTIONS:
            history = history_by_direction.setdefault(key, [])
            if any(existing_id == sample_id for existing_id, _ in history):
                continue
            history.append((sample_id, raw_tiers[key]))
            del history[:-confirm_window]

    candidates = (direction,) if direction is not None else DIRECTIONS
    for key in candidates:
        if raw_tiers[key] < candidate_tier:
            continue
        history = history_by_direction.get(key, [])
        confirmed_count = sum(
            tier >= candidate_tier for _, tier in history[-confirm_window:]
        )
        if confirmed_count >= confirm_samples:
            required = _decimal(normalized[key][candidate_tier - 1]) if len(normalized[key]) >= candidate_tier else None
            if required is None:
                continue
            return {
                "confirmed": True,
                "direction": key,
                "confirmation_mode": "two_of_three",
                "confirm_count": confirmed_count,
                "required_edge_bps": str(required),
                "raw_tier": raw_tiers[key],
            }

    if is_baseline and allow_strong_single_probe:
        for key in candidates:
            if raw_tiers[key] < strong_single_min_tier:
                continue
            values = normalized[key]
            required = (
                _decimal(values[strong_single_min_tier - 1])
                if len(values) >= strong_single_min_tier else None
            )
            if required is not None:
                return {
                    "confirmed": True,
                    "direction": key,
                    "confirmation_mode": "strong_single_probe",
                    "confirm_count": 1,
                    "required_edge_bps": str(required),
                    "raw_tier": raw_tiers[key],
                }

    relevant = direction or max(DIRECTIONS, key=lambda key: raw_tiers[key])
    return {
        "confirmed": False,
        "reason": "gradient_entry_confirmation_pending",
        "direction": relevant,
        "confirm_count": sum(
            tier >= candidate_tier
            for _, tier in history_by_direction.get(relevant, [])[-confirm_window:]
        ),
        "required_confirm_samples": confirm_samples,
        "raw_tier": raw_tiers[relevant],
    }
