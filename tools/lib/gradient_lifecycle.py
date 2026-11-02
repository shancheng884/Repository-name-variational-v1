"""Shared signal-tier and reactivation rules for ETH-aligned gradients."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence


DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")
REARM_BUFFER_BPS = Decimal("0.50")
REARM_CONFIRM_SAMPLES = 3


def raw_gradient_tier(edge: Any, thresholds: Sequence[Any]) -> int:
    try:
        parsed_edge = Decimal(str(edge))
    except Exception:
        return 0
    if not parsed_edge.is_finite():
        return 0
    tier = 0
    for value in thresholds:
        if value is None:
            break
        try:
            threshold = Decimal(str(value))
        except Exception:
            break
        if not threshold.is_finite() or parsed_edge < threshold:
            break
        tier += 1
    return tier


def confirmed_gradient_tiers(
    row: Mapping[str, Any],
    thresholds_by_direction: Mapping[str, Sequence[Any]],
    history_by_direction: Mapping[str, Sequence[tuple[str, int]]],
    *,
    allow_strong_single_probe: bool = False,
    confirm_samples: int = 2,
    confirm_window: int = 3,
    strong_single_min_tier: int = 3,
) -> dict[str, int]:
    """Return the highest currently crossed tier confirmed by unique samples."""
    result: dict[str, int] = {}
    for direction in DIRECTIONS:
        edge_key = "long_edge_bps" if direction == DIRECTIONS[0] else "short_edge_bps"
        values = thresholds_by_direction.get(direction, ())
        raw_tier = raw_gradient_tier(row.get(edge_key), values)
        history = list(history_by_direction.get(direction, ()))[-confirm_window:]
        confirmed = max((tier for tier in range(1, raw_tier + 1)
                         if sum(observed >= tier for _, observed in history) >= confirm_samples),
                        default=0)
        if confirmed == 0 and allow_strong_single_probe and raw_tier >= strong_single_min_tier:
            confirmed = 1
        result[direction] = confirmed
    return result


def tier_is_armed(pilot: Mapping[str, Any], direction: str, tier: int) -> bool:
    states = pilot.get("tier_reactivation_by_direction") or {}
    direction_states = states.get(direction) or {}
    item = direction_states.get(str(tier)) or {}
    return item.get("armed", True) is True


def _timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def advance_pilot_reactivation(
    pilot: dict[str, Any],
    row: Mapping[str, Any],
    thresholds_by_direction: Mapping[str, Sequence[Any]],
    activated_tiers: Mapping[str, int],
    *,
    verified_flat: bool,
) -> bool:
    """Advance disarmed tiers and a completed round using one fresh baseline."""
    if row.get("sample_kind", "baseline") != "baseline":
        return False
    sample_id = str(row.get("sample_id") or "")
    sample_at = _timestamp(row.get("logged_at"))
    if not sample_id or sample_at is None:
        return False
    changed = False
    states_by_direction = pilot.setdefault("tier_reactivation_by_direction", {})
    current_session = str(row.get("session_id") or "") or None

    def continuity_break(item: Mapping[str, Any]) -> bool | None:
        previous_at = _timestamp(item.get("last_sample_at"))
        if previous_at is not None:
            elapsed = (sample_at - previous_at).total_seconds()
            if elapsed <= 0:
                return None
            if elapsed > 120:
                return True
        previous_session = str(item.get("last_session_id") or "") or None
        if previous_session and current_session and previous_session != current_session:
            return True
        return False

    for direction in DIRECTIONS:
        edge_key = "long_edge_bps" if direction == DIRECTIONS[0] else "short_edge_bps"
        try:
            edge = Decimal(str(row.get(edge_key)))
        except Exception:
            continue
        if not edge.is_finite():
            continue
        direction_states = states_by_direction.setdefault(direction, {})
        for tier, raw_threshold in enumerate(thresholds_by_direction.get(direction, ()), start=1):
            if raw_threshold is None:
                break
            try:
                threshold = Decimal(str(raw_threshold))
            except Exception:
                continue
            if not threshold.is_finite():
                continue
            item = direction_states.get(str(tier))
            if not isinstance(item, dict) or item.get("armed", True):
                continue
            if item.get("last_sample_id") == sample_id:
                continue
            closed_at = _timestamp(item.get("closed_at"))
            if closed_at is not None and sample_at <= closed_at:
                continue
            broken = continuity_break(item)
            if broken is None:
                continue
            if broken:
                item["reset_seen"] = False
            item["last_sample_id"] = sample_id
            item["last_sample_at"] = sample_at.isoformat()
            item["last_session_id"] = current_session
            if edge < threshold:
                item["reset_seen"] = True
            if item.get("reset_seen") and int(activated_tiers.get(direction, 0)) >= tier:
                item.update({"armed": True, "reset_seen": False,
                             "rearmed_at": sample_at.isoformat()})
            changed = True

    round_state = pilot.get("round_reactivation")
    if not isinstance(round_state, dict) or round_state.get("required") is not True or round_state.get("armed") is True:
        return changed
    if round_state.get("last_sample_id") == sample_id:
        return changed
    closed_at = _timestamp(round_state.get("closed_at"))
    if closed_at is not None and sample_at <= closed_at:
        return changed
    broken = continuity_break(round_state)
    if broken is None:
        return changed
    if broken:
        round_state["reset_confirmation_count"] = 0
    round_state["last_sample_id"] = sample_id
    round_state["last_sample_at"] = sample_at.isoformat()
    round_state["last_session_id"] = current_session
    direction = round_state.get("direction")
    opposite = (DIRECTIONS[1] if direction == DIRECTIONS[0]
                else DIRECTIONS[0] if direction == DIRECTIONS[1] else None)
    if verified_flat and opposite is not None and int(activated_tiers.get(opposite, 0)) >= 1:
        edge_key = "long_edge_bps" if opposite == DIRECTIONS[0] else "short_edge_bps"
        thresholds = thresholds_by_direction.get(opposite, ())
        edge = raw_gradient_tier(row.get(edge_key), thresholds)
        if edge >= 1:
            round_state.update({"armed": True, "armed_by": "confirmed_opposite_signal",
                                "rearmed_at": sample_at.isoformat(),
                                "reset_confirmation_count": 0})
            return True
    try:
        reset_threshold = Decimal(str(round_state["reset_threshold_bps"]))
        edge = Decimal(str(row.get(
            "long_edge_bps" if direction == DIRECTIONS[0] else "short_edge_bps"
        )))
    except Exception:
        round_state["reset_confirmation_count"] = 0
        return True
    if not edge.is_finite() or not reset_threshold.is_finite():
        round_state["reset_confirmation_count"] = 0
        return True
    count = int(round_state.get("reset_confirmation_count", 0))
    count = count + 1 if edge <= reset_threshold else 0
    round_state["reset_confirmation_count"] = count
    if count >= int(round_state.get("required_reset_samples", REARM_CONFIRM_SAMPLES)):
        round_state.update({"armed": True, "armed_by": "three_sample_reset",
                            "rearmed_at": sample_at.isoformat()})
    return True
