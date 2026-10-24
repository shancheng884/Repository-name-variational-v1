"""ETH-style, directional rolling thresholds for WTI gradient entries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from tools.lib.market_sessions import session_observation_for_sample


DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")
EDGE_FIELDS = {
    "long_var_short_lighter": "long_edge_bps",
    "short_var_long_lighter": "short_edge_bps",
}
ANCHOR_WINDOW_SECONDS = 7 * 24 * 60 * 60
MIN_HISTORY_SAMPLES = 100
MIN_EFFECTIVE_SECONDS = 36 * 60 * 60
THRESHOLD_REFRESH_SECONDS = 5 * 60
MIN_TIER_SPACING_BPS = Decimal("0.25")
ENTRY_PERCENTILES = (Decimal("97.5"), Decimal("98.25"))


@dataclass(frozen=True, slots=True)
class WTIGradientThresholdProfile:
    thresholds_bps: tuple[Decimal, Decimal]
    sample_count: int
    effective_seconds: int
    mature: bool
    p975_bps: Decimal | None
    p9825_bps: Decimal | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "thresholds_bps": [str(value) for value in self.thresholds_bps],
            "sample_count": self.sample_count,
            "effective_seconds": self.effective_seconds,
            "mature": self.mature,
            "p975_bps": None if self.p975_bps is None else str(self.p975_bps),
            "p9825_bps": None if self.p9825_bps is None else str(self.p9825_bps),
        }


def _decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _percentile(values: list[Decimal], percentile: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int(
        (Decimal(len(ordered) - 1) * percentile / Decimal("100")).to_integral_value(
            rounding=ROUND_HALF_UP
        )
    )
    return ordered[max(0, min(index, len(ordered) - 1))]


def build_wti_gradient_threshold_profiles(
    rows: list[dict[str, Any]],
    *,
    as_of: datetime,
    market_id: int,
    multiplier: Decimal,
    first_floor_bps: Decimal,
    second_floor_bps: Decimal,
    roundtrip_cost_bps: Decimal,
    exit_target_bps: Decimal,
    sample_interval_seconds: int = 10,
    window_seconds: int = ANCHOR_WINDOW_SECONDS,
    min_samples: int = MIN_HISTORY_SAMPLES,
    min_effective_seconds: int = MIN_EFFECTIVE_SECONDS,
) -> dict[str, WTIGradientThresholdProfile]:
    """Build no-lookahead thresholds, retaining configured tiers as hard floors."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("wti_threshold_as_of_must_be_timezone_aware")
    numeric = (multiplier, first_floor_bps, second_floor_bps, roundtrip_cost_bps, exit_target_bps)
    if any(not value.is_finite() for value in numeric):
        raise ValueError("wti_threshold_parameters_must_be_finite")
    if (
        multiplier <= 0 or first_floor_bps <= 0
        or second_floor_bps <= first_floor_bps
        or roundtrip_cost_bps < 0 or exit_target_bps < 0
        or sample_interval_seconds <= 0 or window_seconds <= 0
        or min_samples < 1 or min_effective_seconds < 0
    ):
        raise ValueError("wti_threshold_parameters_invalid")

    cutoff = as_of.astimezone(timezone.utc) - timedelta(seconds=window_seconds)
    latest_by_id: dict[str, tuple[dict[str, Any], datetime]] = {}
    conflicting_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        at = _timestamp(row.get("logged_at"))
        sample_id = str(row.get("sample_id") or "").strip()
        row_multiplier = _decimal(row.get("lighter_multiplier"))
        row_market_id = _decimal(row.get("lighter_market_id"))
        if (
            at is None or not cutoff < at < as_of.astimezone(timezone.utc)
            or not sample_id
            or str(row.get("asset") or "").upper() != "WTI"
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
            or str(row.get("lighter_market_status") or "").lower() != "active"
            or row_market_id is None or row_market_id != Decimal(market_id)
            or row_multiplier is None or row_multiplier != multiplier
            or session_observation_for_sample(row) is None
            or any(_decimal(row.get(EDGE_FIELDS[key])) is None for key in DIRECTIONS)
        ):
            continue
        old = latest_by_id.get(sample_id)
        if old is None:
            latest_by_id[sample_id] = (row, at)
        elif old[0] != row:
            conflicting_ids.add(sample_id)

    edge_values: dict[str, list[Decimal]] = {direction: [] for direction in DIRECTIONS}
    for sample_id, (row, _at) in latest_by_id.items():
        if sample_id in conflicting_ids:
            continue
        for direction in DIRECTIONS:
            edge = _decimal(row.get(EDGE_FIELDS[direction]))
            if edge is not None:
                edge_values[direction].append(edge)

    profiles: dict[str, WTIGradientThresholdProfile] = {}
    reserve = roundtrip_cost_bps + exit_target_bps
    for direction in DIRECTIONS:
        values = edge_values[direction]
        effective = min(len(values) * sample_interval_seconds, window_seconds)
        mature = len(values) >= min_samples and effective >= min_effective_seconds
        p975 = _percentile(values, ENTRY_PERCENTILES[0]) if mature else None
        p9825 = _percentile(values, ENTRY_PERCENTILES[1]) if mature else None
        first = max(first_floor_bps, (p975 + reserve) if p975 is not None else first_floor_bps)
        second_candidates = [second_floor_bps, first + MIN_TIER_SPACING_BPS]
        if p9825 is not None:
            second_candidates.append(p9825 + reserve)
        second = max(second_candidates)
        profiles[direction] = WTIGradientThresholdProfile(
            thresholds_bps=(first, second),
            sample_count=len(values),
            effective_seconds=effective,
            mature=mature,
            p975_bps=p975,
            p9825_bps=p9825,
        )
    return profiles
