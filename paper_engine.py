from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


def percent_to_bps(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value * Decimal("100")


@dataclass(slots=True)
class PaperEntryCandidate:
    direction: str
    current_pct: Decimal
    median_pct: Decimal
    deviation_bps: Decimal
    sample_count: int


@dataclass(slots=True)
class PaperPositionState:
    opportunity_id: str
    asset: str
    direction: str
    entered_at_iso: str
    entered_at_monotonic: float
    entry_spread_pct: Decimal
    entry_median_pct: Decimal
    entry_deviation_bps: Decimal
    entry_var_mid: Decimal
    entry_lighter_mid: Decimal
    entry_var_execution_price: Decimal
    entry_lighter_execution_price: Decimal
    entry_var_half_spread_bps: Decimal
    entry_var_spread_cost_usd: Decimal
    entry_lighter_half_spread_bps: Decimal
    entry_lighter_taker_cost_usd: Decimal
    entry_fee_usd: Decimal
    entry_latency_drift_cost_usd: Decimal
    planned_notional_usd: Decimal
    planned_qty: Decimal


def paper_direction_values(snapshot, direction: str):
    if direction == "long_var_short_lighter":
        return snapshot.long_var_short_lighter_pct, snapshot.long_median_5m_pct, snapshot.long_sample_count_5m
    if direction == "short_var_long_lighter":
        return snapshot.short_var_long_lighter_pct, snapshot.short_median_5m_pct, snapshot.short_sample_count_5m
    return None, None, 0


def paper_entry_candidate(snapshot, min_deviation_bps: Decimal, min_samples: int):
    candidates: list[PaperEntryCandidate] = []
    for direction in ("long_var_short_lighter", "short_var_long_lighter"):
        current_pct, median_pct, sample_count = paper_direction_values(snapshot, direction)
        if current_pct is None or median_pct is None:
            continue
        deviation_bps = percent_to_bps(current_pct - median_pct)
        if deviation_bps is None:
            continue
        if deviation_bps >= min_deviation_bps and sample_count >= min_samples:
            candidates.append(
                PaperEntryCandidate(
                    direction=direction,
                    current_pct=current_pct,
                    median_pct=median_pct,
                    deviation_bps=deviation_bps,
                    sample_count=sample_count,
                )
            )
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.deviation_bps)


def paper_var_spread_cost_usd(snapshot, planned_qty: Decimal) -> Decimal:
    if snapshot.var_buy_price is None or snapshot.var_sell_price is None:
        return Decimal("0")
    return ((snapshot.var_buy_price - snapshot.var_sell_price) / Decimal("2")) * planned_qty


def paper_lighter_taker_cost_usd(snapshot, planned_qty: Decimal) -> Decimal:
    return ((snapshot.lighter_buy_fill_price - snapshot.lighter_sell_fill_price) / Decimal("2")) * planned_qty


def paper_fee_cost_usd(notional_usd: Decimal, fee_bps_per_leg: Decimal) -> Decimal:
    if fee_bps_per_leg <= 0 or notional_usd <= 0:
        return Decimal("0")
    return notional_usd * fee_bps_per_leg / Decimal("10000") * Decimal("2")


def paper_latency_drift_cost_usd(notional_usd: Decimal, latency_drift_bps: Decimal) -> Decimal:
    if latency_drift_bps <= 0 or notional_usd <= 0:
        return Decimal("0")
    return notional_usd * latency_drift_bps / Decimal("10000") * Decimal("2")


def paper_entry_execution_prices(snapshot, direction: str):
    if direction == "long_var_short_lighter":
        return snapshot.var_buy_price or snapshot.var_mid, snapshot.lighter_sell_fill_price
    if direction == "short_var_long_lighter":
        return snapshot.var_sell_price or snapshot.var_mid, snapshot.lighter_buy_fill_price
    raise ValueError(f"Unsupported paper direction: {direction}")


def paper_exit_execution_prices(snapshot, direction: str):
    if direction == "long_var_short_lighter":
        return snapshot.var_sell_price or snapshot.var_mid, snapshot.lighter_buy_fill_price
    if direction == "short_var_long_lighter":
        return snapshot.var_buy_price or snapshot.var_mid, snapshot.lighter_sell_fill_price
    raise ValueError(f"Unsupported paper direction: {direction}")
