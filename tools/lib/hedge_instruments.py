"""Explicit venue identities and unit-safe math for paired hedge instruments."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class VenueInstrument:
    venue: str
    symbol: str
    instrument_type: str
    settlement_asset: str
    kind: str | None = None
    funding_interval_s: int | None = None
    exposure_multiplier: Decimal = Decimal("1")


@dataclass(frozen=True, slots=True)
class HedgeInstrument:
    key: str
    var: VenueInstrument
    lighter: VenueInstrument


WTI_HEDGE = HedgeInstrument(
    key="VARIATIONAL_USOILP_SWAP__LIGHTER_WTI_PERP",
    var=VenueInstrument(
        venue="variational",
        symbol="USOILP",
        instrument_type="swap",
        settlement_asset="USDC",
        kind="commodity",
        funding_interval_s=0,
    ),
    lighter=VenueInstrument(
        venue="lighter",
        symbol="WTI",
        instrument_type="perp",
        settlement_asset="USDC",
    ),
)

HEDGE_INSTRUMENTS: dict[str, HedgeInstrument] = {"WTI": WTI_HEDGE}


def instrument_for_asset(asset: str) -> HedgeInstrument | None:
    return HEDGE_INSTRUMENTS.get(str(asset).strip().upper())


def decimal_value(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() else None


def integer_value(value: Any) -> int | None:
    parsed = decimal_value(value)
    if parsed is None or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def normalized_exposure(qty: Decimal, instrument: VenueInstrument) -> Decimal:
    if qty < 0 or instrument.exposure_multiplier <= 0:
        raise ValueError("quantity and exposure multiplier must be non-negative/positive")
    return qty * instrument.exposure_multiplier


def paired_exposure_matches(
    *,
    var_qty: Decimal,
    lighter_qty: Decimal,
    instrument: HedgeInstrument,
    tolerance: Decimal,
) -> bool:
    if tolerance < 0:
        raise ValueError("tolerance must be non-negative")
    var_exposure = normalized_exposure(var_qty, instrument.var)
    lighter_exposure = normalized_exposure(lighter_qty, instrument.lighter)
    return abs(var_exposure - lighter_exposure) <= tolerance


def paired_realized_pnl(
    *,
    direction: str,
    var_entry: Decimal,
    var_exit: Decimal,
    var_qty: Decimal,
    lighter_entry: Decimal,
    lighter_exit: Decimal,
    lighter_qty: Decimal,
    instrument: HedgeInstrument,
    var_fees: Decimal = Decimal("0"),
    lighter_fees: Decimal = Decimal("0"),
    var_funding: Decimal = Decimal("0"),
    lighter_funding: Decimal = Decimal("0"),
) -> Decimal:
    """Return net PnL; funding values are signed account cashflows."""
    if min(var_entry, var_exit, lighter_entry, lighter_exit) <= 0:
        raise ValueError("fill prices must be positive")
    if min(var_qty, lighter_qty) <= 0:
        raise ValueError("filled quantities must be positive")
    if direction == "long_var_short_lighter":
        var_pnl = (var_exit - var_entry) * var_qty * instrument.var.exposure_multiplier
        lighter_pnl = (lighter_entry - lighter_exit) * lighter_qty * instrument.lighter.exposure_multiplier
    elif direction == "short_var_long_lighter":
        var_pnl = (var_entry - var_exit) * var_qty * instrument.var.exposure_multiplier
        lighter_pnl = (lighter_exit - lighter_entry) * lighter_qty * instrument.lighter.exposure_multiplier
    else:
        raise ValueError(f"unsupported hedge direction: {direction}")
    return var_pnl + lighter_pnl - var_fees - lighter_fees + var_funding + lighter_funding


def iter_positions(payload: Any) -> tuple[dict[str, Any], ...] | None:
    """Return a confirmed position list; malformed/error responses are unknown."""
    source = payload
    for _ in range(4):
        if not isinstance(source, dict):
            return None
        if "ok" in source and source["ok"] is not True:
            return None
        for key in ("httpStatus", "status"):
            status = source.get(key)
            if status is not None:
                status_code = integer_value(status)
                if status_code is None or not 200 <= status_code < 300:
                    return None
        if source.get("code") is not None:
            if integer_value(source["code"]) != 200:
                return None
        positions = source.get("positions")
        if isinstance(positions, list):
            return tuple(positions) if all(isinstance(row, dict) for row in positions) else None
        if isinstance(positions, dict):
            nested = positions.get("positions")
            if isinstance(nested, (dict, list)):
                source = positions
                continue
            values = tuple(positions.values())
            return values if all(isinstance(row, dict) for row in values) else None
        result = source.get("result")
        if isinstance(result, dict):
            source = result
            continue
        return None
    return None


def variational_position_qty(payload: Any, instrument: HedgeInstrument) -> Decimal | None:
    """Select an exact instrument; ambiguous or identity-incomplete data fails closed."""
    matches: list[Decimal] = []
    positions = iter_positions(payload)
    if positions is None:
        return None
    for position in positions:
        nested = position.get("position_info") if isinstance(position.get("position_info"), dict) else {}
        identity = position.get("instrument")
        if not isinstance(identity, dict):
            identity = nested.get("instrument")
        if not isinstance(identity, dict):
            identity = position
        underlying = str(identity.get("underlying") or identity.get("symbol") or identity.get("asset") or "").upper()
        kind = str(
            identity.get("instrument_type")
            or identity.get("instrumentType")
            or identity.get("type")
            or ""
        ).lower()
        settlement = str(identity.get("settlement_asset") or identity.get("settlementAsset") or "").upper()
        if not underlying:
            return None
        if underlying != instrument.var.symbol:
            continue
        if not kind or not settlement:
            return None
        if kind != instrument.var.instrument_type or settlement != instrument.var.settlement_asset:
            continue
        actual_kind = str(identity.get("kind") or "").lower()
        if instrument.var.kind is not None and not actual_kind:
            return None
        if instrument.var.kind is not None and actual_kind != instrument.var.kind:
            continue
        if instrument.var.funding_interval_s is not None:
            interval = integer_value(identity.get("funding_interval_s", identity.get("fundingIntervalS")))
            if interval is None:
                return None
            if interval != instrument.var.funding_interval_s:
                continue
        value = next((decimal_value(src.get(key)) for src in (position, nested)
                      for key in ("qty", "quantity", "size", "position", "position_size", "base_amount", "amount")
                      if decimal_value(src.get(key)) is not None), None)
        if value is None:
            return None
        matches.append(value)
    if len(matches) > 1:
        return None
    return matches[0] if matches else Decimal("0")


def lighter_position_qty(payload: Any, *, market_id: int, symbol: str) -> Decimal | None:
    """Match by Lighter market id when present; reject duplicate/ambiguous rows."""
    if not isinstance(payload, dict) or not isinstance(payload.get("accounts"), list):
        return None
    expected_market_id = integer_value(market_id)
    expected_symbol = str(symbol or "").strip().upper()
    if expected_market_id is None or not expected_symbol:
        return None
    if integer_value(payload.get("code")) != 200:
        return None
    if not payload["accounts"]:
        return None
    matches: list[Decimal] = []
    for account in payload["accounts"]:
        if not isinstance(account, dict):
            return None
        account_positions = account.get("positions")
        if not isinstance(account_positions, list):
            return None
        for position in account_positions:
            if not isinstance(position, dict):
                return None
            row_market_id = position.get("market_id", position.get("market_index"))
            row_symbol = str(position.get("symbol") or "").strip().upper()
            if row_market_id is not None:
                parsed_market_id = integer_value(row_market_id)
                if parsed_market_id is None:
                    return None
                if parsed_market_id != expected_market_id:
                    if row_symbol == expected_symbol:
                        return None
                    continue
                if row_symbol and row_symbol != expected_symbol:
                    return None
            elif row_symbol == expected_symbol:
                return None
            elif row_symbol:
                continue
            else:
                return None
            qty = decimal_value(position.get("position"))
            if qty is None or qty < 0:
                return None
            raw_sign = position.get("sign")
            sign = decimal_value(raw_sign) if raw_sign is not None else None
            if qty == 0:
                if sign is not None and sign not in (Decimal("-1"), Decimal("0"), Decimal("1")):
                    return None
                matches.append(Decimal("0"))
                continue
            if sign not in (Decimal("-1"), Decimal("1")):
                return None
            matches.append(qty * sign)
    if len(matches) > 1:
        return None
    return matches[0] if matches else Decimal("0")
