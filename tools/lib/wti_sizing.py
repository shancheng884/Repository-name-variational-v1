"""Shared conservative sizing for paired WTI exposure."""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


def size_paired_layer(
    *,
    target_notional_usd: Decimal,
    max_notional_usd: Decimal,
    multiplier: Decimal,
    qty_step: Decimal,
    min_base_qty: Decimal,
    min_quote_usd: Decimal,
    var_order_price: Decimal,
    lighter_order_price: Decimal,
    lighter_sizing_price: Decimal | None = None,
    lighter_min_quote_price: Decimal | None = None,
    allow_reduce_to_cap: bool = True,
    existing_var_qty: Decimal = Decimal("0"),
    existing_lighter_qty: Decimal = Decimal("0"),
    existing_var_mark_price: Decimal | None = None,
    existing_lighter_mark_price: Decimal | None = None,
) -> Decimal | None:
    """Return a rounded lighter quantity within paired and per-leg caps."""
    sizing_price = (
        lighter_order_price if lighter_sizing_price is None else lighter_sizing_price
    )
    min_quote_price = (
        lighter_order_price
        if lighter_min_quote_price is None else lighter_min_quote_price
    )
    values = (
        target_notional_usd, max_notional_usd, multiplier, qty_step,
        min_base_qty, min_quote_usd, var_order_price, lighter_order_price,
        sizing_price, min_quote_price, existing_var_qty, existing_lighter_qty,
    )
    if any(not value.is_finite() for value in values):
        return None
    if (
        target_notional_usd <= 0 or max_notional_usd <= 0 or multiplier <= 0
        or qty_step <= 0 or min_base_qty < 0 or min_quote_usd < 0
        or var_order_price <= 0 or lighter_order_price <= 0
        or sizing_price <= 0 or min_quote_price <= 0
        or existing_var_qty < 0 or existing_lighter_qty < 0
    ):
        return None

    existing_var_price = (
        var_order_price if existing_var_mark_price is None else existing_var_mark_price
    )
    existing_lighter_price = (
        lighter_order_price
        if existing_lighter_mark_price is None else existing_lighter_mark_price
    )
    if (
        not existing_var_price.is_finite()
        or not existing_lighter_price.is_finite()
        or existing_var_price <= 0 or existing_lighter_price <= 0
    ):
        return None
    existing_var_notional = existing_var_qty * existing_var_price
    existing_lighter_notional = (
        existing_lighter_qty * existing_lighter_price * multiplier
    )

    target_qty = max(
        target_notional_usd / (sizing_price * multiplier),
        min_base_qty,
        min_quote_usd / (min_quote_price * multiplier),
    )
    target_steps = (target_qty / qty_step).to_integral_value(rounding=ROUND_CEILING)
    target_qty = target_steps * qty_step

    remaining_var = max_notional_usd - existing_var_notional
    remaining_lighter = max_notional_usd - existing_lighter_notional
    remaining_pair = (
        Decimal("2") * max_notional_usd
        - existing_var_notional - existing_lighter_notional
    )
    max_qty = min(
        remaining_var / (var_order_price * multiplier),
        remaining_lighter / (lighter_order_price * multiplier),
        remaining_pair / ((var_order_price + lighter_order_price) * multiplier),
    )
    if max_qty <= 0:
        return None
    max_steps = (max_qty / qty_step).to_integral_value(rounding=ROUND_FLOOR)
    allowed_qty = max_steps * qty_step
    if not allow_reduce_to_cap and target_qty > allowed_qty:
        return None
    qty = min(target_qty, allowed_qty)
    if qty <= 0 or qty < min_base_qty:
        return None
    lighter_min_quote_notional = qty * min_quote_price * multiplier
    if lighter_min_quote_notional < min_quote_usd:
        return None
    var_total = existing_var_notional + qty * var_order_price * multiplier
    lighter_total = (
        existing_lighter_notional + qty * lighter_order_price * multiplier
    )
    if (
        max(var_total, lighter_total) > max_notional_usd
        or (var_total + lighter_total) / Decimal("2") > max_notional_usd
    ):
        return None
    return qty
