from decimal import Decimal as D

from tools.lib.wti_sizing import size_paired_layer


def _size(*, var_price="99.8", lighter_price="100", existing="0"):
    return size_paired_layer(
        target_notional_usd=D("10"),
        max_notional_usd=D("20"),
        multiplier=D("1"),
        qty_step=D("0.001"),
        min_base_qty=D("0.060"),
        min_quote_usd=D("10"),
        var_order_price=D(var_price),
        lighter_order_price=D(lighter_price),
        existing_var_qty=D(existing),
        existing_lighter_qty=D(existing),
        existing_var_mark_price=D("99.8"),
        existing_lighter_mark_price=D("100"),
    )


def test_two_minimum_layers_fit_under_twenty_paired_cap_when_both_legs_fit():
    qty = _size(existing="0.100")
    assert qty == D("0.100")
    var_total = D("0.100") * D("99.8") + qty * D("99.8")
    lighter_total = D("0.100") * D("100") + qty * D("100")
    assert var_total <= D("20")
    assert lighter_total <= D("20")
    assert (var_total + lighter_total) / 2 <= D("20")


def test_layer_is_rejected_when_rounded_minimum_would_breach_either_leg_cap():
    assert _size(var_price="100.3", existing="0.100") is None


def test_safety_slippage_is_included_in_notional_cap():
    # The caller supplies the worst-case, slippage-adjusted execution references.
    assert _size(var_price="100.3", lighter_price="100.3", existing="0.100") is None


def test_invalid_or_nonpositive_market_inputs_fail_closed():
    assert size_paired_layer(
        target_notional_usd=D("10"), max_notional_usd=D("20"),
        multiplier=D("1"), qty_step=D("0.001"), min_base_qty=D("0.060"),
        min_quote_usd=D("10"), var_order_price=D("0"),
        lighter_order_price=D("100"),
    ) is None
