from decimal import Decimal as D

import pytest

from tools.lib.wti_sizing import (
    equity_gradient_slot_caps,
    equity_position_cap_usd,
    size_paired_layer,
)
from main import v4_real_gradient_slot_caps


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


def test_five_tier_capacity_uses_the_lower_verified_equity_and_hard_five_x_cap():
    slots = equity_gradient_slot_caps(
        variational_equity_usd=D("100"),
        lighter_equity_usd=D("80"),
        child_notional_usd=D("20"),
        tier_count=5,
    )
    assert slots == (4, 8, 12, 16, 20)
    assert [D(value) * D("20") for value in slots] == [
        D("80"), D("160"), D("240"), D("320"), D("400"),
    ]
    assert equity_position_cap_usd(D("100"), D("80")) == D("400")


def test_wti_five_tier_slots_match_eth_for_same_equity_child_size_and_leverage():
    variational_equity = D("144.741342")
    lighter_equity = D("129.06631")
    child_notional = D("20")
    wti_slots = equity_gradient_slot_caps(
        variational_equity_usd=variational_equity,
        lighter_equity_usd=lighter_equity,
        child_notional_usd=child_notional,
        tier_count=5,
        max_equity_multiple=D("5"),
    )
    eth_slots = v4_real_gradient_slot_caps(
        variational_equity_usd=variational_equity,
        lighter_equity_usd=lighter_equity,
        child_notional_usd=child_notional,
        max_venue_leverage=D("5"),
        tier_count=5,
    )
    assert wti_slots == tuple(eth_slots) == (6, 12, 19, 25, 32)


def test_equity_capacity_rounding_never_exceeds_hard_cap_or_invents_missing_equity():
    slots = equity_gradient_slot_caps(
        variational_equity_usd=D("53"), lighter_equity_usd=D("47"),
        child_notional_usd=D("20"), tier_count=5,
    )
    assert slots == (2, 4, 7, 9, 11)
    assert slots[-1] * D("20") <= D("47") * D("5")
    assert all(slots[index - 1] * D("20") <= D("47") * index for index in range(1, 6))
    assert equity_gradient_slot_caps(
        variational_equity_usd=None, lighter_equity_usd=D("100"),
        child_notional_usd=D("20"), tier_count=5,
    ) is None
    assert equity_position_cap_usd(D("100"), None) is None


@pytest.mark.parametrize("existing", ["0", "0.10"])
def test_large_aggregate_budget_never_expands_twenty_dollar_child_cap(existing):
    qty = size_paired_layer(
        target_notional_usd=D("20"), max_notional_usd=D("100"),
        max_child_notional_usd=D("20"), multiplier=D("1"), qty_step=D("0.01"),
        min_base_qty=D("0.01"), min_quote_usd=D("1"),
        var_order_price=D("100.5"), lighter_order_price=D("100.4"),
        lighter_sizing_price=D("100"), allow_reduce_to_cap=False,
        existing_var_qty=D(existing), existing_lighter_qty=D(existing),
    )
    assert qty == D("0.19")
    assert qty * D("100.5") <= D("20")


def test_child_cap_below_market_minimum_never_rounds_up_past_limit():
    assert size_paired_layer(
        target_notional_usd=D("20"), max_notional_usd=D("100"),
        max_child_notional_usd=D("20"), multiplier=D("1"), qty_step=D("0.01"),
        min_base_qty=D("0.20"), min_quote_usd=D("20.1"),
        var_order_price=D("100.5"), lighter_order_price=D("100.5"),
    ) is None
