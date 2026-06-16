from decimal import Decimal

from inventory_engine import DIRECTION_LONG_VAR_SHORT_LIGHTER, DIRECTION_SHORT_VAR_LONG_LIGHTER, PaperInventoryEngine
from tests.test_paper_fresh_quote_median_inventory import _sample
from tools.paper_fresh_quote_basis_inventory import (
    EwmaBasisState,
    basis_bps,
    direction_signal,
    entry_direction,
    entry_roundtrip_cost_allowed,
    roundtrip_pnl_bps,
    state_row,
)


def test_basis_bps_uses_mid_prices() -> None:
    sample = _sample(ask="100", lighter_bid="101")

    result = basis_bps(sample)

    assert result is not None
    assert result < 0


def test_ewma_basis_signal_is_strictly_causal() -> None:
    state = EwmaBasisState(
        half_life_seconds=10,
        warmup_samples=2,
        gap_reset_seconds=120,
        sigma_floor_bps=0,
    )

    z1, warm1 = state.update(1, 1.0)
    z2, warm2 = state.update(2, 2.0)
    z3, warm3 = state.update(3, 10.0)

    assert z1 == 0.0
    assert warm1 is False
    assert warm2 is False
    assert warm3 is True
    assert z3 > 0


def test_entry_direction_maps_z_to_inventory_direction() -> None:
    assert entry_direction(4.1, warm=True, z_entry=4) == DIRECTION_SHORT_VAR_LONG_LIGHTER
    assert entry_direction(-4.1, warm=True, z_entry=4) == DIRECTION_LONG_VAR_SHORT_LIGHTER
    assert entry_direction(3.9, warm=True, z_entry=4) is None
    assert entry_direction(10, warm=False, z_entry=4) is None


def test_direction_signal_is_positive_for_active_direction() -> None:
    assert direction_signal(DIRECTION_SHORT_VAR_LONG_LIGHTER, 3.5) == Decimal("3.5")
    assert direction_signal(DIRECTION_LONG_VAR_SHORT_LIGHTER, -3.5) == Decimal("3.5")


def test_roundtrip_pnl_bps_measures_immediate_exit_cost() -> None:
    result = roundtrip_pnl_bps(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        var_entry_price=Decimal("100"),
        lighter_entry_price=Decimal("101"),
        var_exit_price=Decimal("99"),
        lighter_exit_price=Decimal("102"),
    )

    assert result == Decimal("-200")


def test_entry_roundtrip_cost_gate_blocks_expensive_new_entries() -> None:
    allowed = entry_roundtrip_cost_allowed(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        var_entry_price=Decimal("100"),
        lighter_entry_price=Decimal("101"),
        var_exit_price=Decimal("99.98"),
        lighter_exit_price=Decimal("101"),
        max_entry_roundtrip_cost_bps=Decimal("3"),
    )
    blocked = entry_roundtrip_cost_allowed(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        var_entry_price=Decimal("100"),
        lighter_entry_price=Decimal("101"),
        var_exit_price=Decimal("99.94"),
        lighter_exit_price=Decimal("101.01"),
        max_entry_roundtrip_cost_bps=Decimal("3"),
    )

    assert allowed is True
    assert blocked is False


def test_state_row_serializes_basis_fields() -> None:
    sample = _sample(ask="100", lighter_bid="101")
    state = EwmaBasisState(
        half_life_seconds=10,
        warmup_samples=1,
        gap_reset_seconds=120,
        sigma_floor_bps=0,
    )
    basis = basis_bps(sample)
    z, warm = state.update(1, float(basis))
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("20"),
        max_lots=2,
        max_total_lots=2,
        entry_bps=Decimal("4"),
        exit_bps=Decimal("0"),
        min_hold_samples=1,
    )

    row = state_row(
        sample=sample,
        engine=engine,
        sample_index=0,
        events=[],
        basis=basis,
        z=z,
        warm=warm,
        state=state,
        signal_direction=None,
    )

    assert row["event"] == "fresh_quote_basis_inventory_paper_state"
    assert row["basis_bps"] is not None
    assert row["long_roundtrip_pnl_bps"] is not None
    assert row["short_roundtrip_pnl_bps"] is not None
    assert row["open_lot_details"] == []
