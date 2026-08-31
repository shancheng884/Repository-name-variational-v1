from decimal import Decimal

from tools.backfill_pnl_volume import actual_pnl_four_leg_volume


def test_actual_pnl_four_leg_volume_uses_all_confirmed_legs() -> None:
    volume = actual_pnl_four_leg_volume(
        {
            "planned_qty": "0.01",
            "entry_var_final_fill_price": "100",
            "entry_lighter_final_fill_price": "101",
            "exit_var_final_fill_price": "99",
            "exit_lighter_final_fill_price": "100",
        }
    )

    assert volume == Decimal("4.00")
