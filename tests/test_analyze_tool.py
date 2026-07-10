from decimal import Decimal

from tools.analyze import basis_state_rows, build_basis_regimes, dynamic_cost_summary


def test_basis_state_metrics_do_not_double_count_matching_block_event() -> None:
    state = {
        "event": "live_inventory_basis_state",
        "asset": "SOL",
        "basis_bps": "-8",
        "normalized_basis_bps": "-2",
        "long_edge_bps": "8",
        "short_edge_bps": "-9",
        "normalized_long_edge_bps": "2",
        "normalized_short_edge_bps": "-3",
        "basis_sample_move_bps": "0.5",
        "var_bid": "100",
        "var_ask": "100.01",
        "lighter_bid": "100",
        "lighter_ask": "100.01",
        "z": "2",
    }
    blocked = {**state, "event": "live_inventory_entry_blocked", "reason": "basis_sample_move_too_large"}
    rows = [state, blocked]

    assert basis_state_rows(rows) == [state]
    regime = build_basis_regimes(rows)["SOL"]
    assert regime["rows"] == 1
    assert regime["blocked_count"] == 1
    assert regime["raw_edge_p95"] == Decimal("8")
    assert dynamic_cost_summary(rows, asset_filter="SOL")["rows"] == 1
