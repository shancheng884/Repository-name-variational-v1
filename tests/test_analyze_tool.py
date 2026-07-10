from decimal import Decimal

from tools.analyze import basis_state_rows, build_basis_regimes, build_basis_v2_replay, build_entry_semantics, dynamic_cost_summary


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


def test_entry_semantics_distinguishes_normalized_primary_from_raw_primary_filter() -> None:
    rows = [
        {
            "event": "live_inventory_basis_state",
            "run_id": "run-1",
            "logged_at": "2026-07-10T00:00:00+00:00",
            "asset": "SOL",
            "basis_bps": "-8",
            "long_edge_bps": "8",
            "short_edge_bps": "-9",
            "normalized_long_edge_bps": "2",
            "normalized_short_edge_bps": "-3",
            "var_bid": "99.9",
            "var_ask": "100",
            "lighter_buy_price": "100.1",
            "lighter_sell_price": "100.2",
            "long_stablecoin_filter_ok": True,
            "short_stablecoin_filter_ok": False,
            "basis_sample_move_ok": True,
        },
        {
            "event": "live_inventory_basis_state",
            "run_id": "run-1",
            "logged_at": "2026-07-10T00:00:01+00:00",
            "asset": "SOL",
            "basis_bps": "-8.1",
            "long_edge_bps": "8.5",
            "short_edge_bps": "-9",
            "normalized_long_edge_bps": "2.1",
            "normalized_short_edge_bps": "-3",
            "var_bid": "100",
            "var_ask": "100.1",
            "lighter_buy_price": "100",
            "lighter_sell_price": "100.2",
            "long_stablecoin_filter_ok": True,
            "short_stablecoin_filter_ok": False,
            "basis_sample_move_ok": True,
        },
    ]

    result = build_entry_semantics(
        rows,
        primary_threshold=Decimal("7"),
        min_abs_entry=Decimal("7"),
        min_normalized_filter=Decimal("1"),
    )["SOL"]

    assert result["normalized_primary_candidates"] == 0
    assert result["raw_primary_candidates"] == 2
    assert result["forward"][1]["attempts"] == 1
    assert result["forward"][1]["retained"] == 1
    assert result["forward"][1]["pnl_bps"] == [Decimal("20")]


def test_basis_v2_uses_real_time_and_does_not_require_normalized_edge() -> None:
    rows = []
    for timestamp, edge, age in (
        ("2026-07-10T00:00:00+00:00", "8", "0.1"),
        ("2026-07-10T00:00:00.100000+00:00", "8.5", "0.1"),
        ("2026-07-10T00:00:01.200000+00:00", "9", "0.1"),
    ):
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-v2",
                "logged_at": timestamp,
                "asset": "SOL",
                "long_edge_bps": edge,
                "short_edge_bps": "-2",
                "var_bid": "99.9",
                "var_ask": "100",
                "lighter_bid": "100.08",
                "lighter_ask": "100.1",
                "lighter_sell_price": "100.08",
                "lighter_buy_price": "100.1",
                "var_quote_age_seconds": age,
            }
        )

    result = build_basis_v2_replay(rows, min_raw_edge_bps=Decimal("7"))["SOL"]

    assert result["candidate_count"] == 3
    assert result["direction_counts"]["long_var_short_lighter"] == 3
    assert result["horizons"][1]["attempts"] == 2
    assert result["horizons"][1]["pnl_bps"]
    assert not result["contexts"]["short_vs_long"]["insufficient_history"][1]["pnl_bps"] == []
