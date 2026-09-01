from decimal import Decimal

from tools.analyze_sample_move_blocks import (
    DIRECTION_LONG,
    DIRECTION_SHORT,
    _shadow_pnl_bps,
    analyze_sample_move_blocks,
)


def test_shadow_pnl_uses_executable_prices_for_both_directions() -> None:
    long_candidate = {"var_ask": "100", "lighter_sell_price": "101"}
    long_followup = {"var_bid": "100.20", "lighter_buy_price": "100.70"}
    assert _shadow_pnl_bps(long_candidate, long_followup, DIRECTION_LONG) == Decimal("50")

    short_candidate = {"var_bid": "100", "lighter_buy_price": "99"}
    short_followup = {"var_ask": "99.80", "lighter_sell_price": "99.30"}
    assert _shadow_pnl_bps(short_candidate, short_followup, DIRECTION_SHORT) == Decimal("50")


def test_analyze_sample_move_block_tracks_retention_and_loss() -> None:
    common = {"run_id": "run-1", "asset": "ETH"}
    candidate = {
        **common,
        "event": "live_inventory_entry_blocked",
        "reason": "basis_sample_move_too_large",
        "logged_at": "2026-09-01T00:00:00+00:00",
        "direction": DIRECTION_LONG,
        "sample_index": 10,
        "edge_bps": "9",
        "v4_entry_threshold_bps": "6",
        "min_entry_edge_bps": "5",
        "min_abs_entry_bps": "1",
        "basis_sample_move_bps": "7",
        "var_ask": "100",
        "lighter_sell_price": "100.09",
    }
    retained = {
        **common,
        "event": "live_inventory_basis_state",
        "logged_at": "2026-09-01T00:00:05+00:00",
        "long_edge_bps": "7",
        "var_bid": "100.02",
        "lighter_buy_price": "100.06",
        "var_quote_age_seconds": "0.1",
        "lighter_book_age_seconds": "0.2",
    }
    lost = {
        **common,
        "event": "live_inventory_basis_state",
        "logged_at": "2026-09-01T00:00:10+00:00",
        "long_edge_bps": "4",
        "var_bid": "100.01",
        "lighter_buy_price": "100.08",
        "var_quote_age_seconds": "0.1",
        "lighter_book_age_seconds": "0.2",
    }

    results = analyze_sample_move_blocks([candidate, retained, lost], horizons=(5, 10))

    assert len(results) == 1
    result = results[0]
    assert result["required_edge_bps"] == Decimal("6")
    assert result["lost_after_seconds"] == Decimal("10.0")
    assert result["horizons"][5]["edge_retained"] is True
    assert result["horizons"][5]["shadow_pnl_bps"] == Decimal("5")
    assert result["horizons"][10]["edge_retained"] is False


def test_stale_followup_is_not_counted_as_retained() -> None:
    rows = [
        {
            "run_id": "run-1",
            "asset": "ETH",
            "event": "live_inventory_entry_blocked",
            "reason": "basis_sample_move_too_large",
            "logged_at": "2026-09-01T00:00:00+00:00",
            "direction": DIRECTION_SHORT,
            "edge_bps": "8",
            "v4_entry_threshold_bps": "6",
            "var_bid": "100",
            "lighter_buy_price": "99.92",
        },
        {
            "run_id": "run-1",
            "asset": "ETH",
            "event": "live_inventory_basis_state",
            "logged_at": "2026-09-01T00:00:05+00:00",
            "short_edge_bps": "9",
            "var_ask": "99.9",
            "lighter_sell_price": "99.95",
            "var_quote_age_seconds": "2",
            "lighter_book_age_seconds": "0.1",
        },
    ]

    result = analyze_sample_move_blocks(rows, horizons=(5,))[0]

    assert result["horizons"][5]["quotes_fresh"] is False
    assert result["horizons"][5]["edge_retained"] is False
    assert result["horizons"][5]["shadow_pnl_bps"] is None


def test_early_loss_marks_later_horizon_as_not_continuously_retained() -> None:
    rows = [
        {
            "run_id": "run-1",
            "asset": "ETH",
            "event": "live_inventory_entry_blocked",
            "reason": "basis_sample_move_too_large",
            "logged_at": "2026-09-01T00:00:00+00:00",
            "direction": DIRECTION_LONG,
            "edge_bps": "8",
            "v4_entry_threshold_bps": "6",
            "var_ask": "100",
            "lighter_sell_price": "100.08",
        },
        {
            "run_id": "run-1",
            "asset": "ETH",
            "event": "live_inventory_basis_state",
            "logged_at": "2026-09-01T00:00:01+00:00",
            "long_edge_bps": "4",
            "var_bid": "99.99",
            "lighter_buy_price": "100.02",
            "var_quote_age_seconds": "0.1",
            "lighter_book_age_seconds": "0.1",
        },
        {
            "run_id": "run-1",
            "asset": "ETH",
            "event": "live_inventory_basis_state",
            "logged_at": "2026-09-01T00:00:30+00:00",
            "long_edge_bps": "7",
            "var_bid": "100.01",
            "lighter_buy_price": "100.03",
            "var_quote_age_seconds": "0.1",
            "lighter_book_age_seconds": "0.1",
        },
    ]

    result = analyze_sample_move_blocks(rows, horizons=(5,))[0]

    assert result["lost_after_seconds"] == Decimal("1.0")
    assert result["horizons"][5]["observed_after_seconds"] == Decimal("1.0")
    assert result["horizons"][5]["edge_retained"] is False
