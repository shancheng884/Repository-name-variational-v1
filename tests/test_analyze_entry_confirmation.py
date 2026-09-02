from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.analyze_entry_confirmation import (
    DIRECTION_SHORT,
    evaluate_single_sample_episodes,
    find_unconfirmed_single_sample_episodes,
)


def _row(
    offset_seconds: int,
    *,
    edge: str,
    raw_tier: int,
    active_tier: int,
    var_bid: str = "100",
    var_ask: str = "100.01",
    lighter_buy: str = "99.95",
    lighter_sell: str = "99.94",
) -> dict:
    observed_at = datetime(2026, 9, 2, tzinfo=timezone.utc) + timedelta(
        seconds=offset_seconds
    )
    return {
        "_observed_at": observed_at,
        "event": "live_inventory_basis_state",
        "logged_at": observed_at.isoformat(),
        "run_id": "run-1",
        "asset": "ETH",
        "open_lots_total": 0,
        "v4_health_ready": True,
        "v4_entry_direction": DIRECTION_SHORT,
        "v4_direction_edges_bps": {DIRECTION_SHORT: edge},
        "v4_direction_thresholds_bps": {DIRECTION_SHORT: "1"},
        "v4_real_gradient_market_tier": raw_tier,
        "v4_real_gradient_active_tier": active_tier,
        "var_quote_age_seconds": "0.1",
        "lighter_book_age_seconds": "0.1",
        "var_bid": var_bid,
        "var_ask": var_ask,
        "lighter_buy_price": lighter_buy,
        "lighter_sell_price": lighter_sell,
    }


def test_find_unconfirmed_single_sample_excludes_confirmed_episode() -> None:
    rows = [
        _row(0, edge="4", raw_tier=3, active_tier=0),
        _row(5, edge="0", raw_tier=0, active_tier=0),
        _row(10, edge="4", raw_tier=3, active_tier=0),
        _row(15, edge="4", raw_tier=3, active_tier=3),
        _row(20, edge="0", raw_tier=0, active_tier=0),
    ]

    episodes = find_unconfirmed_single_sample_episodes(rows)

    assert len(episodes) == 1
    assert episodes[0]["entry_index"] == 0


def test_evaluate_single_sample_reports_three_bps_counterfactual_hit() -> None:
    rows = [
        _row(0, edge="4", raw_tier=3, active_tier=0),
        _row(
            5,
            edge="0",
            raw_tier=0,
            active_tier=0,
            var_ask="99.90",
            lighter_sell="99.95",
        ),
        _row(
            30,
            edge="0",
            raw_tier=0,
            active_tier=0,
            var_ask="99.92",
            lighter_sell="99.95",
        ),
    ]

    results = evaluate_single_sample_episodes(
        rows,
        target_bps=Decimal("3"),
        shortfall_reserve_bps=Decimal("0.5"),
        max_horizon_seconds=60,
    )

    assert len(results) == 1
    assert results[0]["target_hit"] is True
    assert results[0]["target_after_seconds"] == Decimal("5.0")
    assert results[0]["net_mfe_bps"] == Decimal("9.5")
