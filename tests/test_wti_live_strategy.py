from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.lib.wti_live_strategy import decide_wti_action, decide_wti_gradient_action
from tools.lib.wti_replay import ReplayConfig


def _config(**overrides):
    values = {
        "entry_threshold_bps": Decimal("4"),
        "exit_target_bps": Decimal("5"),
        "roundtrip_cost_bps": Decimal("1"),
        "max_hold_seconds": 600,
        "max_sample_gap_seconds": 120,
        "entry_cutoff_seconds": 1800,
    }
    values.update(overrides)
    return ReplayConfig(**values)


def _row(now: datetime, *, closes_in: timedelta = timedelta(hours=2), **overrides):
    row = {
        "asset": "WTI",
        "record_kind": "basis_market_sample",
        "sample_kind": "baseline",
        "sample_quality": "valid",
        "market_availability": "both_feeds_live",
        "logged_at": now.isoformat(),
        "session_status": "open",
        "market_status": "open",
        "session_status_source": "variational_market_status_websocket",
        "session_status_feed_connected": True,
        "session_status_observed_at": now.isoformat(),
        "session_status_feed_heartbeat_at": now.isoformat(),
        "session_schedule_source": "variational_supported_assets_api",
        "session_id": "session-1",
        "session_closing_at": (now + closes_in).isoformat(),
        "var_quote_age_seconds": "0.1",
        "lighter_book_age_seconds": "0.1",
        "lighter_supported_size_decimals": 2,
        "lighter_min_base_amount": "0.01",
        "lighter_min_quote_amount": "1",
        "var_bid": "70.2",
        "var_ask": "70.3",
        "lighter_bid": "70.0",
        "lighter_ask": "70.1",
        "lighter_buy_price": "70.1",
        "lighter_sell_price": "70.0",
        "long_edge_bps": "5",
        "short_edge_bps": "3",
    }
    row.update(overrides)
    return row


def _position(now: datetime, **overrides):
    value = {
        "direction": "long_var_short_lighter",
        "opened_at": (now - timedelta(seconds=30)).isoformat(),
        "session_id": "session-1",
        "var_qty": "0.1",
        "lighter_qty": "0.1",
        "multiplier": "1",
        "var_entry_price": "70.0",
        "lighter_entry_price": "70.0",
    }
    value.update(overrides)
    return value


def test_entry_uses_same_directional_threshold_as_replay():
    now = datetime.now(timezone.utc)
    assert decide_wti_action(_row(now), None, config=_config(), now=now) == {
        "action": "open",
        "direction": "long_var_short_lighter",
        "edge_bps": "5",
    }
    assert decide_wti_action(
        _row(now, long_edge_bps="3.99"), None, config=_config(), now=now
    )["reason"] == "entry_edge_below_threshold"


def test_entry_is_blocked_when_session_status_or_freshness_is_unknown():
    now = datetime.now(timezone.utc)
    assert decide_wti_action(
        _row(now, session_status_feed_connected=False), None,
        config=_config(), now=now,
    )["action"] == "hold"
    assert decide_wti_action(
        _row(now, market_availability="partial_or_unavailable"), None,
        config=_config(), now=now,
    )["reason"] == "wti_market_sample_or_session_unverified"


def test_entry_is_blocked_during_fixed_preclose_cutoff():
    now = datetime.now(timezone.utc)
    result = decide_wti_action(
        _row(now, closes_in=timedelta(minutes=29)), None,
        config=_config(), now=now,
    )
    assert result == {"action": "hold", "reason": "session_entry_cutoff"}


def test_exit_uses_executable_pair_prices_and_cost_assumption():
    now = datetime.now(timezone.utc)
    result = decide_wti_action(
        _row(now, var_bid="70.2", lighter_buy_price="70.1"),
        _position(now), config=_config(exit_target_bps=Decimal("1")), now=now,
    )
    assert result["action"] == "close"
    assert result["reason"] == "net_target_reached"
    assert Decimal(result["estimated_gross_pnl_usd"]) == Decimal("0.01")
    assert Decimal(result["estimated_net_pnl_bps"]) > 1


def test_max_hold_and_entry_cutoff_trigger_position_exit():
    now = datetime.now(timezone.utc)
    max_hold = decide_wti_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2"),
        _position(now, opened_at=(now - timedelta(seconds=700)).isoformat()),
        config=_config(max_hold_seconds=600), now=now,
    )
    assert max_hold["action"] == "close"
    assert max_hold["reason"] == "max_hold"
    cutoff = decide_wti_action(
        _row(now, closes_in=timedelta(minutes=29), var_bid="70.0", lighter_buy_price="70.2"),
        _position(now), config=_config(), now=now,
    )
    assert cutoff["action"] == "close"
    assert cutoff["reason"] == "session_entry_cutoff"


def test_invalid_open_position_metadata_fails_closed():
    now = datetime.now(timezone.utc)
    result = decide_wti_action(
        _row(now), {"direction": "long_var_short_lighter"}, config=_config(), now=now
    )
    assert result == {"action": "hold", "reason": "wti_open_position_metadata_invalid"}


def test_existing_position_is_closed_at_session_boundary_or_without_entry_session():
    now = datetime.now(timezone.utc)
    boundary = decide_wti_action(
        _row(now, session_id="session-2", var_bid="70.0", lighter_buy_price="70.2"),
        _position(now), config=_config(), now=now,
    )
    assert boundary["action"] == "close"
    assert boundary["reason"] == "session_boundary"

    unknown = decide_wti_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2"),
        _position(now, session_id=None), config=_config(), now=now,
    )
    assert unknown["action"] == "close"
    assert unknown["reason"] == "entry_session_unverified"


def _gradient_pilot(*, status="available", opened=None, exhausted=None):
    return {
        "pilot_id": "pilot-1",
        "status": status,
        "completed_roundtrips": 0,
        "max_layers": 2,
        "layers_opened": opened or [],
        "exhausted_tiers": exhausted or [],
        "config": {
            "tier_thresholds_bps": ["4", "8"],
            "layer_notional_usd": "10",
            "max_notional_usd": "20",
            "max_slippage_bps": "8",
        },
    }


def test_gradient_initial_layer_keeps_base_threshold_and_size():
    now = datetime.now(timezone.utc)
    result = decide_wti_gradient_action(
        _row(now), None, _gradient_pilot(), config=_config(), now=now,
    )
    assert result["action"] == "open"
    assert result["gradient_tier"] == 1
    assert result["layer_notional_usd"] == "10"


def test_gradient_add_requires_higher_same_direction_edge_and_pilot_ownership():
    now = datetime.now(timezone.utc)
    position = _position(now, gradient_pilot_id="pilot-1")
    pilot = _gradient_pilot(
        status="in_position", opened=[{"tier": 1, "layer_id": "layer-1"}],
    )
    below = decide_wti_gradient_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2", long_edge_bps="7.99"),
        position, pilot, config=_config(), now=now,
    )
    assert below["action"] == "hold"
    assert below["reason"] == "exit_target_not_reached"
    reached = decide_wti_gradient_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2", long_edge_bps="8"),
        position, pilot, config=_config(), now=now,
    )
    assert reached == {
        "action": "add", "direction": "long_var_short_lighter", "edge_bps": "8",
        "reason": "gradient_tier_threshold_reached", "gradient_tier": 2,
        "layer_notional_usd": "10",
    }


def test_gradient_second_layer_remains_blocked_when_step_rounding_exceeds_cap():
    now = datetime.now(timezone.utc)
    position = _position(
        now, gradient_pilot_id="pilot-1", var_qty="0.15", lighter_qty="0.15",
        var_entry_price="70", lighter_entry_price="70",
    )
    result = decide_wti_gradient_action(
        _row(
            now, var_bid="70.0", lighter_buy_price="70.2",
            lighter_min_quote_amount="10", long_edge_bps="8",
        ),
        position,
        _gradient_pilot(
            status="in_position", opened=[{"tier": 1, "layer_id": "layer-1"}],
        ),
        config=_config(), now=now,
    )
    assert result["action"] == "hold"
    assert result["reason"] == "gradient_aggregate_notional_cap_would_be_exceeded"


def test_gradient_second_layer_blocks_when_minimum_quote_and_basis_exceed_cap():
    now = datetime.now(timezone.utc)
    position = _position(
        now, gradient_pilot_id="pilot-1", var_qty="0.102", lighter_qty="0.102",
        var_entry_price="97.826", lighter_entry_price="98.128",
    )
    result = decide_wti_gradient_action(
        _row(
            now, var_bid="97.7", var_ask="97.826",
            lighter_bid="98.128", lighter_ask="98.129",
            lighter_buy_price="98.129", lighter_sell_price="98.128",
            lighter_supported_size_decimals=3, lighter_min_base_amount="0.060",
            lighter_min_quote_amount="10", long_edge_bps="45.1",
        ),
        position,
        _gradient_pilot(
            status="in_position", opened=[{"tier": 1, "layer_id": "layer-1"}],
        ),
        config=_config(), now=now,
    )
    assert result["action"] == "hold"
    assert result["reason"] == "gradient_aggregate_notional_cap_would_be_exceeded"


def test_gradient_exit_and_session_guards_take_priority_over_add():
    now = datetime.now(timezone.utc)
    position = _position(now, gradient_pilot_id="pilot-1")
    pilot = _gradient_pilot(
        status="in_position", opened=[{"tier": 1, "layer_id": "layer-1"}],
    )
    close = decide_wti_gradient_action(
        _row(
            now, closes_in=timedelta(minutes=29), long_edge_bps="99",
            var_bid="70.0", lighter_buy_price="70.2",
        ),
        position, pilot, config=_config(), now=now,
    )
    assert close["action"] == "close"
    assert close["reason"] == "session_entry_cutoff"
    unmanaged = decide_wti_gradient_action(
        _row(now, session_id="session-2", long_edge_bps="99"),
        position, pilot, config=_config(), now=now,
    )
    assert unmanaged["action"] == "close"
    assert unmanaged["reason"] == "session_boundary"


def test_gradient_never_adds_to_unowned_or_exhausted_layer():
    now = datetime.now(timezone.utc)
    position = _position(now, gradient_pilot_id="other-pilot")
    pilot = _gradient_pilot(
        status="in_position", opened=[{"tier": 1, "layer_id": "layer-1"}],
    )
    unowned = decide_wti_gradient_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2", long_edge_bps="99"),
        position, pilot, config=_config(), now=now,
    )
    assert unowned["action"] == "hold"
    exhausted = decide_wti_gradient_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2", long_edge_bps="99"),
        _position(now, gradient_pilot_id="pilot-1"),
        _gradient_pilot(
            status="in_position", opened=[{"tier": 1, "layer_id": "layer-1"}],
            exhausted=[2],
        ),
        config=_config(), now=now,
    )
    assert exhausted["action"] == "hold"


def test_gradient_adaptive_entry_requires_eth_style_directional_threshold_above_floor():
    now = datetime.now(timezone.utc)
    profiles = {
        "long_var_short_lighter": (Decimal("30"), Decimal("45")),
        "short_var_long_lighter": (Decimal("32"), Decimal("48")),
    }
    below = decide_wti_gradient_action(
        _row(now, long_edge_bps="29", short_edge_bps="0"),
        None, _gradient_pilot(), config=_config(), now=now,
        thresholds_by_direction=profiles,
    )
    assert below["action"] == "hold"
    assert below["reason"] == "gradient_entry_below_adaptive_threshold"
    assert below["effective_tier_thresholds_bps"]["long_var_short_lighter"] == ["30", "45"]

    reached = decide_wti_gradient_action(
        _row(now, long_edge_bps="30", short_edge_bps="0"),
        None, _gradient_pilot(), config=_config(), now=now,
        thresholds_by_direction=profiles,
    )
    assert reached["action"] == "open"
    assert reached["direction"] == "long_var_short_lighter"
    assert reached["gradient_tier"] == 1


def test_gradient_adaptive_profile_cannot_lower_floor_and_controls_second_layer():
    now = datetime.now(timezone.utc)
    invalid = decide_wti_gradient_action(
        _row(now), None, _gradient_pilot(), config=_config(), now=now,
        thresholds_by_direction={
            "long_var_short_lighter": (Decimal("3"), Decimal("7")),
            "short_var_long_lighter": (Decimal("4"), Decimal("8")),
        },
    )
    assert invalid == {"action": "hold", "reason": "gradient_threshold_profile_invalid"}

    position = _position(now, gradient_pilot_id="pilot-1")
    pilot = _gradient_pilot(
        status="in_position", opened=[{"tier": 1, "layer_id": "layer-1"}],
    )
    profiles = {
        "long_var_short_lighter": (Decimal("30"), Decimal("45")),
        "short_var_long_lighter": (Decimal("32"), Decimal("48")),
    }
    below = decide_wti_gradient_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2", long_edge_bps="44"),
        position, pilot, config=_config(), now=now,
        thresholds_by_direction=profiles,
    )
    assert below["action"] == "hold"
    assert below["reason"] == "gradient_add_below_adaptive_threshold"
    assert below["gradient_threshold_bps"] == "45"

    reached = decide_wti_gradient_action(
        _row(now, var_bid="70.0", lighter_buy_price="70.2", long_edge_bps="45"),
        position, pilot, config=_config(), now=now,
        thresholds_by_direction=profiles,
    )
    assert reached["action"] == "add"
    assert reached["gradient_tier"] == 2


def test_adaptive_entry_profile_never_blocks_risk_reducing_exit():
    now = datetime.now(timezone.utc)
    result = decide_wti_gradient_action(
        _row(now, var_bid="70.2", lighter_buy_price="70.1"),
        _position(now),
        _gradient_pilot(status="in_position", opened=[{"tier": 1}]),
        config=_config(exit_target_bps=Decimal("1")), now=now,
        thresholds_by_direction={"long_var_short_lighter": (Decimal("1"),)},
    )
    assert result["action"] == "close"
    assert result["reason"] == "net_target_reached"
