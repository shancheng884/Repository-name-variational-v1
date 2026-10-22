from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.lib.wti_live_strategy import decide_wti_action
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
