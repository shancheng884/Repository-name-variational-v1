from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tools.lib.wti_thresholds import build_wti_gradient_threshold_profiles


AS_OF = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)


def _row(index: int, at: datetime, *, long_edge: int, short_edge: int = 0, **changes):
    row = {
        "asset": "WTI",
        "record_kind": "basis_market_sample",
        "sample_kind": "baseline",
        "sample_quality": "valid",
        "sample_id": f"sample-{index}",
        "logged_at": at.isoformat(),
        "market_availability": "both_feeds_live",
        "variational_underlying": "USOILP",
        "variational_instrument_type": "swap",
        "variational_instrument_kind": "commodity",
        "variational_funding_interval_s": 0,
        "lighter_symbol": "WTI",
        "lighter_market_type": "perp",
        "lighter_market_id": 145,
        "lighter_market_status": "active",
        "lighter_multiplier": "1",
        "long_edge_bps": str(long_edge),
        "short_edge_bps": str(short_edge),
        "session_status": "open",
        "market_status": "open",
        "session_status_source": "variational_market_status_websocket",
        "session_status_observed_at": at.isoformat(),
        "session_status_feed_connected": True,
        "session_status_feed_heartbeat_at": at.isoformat(),
        "session_schedule_source": "variational_supported_assets_api",
        "session_id": "session-1",
        "session_closing_at": (at + timedelta(hours=8)).isoformat(),
    }
    row.update(changes)
    return row


def _build(rows, **overrides):
    values = {
        "as_of": AS_OF,
        "market_id": 145,
        "multiplier": Decimal("1"),
        "first_floor_bps": Decimal("30"),
        "second_floor_bps": Decimal("45"),
        "roundtrip_cost_bps": Decimal("10"),
        "exit_target_bps": Decimal("1"),
    }
    values.update(overrides)
    return build_wti_gradient_threshold_profiles(rows, **values)


def test_immature_history_keeps_existing_directional_floors():
    rows = [
        _row(i, AS_OF - timedelta(seconds=(3 - i) * 10), long_edge=i, short_edge=-i)
        for i in range(3)
    ]

    profiles = _build(rows)

    assert profiles["long_var_short_lighter"].thresholds_bps == (
        Decimal("30"), Decimal("45")
    )
    assert profiles["short_var_long_lighter"].thresholds_bps == (
        Decimal("30"), Decimal("45")
    )
    assert not profiles["long_var_short_lighter"].mature


def test_mature_profile_uses_eth_percentiles_and_adds_conservative_cost_reserve():
    rows = [
        _row(i, AS_OF - timedelta(seconds=(400 - i) * 10), long_edge=i, short_edge=-i)
        for i in range(400)
    ]

    profiles = _build(rows, min_effective_seconds=0)
    long = profiles["long_var_short_lighter"]

    assert long.mature
    assert long.sample_count == 400
    assert long.p975_bps == Decimal("389")
    assert long.p9825_bps == Decimal("392")
    assert long.thresholds_bps == (Decimal("400"), Decimal("403"))
    assert profiles["short_var_long_lighter"].thresholds_bps == (
        Decimal("30"), Decimal("45")
    )


def test_history_effective_time_gate_is_not_satisfied_by_sample_count_alone():
    rows = [
        _row(i, AS_OF - timedelta(seconds=(100 - i) * 10), long_edge=i, short_edge=-i)
        for i in range(100)
    ]

    profile = _build(rows)["long_var_short_lighter"]

    assert profile.sample_count == 100
    assert profile.effective_seconds == 1000
    assert not profile.mature
    assert profile.thresholds_bps == (Decimal("30"), Decimal("45"))


def test_future_wrong_market_and_conflicting_duplicate_samples_are_excluded():
    rows = [
        _row(1, AS_OF - timedelta(seconds=30), long_edge=100),
        _row(2, AS_OF - timedelta(seconds=20), long_edge=200),
        _row(2, AS_OF - timedelta(seconds=20), long_edge=201),
        _row(3, AS_OF - timedelta(seconds=10), long_edge=300, lighter_market_id=146),
        _row(4, AS_OF + timedelta(seconds=1), long_edge=999),
    ]

    profile = _build(rows, min_samples=1, min_effective_seconds=0)[
        "long_var_short_lighter"
    ]

    assert profile.sample_count == 1
    assert profile.p975_bps == Decimal("100")
    assert profile.thresholds_bps == (Decimal("111"), Decimal("111.25"))


def test_bad_parameters_fail_closed():
    try:
        _build([], first_floor_bps=Decimal("45"), second_floor_bps=Decimal("30"))
    except ValueError as exc:
        assert str(exc) == "wti_threshold_parameters_invalid"
    else:
        raise AssertionError("invalid tier ordering was accepted")
