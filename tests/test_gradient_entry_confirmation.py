from decimal import Decimal
from datetime import datetime, timedelta, timezone

from tools.lib.gradient_entry_confirmation import (
    elastic_addon_confirmation_allowed,
    confirm_gradient_entry_with_execution_refresh,
    strictest_entry_threshold,
    update_gradient_entry_confirmation,
)


DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")
THRESHOLDS = {
    DIRECTIONS[0]: tuple(map(Decimal, ("10", "20", "30", "40", "50"))),
    DIRECTIONS[1]: tuple(map(Decimal, ("10", "20", "30", "40", "50"))),
}


def _row(sample_id: str, *, long_edge: str = "10", short_edge: str = "0", **changes):
    row = {
        "sample_id": sample_id,
        "sample_kind": "baseline",
        "long_edge_bps": long_edge,
        "short_edge_bps": short_edge,
    }
    row.update(changes)
    return row


def test_entry_confirmation_requires_two_distinct_samples_in_three():
    history = {direction: [] for direction in DIRECTIONS}
    first = update_gradient_entry_confirmation(
        _row("one"), THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
    )
    middle = update_gradient_entry_confirmation(
        _row("two", long_edge="9.9"), THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
    )
    last = update_gradient_entry_confirmation(
        _row("three"), THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
    )

    assert first["confirmed"] is False
    assert middle["confirmed"] is False
    assert last["confirmed"] is True
    assert last["confirmation_mode"] == "two_of_three"
    assert last["confirm_count"] == 2


def test_duplicate_ids_and_execution_bursts_do_not_manufacture_confirmation():
    history = {direction: [] for direction in DIRECTIONS}
    kwargs = dict(
        thresholds_by_direction=THRESHOLDS,
        history_by_direction=history,
        direction=DIRECTIONS[0],
        candidate_tier=1,
    )
    update_gradient_entry_confirmation(_row("same"), **kwargs)
    duplicate = update_gradient_entry_confirmation(_row("same"), **kwargs)
    burst = update_gradient_entry_confirmation(
        _row("burst", sample_kind="burst"), **kwargs
    )

    assert duplicate["confirmed"] is False
    assert burst["confirmed"] is False
    assert [sample_id for sample_id, _ in history[DIRECTIONS[0]]] == ["same"]


def test_only_tier_three_can_use_a_single_sample_probe_and_it_is_opt_in():
    history = {direction: [] for direction in DIRECTIONS}
    row = _row("strong", long_edge="30")
    pending = update_gradient_entry_confirmation(
        row, THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
    )
    probe = update_gradient_entry_confirmation(
        row, THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
        allow_strong_single_probe=True,
    )

    assert pending["confirmed"] is False
    assert probe["confirmed"] is True
    assert probe["confirmation_mode"] == "strong_single_probe"
    assert probe["required_edge_bps"] == "30"


def test_confirmation_histories_are_directional_and_threshold_failures_fail_closed():
    history = {direction: [] for direction in DIRECTIONS}
    row = _row("long", long_edge="10")
    update_gradient_entry_confirmation(
        row, THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
    )
    short = update_gradient_entry_confirmation(
        _row("short", long_edge="0", short_edge="10"), THRESHOLDS, history,
        direction=DIRECTIONS[1], candidate_tier=1,
    )
    invalid = update_gradient_entry_confirmation(
        _row("invalid"),
        {**THRESHOLDS, DIRECTIONS[0]: (Decimal("10"), None, Decimal("30"))},
        history,
        direction=DIRECTIONS[0], candidate_tier=1,
    )

    assert short["confirmed"] is False
    assert invalid["reason"] == "gradient_threshold_profile_invalid"


def test_execution_keeps_the_stricter_strategy_or_confirmation_threshold():
    assert strictest_entry_threshold("4.25", "4") == Decimal("4.25")
    assert strictest_entry_threshold(None, "45.25") == Decimal("45.25")
    assert strictest_entry_threshold("30", "invalid") is None


def test_elastic_reserve_requires_two_of_three_confirmation():
    assert elastic_addon_confirmation_allowed(True, "two_of_three")
    assert not elastic_addon_confirmation_allowed(True, "strong_single_probe")
    assert elastic_addon_confirmation_allowed(False, "strong_single_probe")


def _refresh_row(sample_id, at, edge, *, kind="burst", signal_sample_id="strong"):
    return {
        "sample_id": sample_id,
        "sample_kind": kind,
        "sample_quality": "valid",
        "market_availability": "both_feeds_live",
        "session_status": "open",
        "session_status_feed_connected": True,
        "session_id": "session-1",
        "logged_at": at.isoformat(),
        "signal_sample_id": signal_sample_id,
        "long_edge_bps": str(edge),
        "short_edge_bps": "0",
    }


def test_strong_single_requires_distinct_fresh_refresh_still_at_tier_three():
    history = {direction: [] for direction in DIRECTIONS}
    signal_at = datetime(2026, 10, 27, tzinfo=timezone.utc)
    signal = _refresh_row("strong", signal_at, "30", kind="baseline")
    refresh = _refresh_row("refresh", signal_at + timedelta(seconds=1), "30")

    result = confirm_gradient_entry_with_execution_refresh(
        signal, refresh, THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
        allow_strong_single_probe=True,
    )

    assert result["confirmed"] is True
    assert result["confirmation_mode"] == "strong_single_probe"
    assert result["required_edge_bps"] == "30"
    assert result["execution_refresh_sample_id"] == "refresh"
    assert [sample_id for sample_id, _ in history[DIRECTIONS[0]]] == ["strong"]


def test_strong_single_fails_closed_when_refreshed_edge_falls_below_tier_three():
    history = {direction: [] for direction in DIRECTIONS}
    signal_at = datetime(2026, 10, 27, tzinfo=timezone.utc)
    signal = _refresh_row("strong", signal_at, "30", kind="baseline")
    refresh = _refresh_row("refresh", signal_at + timedelta(seconds=1), "29.99")

    result = confirm_gradient_entry_with_execution_refresh(
        signal, refresh, THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
        allow_strong_single_probe=True,
    )

    assert result == {
        "confirmed": False,
        "reason": "gradient_strong_signal_refresh_below_required_tier",
        "raw_tier": 2,
        "required_tier": 3,
    }
    assert len(history[DIRECTIONS[0]]) == 1


def test_execution_refresh_must_be_linked_same_session_and_within_two_seconds():
    history = {direction: [] for direction in DIRECTIONS}
    signal_at = datetime(2026, 10, 27, tzinfo=timezone.utc)
    signal = _refresh_row("strong", signal_at, "30", kind="baseline")
    refresh = _refresh_row("refresh", signal_at + timedelta(seconds=2, microseconds=1), "30")

    result = confirm_gradient_entry_with_execution_refresh(
        signal, refresh, THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
        allow_strong_single_probe=True,
    )

    assert result["confirmed"] is False
    assert result["reason"] == "gradient_entry_execution_refresh_outside_window"

    refresh["logged_at"] = (signal_at + timedelta(seconds=1)).isoformat()
    refresh["signal_sample_id"] = "different-signal"
    result = confirm_gradient_entry_with_execution_refresh(
        signal, refresh, THRESHOLDS, history,
        direction=DIRECTIONS[0], candidate_tier=1,
        allow_strong_single_probe=True,
    )
    assert result["reason"] == "gradient_entry_execution_refresh_signal_mismatch"
