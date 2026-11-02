from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from main import v4_real_gradient_confirmed_tier, v4_real_gradient_eligible_tier
from tools.lib.gradient_lifecycle import (
    advance_pilot_reactivation,
    confirmed_gradient_tiers,
    raw_gradient_tier,
)


LONG = "long_var_short_lighter"
SHORT = "short_var_long_lighter"
THRESHOLDS = {
    LONG: (Decimal("10"), Decimal("20"), Decimal("30"), Decimal("40"), Decimal("50")),
    SHORT: (Decimal("10"), Decimal("20"), Decimal("30"), Decimal("40"), Decimal("50")),
}


def _row(sample_id: str, at: datetime, *, long: str, short: str = "0"):
    return {
        "sample_kind": "baseline",
        "sample_id": sample_id,
        "logged_at": at.isoformat(),
        "long_edge_bps": long,
        "short_edge_bps": short,
    }


def _pilot():
    return {
        "tier_reactivation_by_direction": {
            LONG: {"2": {"armed": False, "reset_seen": False,
                         "closed_at": "2026-10-31T00:00:00+00:00"}},
            SHORT: {},
        },
        "round_reactivation": {
            "required": True,
            "armed": False,
            "direction": LONG,
            "reset_threshold_bps": "9.5",
            "reset_confirmation_count": 0,
            "required_reset_samples": 3,
            "closed_at": "2026-10-31T00:00:00+00:00",
            "last_sample_id": None,
        },
    }


def test_raw_tier_matches_highest_crossed_threshold_and_does_not_promote_by_capacity():
    edges = tuple(map(Decimal, ("9", "10", "20", "50", "90")))
    assert [raw_gradient_tier(edge, THRESHOLDS[LONG]) for edge in edges] == [0, 1, 2, 5, 5]
    assert [v4_real_gradient_eligible_tier(edge, list(THRESHOLDS[LONG])) for edge in edges] == [
        raw_gradient_tier(edge, THRESHOLDS[LONG]) for edge in edges
    ]


def test_confirmed_tier_matches_eth_two_of_three_confirmation():
    for window, latest in (((1, 2, 0), 2), ((3, 3, 3), 5), ((2, 4, 4), 4)):
        history = {
            LONG: [(f"s-{index}", tier) for index, tier in enumerate(window)],
            SHORT: [],
        }
        row = _row("latest", datetime(2026, 10, 31, tzinfo=timezone.utc),
                   long=str(Decimal(latest) * Decimal("10")))
        confirmed = confirmed_gradient_tiers(row, THRESHOLDS, history)[LONG]
        assert confirmed == v4_real_gradient_confirmed_tier(window, latest_tier=latest)


def test_closed_tier_requires_a_pullback_then_confirmed_reactivation():
    pilot = _pilot()
    base = datetime(2026, 10, 31, 0, 0, 1, tzinfo=timezone.utc)

    advance_pilot_reactivation(
        pilot, _row("reset", base, long="19.9"), THRESHOLDS,
        {LONG: 0, SHORT: 0}, verified_flat=False,
    )
    assert pilot["tier_reactivation_by_direction"][LONG]["2"]["reset_seen"] is True
    assert pilot["tier_reactivation_by_direction"][LONG]["2"]["armed"] is False

    advance_pilot_reactivation(
        pilot, _row("reconfirm", base + timedelta(seconds=1), long="20"), THRESHOLDS,
        {LONG: 2, SHORT: 0}, verified_flat=False,
    )
    assert pilot["tier_reactivation_by_direction"][LONG]["2"]["armed"] is True


def test_duplicate_sample_does_not_advance_round_reset_counter():
    pilot = _pilot()
    at = datetime(2026, 10, 31, 0, 0, 1, tzinfo=timezone.utc)
    reset = _row("same-sample", at, long="9.5")
    for _ in range(2):
        advance_pilot_reactivation(
            pilot, reset, THRESHOLDS, {LONG: 0, SHORT: 0}, verified_flat=True,
        )
    assert pilot["round_reactivation"]["reset_confirmation_count"] == 1
    assert pilot["round_reactivation"]["armed"] is False


def test_full_round_requires_three_unique_reset_samples_or_verified_opposite_signal():
    pilot = _pilot()
    at = datetime(2026, 10, 31, 0, 0, 1, tzinfo=timezone.utc)
    for index in range(2):
        advance_pilot_reactivation(
            pilot, _row(f"low-{index}", at + timedelta(seconds=index), long="9.5"),
            THRESHOLDS, {LONG: 0, SHORT: 0}, verified_flat=True,
        )
    assert pilot["round_reactivation"]["armed"] is False
    advance_pilot_reactivation(
        pilot, _row("low-2", at + timedelta(seconds=2), long="9.5"),
        THRESHOLDS, {LONG: 0, SHORT: 0}, verified_flat=True,
    )
    assert pilot["round_reactivation"]["armed"] is True
    assert pilot["round_reactivation"]["armed_by"] == "three_sample_reset"

    opposite = _pilot()
    advance_pilot_reactivation(
        opposite, _row("opposite", at, long="0", short="10"), THRESHOLDS,
        {LONG: 0, SHORT: 1}, verified_flat=True,
    )
    assert opposite["round_reactivation"]["armed"] is True
    assert opposite["round_reactivation"]["armed_by"] == "confirmed_opposite_signal"


def test_opposite_signal_does_not_rearm_with_open_position_or_unconfirmed_tier():
    for verified_flat, activated in ((False, 1), (True, 0)):
        pilot = _pilot()
        at = datetime(2026, 10, 31, 0, 0, 1, tzinfo=timezone.utc)
        advance_pilot_reactivation(
            pilot, _row("opposite", at, long="0", short="10"), THRESHOLDS,
            {LONG: 0, SHORT: activated}, verified_flat=verified_flat,
        )
        assert pilot["round_reactivation"]["armed"] is False


def test_confirmation_selects_highest_current_tier_not_capacity_consumed():
    history = {
        LONG: [("s1", 5), ("s2", 5)],
        SHORT: [("s1", 0), ("s2", 0)],
    }
    row = _row("s3", datetime(2026, 10, 31, tzinfo=timezone.utc), long="55")
    assert confirmed_gradient_tiers(row, THRESHOLDS, history)[LONG] == 5
    assert confirmed_gradient_tiers(row, THRESHOLDS, history)[SHORT] == 0


def test_reactivation_confirmation_does_not_bridge_sample_gaps_or_sessions():
    at = datetime(2026, 10, 31, 0, 0, 1, tzinfo=timezone.utc)
    pilot = _pilot()
    advance_pilot_reactivation(
        pilot, {**_row("reset", at, long="19"), "session_id": "session-1"},
        THRESHOLDS, {LONG: 0, SHORT: 0}, verified_flat=True,
    )
    advance_pilot_reactivation(
        pilot, {**_row("tier-return-after-gap", at + timedelta(seconds=121), long="20"),
                "session_id": "session-1"},
        THRESHOLDS, {LONG: 2, SHORT: 0}, verified_flat=True,
    )
    assert pilot["tier_reactivation_by_direction"][LONG]["2"]["armed"] is False
    assert pilot["tier_reactivation_by_direction"][LONG]["2"]["reset_seen"] is False

    round_state = _pilot()
    for index in range(2):
        advance_pilot_reactivation(
            round_state,
            {**_row(f"round-{index}", at + timedelta(seconds=index), long="9"),
             "session_id": "session-1"},
            THRESHOLDS, {LONG: 0, SHORT: 0}, verified_flat=True,
        )
    advance_pilot_reactivation(
        round_state,
        {**_row("round-new-session", at + timedelta(seconds=2), long="9"),
         "session_id": "session-2"},
        THRESHOLDS, {LONG: 0, SHORT: 0}, verified_flat=True,
    )
    assert round_state["round_reactivation"]["armed"] is False
    assert round_state["round_reactivation"]["reset_confirmation_count"] == 1
