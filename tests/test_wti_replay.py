from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tools.lib.wti_replay import ReplayConfig, replay_wti


NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)


def _row(index: int, *, at: datetime | None = None, **overrides):
    sample_at = at or NOW + timedelta(seconds=index * 10)
    row = {
        "asset": "WTI",
        "record_kind": "basis_market_sample",
        "sample_kind": "baseline",
        "sample_quality": "valid",
        "sample_id": f"sample-{index}",
        "logged_at": sample_at.isoformat(),
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
        "var_quote_age_seconds": "0.1",
        "lighter_book_age_seconds": "0.1",
        "var_bid": "69.9",
        "var_ask": "70.0",
        "lighter_bid": "70.2",
        "lighter_ask": "70.3",
        "lighter_buy_price": "70.3",
        "lighter_sell_price": "70.2",
        "variational_qty": "0.29",
        "lighter_qty": "0.29",
        "long_edge_bps": "28.57142857142857142857142857",
        "short_edge_bps": "0",
        "market_status": "open",
        "session_status": "open",
        "session_status_source": "variational_market_status_websocket",
        "session_status_observed_at": sample_at.isoformat(),
        "session_status_feed_connected": True,
        "session_status_feed_heartbeat_at": sample_at.isoformat(),
        "session_schedule_source": "variational_supported_assets_api",
        "session_id": "wti-session-1",
        "session_closing_at": (NOW + timedelta(hours=8)).isoformat(),
    }
    row.update(overrides)
    return row


def _config(**overrides):
    values = {
        "entry_threshold_bps": Decimal("10"),
        "exit_target_bps": Decimal("10"),
        "roundtrip_cost_bps": Decimal("5"),
        "max_hold_seconds": 60,
        "max_sample_gap_seconds": 30,
    }
    values.update(overrides)
    return ReplayConfig(**values)


def test_replay_is_no_lookahead_and_exits_on_net_target():
    rows = [
        _row(0),
        _row(
            1,
            var_bid="70.4",
            var_ask="70.5",
            lighter_bid="70.2",
            lighter_ask="70.3",
            lighter_buy_price="70.3",
            lighter_sell_price="70.2",
            long_edge_bps="0",
            short_edge_bps="0",
        ),
    ]

    report = replay_wti(rows, config=_config())

    assert report.live_authorization is False
    assert report.session_gated is True
    assert report.completed_trades == 1
    assert report.trades[0].entry_at == rows[0]["logged_at"]
    assert report.trades[0].exit_at == rows[1]["logged_at"]
    assert report.trades[0].exit_reason == "target_reached"
    assert Decimal(report.trades[0].net_pnl_usd) > 0
    assert report.unresolved_position is False


def test_replay_waits_for_exit_quotes_covering_the_open_position_size():
    rows = [
        _row(0),
        _row(
            1,
            variational_qty="0.01",
            lighter_qty="0.01",
            var_bid="70.4",
            var_ask="70.5",
            long_edge_bps="0",
            short_edge_bps="0",
        ),
        _row(
            2,
            var_bid="70.4",
            var_ask="70.5",
            lighter_bid="70.2",
            lighter_ask="70.3",
            lighter_buy_price="70.3",
            lighter_sell_price="70.2",
            long_edge_bps="0",
            short_edge_bps="0",
        ),
    ]

    report = replay_wti(rows, config=_config())

    assert report.completed_trades == 1
    assert report.trades[0].exit_at == rows[2]["logged_at"]
    assert report.exit_reasons == {
        "exit_quote_depth_below_position_size": 1,
        "target_reached": 1,
    }
    assert report.unresolved_position is False


def test_replay_does_not_claim_fill_when_position_remains_open_at_end():
    report = replay_wti([_row(0)], config=_config())
    assert report.completed_trades == 0
    assert report.unresolved_position is True
    assert report.net_pnl_usd == "0"


def test_replay_never_closes_on_a_sample_with_the_entry_timestamp():
    at = NOW
    rows = [
        _row(0, at=at),
        _row(
            1,
            at=at,
            var_bid="70.4",
            var_ask="70.5",
            long_edge_bps="0",
            short_edge_bps="0",
        ),
    ]
    report = replay_wti(rows, config=_config())
    assert report.completed_trades == 0
    assert report.unresolved_position is True


def test_replay_stops_at_gap_with_open_position_and_marks_unresolved():
    rows = [_row(0), _row(1, at=NOW + timedelta(minutes=2))]
    report = replay_wti(rows, config=_config(max_sample_gap_seconds=30))
    assert report.sample_gap_count == 1
    assert report.stopped_reason == "sample_gap_exceeded"
    assert report.unresolved_position is True
    assert report.completed_trades == 0


def test_replay_deduplicates_identical_ids_and_rejects_conflicts():
    duplicate = _row(0)
    conflicting = {**duplicate, "var_bid": "69.8"}
    report = replay_wti(
        [duplicate, dict(duplicate), conflicting, _row(1, long_edge_bps="0")],
        config=_config(),
    )
    assert report.duplicate_sample_count == 1
    assert report.conflicting_sample_id_count == 1
    assert report.sample_count == 1
    assert report.completed_trades == 0


def test_replay_rejects_exposure_mismatch_and_market_profile_change():
    mismatch = replay_wti(
        [_row(0, variational_qty="0.28")],
        config=_config(),
    )
    assert mismatch.sample_count == 0
    assert mismatch.rejected_sample_count == 1

    with pytest.raises(ValueError, match="market_profile_changed"):
        replay_wti(
            [_row(0), _row(1, lighter_market_id=146, long_edge_bps="0")],
            config=_config(),
        )


def test_replay_max_hold_closes_at_first_observed_sample_after_deadline():
    rows = [
        _row(0),
        _row(1, at=NOW + timedelta(seconds=70), long_edge_bps="0", short_edge_bps="0"),
    ]
    report = replay_wti(rows, config=_config(max_hold_seconds=60, max_sample_gap_seconds=120))
    assert report.completed_trades == 1
    assert report.trades[0].exit_reason == "max_hold"
    assert report.trades[0].duration_seconds == 70


def test_replay_rejects_unverified_session_and_never_infers_status_from_quotes():
    row = _row(0)
    for key in ("market_status", "session_status", "session_status_source", "session_status_observed_at",
                "session_status_feed_connected", "session_status_feed_heartbeat_at",
                "session_schedule_source", "session_id", "session_closing_at"):
        row.pop(key)
    report = replay_wti([row], config=_config())
    assert report.session_gated is True
    assert report.sample_count == 0
    assert report.rejected_sample_count == 1
    assert report.completed_trades == 0


def test_replay_allows_verified_closed_session_gap_only_when_flat():
    first = _row(0, long_edge_bps="0",
                 session_closing_at=(NOW + timedelta(hours=1)).isoformat())
    second_at = NOW + timedelta(hours=2)
    second = _row(
        1, at=second_at, long_edge_bps="0", session_id="wti-session-2",
        session_closing_at=(NOW + timedelta(hours=10)).isoformat(),
    )
    rows = [first, second]
    report = replay_wti(rows, config=_config(max_sample_gap_seconds=30))
    assert report.sample_count == 2
    assert report.sample_gap_count == 0
    assert report.stopped_reason is None
    assert report.unresolved_position is False


def test_replay_stops_at_verified_session_boundary_if_position_is_still_open():
    first = _row(0, session_closing_at=(NOW + timedelta(hours=1)).isoformat())
    second_at = NOW + timedelta(hours=2)
    second = _row(
        1, at=second_at, session_id="wti-session-2",
        session_closing_at=(NOW + timedelta(hours=10)).isoformat(),
    )
    report = replay_wti([first, second], config=_config(max_sample_gap_seconds=30))
    assert report.sample_gap_count == 0
    assert report.stopped_reason == "position_crossed_session_boundary"
    assert report.unresolved_position is True
    assert report.completed_trades == 0


def test_replay_blocks_entries_in_last_thirty_minutes_and_exits_existing_position():
    close_at = NOW + timedelta(hours=2)
    before_cutoff = close_at - timedelta(minutes=30, seconds=1)
    at_cutoff = close_at - timedelta(minutes=29, seconds=59)
    entry = _row(0, at=before_cutoff, session_closing_at=close_at.isoformat())
    cutoff = _row(
        1, at=at_cutoff, long_edge_bps="28", session_closing_at=close_at.isoformat(),
        var_bid="69.9", var_ask="70.0", lighter_buy_price="70.3",
    )
    report = replay_wti([entry, cutoff], config=_config(max_sample_gap_seconds=30))
    assert report.completed_trades == 1
    assert report.trades[0].exit_reason == "session_entry_cutoff"

    only_cutoff = _row(2, at=at_cutoff, session_closing_at=close_at.isoformat())
    no_entry = replay_wti([only_cutoff], config=_config())
    assert no_entry.completed_trades == 0
    assert no_entry.unresolved_position is False
