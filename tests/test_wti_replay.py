from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pytest

from tools.lib.wti_replay import ReplayConfig, replay_wti, replay_wti_gradient
from tools import wti_strategy_replay


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
        "lighter_supported_size_decimals": 2,
        "lighter_min_base_amount": "0.01",
        "lighter_min_quote_amount": "1",
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


def test_replay_drains_open_position_at_first_executable_sample_after_gap():
    rows = [_row(0), _row(1, at=NOW + timedelta(minutes=2))]
    report = replay_wti(rows, config=_config(max_sample_gap_seconds=30))
    assert report.sample_gap_count == 1
    assert report.stopped_reason is None
    assert report.unresolved_position is False
    assert report.completed_trades == 1
    assert report.trades[0].exit_reason == "sample_gap_requires_position_drain"


def test_replay_revokes_entries_after_flat_gap_until_next_verified_session():
    gap_row = _row(1, at=NOW + timedelta(minutes=2))
    same_session_entry = _row(2, at=NOW + timedelta(minutes=2, seconds=10))
    next_session_entry = _row(
        3,
        at=NOW + timedelta(hours=9),
        session_id="wti-session-2",
        session_closing_at=(NOW + timedelta(hours=17)).isoformat(),
    )
    next_session_exit = _row(
        4,
        at=NOW + timedelta(hours=9, seconds=10),
        session_id="wti-session-2",
        session_closing_at=(NOW + timedelta(hours=17)).isoformat(),
        var_bid="70.4",
        var_ask="70.5",
        lighter_buy_price="70.3",
        long_edge_bps="0",
        short_edge_bps="0",
    )
    rows = [
        _row(0, long_edge_bps="0"),
        gap_row,
        same_session_entry,
        next_session_entry,
        next_session_exit,
    ]
    report = replay_wti(rows, config=_config(max_sample_gap_seconds=30))
    assert report.sample_count == 5
    assert report.sample_gap_count == 1
    assert report.completed_trades == 1
    assert report.trades[0].entry_at == next_session_entry["logged_at"]


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


def test_gradient_replay_uses_two_bounded_layers_and_closes_aggregate():
    rows = [
        _row(0),
        _row(
            1,
            var_bid="69.9", var_ask="70.0",
            lighter_buy_price="70.3", lighter_sell_price="70.2",
            long_edge_bps="21", short_edge_bps="0",
            variational_qty="0.35", lighter_qty="0.35",
        ),
        _row(
            2,
            var_bid="70.4", var_ask="70.5",
            lighter_buy_price="70.3", lighter_sell_price="70.2",
            long_edge_bps="0", short_edge_bps="0",
            variational_qty="0.35", lighter_qty="0.35",
        ),
    ]
    report = replay_wti_gradient(
        rows,
        config=_config(entry_threshold_bps=Decimal("10"), exit_target_bps=Decimal("10")),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("9"),
        aggregate_max_notional_usd=Decimal("20"),
        expected_market_id=145,
        expected_multiplier=Decimal("1"),
    )
    assert report.live_authorization is False
    assert report.completed_trades == 1
    assert report.trades[0].layer_count == 2
    assert report.trades[0].layer_entry_edges_bps == (
        "28.57142857142857142857142857", "21",
    )
    assert report.unresolved_position is False
    assert report.config["max_layers"] == 2


def test_continuous_gradient_replay_runs_sequential_rounds_without_raising_per_round_caps():
    rows = []
    for round_index in range(2):
        start = round_index * 3
        rows.extend([
            _row(start, variational_qty="0.35", lighter_qty="0.35"),
            _row(
                start + 1,
                var_bid="69.9", var_ask="70.0",
                lighter_buy_price="70.3", lighter_sell_price="70.2",
                long_edge_bps="21", short_edge_bps="0",
                variational_qty="0.35", lighter_qty="0.35",
            ),
            _row(
                start + 2,
                var_bid="70.4", var_ask="70.5",
                lighter_buy_price="70.3", lighter_sell_price="70.2",
                long_edge_bps="0", short_edge_bps="0",
                variational_qty="0.35", lighter_qty="0.35",
            ),
        ])
    report = replay_wti_gradient(
        rows,
        config=_config(entry_threshold_bps=Decimal("10"), exit_target_bps=Decimal("10")),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("9"),
        aggregate_max_notional_usd=Decimal("20"),
        expected_market_id=145,
        expected_multiplier=Decimal("1"),
        max_completed_roundtrips=None,
    )

    assert report.live_authorization is False
    assert report.replay_kind == "indicative_continuous_2_tier_gradient_no_lookahead"
    assert report.completed_trades == 2
    assert [trade.layer_count for trade in report.trades] == [2, 2]
    assert report.trades[0].exit_at < report.trades[1].entry_at
    assert report.config["max_layers"] == 2
    assert report.config["max_completed_roundtrips"] is None
    assert report.config["aggregate_max_notional_usd"] == "20"
    assert report.unresolved_position is False


def test_gradient_replay_preserves_depth_gap_and_aggregate_caps():
    rows = [
        _row(0),
        _row(
            1,
            at=NOW + timedelta(seconds=140),
            var_bid="69.9", var_ask="70.0",
            lighter_buy_price="70.3", lighter_sell_price="70.2",
            long_edge_bps="21", short_edge_bps="0",
            variational_qty="0.1", lighter_qty="0.1",
        ),
    ]
    report = replay_wti_gradient(
        rows,
        config=_config(entry_threshold_bps=Decimal("10"), max_sample_gap_seconds=120),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("10"),
        aggregate_max_notional_usd=Decimal("20"),
    )
    assert report.sample_gap_count == 1
    assert report.unresolved_position is True
    assert report.completed_trades == 0


def test_continuous_gradient_gap_revocation_survives_verified_session_transition():
    next_open = NOW + timedelta(hours=9)
    rows = [
        _row(0, long_edge_bps="21", variational_qty="0.35", lighter_qty="0.35"),
        _row(
            1, at=NOW + timedelta(seconds=40), long_edge_bps="0", short_edge_bps="0",
            variational_qty="0.35", lighter_qty="0.35",
        ),
        _row(
            2, at=next_open, session_id="wti-session-2",
            session_status_observed_at=next_open.isoformat(),
            session_status_feed_heartbeat_at=next_open.isoformat(),
            session_closing_at=(next_open + timedelta(hours=8)).isoformat(),
            long_edge_bps="21", variational_qty="0.35", lighter_qty="0.35",
        ),
        _row(
            3, at=next_open + timedelta(seconds=10), session_id="wti-session-2",
            session_status_observed_at=(next_open + timedelta(seconds=10)).isoformat(),
            session_status_feed_heartbeat_at=(next_open + timedelta(seconds=10)).isoformat(),
            session_closing_at=(next_open + timedelta(hours=8)).isoformat(),
            long_edge_bps="21", variational_qty="0.35", lighter_qty="0.35",
        ),
    ]
    report = replay_wti_gradient(
        rows,
        config=_config(entry_threshold_bps=Decimal("10"), max_sample_gap_seconds=30),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("9"),
        aggregate_max_notional_usd=Decimal("20"),
        max_completed_roundtrips=None,
    )
    assert report.sample_gap_count == 2
    assert report.completed_trades == 1
    assert report.trades[0].exit_reason == "sample_gap_requires_position_drain"
    assert report.unresolved_position is False


def test_gradient_replay_rearms_only_with_explicit_verified_flat_restart_assumption():
    next_open = NOW + timedelta(hours=9)
    rows = [
        _row(0, long_edge_bps="21", variational_qty="0.35", lighter_qty="0.35"),
        _row(
            1, at=NOW + timedelta(seconds=40), long_edge_bps="0", short_edge_bps="0",
            variational_qty="0.35", lighter_qty="0.35",
        ),
        _row(
            2, at=next_open, session_id="wti-session-2",
            session_status_observed_at=next_open.isoformat(),
            session_status_feed_heartbeat_at=next_open.isoformat(),
            session_closing_at=(next_open + timedelta(hours=8)).isoformat(),
            long_edge_bps="21", variational_qty="0.35", lighter_qty="0.35",
        ),
        _row(
            3, at=next_open + timedelta(seconds=10), session_id="wti-session-2",
            session_status_observed_at=(next_open + timedelta(seconds=10)).isoformat(),
            session_status_feed_heartbeat_at=(next_open + timedelta(seconds=10)).isoformat(),
            session_closing_at=(next_open + timedelta(hours=8)).isoformat(),
            long_edge_bps="0", short_edge_bps="0",
            variational_qty="0.35", lighter_qty="0.35",
        ),
        _row(
            4, at=next_open + timedelta(seconds=35), session_id="wti-session-2",
            session_status_observed_at=(next_open + timedelta(seconds=35)).isoformat(),
            session_status_feed_heartbeat_at=(next_open + timedelta(seconds=35)).isoformat(),
            session_closing_at=(next_open + timedelta(hours=8)).isoformat(),
            long_edge_bps="0", short_edge_bps="0",
            variational_qty="0.35", lighter_qty="0.35",
        ),
    ]
    report = replay_wti_gradient(
        rows,
        config=_config(
            entry_threshold_bps=Decimal("10"), max_sample_gap_seconds=30,
            max_hold_seconds=20,
        ),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("9"),
        aggregate_max_notional_usd=Decimal("20"),
        max_completed_roundtrips=None,
        assume_verified_flat_session_restarts=True,
    )
    assert report.sample_gap_count == 2
    assert report.completed_trades == 2
    assert report.trades[0].exit_reason == "sample_gap_requires_position_drain"
    assert report.trades[1].exit_reason == "max_hold"
    assert report.unresolved_position is False
    assert report.config["assume_verified_flat_session_restarts"] is True


def test_gradient_replay_reports_runtime_matched_sizing_guards():
    report = replay_wti_gradient(
        [_row(0, variational_qty="0.35", lighter_qty="0.35")],
        config=_config(entry_threshold_bps=Decimal("10")),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("9"),
        aggregate_max_notional_usd=Decimal("20"),
    )
    assert report.config["max_slippage_bps"] == "8"
    assert report.config["aggregate_notional_basis"] == "paired_mean_with_per_leg_cap"


def test_gradient_replay_skips_second_minimum_order_that_exceeds_twenty_dollar_cap():
    rows = [
        _row(0, lighter_min_quote_amount="10", variational_qty="0.35", lighter_qty="0.35"),
        _row(
            1, lighter_min_quote_amount="10", long_edge_bps="21",
            variational_qty="0.35", lighter_qty="0.35",
        ),
        _row(
            2, at=NOW + timedelta(seconds=70), long_edge_bps="0", short_edge_bps="0",
            lighter_min_quote_amount="10", variational_qty="0.35", lighter_qty="0.35",
        ),
    ]
    report = replay_wti_gradient(
        rows,
        config=_config(entry_threshold_bps=Decimal("10"), max_sample_gap_seconds=120),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("9"),
        aggregate_max_notional_usd=Decimal("20"),
    )
    assert report.completed_trades == 1
    assert report.trades[0].layer_count == 1
    assert report.exit_reasons["gradient_add_aggregate_cap"] == 1


@pytest.mark.parametrize(
    ("second", "layer", "cap", "error"),
    [
        (Decimal("10"), Decimal("10"), Decimal("20"), "second_tier"),
        (Decimal("20"), Decimal("11"), Decimal("20"), "aggregate_notional"),
    ],
)
def test_gradient_replay_rejects_configuration_that_weakens_or_exceeds_bounds(
    second, layer, cap, error,
):
    with pytest.raises(ValueError, match=error):
        replay_wti_gradient(
            [_row(0)],
            config=_config(entry_threshold_bps=Decimal("10")),
            second_tier_threshold_bps=second,
            layer_notional_usd=layer,
            aggregate_max_notional_usd=cap,
        )


def test_replay_cli_can_emit_side_by_side_baseline_and_gradient(monkeypatch, capsys):
    rows = [_row(0), _row(1, long_edge_bps="0", short_edge_bps="0")]
    monkeypatch.setattr(wti_strategy_replay, "read_basis_samples", lambda *args, **kwargs: rows)
    result = wti_strategy_replay.main([
        "--sample-root", "unused", "--entry-threshold-bps", "10",
        "--exit-target-bps", "10", "--roundtrip-cost-bps", "5",
        "--max-hold-seconds", "60", "--max-sample-gap-seconds", "30",
        "--lighter-market-id", "145", "--lighter-multiplier", "1",
        "--compare-gradient", "--gradient-second-tier-bps", "20",
        "--gradient-layer-notional-usd", "10",
        "--gradient-aggregate-max-notional-usd", "100",
        "--gradient-equity-usd", "20",
    ])
    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["live_authorization"] is False
    assert output["comparison"]["profitability_claim"] is False
    assert output["baseline"]["replay_kind"] != output["gradient"]["replay_kind"]
    assert output["gradient"]["config"]["max_layers"] == 5
    assert output["gradient"]["config"]["equity_scenario_usd"] == "20"


def test_replay_cli_continuous_gradient_is_explicit_and_never_authorizes_live(monkeypatch, capsys):
    rows = []
    for i in range(2):
        start = i * 3
        rows.extend([
            _row(start),
            _row(start + 1, var_bid="69.9", var_ask="70.0",
                 lighter_buy_price="70.3", lighter_sell_price="70.2",
                 long_edge_bps="21", variational_qty="0.35", lighter_qty="0.35"),
            _row(start + 2, var_bid="70.4", var_ask="70.5",
                 long_edge_bps="0", short_edge_bps="0",
                 variational_qty="0.35", lighter_qty="0.35"),
        ])
    monkeypatch.setattr(wti_strategy_replay, "read_basis_samples", lambda *args, **kwargs: rows)
    result = wti_strategy_replay.main([
        "--sample-root", "unused", "--entry-threshold-bps", "10",
        "--exit-target-bps", "10", "--roundtrip-cost-bps", "5",
        "--max-hold-seconds", "60", "--max-sample-gap-seconds", "30",
        "--lighter-market-id", "145", "--lighter-multiplier", "1",
        "--compare-gradient", "--continuous-gradient",
        "--assume-verified-flat-session-restarts",
        "--gradient-second-tier-bps", "20",
        "--gradient-layer-notional-usd", "9",
        "--gradient-aggregate-max-notional-usd", "20",
    ])
    output = json.loads(capsys.readouterr().out)
    assert result == 0
    assert output["live_authorization"] is False
    assert output["gradient"]["completed_trades"] == 2
    assert output["gradient"]["config"]["max_completed_roundtrips"] is None
    assert output["gradient"]["config"]["assume_verified_flat_session_restarts"] is True


def test_five_tier_equity_scenario_replay_adds_within_tier_and_keeps_higher_tiers_maturity_gated():
    rows = [
        _row(0, long_edge_bps="0", short_edge_bps="0", variational_qty="1", lighter_qty="1"),
        _row(1, long_edge_bps="25", short_edge_bps="0", variational_qty="1", lighter_qty="1"),
        _row(2, long_edge_bps="25", short_edge_bps="0", variational_qty="1", lighter_qty="1"),
        _row(3, long_edge_bps="25", short_edge_bps="0", variational_qty="1", lighter_qty="1"),
        _row(4, long_edge_bps="25", short_edge_bps="0", variational_qty="1", lighter_qty="1"),
        _row(5, long_edge_bps="100", short_edge_bps="0", variational_qty="1", lighter_qty="1"),
        _row(6, long_edge_bps="100", short_edge_bps="0", variational_qty="1", lighter_qty="1"),
    ]
    report = replay_wti_gradient(
        rows,
        config=_config(
            entry_threshold_bps=Decimal("10"), exit_target_bps=Decimal("10"),
            max_hold_seconds=50,
        ),
        second_tier_threshold_bps=Decimal("20"),
        layer_notional_usd=Decimal("10"),
        aggregate_max_notional_usd=Decimal("100"),
        max_layers=5,
        equity_usd=Decimal("20"),
        expected_market_id=145,
        expected_multiplier=Decimal("1"),
    )
    assert report.live_authorization is False
    assert report.config["max_layers"] == 5
    assert report.config["tier_capacity_notional_usd"] == ["20", "40", "60", "80", "100"]
    assert report.config["equity_scenario_usd"] == "20"
    assert report.completed_trades == 1
    assert report.trades[0].layer_count == 3
    assert report.trades[0].layer_entry_edges_bps == ("25", "25", "25")
    assert report.trades[0].exit_reason == "max_hold"


def test_replay_cli_writes_hash_addressed_readiness_evidence(tmp_path: Path, monkeypatch, capsys):
    sample_root = tmp_path / "samples"
    asset_root = sample_root / "WTI"
    asset_root.mkdir(parents=True)
    sample_file = asset_root / "2026-10-22.jsonl"
    sample_file.write_text("{}\n", encoding="utf-8")
    rows = [_row(0), _row(1)]
    monkeypatch.setattr(wti_strategy_replay, "read_basis_samples", lambda *args, **kwargs: rows)
    evidence_path = tmp_path / "evidence.json"

    result = wti_strategy_replay.main([
        "--sample-root", str(sample_root), "--entry-threshold-bps", "10",
        "--exit-target-bps", "10", "--roundtrip-cost-bps", "5",
        "--max-hold-seconds", "60", "--max-sample-gap-seconds", "30",
        "--lighter-market-id", "145", "--lighter-multiplier", "1",
        "--evidence-output", str(evidence_path),
    ])

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    saved_hash = evidence.pop("evidence_sha256")
    canonical = json.dumps(evidence, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    assert saved_hash == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert evidence["input_files"][0]["path"] == "WTI/2026-10-22.jsonl"
    assert evidence["input_files"][0]["sha256"] == hashlib.sha256(sample_file.read_bytes()).hexdigest()
    assert evidence["market_identity"]["lighter_market_id"] == 145
    assert evidence["live_authorization"] is False
    assert evidence["profitability_claim"] is False
    assert output["evidence"]["sha256"] == saved_hash
