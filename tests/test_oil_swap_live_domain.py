from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from tools.lib.hedge_instruments import (
    HedgeInstrument,
    VenueInstrument,
    WTI_HEDGE,
    lighter_position_qty,
    paired_exposure_matches,
    paired_realized_pnl,
    variational_position_qty,
)
from tools.lib.market_sessions import (
    OilSession,
    classify_usoil_session,
    extract_exchange_session_observation,
    parse_market_status_update,
)
from tools.lib.oil_swap_readiness import assess_oil_readiness
from tools.lib.pair_rotation import Pair, RotationAction, decide_rotation, desired_pair


ET = ZoneInfo("America/New_York")


def test_var_position_requires_exact_swap_identity() -> None:
    row = {
        "instrument": {
            "underlying": "USOILP",
            "instrument_type": "swap",
            "settlement_asset": "USDC",
            "kind": "commodity",
            "funding_interval_s": 0,
        },
        "qty": "0.200",
    }
    assert variational_position_qty({"positions": [row]}, WTI_HEDGE) == Decimal("0.200")
    assert variational_position_qty({"positions": [{**row, "instrument": {"underlying": "USOILP"}}]}, WTI_HEDGE) is None
    assert variational_position_qty({"ok": False, "error": "unauthorized"}, WTI_HEDGE) is None
    assert variational_position_qty({"ok": True, "result": {"positions": []}}, WTI_HEDGE) == Decimal("0")
    assert variational_position_qty({"ok": True, "result": {}}, WTI_HEDGE) is None
    assert variational_position_qty(
        {"positions": [{**row, "instrument": {**row["instrument"], "funding_interval_s": 0.5}}]},
        WTI_HEDGE,
    ) is None


def test_lighter_position_matches_market_id_and_signed_quantity() -> None:
    payload = {"code": 200, "accounts": [{"positions": [
        {"market_id": 145, "symbol": "WTI", "position": "0.200", "sign": -1},
        {"market_id": 1, "symbol": "ETH", "position": "3", "sign": 1},
    ]}]}
    assert lighter_position_qty(payload, market_id=145, symbol="WTI") == Decimal("-0.200")
    assert lighter_position_qty(
        {"code": 200, "accounts": [{"positions": [
            {"market_id": 145, "symbol": "WTI", "position": "0.2"}
        ]}]},
        market_id=145,
        symbol="WTI",
    ) is None
    assert lighter_position_qty(
        {"code": 200, "accounts": [{"positions": [
            {"market_id": 145, "symbol": "WTI", "position": "0.2", "sign": 2}
        ]}]},
        market_id=145,
        symbol="WTI",
    ) is None
    assert lighter_position_qty(
        {"code": 200, "accounts": [{"positions": [
            {"market_id": 145, "symbol": "ETH", "position": "0.2", "sign": -1}
        ]}]},
        market_id=145,
        symbol="WTI",
    ) is None
    assert lighter_position_qty(
        {"code": 200, "accounts": [{"positions": [{"market_id": 1, "symbol": "ETH", "position": "3", "sign": 1}]}]},
        market_id=145,
        symbol="WTI",
    ) == Decimal("0")
    assert lighter_position_qty(payload, market_id=4, symbol="WTI") is None
    assert lighter_position_qty({"accounts": [{}]}, market_id=145, symbol="WTI") is None
    assert lighter_position_qty({"accounts": []}, market_id=145, symbol="WTI") is None
    assert lighter_position_qty({"code": 200.5, "accounts": [{"positions": []}]}, market_id=145, symbol="WTI") is None
    assert lighter_position_qty(
        {"code": 200, "accounts": [{"positions": [
            {"market_id": 145.5, "symbol": "WTI", "position": "0", "sign": 0}
        ]}]},
        market_id=145,
        symbol="WTI",
    ) is None


def test_pair_math_uses_each_leg_quantity_and_signed_funding() -> None:
    instrument = HedgeInstrument(
        key="test",
        var=VenueInstrument("var", "A", "swap", "USDC", exposure_multiplier=Decimal("1")),
        lighter=VenueInstrument("lighter", "B", "perp", "USDC", exposure_multiplier=Decimal("2")),
    )
    assert paired_exposure_matches(
        var_qty=Decimal("0.4"), lighter_qty=Decimal("0.2"), instrument=instrument, tolerance=Decimal("0")
    )
    pnl = paired_realized_pnl(
        direction="long_var_short_lighter",
        var_entry=Decimal("100"), var_exit=Decimal("102"), var_qty=Decimal("0.4"),
        lighter_entry=Decimal("101"), lighter_exit=Decimal("99"), lighter_qty=Decimal("0.2"),
        instrument=instrument, var_fees=Decimal("0.2"), lighter_fees=Decimal("0.1"),
        var_funding=Decimal("-0.3"), lighter_funding=Decimal("0.1"),
    )
    assert pnl == Decimal("1.1")


def test_session_windows_use_new_york_dst_and_fail_closed_without_status() -> None:
    open_at = datetime(2026, 1, 7, 15, 0, tzinfo=ZoneInfo("UTC"))  # 10:00 EST
    close_at = datetime(2026, 1, 7, 23, 0, tzinfo=ZoneInfo("UTC"))
    assert classify_usoil_session(
        open_at, exchange_open=True, exchange_status_observed_at=open_at,
        exchange_closing_at=close_at
    ).state == OilSession.OPEN
    cutoff_at = datetime(2026, 1, 7, 22, 45, tzinfo=ZoneInfo("UTC"))
    assert classify_usoil_session(
        cutoff_at, exchange_open=True, exchange_status_observed_at=cutoff_at,
        exchange_closing_at=close_at
    ).state == OilSession.ENTRY_CUTOFF
    assert classify_usoil_session(open_at, exchange_open=None).state == OilSession.STATUS_UNKNOWN
    assert classify_usoil_session(
        open_at, exchange_open=True, exchange_status_observed_at=open_at
    ).state == OilSession.STATUS_UNKNOWN
    assert classify_usoil_session(
        open_at, exchange_open=False, exchange_status_observed_at=open_at
    ).state == OilSession.EXCHANGE_CLOSED


def test_session_status_must_be_fresh_typed_and_not_past_close() -> None:
    now = datetime(2026, 1, 7, 23, 1, tzinfo=ZoneInfo("UTC"))
    close_at = datetime(2026, 1, 7, 23, 0, tzinfo=ZoneInfo("UTC"))
    for observed_at, open_value in (
        (now - timedelta(seconds=31), True),
        (now, "true"),
    ):
        window = classify_usoil_session(
            now,
            exchange_open=open_value,
            exchange_status_observed_at=observed_at,
            exchange_closing_at=close_at,
        )
        assert window.state == OilSession.STATUS_UNKNOWN
        assert not window.allow_new_entries
        assert not window.allow_position_management
    expired = classify_usoil_session(
        now,
        exchange_open=True,
        exchange_status_observed_at=now,
        exchange_closing_at=close_at,
    )
    assert expired.state == OilSession.STATUS_UNKNOWN
    assert not expired.allow_position_management


def test_market_status_parser_uses_actual_websocket_pair_shape() -> None:
    assert parse_market_status_update('["USOILP","open"]') == "open"
    assert parse_market_status_update('[ ["ETH","closed"], ["USOILP","halted_trading"] ]') == "halted_trading"
    assert parse_market_status_update('[["OTHER","open"]]') is None
    assert parse_market_status_update('[["USOILP","open"],["USOILP","closed"]]') == "unknown"
    assert parse_market_status_update('[["USOILP","opening"]]') == "unknown"
    assert parse_market_status_update('{"marketStatus":"open"}') is None


def test_session_extractor_uses_market_status_and_trading_sessions_fields() -> None:
    observed = datetime(2026, 1, 7, 15, 0, tzinfo=ZoneInfo("UTC"))
    sessions = [{"open": "2026-01-07T14:00:00Z", "close": "2026-01-07T23:00:00Z"}]
    valid = extract_exchange_session_observation(
        "open", sessions, observed_at=observed,
        status_observed_at=observed, feed_connected=True, feed_heartbeat_at=observed,
    )
    assert valid["session_status"] == "open"
    assert valid["market_status"] == "open"
    assert valid["session_status_source"] == "variational_market_status_websocket"
    assert valid["session_schedule_source"] == "variational_supported_assets_api"
    assert valid["session_id"] == "2026-01-07T14:00:00+00:00|2026-01-07T23:00:00+00:00"
    assert valid["session_closing_at"] == "2026-01-07T23:00:00+00:00"

    missing = extract_exchange_session_observation(
        None, sessions, observed_at=observed, status_observed_at=None,
        feed_connected=False, feed_heartbeat_at=None,
    )
    assert missing["session_status"] == "unknown"
    halted = extract_exchange_session_observation(
        "halted_trading", sessions, observed_at=observed,
        status_observed_at=observed, feed_connected=True, feed_heartbeat_at=observed,
    )
    assert halted["session_status"] == "closed"
    assert halted["market_status"] == "halted_trading"
    stale = extract_exchange_session_observation(
        "open", sessions, observed_at=observed,
        status_observed_at=observed - timedelta(minutes=2), feed_connected=True,
        feed_heartbeat_at=observed - timedelta(seconds=31),
    )
    assert stale["session_status"] == "unknown"
    future = extract_exchange_session_observation(
        "open", sessions, observed_at=observed,
        status_observed_at=observed + timedelta(seconds=6), feed_connected=True,
        feed_heartbeat_at=observed,
    )
    assert future["session_status"] == "unknown"
    non_boolean_connection = extract_exchange_session_observation(
        "open", sessions, observed_at=observed,
        status_observed_at=observed, feed_connected=1,
        feed_heartbeat_at=observed,
    )
    assert non_boolean_connection["session_status"] == "unknown"
    missing_schedule = extract_exchange_session_observation(
        "open", [], observed_at=observed, status_observed_at=observed,
        feed_connected=True, feed_heartbeat_at=observed,
    )
    assert missing_schedule["session_status"] == "open"
    assert missing_schedule["session_id"] is None
    malformed_schedule = extract_exchange_session_observation(
        "open", [{"open": "2026-01-07T14:00:00", "close": "2026-01-07T23:00:00Z"}],
        observed_at=observed, status_observed_at=observed,
        feed_connected=True, feed_heartbeat_at=observed,
    )
    assert malformed_schedule["session_id"] is None


def test_rotation_drains_and_requires_verified_flat_before_switch() -> None:
    now = datetime(2026, 1, 7, 15, 0, tzinfo=ZoneInfo("UTC"))
    session = classify_usoil_session(
        now,
        exchange_open=True,
        exchange_status_observed_at=now,
        exchange_closing_at=datetime(2026, 1, 7, 23, 0, tzinfo=ZoneInfo("UTC")),
    )
    target, _ = desired_pair(session=session, wti_ready=True, eth_ready=True, current=Pair.ETH)
    decision = decide_rotation(
        current=Pair.ETH, target=target, session=session, has_open_position=True,
        target_ready=True, has_pending_action=False, active_orders_zero=True,
        exchange_reconciled=True,
    )
    assert decision.action == RotationAction.DRAIN_CURRENT
    assert not decision.allow_new_entries
    decision = decide_rotation(
        current=Pair.ETH, target=target, session=session, has_open_position=False,
        target_ready=True, has_pending_action=False, active_orders_zero=False,
        exchange_reconciled=True,
    )
    assert decision.action == RotationAction.DRAIN_CURRENT
    assert decision.reason == "source_active_orders_must_be_verified_before_handoff"


def test_closed_venue_does_not_request_one_sided_flatten_or_switch() -> None:
    closed = classify_usoil_session(
        datetime(2026, 1, 10, 16, 0, tzinfo=ZoneInfo("UTC")),
        exchange_open=False,
        exchange_status_observed_at=datetime(2026, 1, 10, 16, 0, tzinfo=ZoneInfo("UTC")),
    )
    decision = decide_rotation(
        current=Pair.WTI,
        target=Pair.ETH,
        session=closed,
        target_ready=True,
        has_open_position=True,
        has_pending_action=False,
        active_orders_zero=True,
        exchange_reconciled=True,
    )
    assert decision.action == RotationAction.HOLD_CURRENT
    assert not decision.allow_new_entries


def test_rotation_rechecks_wti_entry_window_and_manages_eth_independently() -> None:
    now = datetime(2026, 1, 10, 16, 0, tzinfo=ZoneInfo("UTC"))
    closed = classify_usoil_session(
        now,
        exchange_open=False,
        exchange_status_observed_at=now,
    )
    for has_position, expected_action in (
        (False, RotationAction.WAIT_FOR_READINESS),
        (True, RotationAction.MANAGE_CURRENT),
    ):
        decision = decide_rotation(
            current=Pair.ETH,
            target=Pair.WTI,
            target_ready=True,
            session=closed,
            has_open_position=has_position,
            has_pending_action=False,
            active_orders_zero=True,
            exchange_reconciled=True,
        )
        assert decision.action == expected_action
        assert not decision.allow_new_entries
        if has_position:
            assert decision.reason == "wti_entry_window_closed_recheck_before_switch"


def test_rotation_unknown_wti_session_never_switches_stale_wti_target() -> None:
    unknown = classify_usoil_session(
        datetime(2026, 1, 10, 16, 0, tzinfo=ZoneInfo("UTC")),
        exchange_open=None,
    )
    decision = decide_rotation(
        current=Pair.ETH,
        target=Pair.WTI,
        target_ready=True,
        session=unknown,
        has_open_position=False,
        has_pending_action=False,
        active_orders_zero=True,
        exchange_reconciled=True,
    )
    assert decision.action == RotationAction.WAIT_FOR_READINESS
    assert not decision.allow_new_entries


def test_unknown_active_pair_with_positions_pauses_for_reconciliation() -> None:
    session = classify_usoil_session(
        datetime(2026, 1, 7, 15, 0, tzinfo=ZoneInfo("UTC")),
        exchange_open=True,
        exchange_status_observed_at=datetime(2026, 1, 7, 15, 0, tzinfo=ZoneInfo("UTC")),
        exchange_closing_at=datetime(2026, 1, 7, 23, 0, tzinfo=ZoneInfo("UTC")),
    )
    decision = decide_rotation(
        current=Pair.NONE,
        target=Pair.ETH,
        session=session,
        target_ready=True,
        has_open_position=True,
        has_pending_action=False,
        active_orders_zero=True,
        exchange_reconciled=True,
    )
    assert decision.action == RotationAction.PAUSE_AND_RECONCILE
    assert not decision.allow_new_entries


def test_unready_current_pair_can_manage_existing_only_and_cannot_reopen() -> None:
    now = datetime(2026, 1, 7, 15, 0, tzinfo=ZoneInfo("UTC"))
    session = classify_usoil_session(
        now,
        exchange_open=True,
        exchange_status_observed_at=now,
        exchange_closing_at=datetime(2026, 1, 7, 23, 0, tzinfo=ZoneInfo("UTC")),
    )
    target, _ = desired_pair(
        session=session, wti_ready=False, eth_ready=False, current=Pair.WTI
    )
    assert target == Pair.WTI
    with_position = decide_rotation(
        current=Pair.WTI,
        target=target,
        target_ready=False,
        session=session,
        has_open_position=True,
        has_pending_action=False,
        active_orders_zero=True,
        exchange_reconciled=True,
    )
    assert with_position.action == RotationAction.MANAGE_CURRENT
    assert not with_position.allow_new_entries
    flat = decide_rotation(
        current=Pair.WTI,
        target=target,
        target_ready=False,
        session=session,
        has_open_position=False,
        has_pending_action=False,
        active_orders_zero=True,
        exchange_reconciled=True,
    )
    assert flat.action == RotationAction.WAIT_FOR_READINESS
    assert not flat.allow_new_entries


def _valid_session_rows(day: str) -> list[dict]:
    start = datetime.fromisoformat(day + "T09:00:00-05:00")
    return [
        {
            "asset": "WTI", "sample_kind": "baseline", "sample_quality": "valid",
            "sample_id": f"{day}-{index}",
            "logged_at": (start + timedelta(seconds=20 * index)).isoformat(),
            "variational_underlying": "USOILP",
            "variational_instrument_type": "swap",
            "variational_instrument_kind": "commodity",
            "variational_funding_interval_s": 0,
            "lighter_symbol": "WTI",
            "lighter_market_id": 145,
            "lighter_multiplier": "1",
            "variational_qty": "1",
            "lighter_qty": "1",
            "market_status": "open",
            "session_status": "open",
            "session_status_source": "variational_market_status_websocket",
            "session_status_observed_at": (start + timedelta(seconds=20 * index)).astimezone(ZoneInfo("UTC")).isoformat(),
            "session_status_feed_connected": True,
            "session_status_feed_heartbeat_at": (start + timedelta(seconds=20 * index)).astimezone(ZoneInfo("UTC")).isoformat(),
            "session_schedule_source": "variational_supported_assets_api",
            "session_id": f"wti-session-{day}",
            "session_closing_at": (start.astimezone(ZoneInfo("UTC")) + timedelta(hours=8)).isoformat(),
            "market_availability": "both_feeds_live",
            "var_quote_age_seconds": "0.1",
            "lighter_book_age_seconds": "0.1",
            "var_bid": "100",
            "var_ask": "101",
            "lighter_bid": "100",
            "lighter_ask": "101",
        }
        for index in range(1201)
    ]


def _valid_roundtrip(day: int, index: int) -> dict:
    return {
        "asset": "WTI",
        "record_kind": "oil_swap_roundtrip_validation",
        "logged_at": f"2026-01-0{day}T20:{index:02d}:00+00:00",
        "closed_at": f"2026-01-0{day}T20:{index:02d}:00+00:00",
        "fills_confirmed": True,
        "costs_account_confirmed": True,
        "paired_exposure_confirmed": True,
        "position_reconciled": True,
        "var_instrument_identity": "USOILP:swap:USDC:commodity:0",
        "lighter_market_id": 145,
        "lighter_symbol": "WTI",
        "direction": "long_var_short_lighter",
        "var_entry_side": "buy",
        "var_exit_side": "sell",
        "lighter_entry_side": "sell",
        "lighter_exit_side": "buy",
        "var_entry_avg_price": "100",
        "var_exit_avg_price": "101",
        "var_entry_filled_qty": "0.2",
        "var_exit_filled_qty": "0.2",
        "var_exposure_multiplier": "1",
        "lighter_entry_avg_price": "101",
        "lighter_exit_avg_price": "100",
        "lighter_entry_filled_qty": "0.2",
        "lighter_exit_filled_qty": "0.2",
        "lighter_exposure_multiplier": "1",
        "var_actual_fee_usd": "0.02",
        "lighter_actual_fee_usd": "0.01",
        "var_funding_cashflow_usd": "0.03",
        "lighter_funding_cashflow_usd": "0.01",
        "actual_net_pnl_usd": "0.41",
        "roundtrip_id": f"rt-{day}-{index}",
        "var_entry_trade_id": f"ve-{day}-{index}",
        "lighter_entry_trade_id": f"le-{day}-{index}",
        "var_exit_trade_id": f"vx-{day}-{index}",
        "lighter_exit_trade_id": f"lx-{day}-{index}",
    }


def test_oil_readiness_needs_three_sessions_and_cashflow_cost_evidence() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    ready = assess_oil_readiness(rows, now_utc=now)
    assert ready.market_data_ready
    assert ready.data_fresh
    assert ready.ready_for_bounded_live_review
    assert not ready.ready_for_production_rotation
    assert ready.holdout_session == "2026-01-07"
    assert ready.train_session_count == 2
    assert "insufficient_confirmed_roundtrip_cost_evidence" in ready.reasons
    duplicate_sample = assess_oil_readiness(rows + [dict(rows[-1])], now_utc=now)
    assert duplicate_sample.valid_sample_count == len(rows)

    roundtrips = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    rows.extend(roundtrips + [dict(roundtrips[-1])])
    inferred_id_only = assess_oil_readiness(rows, now_utc=now)
    assert inferred_id_only.ready_for_bounded_live_review
    assert not inferred_id_only.ready_for_production_rotation
    assert not inferred_id_only.lighter_market_id_explicitly_configured
    assert "expected_lighter_market_id_not_configured" in inferred_id_only.reasons
    promoted = assess_oil_readiness(
        rows,
        now_utc=now,
        expected_lighter_market_id=145,
        expected_lighter_exposure_multiplier=Decimal("1"),
    )
    assert promoted.ready_for_bounded_live_review
    assert promoted.historical_promotion_evidence_ready
    assert promoted.ready_for_production_rotation
    assert "live_execution_and_session_gates_not_verified" not in promoted.reasons
    assert promoted.lighter_market_id_explicitly_configured
    assert promoted.lighter_exposure_multiplier == "1"
    assert promoted.lighter_exposure_multiplier_explicitly_configured
    assert promoted.confirmed_roundtrips == 5
    assert promoted.profitable_roundtrips == 5
    assert promoted.validation_session_count == 3
    assert Decimal(promoted.confirmed_net_pnl_usd) == Decimal("2.05")
    assert promoted.rejected_roundtrips == 1

    stale = assess_oil_readiness(rows, now_utc=now + timedelta(days=2))
    assert stale.market_data_ready
    assert not stale.data_fresh
    assert not stale.ready_for_bounded_live_review
    assert not stale.ready_for_production_rotation


def test_oil_readiness_does_not_infer_sessions_or_relax_intrasession_gap() -> None:
    days = ("2026-01-05", "2026-01-06", "2026-01-07")
    rows = sum((_valid_session_rows(day) for day in days), [])
    third_session_start = 2 * 1201
    for row in rows[third_session_start:]:
        for key in ("market_status", "session_status", "session_status_source", "session_status_observed_at",
                    "session_status_feed_connected", "session_status_feed_heartbeat_at",
                    "session_schedule_source", "session_id", "session_closing_at"):
            row.pop(key, None)
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    unverified = assess_oil_readiness(rows, now_utc=now)
    assert unverified.session_count == 2
    assert not unverified.market_data_ready
    assert unverified.unverified_session_sample_count == 1201
    assert "exchange_session_status_unverified_or_inconsistent" in unverified.reasons
    assert "no_verified_exchange_session_samples" not in unverified.reasons

    no_session_rows = sum((_valid_session_rows(day) for day in days), [])
    for row in no_session_rows:
        for key in ("market_status", "session_status", "session_status_source", "session_status_observed_at",
                    "session_status_feed_connected", "session_status_feed_heartbeat_at",
                    "session_schedule_source", "session_id", "session_closing_at"):
            row.pop(key, None)
    none_verified = assess_oil_readiness(no_session_rows, now_utc=now + timedelta(days=1))
    assert none_verified.valid_sample_count == 0
    assert "no_verified_exchange_session_samples" in none_verified.reasons

    rows = sum((_valid_session_rows(day) for day in days), [])
    for row in rows[100:1201]:
        shifted = datetime.fromisoformat(row["logged_at"]) + timedelta(seconds=101)
        row["logged_at"] = shifted.isoformat()
        row["session_status_observed_at"] = shifted.isoformat()
        row["session_status_feed_heartbeat_at"] = shifted.isoformat()
    gap_blocked = assess_oil_readiness(rows, now_utc=now + timedelta(days=1))
    assert not gap_blocked.market_data_ready
    assert any(session.max_gap_seconds == 121 for session in gap_blocked.sessions)


def test_roundtrip_gate_rejects_declared_pnl_with_opposite_sign() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    for row in candidates:
        row.update(
            var_exit_avg_price="100.01",
            lighter_entry_avg_price="101",
            lighter_exit_avg_price="100.99",
            var_actual_fee_usd="0",
            lighter_actual_fee_usd="0",
            var_funding_cashflow_usd="0",
            lighter_funding_cashflow_usd="0",
            actual_net_pnl_usd="-0.005",
        )
    report = assess_oil_readiness(
        rows + candidates,
        now_utc=now,
        expected_lighter_market_id=145,
        expected_lighter_exposure_multiplier=Decimal("1"),
    )
    assert report.confirmed_roundtrips == 0
    assert report.rejected_roundtrips == 5
    assert Decimal(report.confirmed_net_pnl_usd) == 0
    assert not report.ready_for_production_rotation


def test_roundtrip_gate_does_not_promote_declared_profit_when_recomputed_pnl_is_zero() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    for row in candidates:
        row.update(
            var_exit_avg_price="100",
            lighter_exit_avg_price="101",
            var_actual_fee_usd="0",
            lighter_actual_fee_usd="0",
            var_funding_cashflow_usd="0",
            lighter_funding_cashflow_usd="0",
            actual_net_pnl_usd="0.005",
        )
    report = assess_oil_readiness(
        rows + candidates,
        now_utc=now,
        expected_lighter_market_id=145,
        expected_lighter_exposure_multiplier=Decimal("1"),
    )
    assert report.confirmed_roundtrips == 5
    assert Decimal(report.confirmed_net_pnl_usd) == 0
    assert report.profitable_roundtrips == 0
    assert "confirmed_roundtrip_net_pnl_not_positive" in report.reasons
    assert not report.ready_for_production_rotation


def test_roundtrip_gate_rejects_partial_exit_even_when_flags_claim_flat() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    for row in candidates:
        row["var_exit_filled_qty"] = "0.1"
        row["lighter_exit_filled_qty"] = "0.1"
        row["position_reconciled"] = True
    report = assess_oil_readiness(
        rows + candidates,
        now_utc=datetime.fromisoformat("2026-01-07T20:40:00+00:00"),
        expected_lighter_market_id=145,
        expected_lighter_exposure_multiplier=Decimal("1"),
    )
    assert report.confirmed_roundtrips == 0
    assert report.rejected_roundtrips == 5
    assert Decimal(report.confirmed_net_pnl_usd) == 0
    assert not report.ready_for_production_rotation


def test_roundtrip_gate_rejects_conflicting_correction_independent_of_order() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    correction = dict(candidates[-1], var_exit_avg_price="95", actual_net_pnl_usd="-0.79")
    expected_net = Decimal("1.64")
    reports = [
        assess_oil_readiness(
            rows + records,
            now_utc=datetime.fromisoformat("2026-01-07T20:40:00+00:00"),
            expected_lighter_market_id=145,
            expected_lighter_exposure_multiplier=Decimal("1"),
        )
        for records in (candidates + [correction], [correction] + candidates)
    ]
    for report in reports:
        assert report.confirmed_roundtrips == 4
        assert Decimal(report.confirmed_net_pnl_usd) == expected_net
        assert report.rejected_roundtrips == 2
        assert "conflicting_roundtrip_id_evidence" in report.reasons
        assert not report.ready_for_production_rotation


def test_roundtrip_gate_uses_configured_multipliers_not_recorded_multipliers() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    for row in candidates:
        row.update(
            var_exposure_multiplier="100",
            lighter_exposure_multiplier="100",
            actual_net_pnl_usd="40.01",
        )
    report = assess_oil_readiness(
        rows + candidates,
        now_utc=now,
        expected_lighter_market_id=145,
        expected_lighter_exposure_multiplier=Decimal("1"),
    )
    assert report.confirmed_roundtrips == 0
    assert report.rejected_roundtrips == 5
    assert Decimal(report.confirmed_net_pnl_usd) == 0
    assert not report.ready_for_production_rotation


def test_promotion_requires_explicit_lighter_multiplier_configuration() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    rows.extend(_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1)))
    report = assess_oil_readiness(
        rows,
        now_utc=datetime.fromisoformat("2026-01-07T20:40:00+00:00"),
        expected_lighter_market_id=145,
    )
    assert report.confirmed_roundtrips == 5
    assert not report.lighter_exposure_multiplier_explicitly_configured
    assert "expected_lighter_exposure_multiplier_not_configured" in report.reasons
    assert not report.ready_for_production_rotation


def test_roundtrip_gate_rejects_reused_trade_ids_and_declared_pnl_mismatch() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    candidates[1]["var_entry_trade_id"] = candidates[0]["var_entry_trade_id"]
    candidates[3]["lighter_entry_filled_qty"] = "0.3"
    candidates[4]["actual_net_pnl_usd"] = "1000"
    rows.extend(candidates)
    report = assess_oil_readiness(rows, now_utc=now)
    assert report.confirmed_roundtrips == 1
    assert report.rejected_roundtrips == 4
    assert "trade_id_reused_across_roundtrips" in report.reasons
    assert not report.ready_for_production_rotation


def test_readiness_rejects_market_id_mismatch_and_mixed_sample_markets() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    for row in candidates:
        row["lighter_market_id"] = 999
    mismatch = assess_oil_readiness(rows + candidates, now_utc=now)
    assert mismatch.lighter_market_id == 145
    assert mismatch.confirmed_roundtrips == 0
    assert mismatch.rejected_roundtrips == 5
    assert not mismatch.ready_for_production_rotation

    mixed_rows = rows + [dict(rows[0], sample_id="other-market", lighter_market_id=999)]
    mixed = assess_oil_readiness(mixed_rows, now_utc=now)
    assert not mixed.market_data_ready
    assert "lighter_market_id_unavailable_or_inconsistent" in mixed.reasons


def test_readiness_rejects_future_roundtrip_dates_and_missing_close_time() -> None:
    rows = sum(
        (_valid_session_rows(day) for day in ("2026-01-05", "2026-01-06", "2026-01-07")),
        [],
    )
    now = datetime.fromisoformat("2026-01-07T20:40:00+00:00")
    candidates = [_valid_roundtrip(day, index) for day, index in ((5, 1), (5, 2), (6, 1), (6, 2), (7, 1))]
    for index, row in enumerate(candidates, start=1):
        row["closed_at"] = (now + timedelta(days=index)).isoformat()
    candidates[-1].pop("closed_at")
    candidates[0]["closed_at"] = (now + timedelta(seconds=1)).isoformat()
    future = assess_oil_readiness(rows + candidates, now_utc=now)
    assert future.confirmed_roundtrips == 0
    assert future.rejected_roundtrips == 5
    assert future.validation_session_count == 0
    assert not future.ready_for_production_rotation
