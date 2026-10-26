from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import wti_live


@pytest.mark.parametrize("mode", ["observe", "live"])
def test_cli_main_reaches_runtime_with_parsed_arguments(monkeypatch, mode):
    argv = ["tools/wti_live.py", "--mode", mode]
    if mode == "live":
        argv += [
            "--confirm-eth-flat", "--i-understand-live-orders",
            "--confirm-bounded-live-pilot", "--allow-unqualified-data-pilot",
            "--expected-lighter-market-id", "145",
            "--entry-threshold-bps", "30", "--exit-target-bps", "1",
            "--roundtrip-cost-bps", "10", "--max-notional-usd", "20",
        ]
    captured = []

    async def fake_runtime(args):
        captured.append(args)

    monkeypatch.setattr(wti_live.sys, "argv", argv)
    monkeypatch.setattr(wti_live, "_amain", fake_runtime)
    assert wti_live.main() == 0
    assert len(captured) == 1
    assert captured[0].mode == mode
    if mode == "live":
        assert captured[0].allow_unqualified_data_pilot is True
        assert captured[0]._validated_notional == Decimal("20")


def test_gradient_cli_requires_explicit_two_tier_bounded_budget():
    args = wti_live.parse_args([
        "--mode", "live", "--confirm-eth-flat", "--i-understand-live-orders",
        "--gradient-live", "--confirm-bounded-gradient-pilot",
        "--allow-unqualified-data-pilot", "--expected-lighter-market-id", "145",
        "--entry-threshold-bps", "30", "--gradient-second-tier-bps", "45",
        "--gradient-layer-notional-usd", "10", "--exit-target-bps", "1",
        "--roundtrip-cost-bps", "10", "--max-notional-usd", "20",
    ])
    assert args.gradient_live is True
    assert args._validated_gradient_second_tier == Decimal("45")
    assert args._validated_gradient_layer_notional == Decimal("10")
    with pytest.raises(SystemExit):
        wti_live.parse_args([
            "--mode", "live", "--confirm-eth-flat", "--i-understand-live-orders",
            "--gradient-live", "--confirm-bounded-gradient-pilot",
            "--expected-lighter-market-id", "145", "--entry-threshold-bps", "30",
            "--gradient-second-tier-bps", "30", "--exit-target-bps", "1",
            "--roundtrip-cost-bps", "10",
        ])


def test_history_bypass_requires_live_mode_and_explicit_bounded_confirmation():
    with pytest.raises(SystemExit):
        wti_live.parse_args([
            "--mode", "observe", "--allow-unqualified-data-pilot",
            "--confirm-bounded-live-pilot",
        ])
    with pytest.raises(SystemExit):
        wti_live.parse_args(["--mode", "live", "--allow-unqualified-data-pilot"])


def test_rotation_test_id_is_bound_to_gradient_live_mode():
    args = wti_live.parse_args([
        "--mode", "live", "--confirm-eth-flat", "--i-understand-live-orders",
        "--gradient-live", "--confirm-bounded-gradient-pilot",
        "--rotation-test-id", "a" * 32,
        "--expected-lighter-market-id", "145", "--entry-threshold-bps", "30",
        "--gradient-second-tier-bps", "45", "--gradient-layer-notional-usd", "10",
        "--exit-target-bps", "1", "--roundtrip-cost-bps", "10",
        "--max-notional-usd", "20",
    ])
    assert args.rotation_test_id == "a" * 32
    continuous = wti_live.parse_args([
        "--mode", "live", "--confirm-eth-flat", "--i-understand-live-orders",
        "--gradient-live", "--confirm-bounded-gradient-pilot",
        "--continuous-gradient-rotation", "--confirm-continuous-gradient-rotation",
        "--allow-unqualified-data-pilot", "--rotation-test-id", "a" * 32,
        "--expected-lighter-market-id", "145", "--entry-threshold-bps", "30",
        "--gradient-second-tier-bps", "45", "--gradient-layer-notional-usd", "10",
        "--exit-target-bps", "1", "--roundtrip-cost-bps", "10",
        "--max-notional-usd", "20",
    ])
    assert wti_live._unqualified_data_pilot_enabled(continuous)
    with pytest.raises(SystemExit):
        wti_live.parse_args([
            "--mode", "live", "--confirm-eth-flat", "--i-understand-live-orders",
            "--gradient-live", "--confirm-bounded-gradient-pilot",
            "--rotation-test-id", "not-an-id", "--expected-lighter-market-id", "145",
            "--entry-threshold-bps", "30", "--gradient-second-tier-bps", "45",
            "--exit-target-bps", "1", "--roundtrip-cost-bps", "10",
        ])


def test_closed_session_uses_fresh_status_feed_heartbeat_not_old_change_time():
    now = datetime.now(timezone.utc)
    row = _live_sample(now)
    row.update({
        "market_status": "closed",
        "session_status": "closed",
        "session_status_observed_at": (now - timedelta(hours=3)).isoformat(),
        "session_status_feed_heartbeat_at": now.isoformat(),
        "session_closing_at": (now - timedelta(hours=2)).isoformat(),
    })
    health = {
        "market_status": "closed",
        "market_status_feed_connected": True,
        "market_status_feed_heartbeat_at": now.isoformat(),
    }
    window = wti_live._session_window(row, now, 1800, feed_health=health)
    assert window.state.value == "exchange_closed"
    assert window.allow_new_entries is False
    assert window.allow_position_management is False
    stale_health = {
        **health,
        "market_status_feed_heartbeat_at": (now - timedelta(seconds=31)).isoformat(),
    }
    stale = wti_live._session_window(row, now, 1800, feed_health=stale_health)
    assert stale.state.value == "status_unknown"


def test_stale_sample_yields_to_fresh_closed_feed_but_fresh_conflict_blocks():
    now = datetime.now(timezone.utc)
    closed_health = {
        "market_status": "closed",
        "market_status_feed_connected": True,
        "market_status_feed_heartbeat_at": now.isoformat(),
    }
    stale_open_row = _live_sample(now - timedelta(minutes=5))
    closed = wti_live._session_window(stale_open_row, now, 1800, feed_health=closed_health)
    assert closed.state == wti_live.OilSession.EXCHANGE_CLOSED
    assert closed.allow_new_entries is False
    assert closed.allow_position_management is False

    fresh_conflict = _live_sample(now)
    blocked = wti_live._session_window(fresh_conflict, now, 1800, feed_health=closed_health)
    assert blocked.state == wti_live.OilSession.STATUS_UNKNOWN


def test_eth_flat_requires_explicit_confirmation_and_exact_flat_state(tmp_path: Path):
    path = tmp_path / "eth.json"
    path.write_text(json.dumps({
        "asset": "ETH", "status": "flat", "open_lots": [], "pending_actions": [],
    }))
    with pytest.raises(RuntimeError, match="explicit_eth_flat_confirmation_required"):
        wti_live.validate_eth_flat_confirmation(path, confirmed=False)
    assert wti_live.validate_eth_flat_confirmation(path, confirmed=True)["status"] == "flat"
    path.write_text(json.dumps({"asset": "ETH", "status": "open", "open_lots": [{}]}))
    with pytest.raises(RuntimeError, match="eth_local_state_not_confirmed_flat"):
        wti_live.validate_eth_flat_confirmation(path, confirmed=True)


def test_rotation_drain_is_bound_to_pid_and_run_and_revokes_entries(tmp_path: Path, monkeypatch):
    runtime = object.__new__(wti_live.WTILiveRuntime)
    control_path = tmp_path / "wti-control.json"
    runtime.args = SimpleNamespace(control_path=control_path)
    runtime.run_id = "run-1"
    runtime.manage_existing_only = False
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None
    runtime.entry_authorization_permanently_revoked = False
    runtime.last_action = {}
    request = {
        "schema_version": 1, "action": "drain_after_flat", "status": "requested",
        "asset": "WTI", "target_pid": __import__("os").getpid(),
        "target_run_id": "run-1", "request_id": "request-1",
    }
    control_path.write_text(json.dumps(request), encoding="utf-8")
    assert runtime._poll_rotation_drain() is True
    assert runtime.manage_existing_only is True
    assert runtime.entry_authorized is False
    assert runtime.entry_authorization_permanently_revoked is True
    assert json.loads(control_path.read_text()) ["status"] == "draining"

    runtime.run_id = "different-run"
    runtime.rotation_drain_request = None
    runtime.manage_existing_only = False
    assert runtime._poll_rotation_drain() is False
    assert runtime.manage_existing_only is False


def test_rotation_drain_completes_only_with_fresh_verified_flat_snapshot(tmp_path: Path):
    runtime = object.__new__(wti_live.WTILiveRuntime)
    control_path = tmp_path / "wti-control.json"
    now = datetime.now(timezone.utc)
    request = {
        "schema_version": 1, "action": "drain_after_flat", "status": "draining",
        "asset": "WTI", "target_pid": 1, "target_run_id": "run-1",
        "request_id": "request-1",
    }
    control_path.write_text(json.dumps(request), encoding="utf-8")
    runtime.args = SimpleNamespace(control_path=control_path, expected_lighter_market_id=145)
    runtime.rotation_drain_request = request
    runtime.startup_reconciled = True
    runtime.stop = False
    runtime.collector = SimpleNamespace(request_stop=lambda: None)
    runtime.last_action = {}
    runtime.request_stop = lambda: setattr(runtime, "stop", True)
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "last_account_reconciliation": {
            "verified": True, "observed_at": now.isoformat(), "market_id": 145,
            "variational_qty": "0", "lighter_qty": "0", "active_order_count": 0,
        },
    }
    assert runtime._complete_rotation_drain_if_verified_flat(state) is True
    completed = json.loads(control_path.read_text(encoding="utf-8"))
    assert completed["status"] == "completed"
    assert completed["active_order_count"] == 0
    from tools.lib.rotation_supervisor import verified_drain_completion
    assert verified_drain_completion(
        completed, request_id="request-1", source_pid=1, source_run_id="run-1",
    )
    assert runtime.stop is True


def test_rotation_drain_does_not_complete_for_stale_or_nonflat_evidence(tmp_path: Path):
    runtime = object.__new__(wti_live.WTILiveRuntime)
    control_path = tmp_path / "wti-control.json"
    request = {
        "schema_version": 1, "action": "drain_after_flat", "status": "draining",
        "asset": "WTI", "target_pid": 1, "target_run_id": "run-1",
        "request_id": "request-1",
    }
    control_path.write_text(json.dumps(request), encoding="utf-8")
    runtime.args = SimpleNamespace(control_path=control_path, expected_lighter_market_id=145)
    runtime.rotation_drain_request = request
    runtime.startup_reconciled = True
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "last_account_reconciliation": {
            "verified": True, "observed_at": "2020-01-01T00:00:00+00:00", "market_id": 145,
            "variational_qty": "0", "lighter_qty": "0", "active_order_count": 0,
        },
    }
    assert runtime._complete_rotation_drain_if_verified_flat(state) is False
    state["last_account_reconciliation"]["observed_at"] = datetime.now(timezone.utc).isoformat()
    state["last_account_reconciliation"]["lighter_qty"] = "0.1"
    assert runtime._complete_rotation_drain_if_verified_flat(state) is False
    assert json.loads(control_path.read_text(encoding="utf-8"))["status"] == "draining"


def test_continuous_gradient_runtime_rearms_only_after_fresh_account_reconcile():
    session_id = "e" * 32
    previous = {"pilot_id": "pilot-round-1", "status": "completed", "round_number": 1}
    reconciled = {
        "status": "flat", "open_position": None, "pending_action": None,
        "last_account_reconciliation": {
            "verified": True, "variational_qty": "0", "lighter_qty": "0",
            "active_order_count": 0,
        },
    }

    class PilotStore:
        calls = []

        def initialize_gradient_pilot(self, **kwargs):
            self.calls.append(kwargs)
            return {"pilot_id": "pilot-round-2", "round_number": 2, "status": "available"}

    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(
        continuous_gradient_rotation=True, rotation_test_id=session_id,
    )
    runtime.rotation_drain_request = None
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None
    runtime.pilot_store = PilotStore()
    runtime.market_metadata = {"market_id": 145, "multiplier": "1"}
    runtime.logger = logging.getLogger("test_continuous_gradient_runtime_rearm")
    runtime.gradient_pilot = previous
    runtime._gradient_pilot_config = lambda: {"max_completed_roundtrips": 1}
    account_reads = []

    async def read_account_only():
        account_reads.append(True)
        return reconciled

    runtime._read_account_only_reconcile = read_account_only
    runtime._poll_rotation_drain = lambda: None

    result = asyncio.run(runtime._advance_continuous_gradient_round({
        "status": "flat", "open_position": None, "pending_action": None,
        "gradient_pilot": previous,
    }))

    assert result is reconciled
    assert len(account_reads) == 1
    assert runtime.pilot_store.calls == [{
        "market_id": 145, "multiplier": Decimal("1"),
        "config": {"max_completed_roundtrips": 1}, "max_layers": 5,
        "rotation_test_id": session_id, "continuous_rotation": True,
        "new_pilot_after": "pilot-round-1",
    }]
    assert runtime.gradient_pilot["pilot_id"] == "pilot-round-2"
    assert runtime.entry_authorized is False


def test_continuous_gradient_runtime_drain_never_rearms_next_round():
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(continuous_gradient_rotation=True)
    runtime.rotation_drain_request = {"status": "draining"}
    runtime.entry_authorized = True
    runtime._poll_rotation_drain = lambda: None

    async def should_not_reconcile():
        pytest.fail("draining runtime must not reconcile into a new entry round")

    runtime._read_account_only_reconcile = should_not_reconcile
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "gradient_pilot": {"pilot_id": "completed", "status": "completed"},
    }
    result = asyncio.run(runtime._advance_continuous_gradient_round(state))
    assert result is state
    assert runtime.entry_authorized is False


def test_observe_mode_does_not_require_live_eth_confirmation(tmp_path: Path):
    args = SimpleNamespace(
        mode="observe", confirm_eth_flat=False,
        i_understand_live_orders=False, expected_lighter_market_id=None,
    )
    wti_live.validate_startup_preconditions(args, eth_state_path=tmp_path / "missing.json")


def test_pilot_restart_recognizes_pending_close_as_owned_exposure():
    pilot_id = "pilot-123"
    state = {
        "status": "manual_review_required",
        "open_position": None,
        "pending_action": {
            "kind": "close",
            "previous_position": {"single_round_pilot_id": pilot_id},
        },
        "single_round_pilot": {"pilot_id": pilot_id, "status": "in_position"},
    }
    assert wti_live.single_round_pilot_owns_exposure(state)

    state["pending_action"]["previous_position"].pop("single_round_pilot_id")
    assert not wti_live.single_round_pilot_owns_exposure(state)


def test_live_config_pins_existing_data_gap_cutoff_hold_and_notional_caps():
    args = SimpleNamespace(
        entry_threshold_bps=Decimal("4"),
        exit_target_bps=Decimal("2"),
        roundtrip_cost_bps=Decimal("3"),
        max_notional_usd=Decimal("20"),
        max_slippage_bps=Decimal("8"),
        max_hold_seconds=21600,
        entry_cutoff_seconds=1800,
    )
    replay = wti_live.build_replay_config(args)
    assert replay.max_sample_gap_seconds == 120
    assert replay.entry_cutoff_seconds == 1800
    assert replay.max_hold_seconds == 21600
    assert args._validated_notional == Decimal("20")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_notional_usd", Decimal("25.01")),
        ("entry_cutoff_seconds", 1799),
        ("max_hold_seconds", 21601),
        ("max_slippage_bps", Decimal("8.01")),
        ("roundtrip_cost_bps", Decimal("0")),
    ],
)
def test_live_config_rejects_any_relaxed_safety_bound(field, value):
    args = SimpleNamespace(
        entry_threshold_bps=Decimal("4"),
        exit_target_bps=Decimal("2"),
        roundtrip_cost_bps=Decimal("3"),
        max_notional_usd=Decimal("20"),
        max_slippage_bps=Decimal("8"),
        max_hold_seconds=21600,
        entry_cutoff_seconds=1800,
    )
    setattr(args, field, value)
    with pytest.raises(ValueError):
        wti_live.build_replay_config(args)


def test_readiness_for_entries_keeps_three_session_sample_and_gap_rules(monkeypatch):
    captured = {}

    def fake_assess(rows, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            ready_for_bounded_live_review=False,
            market_data_ready=False,
            data_fresh=True,
            ready_for_production_rotation=False,
            lighter_market_id=145,
            lighter_market_id_explicitly_configured=True,
            lighter_exposure_multiplier="1",
            lighter_exposure_multiplier_explicitly_configured=True,
            sessions=(SimpleNamespace(valid_samples=54, max_gap_seconds=120),),
            reasons=("not_enough_sessions",),
            session_count=1,
            valid_sample_count=54,
            confirmed_roundtrips=0,
        )

    monkeypatch.setattr(wti_live, "assess_oil_readiness", fake_assess)
    report = wti_live.readiness_for_new_entries(
        [], market_id=145, multiplier=Decimal("1"),
        now=datetime(2026, 10, 23, tzinfo=timezone.utc),
    )
    assert report["ready"] is False
    assert report["market_data_ready"] is False
    assert report["data_fresh"] is True
    assert report["historical_volume_bypass_ready"] is True
    assert report["historical_volume_bypass_evidence_sessions"] == 1
    assert captured["minimum_sessions"] == 3
    assert captured["minimum_valid_samples_per_session"] == 1200
    assert captured["minimum_coverage_seconds"] == 21600
    assert captured["maximum_gap_seconds"] == 120
    assert captured["maximum_sample_age_seconds"] == 120
    assert captured["expected_lighter_market_id"] == 145
    assert captured["expected_lighter_exposure_multiplier"] == Decimal("1")


@pytest.mark.parametrize(
    ("session_gap", "market_id", "multiplier", "expected"),
    [
        (120, 145, "1", True),
        (120.01, 145, "1", False),
        (120, 146, "1", False),
        (120, 145, "2", False),
    ],
)
def test_history_volume_bypass_keeps_identity_and_gap_evidence(
    monkeypatch, session_gap, market_id, multiplier, expected,
):
    report = SimpleNamespace(
        ready_for_bounded_live_review=False,
        market_data_ready=False,
        data_fresh=False,
        ready_for_production_rotation=False,
        lighter_market_id=market_id,
        lighter_market_id_explicitly_configured=True,
        lighter_exposure_multiplier=multiplier,
        lighter_exposure_multiplier_explicitly_configured=True,
        sessions=(SimpleNamespace(valid_samples=1, max_gap_seconds=session_gap),),
        reasons=("insufficient_qualified_sessions_or_holdout",),
        session_count=0,
        valid_sample_count=1,
        confirmed_roundtrips=0,
    )
    monkeypatch.setattr(wti_live, "assess_oil_readiness", lambda *_args, **_kwargs: report)
    result = wti_live.readiness_for_new_entries(
        [], market_id=145, multiplier=Decimal("1"), now=datetime.now(timezone.utc)
    )
    assert result["historical_volume_bypass_ready"] is expected


def test_live_start_rechecks_readiness_instead_of_reusing_old_pass(monkeypatch, tmp_path):
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(mode="live", sample_root=tmp_path)
    runtime.market_metadata = {"market_id": 145, "multiplier": Decimal("1")}
    runtime.readiness = {"ready": True, "reasons": []}
    captured = {}

    monkeypatch.setattr(wti_live, "read_basis_samples", lambda *args, **kwargs: [])

    def not_ready(rows, **kwargs):
        captured.update(kwargs)
        return {"ready": False, "reasons": ["data_stale"]}

    monkeypatch.setattr(wti_live, "readiness_for_new_entries", not_ready)
    runtime._refresh_readiness()
    assert runtime.readiness == {"ready": False, "reasons": ["data_stale"]}
    assert captured["market_id"] == 145
    assert captured["multiplier"] == Decimal("1")


def test_session_classifier_rejects_expired_exchange_close_time():
    now = datetime.now(timezone.utc)
    row = {
        "market_status": "open",
        "session_status_observed_at": now.isoformat(),
        "session_closing_at": (now.replace(microsecond=0)).isoformat(),
    }
    session = wti_live._session_window(row, now, 1800)
    assert session.exchange_open is None
    assert session.state.value == "status_unknown"


def _live_sample(now: datetime, *, sample_id="sample-1"):
    return {
        "sample_id": sample_id,
        "asset": "WTI",
        "sample_quality": "valid",
        "market_availability": "both_feeds_live",
        "lighter_continuity_ok": True,
        "logged_at": now.isoformat(),
        "variational_underlying": "USOILP",
        "variational_instrument_type": "swap",
        "variational_instrument_kind": "commodity",
        "variational_funding_interval_s": 0,
        "lighter_symbol": "WTI",
        "lighter_market_type": "perp",
        "lighter_market_status": "active",
        "lighter_market_id": 145,
        "lighter_multiplier": "1",
        "lighter_supported_size_decimals": 2,
        "lighter_supported_price_decimals": 2,
        "lighter_min_base_amount": "0.01",
        "lighter_min_quote_amount": "1",
        "variational_qty": "0.29",
        "lighter_qty": "0.29",
        "var_bid": "70",
        "var_ask": "70.1",
        "lighter_bid": "70.02",
        "lighter_ask": "70.08",
        "lighter_buy_price": "70.08",
        "lighter_sell_price": "70.02",
        "long_edge_bps": "0",
        "short_edge_bps": "0",
        "var_quote_age_seconds": "0.2",
        "lighter_book_age_seconds": "0.2",
        "market_status": "open",
        "session_status": "open",
        "session_status_source": "variational_market_status_websocket",
        "session_status_observed_at": now.isoformat(),
        "session_status_feed_connected": True,
        "session_status_feed_heartbeat_at": now.isoformat(),
        "session_schedule_source": "variational_supported_assets_api",
        "session_id": "wti-session-1",
        "session_closing_at": (now + timedelta(hours=8)).isoformat(),
    }


def test_runtime_requires_current_dual_feed_and_verified_session_in_health():
    now = datetime.now(timezone.utc)
    row = _live_sample(now)
    assert wti_live.WTILiveRuntime._sample_health_fresh(row, now)
    assert not wti_live.WTILiveRuntime._sample_health_fresh(
        row, now + timedelta(seconds=2)
    )
    assert not wti_live.WTILiveRuntime._sample_health_fresh(
        {**row, "market_availability": "partial_or_unavailable"}, now
    )
    assert not wti_live.WTILiveRuntime._sample_health_fresh(
        {**row, "session_status_feed_connected": False}, now
    )
    assert not wti_live.WTILiveRuntime._sample_health_fresh(
        {**row, "lighter_continuity_ok": False}, now
    )


def test_execution_session_uses_validated_heartbeat_without_requiring_state_change():
    now = datetime.now(timezone.utc)
    row = _live_sample(now)
    row["session_status_observed_at"] = (now - timedelta(hours=2)).isoformat()
    window = wti_live._session_window(row, now, 1800)
    assert window.allow_new_entries is True
    assert window.observed_at == row["session_status_feed_heartbeat_at"]
    for changed in (
        {"session_status_feed_connected": False},
        {"session_status_feed_heartbeat_at": (now - timedelta(seconds=31)).isoformat()},
        {"session_status_feed_heartbeat_at": (now + timedelta(seconds=6)).isoformat()},
        {"session_status_source": "unverified"},
    ):
        blocked = wti_live._session_window({**row, **changed}, now, 1800)
        assert blocked.allow_new_entries is False
        assert blocked.allow_position_management is False


def test_runtime_skips_sample_that_ages_past_executor_quote_limit():
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.last_processed_sample_id = None
    runtime.last_processed_sample_at = None
    runtime.last_sample_gap_seconds = None
    runtime.pending_gap_exit = False
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None
    runtime.entry_authorization_permanently_revoked = False
    runtime.last_action = {}

    now = datetime.now(timezone.utc)
    aged = _live_sample(now - timedelta(seconds=0.5), sample_id="aged-quote")
    aged["var_quote_age_seconds"] = "1.8"
    assert runtime._record_new_sample(aged) is False
    assert runtime.last_action["reason"] == "wti_current_sample_or_session_not_fresh"
    assert runtime.entry_authorized is False
    assert runtime.entry_authorization_permanently_revoked is False

    assert runtime._record_new_sample(
        _live_sample(now, sample_id="fresh-quote")
    ) is True


def test_runtime_revokes_entry_after_sample_gap_without_reprocessing_same_id():
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.last_processed_sample_id = "sample-1"
    runtime.last_processed_sample_at = datetime.now(timezone.utc) - timedelta(seconds=121)
    runtime.last_sample_gap_seconds = None
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None
    runtime.pending_gap_exit = False
    runtime.last_action = {}

    now = datetime.now(timezone.utc)
    assert runtime._record_new_sample(_live_sample(now, sample_id="sample-2")) is True
    assert runtime.entry_authorized is False
    assert runtime.entry_gate_block_reason == "wti_entry_authorization_revoked_after_sample_gap"
    assert runtime.pending_gap_exit is True
    assert runtime.last_sample_gap_seconds > 120
    assert runtime._record_new_sample(_live_sample(now, sample_id="sample-2")) is None


def test_runtime_keeps_entry_gate_when_sample_gap_is_within_existing_limit():
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.last_processed_sample_id = "sample-1"
    runtime.last_processed_sample_at = datetime.now(timezone.utc) - timedelta(seconds=119)
    runtime.last_sample_gap_seconds = None
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None
    runtime.pending_gap_exit = False
    runtime.last_action = {}

    assert runtime._record_new_sample(
        _live_sample(datetime.now(timezone.utc), sample_id="sample-2")
    ) is True
    assert runtime.entry_authorized is True
    assert runtime.entry_gate_block_reason is None


def _runtime_for_entry_gate(*, allow_unqualified: bool):
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(
        mode="live", allow_unqualified_data_pilot=allow_unqualified,
        confirm_bounded_live_pilot=allow_unqualified,
        confirm_bounded_gradient_pilot=False, gradient_live=False,
        continuous_gradient_rotation=False, manage_existing_wti_position=False,
        rotation_test_id=None,
    )
    runtime.readiness = {
        "ready": False, "market_data_ready": False,
        "historical_volume_bypass_ready": True,
    }
    runtime.single_round_pilot = {"pilot_id": "pilot-1"}
    runtime.manage_existing_only = False
    runtime.entry_authorized = False
    runtime.entry_gate_block_reason = None
    runtime.entry_authorization_permanently_revoked = False
    return runtime


def test_health_exposes_pending_lookup_reason_and_retains_original_submission_receipt(tmp_path):
    runtime = _runtime_for_entry_gate(allow_unqualified=False)
    runtime.args.health_path = tmp_path / "health.json"
    runtime.run_id = "test-pending-diagnostics"
    runtime.stop = False
    runtime.startup_reconciled = False
    runtime.last_sample_gap_seconds = None
    runtime.last_risk = {}
    runtime.last_action = {"action": "reconcile"}
    submission = {"transaction_hash": "tx-accepted", "submission_acknowledged": True}
    state = {
        "status": "manual_review_required", "manual_review_reason": "pending_order_terminal_status_unconfirmed",
        "pending_action": {"legs": {"lighter": {
            "submission_started": True, "submission_result": submission,
            "result": {"classification": "unknown", "lookup_stage": "active_orders",
                       "lookup_reason": "lighter_order_absent_from_active_and_inactive"},
        }}},
    }
    runtime._write_health(state=state, row=_live_sample(datetime.now(timezone.utc)))
    health = json.loads(runtime.args.health_path.read_text())
    diagnostic = health["pending_order_diagnostics"]["lighter"]
    assert diagnostic["lookup_reason"] == "lighter_order_absent_from_active_and_inactive"
    assert diagnostic["submission_result"] == submission
    assert health["startup_reconciled"] is False


def test_unqualified_data_pilot_bypasses_history_only_with_fresh_verified_sample():
    now = datetime.now(timezone.utc)
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {
            "pilot_id": "pilot-1", "status": "available",
            "completed_roundtrips": 0, "entry_attempts": 0,
            "max_entry_attempts": 3,
        },
    }
    row = _live_sample(now)

    ordinary = _runtime_for_entry_gate(allow_unqualified=False)
    assert not ordinary._sync_entry_authorization(state, row, now=now)
    assert ordinary.entry_gate_block_reason == "wti_data_readiness_not_met"

    pilot = _runtime_for_entry_gate(allow_unqualified=True)
    assert pilot._sync_entry_authorization(state, row, now=now)

    stale = _runtime_for_entry_gate(allow_unqualified=True)
    assert not stale._sync_entry_authorization(
        state, {**row, "var_quote_age_seconds": "3"}, now=now
    )
    assert stale.entry_gate_block_reason == "wti_current_sample_or_session_not_fresh"


def test_history_volume_bypass_does_not_authorize_without_clean_historical_evidence():
    now = datetime.now(timezone.utc)
    runtime = _runtime_for_entry_gate(allow_unqualified=True)
    runtime.readiness["historical_volume_bypass_ready"] = False
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {
            "pilot_id": "pilot-1", "status": "available",
            "completed_roundtrips": 0, "entry_attempts": 0,
            "max_entry_attempts": 3,
        },
    }
    assert not runtime._sync_entry_authorization(state, _live_sample(now), now=now)
    assert runtime.entry_gate_block_reason == "wti_historical_volume_bypass_evidence_not_met"


def test_degraded_startup_sample_blocks_entry_then_recovers_on_valid_sample():
    now = datetime.now(timezone.utc)
    runtime = _runtime_for_entry_gate(allow_unqualified=True)
    runtime.last_processed_sample_id = None
    runtime.last_processed_sample_at = None
    runtime.last_sample_gap_seconds = None
    runtime.pending_gap_exit = False
    runtime.last_action = {}
    degraded = {**_live_sample(now), "sample_quality": "degraded"}
    assert runtime._record_new_sample(degraded) is False
    assert runtime.entry_authorized is False
    assert runtime.entry_authorization_permanently_revoked is False
    valid = _live_sample(now, sample_id="sample-2")
    assert runtime._record_new_sample(valid) is True
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {
            "pilot_id": "pilot-1", "status": "available",
            "completed_roundtrips": 0, "entry_attempts": 0,
            "max_entry_attempts": 3,
        },
    }
    assert runtime._sync_entry_authorization(state, valid, now=now)


def test_run_defers_startup_reconcile_age_race_and_recovers_in_live_loop(
    monkeypatch, tmp_path: Path
):
    now = datetime.now(timezone.utc)
    first = _live_sample(now, sample_id="startup-sample")
    following = _live_sample(now, sample_id="loop-sample")

    class FakeCollector:
        def __init__(self):
            self.latest_sample = first
            self.stopped = False

        async def run(self):
            await asyncio.sleep(0.1)
            self.latest_sample = following
            await asyncio.sleep(1.5)

        def request_stop(self):
            self.stopped = True

    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(
        mode="live",
        state_path=tmp_path / "wti-state.json",
        health_path=tmp_path / "wti-health.json",
        allow_unqualified_data_pilot=True,
        confirm_bounded_live_pilot=True,
        confirm_bounded_gradient_pilot=False,
        gradient_live=False,
        continuous_gradient_rotation=False,
        manage_existing_wti_position=False,
        rotation_test_id=None,
    )
    runtime.args.state_path.write_text(json.dumps({
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {"pilot_id": "pilot-1", "status": "available"},
    }))
    runtime.logger = logging.getLogger("test_wti_live_startup_race")
    runtime.collector = FakeCollector()
    runtime.collector_task = None
    runtime.servers = []
    runtime.account_client = None
    runtime.signer_client = None
    runtime.gateway = SimpleNamespace(latest_risk={})
    runtime.readiness = {"ready": False, "market_data_ready": False}
    runtime.run_id = "test-run"
    runtime.stop = False
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None
    runtime.entry_authorization_permanently_revoked = False
    runtime.manage_existing_only = False
    runtime.startup_reconciled = False
    runtime.last_processed_sample_id = None
    runtime.last_processed_sample_at = None
    runtime.last_sample_gap_seconds = None
    runtime.pending_gap_exit = False
    runtime.last_risk = {}
    runtime.last_action = {"action": "none"}
    runtime.last_health_write_monotonic = 0.0
    runtime.last_runtime_error = None
    runtime.single_round_pilot = {"pilot_id": "pilot-1"}
    reconciled_samples = []

    async def no_op():
        return None

    async def first_sample():
        return first

    async def reconcile(row):
        reconciled_samples.append(row["sample_id"])
        if len(reconciled_samples) == 1:
            raise wti_live.WTIExecutionError("variational_quote_stale_or_invalid")
        return {
            "status": "flat", "open_position": None, "pending_action": None,
            "single_round_pilot": {
                "pilot_id": "pilot-1", "status": "completed",
                "completed_roundtrips": 1,
            },
        }

    runtime._start_forwarder = no_op
    runtime._initialize_execution = no_op
    runtime._wait_for_first_sample = first_sample
    runtime._refresh_readiness = lambda: None
    runtime._read_reconcile = reconcile
    runtime._sync_entry_authorization = lambda *_args, **_kwargs: False
    monkeypatch.setattr(wti_live, "validate_startup_preconditions", lambda _args: None)

    asyncio.run(runtime.run())

    assert reconciled_samples == ["startup-sample", "loop-sample"]
    assert runtime.startup_reconciled is True
    assert runtime.entry_authorization_permanently_revoked is False
    assert runtime.last_runtime_error is None


def test_management_runtime_reconciles_account_and_stays_alive_when_market_closed(
    monkeypatch, tmp_path: Path,
):
    class ClosedCollector:
        def __init__(self):
            self.latest_sample = None
            self.stopped = False
            self.market_status_feed = SimpleNamespace(
                status="closed", connected=True, heartbeat_at=datetime.now(timezone.utc),
            )

        async def run(self):
            await asyncio.sleep(0.05)

        def request_stop(self):
            self.stopped = True

    class AccountOnlyExecutor:
        def __init__(self):
            self.calls = []

        def reconcile_account_only(self, **kwargs):
            self.calls.append(kwargs)
            return {"status": "flat", "open_position": None, "pending_action": None}

    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(
        mode="live", manage_existing_wti_position=True,
        initial_sample_timeout_seconds=1, entry_cutoff_seconds=1800,
        state_path=tmp_path / "wti-state.json",
    )
    runtime.args.state_path.write_text(json.dumps({
        "status": "flat", "open_position": None, "pending_action": None,
    }))
    runtime.logger = logging.getLogger("test_wti_closed_management_startup")
    runtime.collector = ClosedCollector()
    runtime.collector_task = None
    runtime.servers = []
    runtime.account_client = None
    runtime.signer_client = None
    runtime.gateway = None
    runtime.executor = None
    runtime.run_id = "closed-session-run"
    runtime.stop = False
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None
    runtime.manage_existing_only = True
    runtime.startup_reconciled = False
    runtime.last_risk = {}
    runtime.last_action = {"action": "starting"}
    runtime.last_health_write_monotonic = 0.0
    runtime.last_account_only_reconcile_monotonic = 0.0
    runtime.last_runtime_error = None
    runtime.last_reconcile_elapsed_ms = None
    runtime.rotation_drain_request = None
    runtime.readiness = {"ready": False, "reasons": ["closed"]}
    runtime.single_round_pilot = None
    runtime.gradient_pilot = None
    executor = AccountOnlyExecutor()
    health_writes = []

    async def initialize():
        runtime.executor = executor
        runtime.gateway = SimpleNamespace(latest_risk={"risk_action": "normal"})

    async def no_op():
        return None

    runtime._start_forwarder = no_op
    runtime._initialize_execution = initialize
    runtime._write_health = lambda **kwargs: health_writes.append(kwargs)
    runtime._poll_rotation_drain = lambda: False
    runtime._complete_rotation_drain_if_verified_flat = lambda _state: False
    monkeypatch.setattr(wti_live, "validate_startup_preconditions", lambda _args: None)

    asyncio.run(runtime.run())

    assert runtime.startup_reconciled is True
    assert runtime.entry_authorized is False
    assert runtime.last_action == {"action": "hold", "reason": "exchange_closed_no_market_sample"}
    assert len(executor.calls) == 1
    assert executor.calls[0]["runtime_run_id"] == "closed-session-run"
    assert all(call["row"] is None for call in health_writes)


class _FlatLoopGateway:
    def __init__(self, *, delay=0):
        self.delay = delay
        self.read_calls = 0
        self.after_read = None
        self.latest_risk = {"risk_action": "normal", "risk_reason": "account_risk_normal"}

    def read_account(self):
        time.sleep(self.delay)
        self.read_calls += 1
        if self.after_read:
            self.after_read(self.read_calls)
        return {
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "variational_positions": {"ok": True, "positions": []},
            "lighter_positions": {"code": 200, "accounts": [{"positions": []}]},
            "active_orders": {"ok": True, "orders": []},
        }

    def set_current_notional(self, _value):
        pass

    def set_proposed_notional(self, _value, *, equity_multiple=None):
        pass

    def submit_order(self, _request):
        raise AssertionError("below-threshold samples must never submit")

    def lookup_order(self, *_args, **_kwargs):
        raise AssertionError("flat state has no orders to recover")


def _runtime_with_real_executor(tmp_path, collector, gateway, *, gradient=False):
    runtime = _runtime_for_entry_gate(allow_unqualified=True)
    argv = [
        "--mode", "live", "--confirm-eth-flat", "--i-understand-live-orders",
        "--expected-lighter-market-id", "145", "--entry-threshold-bps", "30",
        "--exit-target-bps", "1", "--roundtrip-cost-bps", "10",
        "--state-path", str(tmp_path / "wti-state.json"),
        "--health-path", str(tmp_path / "wti-health.json"),
        "--sample-root", str(tmp_path / "samples"),
    ]
    if gradient:
        argv += [
            "--gradient-live", "--confirm-bounded-gradient-pilot",
            "--allow-unqualified-data-pilot", "--gradient-second-tier-bps", "45",
            "--gradient-layer-notional-usd", "9",
        ]
    else:
        argv += ["--confirm-bounded-live-pilot", "--allow-unqualified-data-pilot"]
    runtime.args = wti_live.parse_args(argv)
    runtime.config = wti_live.build_replay_config(runtime.args)
    runtime.logger = logging.getLogger("test_wti_real_loop")
    runtime.collector = collector
    runtime.collector_task = None
    runtime.servers = []
    runtime.account_client = None
    runtime.signer_client = None
    runtime.gateway = gateway
    runtime.market_metadata = {"market_id": 145, "multiplier": Decimal("1")}
    runtime.run_id = "test-real-loop"
    runtime.stop = False
    runtime.startup_reconciled = False
    runtime.last_processed_sample_id = None
    runtime.last_processed_sample_at = None
    runtime.last_sample_gap_seconds = None
    runtime.pending_gap_exit = False
    runtime.last_risk = {}
    runtime.last_action = {"action": "none"}
    runtime.last_health_write_monotonic = 0
    runtime.last_runtime_error = None
    runtime.pilot_store = wti_live.WTIExecutionStore(runtime.args.state_path)
    runtime.gradient_pilot = None
    if gradient:
        runtime.single_round_pilot = None
        runtime.gradient_pilot = runtime.pilot_store.initialize_gradient_pilot(
            market_id=145, multiplier=Decimal("1"),
            config=runtime._gradient_pilot_config(),
            max_layers=wti_live.WTI_GRADIENT_MAX_LAYERS,
        )
    else:
        runtime.single_round_pilot = runtime.pilot_store.initialize_single_round_pilot(
            market_id=145, multiplier=Decimal("1"),
            config=runtime._pilot_config(), readiness=runtime.readiness,
        )
    runtime.executor = wti_live.WTIPairExecutor(
        state_path=runtime.args.state_path, gateway=gateway,
        max_slippage_bps=Decimal("8"),
        market_reader=lambda: collector.latest_sample,
        expected_lighter_market_id=145, expected_lighter_multiplier=Decimal("1"),
        expected_lighter_size_decimals=2, expected_lighter_price_decimals=2,
        max_notional_usd=Decimal("20"),
    )
    return runtime


def test_gradient_equity_capacity_requires_fresh_complete_dual_equity_snapshot():
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(_validated_gradient_layer_notional=Decimal("20"))
    runtime.last_risk = {
        "risk_data_complete": True,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "variational_equity_usd": "100",
        "lighter_equity_usd": "80",
    }
    assert runtime._gradient_equity_capacity_by_tier() == tuple(map(
        Decimal, ("80", "160", "240", "320", "400"),
    ))

    runtime.last_risk["observed_at"] = (
        datetime.now(timezone.utc) - timedelta(seconds=16)
    ).isoformat()
    assert runtime._gradient_equity_capacity_by_tier() is None
    runtime.last_risk["observed_at"] = datetime.now(timezone.utc).isoformat()
    runtime.last_risk["risk_data_complete"] = False
    assert runtime._gradient_equity_capacity_by_tier() is None


def test_run_keeps_collector_alive_during_slow_history_scan_and_reconciles_fresh_sample(
    monkeypatch, tmp_path
):
    class StreamingCollector:
        latest_sample = None
        frames = 0
        stopped = False

        async def run(self):
            while not self.stopped:
                self.frames += 1
                self.latest_sample = _live_sample(
                    datetime.now(timezone.utc), sample_id=f"stream-{self.frames}"
                )
                await asyncio.sleep(0.05)

        def request_stop(self):
            self.stopped = True

    collector = StreamingCollector()
    gateway = _FlatLoopGateway()
    runtime = _runtime_with_real_executor(tmp_path, collector, gateway)
    scans = []

    def slow_history(*_args, **_kwargs):
        before = collector.frames
        time.sleep(2.1)
        scans.append(collector.frames - before)
        return []

    async def no_op():
        pass

    async def initialize():
        loop = asyncio.get_running_loop()
        gateway.after_read = lambda n: loop.call_soon_threadsafe(runtime.request_stop) if n == 2 else None
        await runtime._load_startup_readiness()

    runtime._start_forwarder = no_op
    runtime._initialize_execution = initialize
    monkeypatch.setattr(wti_live, "read_basis_samples", slow_history)
    monkeypatch.setattr(wti_live, "readiness_for_new_entries", lambda *_args, **_kwargs: {
        "ready": False, "market_data_ready": False, "data_fresh": False,
        "historical_volume_bypass_ready": True,
    })
    monkeypatch.setattr(wti_live, "validate_startup_preconditions", lambda _args: None)

    async def run_bounded():
        await asyncio.wait_for(runtime.run(), timeout=8)

    asyncio.run(run_bounded())

    assert len(scans) == 1
    assert scans[0] >= 10
    assert gateway.read_calls == 2
    assert runtime.startup_reconciled is True
    assert runtime.entry_authorized is True
    assert runtime.last_action["reason"] == "entry_edge_below_threshold"
    assert runtime.last_runtime_error is None
    assert runtime.pilot_store.load()["single_round_pilot"]["entry_attempts"] == 0


@pytest.mark.parametrize("quality", ["valid", "degraded"])
def test_real_reconcile_refreshes_quote_after_slow_account_read(tmp_path, quality):
    class RefreshingCollector:
        latest_sample = _live_sample(datetime.now(timezone.utc))
        refreshes = 0

        async def collect_fresh_sample(self):
            self.refreshes += 1
            self.latest_sample = {
                **_live_sample(datetime.now(timezone.utc), sample_id="after-account"),
                "sample_quality": quality,
            }
            return self.latest_sample

    collector = RefreshingCollector()
    gateway = _FlatLoopGateway(delay=2.1)
    runtime = _runtime_with_real_executor(tmp_path, collector, gateway)
    row = dict(collector.latest_sample)
    assert runtime._record_new_sample(row) is True

    state = asyncio.run(runtime._process_sample(row))

    assert state["status"] == "flat"
    assert collector.refreshes == 1
    assert runtime.last_reconcile_elapsed_ms >= 2000
    assert runtime.entry_authorization_permanently_revoked is False
    assert runtime.pilot_store.load()["single_round_pilot"]["entry_attempts"] == 0
    if quality == "valid":
        assert runtime.entry_authorized is True
        assert runtime.last_action["reason"] == "entry_edge_below_threshold"
    else:
        assert runtime.entry_authorized is False
        assert runtime.last_action["reason"] == "wti_current_sample_or_session_not_fresh"


def test_real_strategy_executor_roundtrip_after_slow_account_read_stops_one_pilot(tmp_path):
    class FillingGateway(_FlatLoopGateway):
        def __init__(self):
            super().__init__(delay=2.1)
            self.var_qty = Decimal("0")
            self.lighter_qty = Decimal("0")
            self.requests = []
            self.after_read = lambda _n: setattr(self, "delay", 0)

        def read_account(self):
            snapshot = super().read_account()
            snapshot["variational_positions"]["positions"] = [{
                "instrument": {
                    "underlying": "USOILP", "instrument_type": "swap",
                    "kind": "commodity", "settlement_asset": "USDC", "funding_interval_s": 0,
                },
                "qty": str(self.var_qty),
            }]
            snapshot["lighter_positions"]["accounts"][0]["positions"] = [{
                "market_id": 145, "symbol": "WTI", "position": str(abs(self.lighter_qty)),
                "sign": 1 if self.lighter_qty >= 0 else -1,
            }]
            return snapshot

        def submit_order(self, request):
            self.requests.append(dict(request))
            qty = Decimal(request["qty"])
            signed = qty if request["side"] == "buy" else -qty
            if request["venue"] == "variational":
                self.var_qty += signed
            else:
                self.lighter_qty += signed
            return {
                "ok": True, "final": True, "status": "filled",
                "client_order_id": request["client_order_id"],
                "order_id": f"order-{len(self.requests)}", "filled_qty": str(qty),
                "average_price": request["reference_price"], "fee_usd": "0",
                "trade_ids": [f"trade-{len(self.requests)}"],
            }

    class PairCollector:
        phase = "entry"
        refreshes = 0
        stopped = False

        def __init__(self):
            self.closing_at = (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat()
            self.latest_sample = self.sample()
            self.requested_sizes = []

        def sample(self, *, var_qty=None, lighter_qty=None):
            self.refreshes += 1
            row = _live_sample(datetime.now(timezone.utc), sample_id=f"pair-{self.refreshes}")
            row["session_closing_at"] = self.closing_at
            if self.phase == "entry":
                row.update({
                    "lighter_bid": "70.5", "lighter_ask": "70.6",
                    "lighter_sell_price": "70.5", "lighter_buy_price": "70.6",
                    "long_edge_bps": "50",
                })
            else:
                row.update({
                    "var_bid": "70.45", "var_ask": "70.55",
                    "lighter_bid": "70", "lighter_ask": "70.1",
                    "lighter_sell_price": "70", "lighter_buy_price": "70.1",
                    "variational_qty": "0.27", "lighter_qty": "0.27",
                })
            if var_qty is not None:
                row["variational_qty"] = str(var_qty)
            if lighter_qty is not None:
                row["lighter_qty"] = str(lighter_qty)
            return row

        async def collect_fresh_sample(self, *, var_qty=None, lighter_qty=None):
            if var_qty is not None or lighter_qty is not None:
                self.requested_sizes.append((var_qty, lighter_qty))
            self.latest_sample = self.sample(var_qty=var_qty, lighter_qty=lighter_qty)
            return dict(self.latest_sample)

        def request_stop(self):
            self.stopped = True

    collector = PairCollector()
    gateway = FillingGateway()
    runtime = _runtime_with_real_executor(tmp_path, collector, gateway)

    async def roundtrip():
        loop = asyncio.get_running_loop()
        runtime.executor.market_reader = lambda: asyncio.run_coroutine_threadsafe(
            collector.collect_fresh_sample(), loop
        ).result(timeout=3)
        runtime.executor.market_reader_for_qty = lambda var_qty, lighter_qty: asyncio.run_coroutine_threadsafe(
            collector.collect_fresh_sample(var_qty=var_qty, lighter_qty=lighter_qty), loop
        ).result(timeout=3)
        first = dict(collector.latest_sample)
        assert runtime._record_new_sample(first) is True
        opened = await runtime._process_sample(first)
        assert opened["status"] == "open"
        assert opened["open_position"]["var_qty"] == "0.28"
        assert runtime.stop is False
        collector.requested_sizes.clear()
        collector.phase = "exit"
        exit_row = await collector.collect_fresh_sample()
        assert exit_row["variational_qty"] == exit_row["lighter_qty"] == "0.27"
        assert runtime._record_new_sample(exit_row) is True
        return await runtime._process_sample(exit_row)

    closed = asyncio.run(roundtrip())

    assert closed["status"] == "flat"
    assert closed["pending_action"] is None
    assert closed["single_round_pilot"]["completed_roundtrips"] == 1
    assert closed["single_round_pilot"]["entry_attempts"] == 1
    assert runtime.stop is True and collector.stopped is True
    assert gateway.var_qty == gateway.lighter_qty == 0
    assert len(gateway.requests) == 4
    assert all(Decimal(r["qty"]) == Decimal("0.28") for r in gateway.requests)
    assert [r["reduce_only"] for r in gateway.requests] == [False, False, True, True]


def test_real_strategy_executor_runs_two_layer_gradient_once_and_completes_durable_budget(tmp_path):
    class GradientGateway(_FlatLoopGateway):
        def __init__(self):
            super().__init__()
            self.var_qty = Decimal("0")
            self.lighter_qty = Decimal("0")
            self.requests = []

        def read_account(self):
            snapshot = super().read_account()
            snapshot["variational_positions"]["positions"] = [{
                "instrument": {
                    "underlying": "USOILP", "instrument_type": "swap",
                    "kind": "commodity", "settlement_asset": "USDC", "funding_interval_s": 0,
                },
                "qty": str(self.var_qty),
            }]
            snapshot["lighter_positions"]["accounts"][0]["positions"] = [{
                "market_id": 145, "symbol": "WTI", "position": str(abs(self.lighter_qty)),
                "sign": 1 if self.lighter_qty >= 0 else -1,
            }]
            return snapshot

        def submit_order(self, request):
            self.requests.append(dict(request))
            qty = Decimal(request["qty"])
            signed = qty if request["side"] == "buy" else -qty
            if request["venue"] == "variational":
                self.var_qty += signed
            else:
                self.lighter_qty += signed
            return {
                "ok": True, "final": True, "status": "filled",
                "client_order_id": request["client_order_id"],
                "order_id": f"gradient-order-{len(self.requests)}",
                "filled_qty": str(qty), "average_price": request["reference_price"],
                "fee_usd": "0", "trade_ids": [f"gradient-trade-{len(self.requests)}"],
            }

    class GradientCollector:
        stopped = False
        latest_sample = None

        def __init__(self):
            self.phase = "first"
            self.requested_sizes = []
            self.closing = (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat()
            self.latest_sample = self.sample()

        def sample(self, *, var_qty=None, lighter_qty=None):
            row = _live_sample(datetime.now(timezone.utc), sample_id=f"gradient-{uuid.uuid4().hex}")
            row["session_closing_at"] = self.closing
            row["variational_qty"] = str(var_qty or "0.29")
            row["lighter_qty"] = str(lighter_qty or "0.29")
            row["long_edge_bps"] = "50" if self.phase != "exit" else "0"
            if self.phase == "add":
                row["var_bid"] = "70.0"
                row["lighter_buy_price"] = "70.1"
            elif self.phase == "exit":
                row["var_bid"] = "70.3"
                row["var_ask"] = "70.4"
                row["lighter_buy_price"] = "69.8"
            return row

        async def collect_fresh_sample(self, *, var_qty=None, lighter_qty=None):
            if var_qty is not None and lighter_qty is not None:
                self.requested_sizes.append((Decimal(str(var_qty)), Decimal(str(lighter_qty))))
            self.latest_sample = self.sample(var_qty=var_qty, lighter_qty=lighter_qty)
            return dict(self.latest_sample)

        def request_stop(self):
            self.stopped = True

    collector = GradientCollector()
    gateway = GradientGateway()
    runtime = _runtime_with_real_executor(tmp_path, collector, gateway, gradient=True)

    async def run_cycle():
        loop = asyncio.get_running_loop()
        runtime.executor.market_reader = lambda: asyncio.run_coroutine_threadsafe(
            collector.collect_fresh_sample(), loop
        ).result(timeout=3)
        runtime.executor.market_reader_for_qty = lambda var_qty, lighter_qty: asyncio.run_coroutine_threadsafe(
            collector.collect_fresh_sample(var_qty=var_qty, lighter_qty=lighter_qty), loop
        ).result(timeout=3)
        row = dict(collector.latest_sample)
        assert runtime._record_new_sample(row) is True
        first = await runtime._process_sample(row)
        assert first["status"] == "open"
        assert len(first["open_position"]["layers"]) == 1
        collector.phase = "add"
        row = await collector.collect_fresh_sample()
        assert runtime._record_new_sample(row) is True
        second = await runtime._process_sample(row)
        assert second["status"] == "open"
        assert len(second["open_position"]["layers"]) == 2
        collector.phase = "exit"
        row = await collector.collect_fresh_sample()
        assert runtime._record_new_sample(row) is True
        return await runtime._process_sample(row)

    closed = asyncio.run(run_cycle())
    assert closed["status"] == "flat"
    assert closed["gradient_pilot"]["status"] == "completed"
    assert closed["gradient_pilot"]["completed_roundtrips"] == 1
    assert [layer["tier"] for layer in closed["last_closed_position"]["layers"]] == [1, 2]
    assert gateway.var_qty == gateway.lighter_qty == 0
    assert len(gateway.requests) == 6
    assert [request["reduce_only"] for request in gateway.requests] == [
        False, False, False, False, True, True,
    ]
    assert runtime.stop is True and collector.stopped is True
    assert collector.requested_sizes == [
        (Decimal("0.13"), Decimal("0.13")),
        (Decimal("0.13"), Decimal("0.13")),
        (Decimal("0.13"), Decimal("0.13")),
        (Decimal("0.13"), Decimal("0.13")),
        (Decimal("0.26"), Decimal("0.26")),
        (Decimal("0.26"), Decimal("0.26")),
    ]


def test_single_round_pilot_finishes_only_after_confirmed_flat_state():
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.stop = False
    runtime.collector = SimpleNamespace(request_stop=lambda: setattr(runtime, "stop", True))
    runtime.single_round_pilot = None
    runtime.entry_authorized = True
    runtime.entry_gate_block_reason = None

    open_state = {
        "status": "open", "open_position": {"var_qty": "0.2"},
        "pending_action": None,
        "single_round_pilot": {"status": "completed", "completed_roundtrips": 1},
    }
    assert not runtime._finish_single_round_pilot_if_done(open_state)
    assert runtime.stop is False

    flat_state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {"status": "completed", "completed_roundtrips": 1},
    }
    assert runtime._finish_single_round_pilot_if_done(flat_state)
    assert runtime.stop is True
    assert runtime.entry_authorized is False
    assert runtime.entry_gate_block_reason == "single_round_pilot_completed"


def test_completed_legacy_single_round_does_not_stop_gradient_pilot():
    runtime = _runtime_for_entry_gate(allow_unqualified=False)
    runtime.args.gradient_live = True
    runtime.stop = False
    runtime.collector = SimpleNamespace(
        request_stop=lambda: pytest.fail("legacy single-round record must not stop gradient runtime")
    )
    runtime.entry_authorized = True
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {"status": "completed", "completed_roundtrips": 1},
        "gradient_pilot": {
            "pilot_id": "gradient-pilot-1", "status": "available",
            "completed_roundtrips": 0,
        },
    }
    assert not runtime._finish_single_round_pilot_if_done(state)
    assert runtime.stop is False
    assert runtime.entry_authorized is True


@pytest.mark.parametrize("pilot_status", ["completed", "exhausted"])
def test_management_keeps_collector_and_forwarder_after_pilot_finishes(pilot_status):
    runtime = _runtime_for_entry_gate(allow_unqualified=False)
    runtime.args.manage_existing_wti_position = True
    runtime.manage_existing_only = True
    runtime.stop = False
    runtime.collector = SimpleNamespace(request_stop=lambda: pytest.fail("collector must keep running"))
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {"status": pilot_status, "completed_roundtrips": 1},
    }
    assert runtime._finish_single_round_pilot_if_done(state)
    assert runtime.stop is False
    assert runtime.entry_authorized is False


def test_resumed_bounded_pilot_still_stops_after_one_round():
    runtime = _runtime_for_entry_gate(allow_unqualified=True)
    runtime.args.manage_existing_wti_position = False
    runtime.manage_existing_only = True
    runtime.stop = False
    runtime.collector = SimpleNamespace(request_stop=lambda: None)
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {"status": "completed", "completed_roundtrips": 1},
    }
    assert runtime._finish_single_round_pilot_if_done(state)
    assert runtime.stop is True


def test_explicit_live_test_keeps_collecting_without_renewing_entry_budget():
    runtime = _runtime_for_entry_gate(allow_unqualified=True)
    runtime.args.keep_collecting_after_pilot = True
    runtime.stop = False
    runtime.collector = SimpleNamespace(request_stop=lambda: pytest.fail("collector must keep running"))
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {"status": "completed", "completed_roundtrips": 1},
    }
    assert runtime._finish_single_round_pilot_if_done(state)
    assert runtime.stop is False and runtime.entry_authorized is False
    assert not runtime._sync_entry_authorization(state, _live_sample(datetime.now(timezone.utc)), now=datetime.now(timezone.utc))


@pytest.mark.parametrize("scenario", ["manage_flat", "manage_exposure", "new_test", "resume_test", "wrong_test"])
def test_initialization_handles_management_and_explicit_new_test_without_renewing_budget(
    monkeypatch, tmp_path, scenario,
):
    import lighter.signer_client
    from tools import wti_account_probe
    from tools.lib import wti_readonly_adapter

    class Signer:
        def __init__(self, **_kwargs):
            pass

        def check_client(self):
            return None

    class Forwarder:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

    collector = SimpleNamespace(latest_sample=None)
    runtime = _runtime_with_real_executor(tmp_path, collector, _FlatLoopGateway())
    runtime.args.manage_existing_wti_position = True
    runtime.args.confirm_bounded_live_pilot = False
    runtime.args.allow_unqualified_data_pilot = False
    state = runtime.pilot_store.load()
    if scenario == "manage_exposure":
        state.update(status="manual_review_required", pending_action={"kind": "close"})
    if scenario in {"new_test", "resume_test", "wrong_test"}:
        runtime.args.manage_existing_wti_position = False
        runtime.args.confirm_bounded_live_pilot = True
        runtime.args.allow_unqualified_data_pilot = True
        runtime.args.new_pilot_after = state["single_round_pilot"]["pilot_id"]
        state["single_round_pilot"].update(status="completed", completed_roundtrips=1)
    if scenario in {"resume_test", "wrong_test"}:
        runtime.args.new_pilot_after = "exact-previous-test"
        state["single_round_pilot"].update(
            status="in_position", completed_roundtrips=0, previous_pilot_id="exact-previous-test",
        )
        state.update(status="open", open_position={
            "single_round_pilot_id": state["single_round_pilot"]["pilot_id"],
        })
    if scenario == "wrong_test":
        runtime.args.new_pilot_after = "wrong-previous-test"
    runtime.pilot_store.save(state)
    before = runtime.args.state_path.read_bytes()

    async def readiness():
        runtime.readiness = {
            "ready": False, "market_data_ready": False,
            "historical_volume_bypass_ready": True,
        }

    market = {
        "market_id": 145, "symbol": "WTI", "market_type": "perp", "status": "active",
        "multiplier": Decimal("1"), "supported_size_decimals": 3,
        "supported_price_decimals": 3, "min_base_amount": "0.001", "min_quote_amount": "1",
    }
    runtime._load_startup_readiness = readiness
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "1")
    monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "2")
    monkeypatch.setenv("API_KEY_PRIVATE_KEY", "test-only-not-used")
    monkeypatch.setattr(lighter.signer_client, "SignerClient", Signer)
    monkeypatch.setattr(wti_account_probe, "fetch_lighter_market_metadata", lambda *_args, **_kwargs: market)
    monkeypatch.setattr(wti_readonly_adapter, "VariationalForwarderClient", Forwarder)
    if scenario == "wrong_test":
        with pytest.raises(RuntimeError, match="requires_flat_wti_state"):
            asyncio.run(runtime._initialize_execution())
        assert runtime.args.state_path.read_bytes() == before
        return
    asyncio.run(runtime._initialize_execution())
    assert runtime.manage_existing_only is (scenario != "new_test")
    if scenario == "new_test":
        after = runtime.pilot_store.load()
        assert after["single_round_pilot"]["previous_pilot_id"] == runtime.args.new_pilot_after
        assert after["single_round_pilot"]["status"] == "available"
        assert len(after["single_round_pilot_history"]) == 1
    else:
        assert runtime.args.state_path.read_bytes() == before
    assert runtime.entry_authorized is False


def test_full_run_recovers_real_0198_close_once_then_continues_collecting(monkeypatch, tmp_path):
    class Gateway(_FlatLoopGateway):
        def __init__(self):
            super().__init__()
            self.var_qty = Decimal("0")
            self.lighter_qty = Decimal("0")
            self.requests = []
            self.lose_var_close_reply = False

        def read_account(self):
            snapshot = super().read_account()
            snapshot["variational_positions"]["positions"] = [{
                "instrument": {"underlying": "USOILP", "instrument_type": "swap",
                               "kind": "commodity", "settlement_asset": "USDC", "funding_interval_s": 0},
                "qty": str(self.var_qty),
            }]
            snapshot["lighter_positions"]["accounts"][0]["positions"] = [{
                "market_id": 145, "symbol": "WTI", "position": str(abs(self.lighter_qty)),
                "sign": 1 if self.lighter_qty >= 0 else -1,
            }]
            return snapshot

        def submit_order(self, request):
            self.requests.append(dict(request))
            qty = Decimal(request["qty"])
            signed = qty if request["side"] == "buy" else -qty
            if request["venue"] == "variational":
                self.var_qty += signed
                if self.lose_var_close_reply and request["reduce_only"]:
                    return {"ok": False, "final": False, "status": "unknown"}
            else:
                self.lighter_qty += signed
            return {
                "ok": True, "final": True, "status": "filled", "filled_qty": str(qty),
                "order_id": f"order-{len(self.requests)}", "client_order_id": request["client_order_id"],
                "average_price": request["reference_price"],
            }

        def lookup_order(self, *_args, **_kwargs):
            return {"ok": False, "final": False, "status": "unknown"}

    class Collector:
        latest_sample = None
        stopped = False
        frames = 0

        def __init__(self):
            self.closing = (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat()

        def sample(self, *, var_qty=None, lighter_qty=None):
            self.frames += 1
            row = _live_sample(datetime.now(timezone.utc), sample_id=f"recovery-{self.frames}")
            row.update({
                "var_bid": "100.420", "var_ask": "100.426",
                "lighter_bid": "100.800", "lighter_ask": "100.810",
                "lighter_buy_price": "100.810", "lighter_sell_price": "100.800",
                "long_edge_bps": "38", "short_edge_bps": "-39",
                "variational_qty": str(var_qty or "0.195"),
                "lighter_qty": str(lighter_qty or "0.195"),
                "lighter_supported_size_decimals": 3, "lighter_supported_price_decimals": 3,
                "session_closing_at": self.closing,
            })
            return row

        async def collect_fresh_sample(self, **kwargs):
            self.latest_sample = self.sample(**kwargs)
            return dict(self.latest_sample)

        async def run(self):
            while not self.stopped:
                await self.collect_fresh_sample()
                if processed:
                    self.kept_collecting_after_flat = True
                    runtime.request_stop()
                await asyncio.sleep(0.02)

        def request_stop(self):
            self.stopped = True

    processed = []
    gateway, collector = Gateway(), Collector()
    runtime = _runtime_with_real_executor(tmp_path, collector, gateway)
    runtime.args.manage_existing_wti_position = True
    runtime.args.confirm_bounded_live_pilot = False
    runtime.args.allow_unqualified_data_pilot = False
    runtime.args.forwarder_ws_port = runtime.args.forwarder_rest_port = runtime.args.forwarder_command_port = 0
    runtime.manage_existing_only = True
    runtime.executor = wti_live.WTIPairExecutor(
        state_path=runtime.args.state_path, gateway=gateway, max_slippage_bps=Decimal("8"),
        market_reader=lambda: collector.sample(),
        market_reader_for_qty=lambda v, l: collector.sample(var_qty=v, lighter_qty=l),
        expected_lighter_market_id=145, expected_lighter_multiplier=Decimal("1"),
        expected_lighter_size_decimals=3, expected_lighter_price_decimals=3,
        max_notional_usd=Decimal("20"),
    )
    pilot = runtime.single_round_pilot
    reservation = runtime.pilot_store.reserve_single_round_pilot_entry(pilot["pilot_id"])
    sample = collector.sample(var_qty=Decimal("0.198"), lighter_qty=Decimal("0.198"))
    opened = runtime.executor.open(
        sample=sample, direction="long_var_short_lighter", notional_usd=Decimal("19.9"),
        session=wti_live._session_window(sample, datetime.now(timezone.utc), 1800),
        pilot_id=pilot["pilot_id"], pilot_reservation_id=reservation,
    )
    assert opened["open_position"]["var_qty"] == "0.198"
    gateway.lose_var_close_reply = True
    interrupted = runtime.executor.close(
        sample=sample, session=wti_live._session_window(sample, datetime.now(timezone.utc), 1800),
    )
    assert interrupted["status"] == "manual_review_required"
    assert gateway.var_qty == 0 and gateway.lighter_qty == Decimal("-0.198")
    real_process_sample = runtime._process_sample

    async def initialize():
        assert len(runtime.servers) == 3
        assert all(server.sockets for server in runtime.servers)

    async def process(row):
        result = await real_process_sample(row)
        assert runtime.stop is False
        processed.append(result["status"])
        return result

    runtime._initialize_execution = initialize
    runtime._process_sample = process
    monkeypatch.setattr(wti_live, "validate_startup_preconditions", lambda _args: None)
    asyncio.run(asyncio.wait_for(runtime.run(), timeout=8))
    recovered = runtime.pilot_store.load()
    assert processed == ["flat"]
    assert collector.kept_collecting_after_flat
    assert recovered["status"] == "flat" and recovered["pending_action"] is None
    assert recovered["single_round_pilot"]["completed_roundtrips"] == 1
    assert gateway.var_qty == gateway.lighter_qty == 0
    assert len(gateway.requests) == 4
    assert gateway.requests[-1]["venue"] == "lighter"
    assert gateway.requests[-1]["side"] == "buy"
    assert gateway.requests[-1]["qty"] == "0.198"
    assert gateway.requests[-1]["reduce_only"] is True
    assert runtime.entry_authorized is False


def test_unqualified_pilot_does_not_bypass_live_account_risk_block(monkeypatch):
    now = datetime.now(timezone.utc)
    row = _live_sample(now)
    state = {
        "status": "flat", "open_position": None, "pending_action": None,
        "single_round_pilot": {
            "pilot_id": "pilot-1", "status": "available",
            "completed_roundtrips": 0, "entry_attempts": 0,
            "max_entry_attempts": 3,
        },
    }

    class NoEntryStore:
        def reserve_single_round_pilot_entry(self, _pilot_id):
            raise AssertionError("risk block must run before pilot reservation")

    class RiskBlockedGateway:
        latest_risk = {"risk_action": "block_entry", "risk_reason": "margin_block"}

        def set_current_notional(self, _notional):
            pass

    runtime = _runtime_for_entry_gate(allow_unqualified=True)
    runtime.config = SimpleNamespace(entry_cutoff_seconds=1800)
    runtime.executor = object()
    runtime.gateway = RiskBlockedGateway()
    runtime.pilot_store = NoEntryStore()
    runtime.single_round_pilot = {"pilot_id": "pilot-1"}
    runtime.last_risk = {}
    runtime.pending_gap_exit = False
    runtime.last_action = {}

    async def read_state(_row):
        return state

    runtime._read_reconcile = read_state
    monkeypatch.setattr(
        wti_live, "decide_wti_action",
        lambda *_args, **_kwargs: {"action": "open", "direction": "long_var_short_lighter", "edge_bps": "8"},
    )

    result = asyncio.run(runtime._process_sample(row))
    assert result is state
    assert runtime.last_action["risk_action"] == "block_entry"
    assert runtime.entry_authorized is True  # Data gate is clear; account risk still blocks submission.
