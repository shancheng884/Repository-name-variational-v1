from __future__ import annotations

import asyncio
import json
import logging
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
    assert captured["minimum_sessions"] == 3
    assert captured["minimum_valid_samples_per_session"] == 1200
    assert captured["minimum_coverage_seconds"] == 21600
    assert captured["maximum_gap_seconds"] == 120
    assert captured["maximum_sample_age_seconds"] == 120
    assert captured["expected_lighter_market_id"] == 145
    assert captured["expected_lighter_exposure_multiplier"] == Decimal("1")


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
    runtime.args = SimpleNamespace(mode="live", allow_unqualified_data_pilot=allow_unqualified)
    runtime.readiness = {"ready": False, "market_data_ready": False}
    runtime.single_round_pilot = {"pilot_id": "pilot-1"}
    runtime.manage_existing_only = False
    runtime.entry_authorized = False
    runtime.entry_gate_block_reason = None
    runtime.entry_authorization_permanently_revoked = False
    return runtime


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
