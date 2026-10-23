from __future__ import annotations

import asyncio
import json
import logging
import time
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
    runtime.args = SimpleNamespace(mode="live", allow_unqualified_data_pilot=allow_unqualified)
    runtime.readiness = {"ready": False, "market_data_ready": False}
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

    def set_proposed_notional(self, _value):
        pass

    def submit_order(self, _request):
        raise AssertionError("below-threshold samples must never submit")

    def lookup_order(self, *_args, **_kwargs):
        raise AssertionError("flat state has no orders to recover")


def _runtime_with_real_executor(tmp_path, collector, gateway):
    runtime = _runtime_for_entry_gate(allow_unqualified=True)
    runtime.args = wti_live.parse_args([
        "--mode", "live", "--confirm-eth-flat", "--i-understand-live-orders",
        "--confirm-bounded-live-pilot", "--allow-unqualified-data-pilot",
        "--expected-lighter-market-id", "145", "--entry-threshold-bps", "30",
        "--exit-target-bps", "1", "--roundtrip-cost-bps", "10",
        "--state-path", str(tmp_path / "wti-state.json"),
        "--health-path", str(tmp_path / "wti-health.json"),
        "--sample-root", str(tmp_path / "samples"),
    ])
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

        def sample(self):
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
                })
            return row

        async def collect_fresh_sample(self):
            self.latest_sample = self.sample()
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
        first = dict(collector.latest_sample)
        assert runtime._record_new_sample(first) is True
        opened = await runtime._process_sample(first)
        assert opened["status"] == "open"
        assert opened["open_position"]["var_qty"] == "0.28"
        assert runtime.stop is False
        collector.phase = "exit"
        exit_row = await collector.collect_fresh_sample()
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
