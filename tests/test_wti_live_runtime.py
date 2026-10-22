from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import wti_live


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
        {**row, "market_availability": "partial_or_unavailable"}, now
    )
    assert not wti_live.WTILiveRuntime._sample_health_fresh(
        {**row, "session_status_feed_connected": False}, now
    )
    assert not wti_live.WTILiveRuntime._sample_health_fresh(
        {**row, "lighter_continuity_ok": False}, now
    )


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
