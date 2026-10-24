from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import rotation_supervisor as supervisor_module
from tools.lib.pair_rotation import Pair
from tools.lib.rotation_supervisor import (
    RotationPhase,
    initial_journal,
    verified_drain_completion,
    verified_flat_account_probe,
    write_object_atomic,
)
from tools.rotation_supervisor import (
    RotationSupervisor,
    _wti_account_proof,
    rehearsal_plan,
)


def _snapshot(*, current="NONE", target="ETH", eth_state=None, wti_state=None):
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat()
    return {
        "observed_at": timestamp,
        "current_pair": current,
        "target_pair": target,
        "target_reason": "test",
        "session": {
            "state": "open", "allow_new_entries": True,
            "allow_position_management": True, "reason": "verified",
            "observed_at": timestamp,
            "closing_at": (now + timedelta(hours=8)).isoformat(),
        },
        "readiness": {"production_ready": target == "WTI", "reasons": []},
        "wti_production_gate_ready": target == "WTI",
        "wti_bounded_test_gate_ready": False,
        "bounded_rotation_test": False,
        "lighter_market_id": 145,
        "wti_pilot_status": None,
        "eth_ready": True,
        "wti_ready": target == "WTI",
        "runtimes": {
            "ETH": {"pids": [123] if current == "ETH" else [], "matched_healthy": current == "ETH", "run_id": "eth-run", "health": {"startup_reconciled": True}},
            "WTI": {"pids": [456] if current == "WTI" else [], "matched_healthy": current == "WTI", "run_id": "wti-run", "health": {"pid": 456, "run_id": "wti-run", "status": "running", "updated_at": timestamp, "startup_reconciled": True, "manage_existing_only": False, "wti_entry_authorized": True}},
        },
        "states": {
            "ETH": eth_state or {"asset": "ETH", "status": "flat", "open_lots": [], "pending_actions": []},
            "WTI": wti_state or {
                "asset": "WTI", "status": "flat", "open_position": None,
                "pending_action": None,
                "last_account_reconciliation": {
                    "verified": True, "observed_at": timestamp, "market_id": 145,
                    "runtime_pid": 456, "runtime_run_id": "wti-run",
                    "variational_qty": "0", "lighter_qty": "0",
                    "active_order_count": 0,
                },
            },
        },
        "collector": {},
        "collector_pids": [],
        "process_conflict": False,
    }


def test_rehearsal_is_read_only_and_unready_wti_does_not_become_target():
    snapshot = _snapshot(current="NONE", target="ETH")
    snapshot["session"]["state"] = "exchange_closed"
    snapshot["session"]["allow_new_entries"] = False
    snapshot["session"]["allow_position_management"] = False
    snapshot["wti_ready"] = False
    snapshot["readiness"] = {"production_ready": False, "reasons": ["not_enough_sessions"]}
    plan = rehearsal_plan(snapshot)
    assert plan["mode"] == "read_only_rehearsal"
    assert plan["side_effects"] is False
    assert plan["wti_production_ready"] is False
    assert plan["allow_new_entries"] is False
    assert plan["execution_ready"] is False
    assert "initial_both_account_flat_confirmation_required" in plan["blocking_reasons"]


def test_drain_completion_requires_exact_runtime_and_both_zero_order_proof():
    base = {
        "status": "completed", "request_id": "req", "target_pid": 10,
        "target_run_id": "run", "variational_position_qty": "0",
        "lighter_position_qty": "0", "variational_active_orders": 0,
        "lighter_active_orders": 0,
    }
    assert verified_drain_completion(base, request_id="req", source_pid=10, source_run_id="run")
    assert not verified_drain_completion(base, request_id="other", source_pid=10, source_run_id="run")
    assert not verified_drain_completion({**base, "lighter_active_orders": 1}, request_id="req", source_pid=10, source_run_id="run")
    assert not verified_drain_completion({**base, "variational_position_qty": "0.001"}, request_id="req", source_pid=10, source_run_id="run")
    assert not verified_drain_completion({**base, "lighter_position_qty": "1e-1000"}, request_id="req", source_pid=10, source_run_id="run")


def test_closed_session_probe_requires_fresh_zero_state_and_exact_market_identity():
    now = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)
    report = {
        "status": "verified_read", "mode": "read_only",
        "orders_attempted": False, "execution_enabled": False,
        "observed_at": (now - timedelta(seconds=2)).isoformat(),
        "read_window_ms": 350, "active_order_count": 0,
        "lighter_market": {"market_id": 145},
        "snapshot": {
            "variational_qty": "0", "lighter_qty": "0",
            "active_order_count": 0,
        },
    }
    assert verified_flat_account_probe(report, expected_market_id=145, now=now)
    assert not verified_flat_account_probe(
        {**report, "observed_at": (now - timedelta(seconds=16)).isoformat()},
        expected_market_id=145, now=now,
    )
    assert not verified_flat_account_probe(
        {**report, "lighter_market": {"market_id": 146}},
        expected_market_id=145, now=now,
    )
    assert not verified_flat_account_probe(
        {**report, "orders_attempted": True}, expected_market_id=145, now=now,
    )


@pytest.mark.parametrize("bad_report", ["stale", "wrong_market"])
def test_closed_session_fallback_never_stops_on_stale_or_wrong_market_probe(
    tmp_path: Path, monkeypatch, bad_report: str,
):
    now = datetime.now(timezone.utc)
    report = {
        "status": "verified_read", "mode": "read_only",
        "orders_attempted": False, "execution_enabled": False,
        "observed_at": (now - timedelta(seconds=25) if bad_report == "stale" else now).isoformat(),
        "read_window_ms": 200, "active_order_count": 0,
        "lighter_market": {"market_id": 145 if bad_report == "stale" else 146},
        "snapshot": {"variational_qty": "0", "lighter_qty": "0", "active_order_count": 0},
    }
    monkeypatch.setattr(supervisor_module.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps(report)))
    monkeypatch.setattr(supervisor_module, "script_processes", lambda *a, **kw: [123])
    killed = []
    monkeypatch.setattr(supervisor_module.os, "kill", lambda *args: killed.append(args))
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=False,
    )
    write_object_atomic(tmp_path / "log/wti_live_risk_health.json", {
        "pid": 123, "run_id": "run", "status": "running",
        "updated_at": now.isoformat(),
    })
    write_object_atomic(tmp_path / "log/wti_live_control.json", {
        "action": "drain_after_flat", "status": "draining",
        "request_id": "req", "target_pid": 123, "target_run_id": "run",
    })
    journal = {"request_id": "req", "source_pid": 123, "source_run_id": "run"}
    runtime = {
        "pids": [123], "run_id": "run",
        "health": {"manage_existing_only": True, "wti_entry_authorized": False},
    }
    state = {"asset": "WTI", "status": "flat", "open_position": None, "pending_action": None}

    assert not supervisor._stop_flat_wti_after_readonly_probe(runtime, state, journal)
    assert killed == []


def test_closed_session_fallback_stops_only_after_fresh_dual_flat_probe(tmp_path: Path, monkeypatch):
    now = datetime.now(timezone.utc)
    report = {
        "status": "verified_read", "mode": "read_only",
        "orders_attempted": False, "execution_enabled": False,
        "observed_at": now.isoformat(), "read_window_ms": 200,
        "active_order_count": 0, "lighter_market": {"market_id": 145},
        "snapshot": {"variational_qty": "0", "lighter_qty": "0", "active_order_count": 0},
    }
    monkeypatch.setattr(supervisor_module.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps(report)))
    monkeypatch.setattr(supervisor_module, "script_processes", lambda *a, **kw: [123])
    killed = []
    monkeypatch.setattr(supervisor_module.os, "kill", lambda *args: killed.append(args))
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=False,
    )
    write_object_atomic(tmp_path / "log/wti_live_risk_health.json", {
        "pid": 123, "run_id": "run", "status": "running",
        "updated_at": now.isoformat(),
    })
    control_path = tmp_path / "log/wti_live_control.json"
    write_object_atomic(control_path, {
        "action": "drain_after_flat", "status": "draining",
        "request_id": "req", "target_pid": 123, "target_run_id": "run",
    })
    journal = {"request_id": "req", "source_pid": 123, "source_run_id": "run"}
    runtime = {
        "pids": [123], "run_id": "run",
        "health": {"manage_existing_only": True, "wti_entry_authorized": False},
    }
    state = {"asset": "WTI", "status": "flat", "open_position": None, "pending_action": None}

    assert supervisor._stop_flat_wti_after_readonly_probe(runtime, state, journal)
    assert killed == [(123, supervisor_module.signal.SIGTERM)]
    assert json.loads(control_path.read_text(encoding="utf-8"))["completion_source"] == "read_only_wti_account_probe"


def test_rotation_commands_keep_production_gate_and_management_recovery_entry_disabled(tmp_path: Path):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True,
    )
    pilot = supervisor._start_command(Pair.WTI)
    recovery = supervisor._start_command(Pair.WTI, recovery=True)
    assert "--allow-unqualified-data-pilot" not in pilot
    assert "--gradient-live" in pilot
    assert "--manage-existing-wti-position" in recovery
    assert "--gradient-live" not in recovery
    assert "--confirm-bounded-gradient-pilot" not in recovery
    test_id = "a" * 32
    bounded = supervisor._start_command(
        Pair.WTI, bounded_test=True, rotation_test_id=test_id,
    )
    assert "--gradient-live" in bounded
    assert "--allow-unqualified-data-pilot" in bounded
    assert "--rotation-test-id" in bounded
    assert bounded[bounded.index("--rotation-test-id") + 1] == test_id
    assert bounded[bounded.index("--max-notional-usd") + 1] == "20"
    assert bounded[bounded.index("--gradient-layer-notional-usd") + 1] == "10"
    assert bounded[bounded.index("--gradient-second-tier-bps") + 1] == "45"
    with pytest.raises(RuntimeError, match="rotation_test_id_missing_or_invalid"):
        supervisor._start_command(Pair.WTI, bounded_test=True, rotation_test_id="bad")


def test_wti_account_proof_is_bound_to_fresh_pid_run_and_market_identity():
    now = datetime.now(timezone.utc)
    state = {
        "asset": "WTI", "status": "flat", "open_position": None,
        "pending_action": None,
        "last_account_reconciliation": {
            "verified": True, "observed_at": now.isoformat(), "market_id": 145,
            "runtime_pid": 456, "runtime_run_id": "wti-run",
            "variational_qty": "0", "lighter_qty": "0", "active_order_count": 0,
        },
    }
    health = {"pid": 456, "run_id": "wti-run", "startup_reconciled": True}
    proof = _wti_account_proof(state, health, market_id=145, now=now)
    assert proof["verified"] is True
    assert proof["active_orders_zero"] is True
    for changed_state, changed_health, expected in (
        ({**state, "last_account_reconciliation": {**state["last_account_reconciliation"], "market_id": 146}}, health, "wti_account_proof_market_id_mismatch"),
        (state, {**health, "pid": 457}, "wti_account_proof_pid_mismatch"),
        (state, {**health, "run_id": "other-run"}, "wti_account_proof_run_id_mismatch"),
        ({**state, "last_account_reconciliation": {**state["last_account_reconciliation"], "observed_at": (now - timedelta(seconds=21)).isoformat()}}, health, "wti_account_proof_stale_or_invalid"),
        ({**state, "last_account_reconciliation": {**state["last_account_reconciliation"], "active_order_count": None}}, health, "wti_active_order_count_unverified"),
        ({**state, "last_account_reconciliation": {**state["last_account_reconciliation"], "variational_qty": "0.1"}}, health, "wti_local_exchange_position_mismatch"),
    ):
        rejected = _wti_account_proof(changed_state, changed_health, market_id=145, now=now)
        assert rejected["verified"] is False
        assert expected in rejected["reasons"]


def test_open_and_closed_session_rehearsals_route_without_side_effects():
    snapshot = _snapshot(current="ETH", target="ETH")
    snapshot["readiness"] = {"production_ready": False, "reasons": ["history_short"]}
    snapshot["wti_production_gate_ready"] = False
    snapshot["bounded_rotation_test"] = True
    snapshot["bounded_test_authorized"] = True
    snapshot["wti_bounded_test_gate_ready"] = True
    snapshot["continuous_gradient_rotation"] = True
    snapshot["continuous_gradient_gate_ready"] = True
    snapshot["wti_ready"] = False
    snapshot["wti_gate_reasons"] = []
    snapshot["runtimes"]["ETH"]["health"].update({
        "account_snapshot_observed_at": datetime.now(timezone.utc).isoformat(),
        "variational_account_snapshot_usable": True,
    })

    open_plan = rehearsal_plan(snapshot, session_scenario="open")
    closed_snapshot = _snapshot(current="WTI", target="WTI")
    closed_snapshot["bounded_rotation_test"] = True
    closed_snapshot["bounded_test_authorized"] = True
    closed_snapshot["wti_bounded_test_gate_ready"] = True
    closed_snapshot["continuous_gradient_rotation"] = True
    closed_snapshot["continuous_gradient_gate_ready"] = False
    closed_snapshot["wti_ready"] = True
    closed_snapshot["wti_production_gate_ready"] = False
    closed_plan = rehearsal_plan(closed_snapshot, session_scenario="closed")

    assert open_plan["target_pair"] == "WTI"
    assert open_plan["decision"] == "drain_current"
    assert open_plan["allow_new_entries"] is False
    assert open_plan["side_effects"] is False
    assert open_plan["continuous_gradient_rotation"] is True
    assert open_plan["continuous_gradient_ready"] is True
    assert closed_plan["target_pair"] == "ETH"
    assert closed_plan["decision"] == "switch_to_target"
    assert closed_plan["allow_new_entries"] is False
    assert closed_plan["side_effects"] is False
    assert closed_plan["continuous_gradient_rotation"] is True
    assert closed_plan["continuous_gradient_ready"] is False


def test_continuous_rotation_cli_requires_explicit_live_confirmations(monkeypatch):
    with pytest.raises(SystemExit):
        supervisor_module.main([
            "--execute", "--continuous-gradient-rotation",
            "--lighter-market-id", "145", "--lighter-multiplier", "1",
        ])
    with pytest.raises(SystemExit):
        supervisor_module.main([
            "--execute", "--bounded-rotation-test", "--confirm-bounded-rotation-test",
            "--continuous-gradient-rotation", "--confirm-continuous-gradient-rotation",
            "--confirm-automatic-rotation", "--i-understand-live-orders",
            "--lighter-market-id", "145", "--lighter-multiplier", "1",
        ])
    monkeypatch.setattr(RotationSupervisor, "run", lambda self, **kwargs: 0)
    assert supervisor_module.main([
        "--execute", "--continuous-gradient-rotation",
        "--confirm-continuous-gradient-rotation",
        "--confirm-automatic-rotation", "--i-understand-live-orders",
        "--lighter-market-id", "145", "--lighter-multiplier", "1",
    ]) == 0


def test_bounded_test_authorization_is_durable_and_config_bound(tmp_path: Path):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, bounded_rotation_test=True,
    )
    first_journal = initial_journal()
    first = supervisor._prepare_bounded_rotation_test(first_journal)
    assert first["status"] == "authorized"
    test_id = first["test_id"]
    assert first["max_completed_roundtrips"] == 1
    assert first["max_layers"] == 2
    assert first["layer_notional_usd"] == "10"
    assert first["max_notional_usd"] == "20"

    restarted = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, bounded_rotation_test=True,
    )
    resumed = restarted._prepare_bounded_rotation_test(
        json.loads(restarted.journal_path.read_text(encoding="utf-8"))
    )
    assert resumed["test_id"] == test_id
    assert resumed["entry_threshold_bps"] == "30"
    assert resumed["second_tier_bps"] == "45"
    assert resumed["max_slippage_bps"] == "8"

    changed_market = RotationSupervisor(
        root=tmp_path, market_id=146, multiplier=Decimal("1"),
        confirm_initial_flat=False, bounded_rotation_test=True,
    )
    with pytest.raises(RuntimeError, match="journal_invalid"):
        changed_market._prepare_bounded_rotation_test(
            json.loads(changed_market.journal_path.read_text(encoding="utf-8"))
        )


def test_completed_bounded_test_cannot_be_reauthorized_after_restart(tmp_path: Path):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, bounded_rotation_test=True,
    )
    journal = initial_journal()
    test = supervisor._prepare_bounded_rotation_test(journal)
    test_id = test["test_id"]
    test["pilot_id"] = "pilot-1"
    test["status"] = "active"
    supervisor._save_journal(journal)
    write_object_atomic(tmp_path / "log/wti_live_state.json", {
        "gradient_pilot": {
            "pilot_id": "pilot-1", "rotation_test_id": test_id,
            "status": "completed", "completed_at": datetime.now(timezone.utc).isoformat(),
            "completed_roundtrips": 1,
        },
    })

    completed = supervisor._prepare_bounded_rotation_test(
        json.loads(supervisor.journal_path.read_text(encoding="utf-8"))
    )
    repeated = supervisor._prepare_bounded_rotation_test(
        json.loads(supervisor.journal_path.read_text(encoding="utf-8"))
    )

    assert completed["status"] == "completed"
    assert repeated["status"] == "completed"
    assert repeated["test_id"] == test_id
    assert repeated["completed_roundtrips"] == 1


def test_continuous_gradient_authorization_is_durable_and_keeps_two_layer_budget(tmp_path: Path):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, continuous_gradient_rotation=True,
    )
    first = supervisor._prepare_continuous_gradient_rotation(initial_journal())
    assert first["status"] == "authorized"
    assert first["max_rounds"] is None
    assert first["entry_threshold_bps"] == "30"
    assert first["second_tier_bps"] == "45"
    assert first["max_layers"] == 2
    assert first["layer_notional_usd"] == "10"
    assert first["max_notional_usd"] == "20"

    restarted = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, continuous_gradient_rotation=True,
    )
    journal = json.loads(restarted.journal_path.read_text(encoding="utf-8"))
    resumed = restarted._prepare_continuous_gradient_rotation(journal)
    assert resumed["session_id"] == first["session_id"]
    assert resumed["rounds_started"] == 0
    assert resumed["rounds_completed"] == 0


def test_continuous_gradient_restart_adopts_active_round_without_duplicate_launch(tmp_path: Path, monkeypatch):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, continuous_gradient_rotation=True,
    )
    journal = initial_journal()
    authorization = supervisor._prepare_continuous_gradient_rotation(journal)
    session_id = authorization["session_id"]
    pilot = {
        "pilot_id": "pilot-active", "rotation_test_id": session_id,
        "continuous_rotation": True, "round_number": 3,
        "status": "in_position", "completed_roundtrips": 0,
        "max_layers": 2, "market_id": 145, "multiplier": "1",
        "config": {
            "entry_threshold_bps": "30", "exit_target_bps": "1",
            "roundtrip_cost_bps": "10", "max_notional_usd": "20",
            "max_slippage_bps": "8", "max_hold_seconds": 21600,
            "entry_cutoff_seconds": 1800,
            "tier_thresholds_bps": ["30", "45"],
            "layer_notional_usd": "10", "max_layers": 2,
            "max_completed_roundtrips": 1,
        },
    }
    state_session = {
        "session_id": session_id, "status": "active", "market_id": 145,
        "multiplier": "1", "rounds_started": 3, "rounds_completed": 2,
    }
    write_object_atomic(tmp_path / "log/wti_live_state.json", {
        "asset": "WTI", "status": "open",
        "open_position": {"gradient_pilot_id": pilot["pilot_id"]},
        "pending_action": None, "gradient_pilot": pilot,
        "gradient_rotation_session": state_session,
    })
    snapshot = _snapshot(current="WTI", target="WTI")
    snapshot.update({
        "continuous_gradient_rotation": True,
        "continuous_gradient_authorized": True,
        "continuous_gradient_gate_ready": True,
    })
    snapshot["states"]["WTI"].update({
        "status": "open", "open_position": {"gradient_pilot_id": pilot["pilot_id"]},
        "gradient_pilot": pilot, "gradient_rotation_session": state_session,
    })
    launches = []
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *a, **kw: snapshot)
    monkeypatch.setattr(supervisor, "_launch", lambda *a, **kw: launches.append((a, kw)))

    first = supervisor.run_once()
    second = supervisor.run_once()

    assert first["action"] == second["action"] == "continue_continuous_gradient_rotation"
    assert first["round_number"] == 3
    assert launches == []


def test_continuous_rotation_start_command_is_explicit_and_config_pinned(tmp_path: Path):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    command = supervisor._start_command(
        Pair.WTI, continuous_rotation=True, continuous_rotation_id="f" * 32,
    )
    assert "--continuous-gradient-rotation" in command
    assert "--confirm-continuous-gradient-rotation" in command
    assert "--allow-unqualified-data-pilot" in command
    assert command[command.index("--entry-threshold-bps") + 1] == "30"
    assert command[command.index("--gradient-second-tier-bps") + 1] == "45"
    assert command[command.index("--gradient-layer-notional-usd") + 1] == "10"
    assert command[command.index("--max-notional-usd") + 1] == "20"
    assert command[command.index("--rotation-test-id") + 1] == "f" * 32


def test_management_only_wti_upgrades_via_verified_drain_not_duplicate_start(tmp_path: Path, monkeypatch):
    snapshot = _snapshot(current="WTI", target="WTI")
    snapshot["bounded_rotation_test"] = True
    snapshot["wti_bounded_test_gate_ready"] = True
    snapshot["wti_ready"] = True
    snapshot["runtimes"]["WTI"]["health"].update({
        "manage_existing_only": True, "wti_entry_authorized": False,
    })
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, bounded_rotation_test=True,
    )

    first = supervisor.run_once()
    control = json.loads((tmp_path / "log/wti_live_control.json").read_text(encoding="utf-8"))
    second = supervisor.run_once()
    journal = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))

    assert first["action"] == "request_bounded_test_runtime_upgrade"
    assert second["action"] == "wait_for_drain"
    assert control["target_pid"] == 456
    assert control["target_run_id"] == "wti-run"
    assert control["request_id"] == journal["request_id"]
    assert journal["bounded_rotation_test"]["test_id"]


def test_management_only_wti_without_fresh_flat_account_proof_is_not_restarted(tmp_path: Path, monkeypatch):
    snapshot = _snapshot(current="WTI", target="WTI")
    snapshot["bounded_rotation_test"] = True
    snapshot["wti_bounded_test_gate_ready"] = True
    snapshot["wti_ready"] = True
    snapshot["runtimes"]["WTI"]["health"].update({
        "manage_existing_only": True, "wti_entry_authorized": False,
    })
    snapshot["states"]["WTI"].pop("last_account_reconciliation")
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, bounded_rotation_test=True,
    )

    result = supervisor.run_once()

    assert result["action"] == "hold_current_wti_account_or_pilot_proof_unverified"
    assert not (tmp_path / "log/wti_live_control.json").exists()


def test_drain_request_is_durable_and_idempotent_across_supervisor_iterations(tmp_path: Path, monkeypatch):
    snapshot = _snapshot(current="ETH", target="WTI", eth_state={
        "asset": "ETH", "status": "open", "open_lots": [{"lot_id": 1}], "pending_actions": [],
    })
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False,
    )
    first = supervisor.run_once()
    request_path = tmp_path / "log/live_inventory_control.json"
    request = json.loads(request_path.read_text(encoding="utf-8"))
    second = supervisor.run_once()
    request_again = json.loads(request_path.read_text(encoding="utf-8"))
    journal = json.loads((tmp_path / "log/rotation_supervisor_state.json").read_text(encoding="utf-8"))
    assert first["action"] == "request_safe_drain"
    assert second["action"] == "wait_for_drain"
    assert request["request_id"] == request_again["request_id"] == journal["request_id"]
    assert request["target_pid"] == 123
    assert request["target_run_id"] == "eth-run"
    assert journal["phase"] == RotationPhase.DRAINING.value


def test_starting_phase_adopts_tmux_session_instead_of_spawning_duplicate(tmp_path: Path, monkeypatch):
    snapshot = _snapshot(current="NONE", target="ETH")
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True,
    )
    journal = initial_journal()
    journal.update({
        "phase": RotationPhase.STARTING.value,
        "target_pair": "ETH", "launch_attempts": 1,
        "tmux_session": "rotation-eth-existing",
    })
    write_object_atomic(supervisor.journal_path, journal)
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda _name: True)
    monkeypatch.setattr(supervisor, "_start_target", lambda *a, **kw: pytest.fail("duplicate launch"))
    result = supervisor.run_once()
    assert result["action"] == "await_starting_tmux_session"


def test_orphaned_wti_exposure_recovers_only_into_management_mode(tmp_path: Path, monkeypatch):
    wti_state = {
        "asset": "WTI", "status": "open", "open_position": {"var_qty": "0.2"},
        "pending_action": None,
    }
    snapshot = _snapshot(current="NONE", target="WTI", wti_state=wti_state)
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False,
    )
    launched = []
    monkeypatch.setattr(supervisor, "_launch", lambda pair, *, recovery, session_name: launched.append((pair, recovery, session_name)) or session_name)
    result = supervisor.run_once()
    journal = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))
    assert result["action"] == "restart_management_only"
    assert launched and launched[0][0] == Pair.WTI and launched[0][1] is True
    assert journal["phase"] == RotationPhase.RECOVERING.value
    assert "--manage-existing-wti-position" in supervisor._start_command(Pair.WTI, recovery=True)


def test_eth_runtime_starts_readonly_collector_once_and_adopts_it(tmp_path: Path, monkeypatch):
    snapshot = _snapshot(current="ETH", target="ETH")
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=False,
    )
    launches = []
    monkeypatch.setattr(supervisor, "_forwarder_available", lambda: True)
    monkeypatch.setattr(supervisor, "_launch_oil_collector", lambda name: launches.append(name))
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda name: bool(launches and name == launches[0]))

    first = supervisor.run_once()
    second = supervisor.run_once()

    assert first["collector"] == "collector_started"
    assert second["collector"] == "await_collector_process"
    assert len(launches) == 1


def test_eth_runtime_adopts_fresh_collector_without_starting_duplicate(tmp_path: Path, monkeypatch):
    snapshot = _snapshot(current="ETH", target="ETH")
    snapshot["collector_pids"] = [789]
    snapshot["collector"] = {
        "pid": 789, "status": "running",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=False,
    )
    monkeypatch.setattr(supervisor, "_launch_oil_collector", lambda *_: pytest.fail("duplicate collector"))

    result = supervisor.run_once()

    assert result["collector"] == "collector_running"


def test_wti_start_waits_for_existing_standalone_collector_to_exit(tmp_path: Path, monkeypatch):
    snapshot = _snapshot(current="NONE", target="WTI")
    snapshot["collector_pids"] = [789]
    snapshot["wti_ready"] = True
    snapshot["readiness"] = {"production_ready": True, "reasons": []}
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=True,
    )
    monkeypatch.setattr(supervisor, "_launch", lambda *a, **kw: pytest.fail("must wait for old collector"))

    result = supervisor.run_once()

    assert result["action"] == "wait_for_standalone_collector_exit"


def _unknown_cold_start_snapshot():
    snapshot = _snapshot(current="NONE", target="NONE")
    snapshot["session"].update({
        "state": "status_unknown",
        "allow_new_entries": False,
        "allow_position_management": False,
        "reason": "exchange_session_status_unconfirmed",
        "closing_at": None,
    })
    snapshot["wti_ready"] = False
    snapshot["readiness"] = {"production_ready": False, "reasons": ["no_fresh_session_sample"]}
    return snapshot


def test_cold_start_unknown_session_bootstraps_wti_management_only_after_flat_confirmation(
    tmp_path: Path, monkeypatch,
):
    snapshot = _unknown_cold_start_snapshot()
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda _name: False)
    launches = []
    monkeypatch.setattr(
        supervisor, "_launch",
        lambda pair, **kwargs: launches.append((pair, kwargs)),
    )

    result = supervisor.run_once()
    journal = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))

    assert result["action"] == "start_wti_session_bootstrap_management_only"
    assert launches == [(Pair.WTI, {"recovery": True, "session_name": journal["tmux_session"]})]
    assert journal["phase"] == RotationPhase.RECOVERING.value
    assert journal["target_pair"] == "WTI"
    command = supervisor._start_command(Pair.WTI, recovery=True)
    assert "--manage-existing-wti-position" in command
    assert "--gradient-live" not in command
    assert "--confirm-continuous-gradient-rotation" not in command


def test_cold_start_unknown_session_without_flat_confirmation_does_not_launch(tmp_path: Path, monkeypatch):
    snapshot = _unknown_cold_start_snapshot()
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, continuous_gradient_rotation=True,
    )
    launches = []
    monkeypatch.setattr(supervisor, "_launch", lambda *args, **kwargs: launches.append(args))

    result = supervisor.run_once()

    assert result["action"] == "wait_for_initial_both_accounts_flat_confirmation"
    assert not launches


def test_interrupted_eth_start_bootstraps_session_once_and_keeps_authorization(
    tmp_path: Path, monkeypatch,
):
    snapshot = _unknown_cold_start_snapshot()
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    journal = initial_journal()
    authorization = dict(supervisor._prepare_continuous_gradient_rotation(journal))
    journal.update({
        "phase": "starting", "target_pair": "ETH", "launch_attempts": 1,
        "tmux_session": "rotation-eth-1024083341",
    })
    write_object_atomic(supervisor.journal_path, journal)
    live_sessions = set()
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda name: name in live_sessions)
    launches = []

    def launch(pair, **kwargs):
        launches.append((pair, kwargs))
        live_sessions.add(kwargs["session_name"])

    monkeypatch.setattr(supervisor, "_launch", launch)
    result = supervisor.run_once()
    repeated = supervisor.run_once()
    saved = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))

    assert result["action"] == "recover_interrupted_eth_start_via_wti_session_bootstrap"
    assert repeated["action"] == "await_wti_session_bootstrap_runtime"
    assert len(launches) == 1
    assert launches[0][0] == Pair.WTI and launches[0][1]["recovery"] is True
    assert saved["phase"] == "recovering" and saved["target_pair"] == "WTI"
    assert saved["tmux_session"] != "rotation-eth-1024083341"
    assert saved["launch_attempts"] == 1
    assert saved["continuous_gradient_rotation"] == authorization
    assert saved["superseded_starts"][0]["tmux_session"] == "rotation-eth-1024083341"
    assert saved["superseded_starts"][0]["launch_attempts"] == 1


@pytest.mark.parametrize("blocker, expected", [
    ("session", "await_interrupted_eth_start_runtime"),
    ("confirmation", "wait_for_initial_both_accounts_flat_confirmation"),
    ("collector", "wait_for_standalone_collector_exit"),
    ("process", "await_starting_runtime_health"),
])
def test_interrupted_eth_start_never_retargets_while_start_is_unconfirmed(
    tmp_path: Path, monkeypatch, blocker, expected,
):
    snapshot = _unknown_cold_start_snapshot()
    if blocker == "collector":
        snapshot["collector_pids"] = [789]
    if blocker == "process":
        snapshot["process_conflict"] = True
        snapshot["runtimes"]["ETH"]["pids"] = [123]
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=blocker != "confirmation", continuous_gradient_rotation=True,
    )
    journal = initial_journal()
    journal.update({
        "phase": "starting", "target_pair": "ETH", "launch_attempts": 1,
        "tmux_session": "rotation-eth-existing",
    })
    write_object_atomic(supervisor.journal_path, journal)
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda _name: blocker == "session")
    monkeypatch.setattr(supervisor, "_launch", lambda *a, **kw: pytest.fail("duplicate launch"))

    assert supervisor.run_once()["action"] == expected
    saved = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))
    assert saved["target_pair"] == "ETH" and saved["launch_attempts"] == 1
    assert "superseded_starts" not in saved


def test_cold_start_bootstrap_resume_is_idempotent_after_supervisor_interrupt(
    tmp_path: Path, monkeypatch,
):
    snapshot = _unknown_cold_start_snapshot()
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    session_present = False
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda _name: session_present)
    launches = []
    monkeypatch.setattr(
        supervisor, "_launch",
        lambda pair, **kwargs: launches.append((pair, kwargs)),
    )

    first = supervisor.run_once()
    session_present = True
    interrupted = supervisor.run_once()
    assert first["action"] == "start_wti_session_bootstrap_management_only"
    assert interrupted["action"] == "await_wti_session_bootstrap_runtime"
    assert len(launches) == 1

    session_present = False
    snapshot["collector_pids"] = [987]
    collector_blocked = supervisor.run_once()
    assert collector_blocked["action"] == "wait_for_standalone_collector_exit"
    assert len(launches) == 1

    snapshot["collector_pids"] = []
    retried = supervisor.run_once()
    assert retried["action"] == "retry_wti_session_bootstrap_management_only"
    assert launches[1][0] == Pair.WTI
    assert launches[1][1]["recovery"] is True

    session_present = True
    resumed = supervisor.run_once()
    assert resumed["action"] == "await_wti_session_bootstrap_runtime"
    assert len(launches) == 2


def test_bootstrapped_wti_closed_session_drains_to_eth_without_enabling_entries(
    tmp_path: Path, monkeypatch,
):
    snapshot = _snapshot(current="WTI", target="ETH")
    snapshot["session"].update({
        "state": "exchange_closed", "allow_new_entries": False,
        "allow_position_management": False, "closing_at": None,
    })
    snapshot["runtimes"]["WTI"]["health"].update({
        "manage_existing_only": True, "wti_entry_authorized": False,
    })
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, continuous_gradient_rotation=True,
    )

    result = supervisor.run_once()
    control = json.loads((tmp_path / "log/wti_live_control.json").read_text(encoding="utf-8"))

    assert result["action"] == "request_safe_drain"
    assert control["action"] == "drain_after_flat"
    assert control["status"] == "requested"
    assert control["asset"] == "WTI"


def test_bootstrapped_wti_open_session_upgrades_only_after_fresh_flat_account_proof(
    tmp_path: Path, monkeypatch,
):
    snapshot = _snapshot(current="WTI", target="WTI")
    snapshot["continuous_gradient_rotation"] = True
    snapshot["continuous_gradient_authorized"] = True
    snapshot["continuous_gradient_gate_ready"] = True
    snapshot["session"].update({"state": "open", "allow_new_entries": True})
    snapshot["runtimes"]["WTI"]["health"].update({
        "manage_existing_only": True, "wti_entry_authorized": False,
    })
    monkeypatch.setattr(supervisor_module, "collect_snapshot", lambda *args, **kwargs: snapshot)
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=False, continuous_gradient_rotation=True,
    )

    result = supervisor.run_once()
    control = json.loads((tmp_path / "log/wti_live_control.json").read_text(encoding="utf-8"))

    assert result["action"] == "request_continuous_rotation_runtime_start"
    assert control["target_pid"] == 456
    assert control["target_run_id"] == "wti-run"
    assert control["status"] == "requested"


def test_supervisor_retries_transient_errors_then_clears_failure_streak(tmp_path: Path, monkeypatch):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=False,
    )
    results = iter([RuntimeError("temporary"), {"action": "recovered"}])

    def run_once():
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    sleeps = 0

    def interrupt_sleep(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise KeyboardInterrupt

    monkeypatch.setattr(supervisor, "run_once", run_once)
    monkeypatch.setattr(supervisor_module.time, "sleep", interrupt_sleep)
    with pytest.raises(KeyboardInterrupt):
        supervisor.run(once=False, poll_seconds=1)
    journal = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))
    assert journal["consecutive_failures"] == 0
    assert journal["phase"] == RotationPhase.STABLE.value


def test_supervisor_blocks_only_after_three_consecutive_errors(tmp_path: Path, monkeypatch, capsys):
    supervisor = RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=False,
    )
    monkeypatch.setattr(supervisor, "run_once", lambda: (_ for _ in ()).throw(RuntimeError("persistent")))
    monkeypatch.setattr(supervisor_module.time, "sleep", lambda _seconds: None)

    assert supervisor.run(once=False, poll_seconds=1) == 2
    journal = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))
    assert journal["consecutive_failures"] == 3
    assert journal["phase"] == RotationPhase.BLOCKED.value
    output = capsys.readouterr().out
    assert '"action": "retry_after_error"' in output
    assert '"action": "blocked"' in output
