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
from tools.rotation_supervisor import RotationSupervisor, rehearsal_plan


def _snapshot(*, current="NONE", target="ETH", eth_state=None, wti_state=None):
    return {
        "observed_at": "2026-10-24T00:00:00+00:00",
        "current_pair": current,
        "target_pair": target,
        "target_reason": "test",
        "session": {
            "state": "open", "allow_new_entries": True,
            "allow_position_management": True, "reason": "verified",
            "observed_at": "2026-10-24T00:00:00+00:00",
            "closing_at": "2026-10-24T08:00:00+00:00",
        },
        "readiness": {"production_ready": target == "WTI", "reasons": []},
        "wti_pilot_status": None,
        "eth_ready": True,
        "wti_ready": target == "WTI",
        "runtimes": {
            "ETH": {"pids": [123] if current == "ETH" else [], "matched_healthy": current == "ETH", "run_id": "eth-run", "health": {"startup_reconciled": True}},
            "WTI": {"pids": [456] if current == "WTI" else [], "matched_healthy": current == "WTI", "run_id": "wti-run", "health": {"startup_reconciled": True, "manage_existing_only": False, "wti_entry_authorized": True}},
        },
        "states": {
            "ETH": eth_state or {"asset": "ETH", "status": "flat", "open_lots": [], "pending_actions": []},
            "WTI": wti_state or {"asset": "WTI", "status": "flat", "open_position": None, "pending_action": None},
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
