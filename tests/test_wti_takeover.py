from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
from types import SimpleNamespace

import pytest

from tools import wti_takeover as takeover


def _state_files(root):
    (root / ".env").touch()
    (root / "log").mkdir(exist_ok=True)
    (root / "log/live_inventory_state.json").write_text(json.dumps({
        "asset": "ETH", "status": "flat", "open_lots": [], "pending_actions": [],
    }))
    (root / "log/wti_live_state.json").write_text(json.dumps({
        "status": "manual_review_required", "pending_action": {"kind": "close"},
    }))


def _cycle_evidence(root, *, run_id, kind="market_strategy"):
    now = datetime.now(timezone.utc).isoformat()
    path = root / "log/wti_live_state.json"
    state = json.loads(path.read_text())
    state["last_account_reconciliation"] = {
        "verified": True, "runtime_pid": 42, "runtime_run_id": run_id,
        "observed_at": now, "market_id": 145, "active_order_count": 0,
        "variational_qty": "0", "lighter_qty": "0",
    }
    path.write_text(json.dumps(state))
    return {
        "verified_cycle_count": 2, "last_verified_cycle_at": now,
        "last_verified_cycle_kind": kind,
        "entry_authorization_permanently_revoked": False,
        "runtime_error": None, "runtime_cycle_error": None, "errors": [],
        "last_action": {"action": "hold", "reason": "management_only_flat_no_new_entries"},
    }


def test_process_identity_accepts_relative_and_absolute_python_but_not_tmux(monkeypatch, tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    commands = {
        11: [".venv/bin/python", "tools/wti_live.py", "--mode", "live"],
        12: [str(root / ".venv/bin/python"), str(root / "tools/wti_live.py")],
        13: ["tmux", "new-session", "exec .venv/bin/python tools/wti_live.py"],
        14: ["python", "tools/wti_live.py.backup"],
        15: ["python", str(tmp_path / "other/tools/wti_live.py")],
        16: ["python3.14", "-u", "-B", "tools/wti_live.py"],
        17: ["python", "other_script.py", "tools/wti_live.py"],
    }
    for pid, argv in commands.items():
        entry = proc / str(pid)
        entry.mkdir()
        (entry / "cmdline").write_bytes(b"\0".join(str(arg).encode() for arg in argv) + b"\0")
    original_resolve = Path.resolve

    def resolve(path, *args, **kwargs):
        return root if path.name == "cwd" else original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    assert takeover.runtime_processes(root, proc) == [11, 12, 16]


@pytest.mark.parametrize("pids", [[], [1005477]])
def test_takeover_accepts_stopped_or_relative_path_runtime_and_starts_before_account_checks(
    monkeypatch, tmp_path, pids,
):
    _state_files(tmp_path)
    (tmp_path / ".venv/bin").mkdir(parents=True)
    (tmp_path / ".venv/bin/python").touch()
    events = []
    monkeypatch.setattr(takeover, "script_processes", lambda *_args: [])
    reads = iter([pids, []])
    monkeypatch.setattr(takeover, "runtime_processes", lambda *_args: next(reads))
    monkeypatch.setattr(takeover.os, "kill", lambda pid, sig: events.append(("stop", pid)))
    monkeypatch.setattr(takeover, "require_free_ports", lambda: events.append("ports_free"))

    def run(command, **_kwargs):
        events.append(command[:2])
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(takeover.subprocess, "run", run)

    def status(*_args, **_kwargs):
        assert ["tmux", "new-session"] in events
        events.append("fresh_runtime_account_reconciled")
        return {"recovered": True, "state": "flat", "pending": False}

    monkeypatch.setattr(takeover, "recovery_status", status)
    report = takeover.execute_takeover(tmp_path)
    assert report["recovered"] is True
    assert events.index("ports_free") < events.index(["tmux", "new-session"])
    assert events[-1] == "fresh_runtime_account_reconciled"
    assert (("stop", 1005477) in events) is bool(pids)
    command = takeover.management_command(tmp_path)
    assert "--manage-existing-wti-position" in command
    assert "--confirm-bounded-live-pilot" not in command
    assert "--allow-unqualified-data-pilot" not in command


def test_duplicate_runtime_never_killed_or_restarted(monkeypatch, tmp_path):
    monkeypatch.setattr(takeover, "runtime_processes", lambda *_args: [10, 11])
    monkeypatch.setattr(takeover.os, "kill", lambda *_args: pytest.fail("must not kill"))
    with pytest.raises(RuntimeError, match="multiple_wti_runtimes"):
        takeover.stop_old_runtime(tmp_path)


def test_shutdown_timeout_does_not_force_kill(monkeypatch, tmp_path):
    monkeypatch.setattr(takeover, "runtime_processes", lambda *_args: [42])
    signals = []
    monkeypatch.setattr(takeover.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    with pytest.raises(RuntimeError, match="no_force_kill"):
        takeover.stop_old_runtime(tmp_path, timeout=0)
    assert signals == [(42, takeover.signal.SIGTERM)]


def test_eth_runtime_blocks_takeover_before_stopping_wti(monkeypatch, tmp_path):
    _state_files(tmp_path)
    monkeypatch.setattr(takeover, "script_processes", lambda *_args: [41])
    monkeypatch.setattr(takeover, "stop_old_runtime", lambda *_args: pytest.fail("must not stop WTI"))
    with pytest.raises(RuntimeError, match="eth_runtime_still_running"):
        takeover.execute_takeover(tmp_path)


def test_busy_port_prevents_new_runtime(monkeypatch, tmp_path):
    _state_files(tmp_path)
    (tmp_path / ".venv/bin").mkdir(parents=True)
    (tmp_path / ".venv/bin/python").touch()
    monkeypatch.setattr(takeover, "script_processes", lambda *_args: [])
    monkeypatch.setattr(takeover, "stop_old_runtime", lambda *_args: None)
    commands = []
    monkeypatch.setattr(takeover.subprocess, "run", lambda command, **_kwargs: commands.append(command))

    def occupied():
        raise RuntimeError("forwarder_port_unavailable:8768")

    monkeypatch.setattr(takeover, "require_free_ports", occupied)
    with pytest.raises(RuntimeError, match="port_unavailable"):
        takeover.execute_takeover(tmp_path)
    assert all("new-session" not in command for command in commands)


def test_port_check_rejects_actual_listener_without_connecting(monkeypatch):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        monkeypatch.setattr(takeover, "PORTS", (listener.getsockname()[1],))
        with pytest.raises(RuntimeError, match="forwarder_port_unavailable"):
            takeover.require_free_ports()


def test_missing_env_does_not_stop_existing_runtime(monkeypatch, tmp_path):
    _state_files(tmp_path)
    (tmp_path / ".env").unlink()
    (tmp_path / ".venv/bin").mkdir(parents=True)
    (tmp_path / ".venv/bin/python").touch()
    monkeypatch.setattr(takeover, "script_processes", lambda *_args: [])
    monkeypatch.setattr(takeover.subprocess, "run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(takeover, "stop_old_runtime", lambda *_args: pytest.fail("must not stop"))
    with pytest.raises(RuntimeError, match="workspace_env_missing"):
        takeover.execute_takeover(tmp_path)


@pytest.mark.parametrize("case", [
    "stale", "old_run", "old_pid", "pending", "entry_enabled", "wrong_mode",
    "cycle_missing", "cycle_stale", "cycle_error", "revoked", "errors",
    "proof_pid", "proof_run", "proof_stale", "proof_orders", "proof_positions", "blocked_cycle",
])
def test_takeover_never_accepts_old_or_incomplete_health(monkeypatch, tmp_path, case):
    _state_files(tmp_path)
    if case != "pending":
        (tmp_path / "log/wti_live_state.json").write_text(json.dumps({"status": "flat"}))
    health = {
        **_cycle_evidence(tmp_path, run_id="new-run"),
        "pid": 42, "run_id": "new-run", "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": "running", "startup_reconciled": True, "manage_existing_only": True,
        "wti_entry_authorized": False,
    }
    if case == "stale":
        health["updated_at"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    elif case == "old_run":
        health["run_id"] = "previous"
    elif case == "old_pid":
        health["pid"] = 41
    elif case == "entry_enabled":
        health["wti_entry_authorized"] = True
    elif case == "wrong_mode":
        health["manage_existing_only"] = False
    elif case == "cycle_missing":
        health["verified_cycle_count"] = 0
    elif case == "cycle_stale":
        health["last_verified_cycle_at"] = (datetime.now(timezone.utc) - timedelta(seconds=31)).isoformat()
    elif case == "cycle_error":
        health["runtime_cycle_error"] = "AttributeError:missing_gradient_layer_notional"
    elif case == "revoked":
        health["entry_authorization_permanently_revoked"] = True
    elif case == "errors":
        health["errors"] = ["AttributeError"]
    elif case == "blocked_cycle":
        health["last_action"] = {"action": "blocked", "reason": "AttributeError"}
    elif case.startswith("proof_"):
        path = tmp_path / "log/wti_live_state.json"
        state = json.loads(path.read_text())
        proof = state["last_account_reconciliation"]
        if case == "proof_pid":
            proof["runtime_pid"] = 41
        elif case == "proof_run":
            proof["runtime_run_id"] = "old-run"
        elif case == "proof_orders":
            proof["active_order_count"] = 1
        elif case == "proof_positions":
            proof["variational_qty"] = "0.198"
        else:
            proof["observed_at"] = (datetime.now(timezone.utc) - timedelta(seconds=16)).isoformat()
        path.write_text(json.dumps(state))
    (tmp_path / "log/wti_live_risk_health.json").write_text(json.dumps(health))
    collector_root = tmp_path / "log/oil_swap/basis_samples"
    collector_root.mkdir(parents=True)
    (collector_root / "health.json").write_text(json.dumps({
        "pid": 42, "status": "running", "samples_this_run": 3,
        "last_sample_at": datetime.now(timezone.utc).isoformat(),
    }))
    monkeypatch.setattr(takeover, "runtime_processes", lambda *_args: [42])
    assert takeover.recovery_status(tmp_path, previous_run="previous")["recovered"] is False


def test_wait_timeout_leaves_new_runtime_alive_and_reports_pending(monkeypatch, tmp_path):
    _state_files(tmp_path)
    (tmp_path / ".venv/bin").mkdir(parents=True)
    (tmp_path / ".venv/bin/python").touch()
    monkeypatch.setattr(takeover, "script_processes", lambda *_args: [])
    monkeypatch.setattr(takeover, "stop_old_runtime", lambda *_args: None)
    monkeypatch.setattr(takeover, "require_free_ports", lambda: None)
    commands = []
    monkeypatch.setattr(takeover.subprocess, "run", lambda command, **_kwargs: commands.append(command))
    monkeypatch.setattr(takeover, "recovery_status", lambda *_args, **_kwargs: {
        "recovered": False, "pending": True, "reason": "pending_order_terminal_status_unconfirmed",
    })
    report = takeover.execute_takeover(tmp_path, wait_seconds=0)
    assert report["pending"] is True
    assert report["reason"] == "pending_order_terminal_status_unconfirmed"
    assert [command[1] for command in commands] == ["-V", "new-session"]


@pytest.mark.parametrize("collector_pid", [41, 42])
def test_success_requires_current_runtime_collection(monkeypatch, tmp_path, collector_pid):
    _state_files(tmp_path)
    (tmp_path / "log/wti_live_state.json").write_text(json.dumps({"status": "flat"}))
    (tmp_path / "log/wti_live_risk_health.json").write_text(json.dumps({
        **_cycle_evidence(tmp_path, run_id="new"),
        "pid": 42, "run_id": "new", "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": "running", "startup_reconciled": True, "manage_existing_only": True,
        "wti_entry_authorized": False,
    }))
    collector_root = tmp_path / "log/oil_swap/basis_samples"
    collector_root.mkdir(parents=True)
    (collector_root / "health.json").write_text(json.dumps({
        "pid": collector_pid, "status": "running", "samples_this_run": 2,
        "last_sample_at": datetime.now(timezone.utc).isoformat(),
    }))
    monkeypatch.setattr(takeover, "runtime_processes", lambda *_args: [42])
    report = takeover.recovery_status(tmp_path, previous_run="previous")
    assert report["recovered"] is (collector_pid == 42)
    assert report["collecting"] is (collector_pid == 42)


@pytest.mark.parametrize(("heartbeat_age", "expected"), [(3, True), (31, False)])
def test_closed_market_takeover_accepts_fresh_feed_without_samples(
    monkeypatch, tmp_path, heartbeat_age, expected,
):
    _state_files(tmp_path)
    (tmp_path / "log/wti_live_state.json").write_text(json.dumps({
        "status": "flat", "open_position": None, "pending_action": None,
    }))
    now = datetime.now(timezone.utc)
    (tmp_path / "log/wti_live_risk_health.json").write_text(json.dumps({
        **_cycle_evidence(tmp_path, run_id="closed-run", kind="closed_account_only"),
        "pid": 42, "run_id": "closed-run", "updated_at": now.isoformat(),
        "status": "running", "startup_reconciled": True, "manage_existing_only": True,
        "wti_entry_authorized": False,
    }))
    collector_root = tmp_path / "log/oil_swap/basis_samples"
    collector_root.mkdir(parents=True)
    (collector_root / "health.json").write_text(json.dumps({
        "pid": 42, "status": "running", "samples_this_run": 0,
        "last_sample_at": None, "updated_at": now.isoformat(),
        "market_status": "closed", "market_status_feed_connected": True,
        "market_status_feed_heartbeat_at": (
            now - timedelta(seconds=heartbeat_age)
        ).isoformat(),
    }))
    monkeypatch.setattr(takeover, "runtime_processes", lambda *_args: [42])

    report = takeover.recovery_status(tmp_path, previous_run="previous")

    assert report["collecting"] is expected
    assert report["recovered"] is expected
    assert report["collector_mode"] == (
        "closed_market_monitoring" if expected else "unverified"
    )


def test_new_live_test_launches_exact_completed_predecessor(monkeypatch, tmp_path):
    _state_files(tmp_path)
    (tmp_path / "log/wti_live_state.json").write_text(json.dumps({
        "status": "flat", "single_round_pilot": {"pilot_id": "completed-id", "status": "completed"},
    }))
    (tmp_path / ".venv/bin").mkdir(parents=True)
    (tmp_path / ".venv/bin/python").touch()
    monkeypatch.setattr(takeover, "script_processes", lambda *_args: [])
    monkeypatch.setattr(takeover, "stop_old_runtime", lambda *_args: None)
    monkeypatch.setattr(takeover, "require_free_ports", lambda: None)
    commands = []
    monkeypatch.setattr(takeover.subprocess, "run", lambda command, **_kwargs: commands.append(command))

    def status(*_args, **kwargs):
        assert kwargs["new_pilot_after"] == "completed-id"
        return {"recovered": True}

    monkeypatch.setattr(takeover, "recovery_status", status)
    assert takeover.execute_takeover(tmp_path, start_pilot=True)["recovered"]
    launched = commands[-1][-1]
    assert "--new-pilot-after completed-id" in launched
    assert "--confirm-bounded-live-pilot" in launched
    assert "--keep-collecting-after-pilot" in launched
    assert "--manage-existing-wti-position" not in launched


def test_new_test_does_not_restart_existing_pending_round(monkeypatch, tmp_path):
    _state_files(tmp_path)
    monkeypatch.setattr(takeover, "stop_old_runtime", lambda *_args: pytest.fail("must not stop"))
    with pytest.raises(RuntimeError, match="terminal_previous_pilot_and_flat"):
        takeover.execute_takeover(tmp_path, start_pilot=True)


@pytest.mark.parametrize("authorized", [False, True])
@pytest.mark.parametrize("risk_action", ["normal", "block_entry"])
def test_new_live_test_acceptance_requires_actual_new_entry_authorization(monkeypatch, tmp_path, authorized, risk_action):
    _state_files(tmp_path)
    (tmp_path / "log/wti_live_state.json").write_text(json.dumps({
        "status": "flat", "single_round_pilot": {
            "pilot_id": "new-pilot", "previous_pilot_id": "old-pilot", "status": "available",
        },
    }))
    (tmp_path / "log/wti_live_risk_health.json").write_text(json.dumps({
        **_cycle_evidence(tmp_path, run_id="new-run"),
        "pid": 42, "run_id": "new-run", "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": "running", "startup_reconciled": True,
        "manage_existing_only": False, "wti_entry_authorized": authorized,
        "risk_action": risk_action, "risk_data_complete": True,
    }))
    collector_root = tmp_path / "log/oil_swap/basis_samples"
    collector_root.mkdir(parents=True)
    (collector_root / "health.json").write_text(json.dumps({
        "pid": 42, "status": "running", "samples_this_run": 3,
        "last_sample_at": datetime.now(timezone.utc).isoformat(),
    }))
    monkeypatch.setattr(takeover, "runtime_processes", lambda *_args: [42])
    report = takeover.recovery_status(tmp_path, previous_run="old-run", new_pilot_after="old-pilot")
    assert report["recovered"] is (authorized and risk_action == "normal")
    assert report["purpose"] == "single_round_live_test"
