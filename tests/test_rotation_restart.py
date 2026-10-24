from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import rotation_supervisor as rotation
from tools.lib.rotation_supervisor import exclusive_lock, write_object_atomic


def test_supervisor_identity_finds_relative_and_absolute_python_and_excludes_restart_helpers(
    monkeypatch, tmp_path,
):
    root = tmp_path / "repo"
    root.mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    commands = {
        11: [".venv/bin/python", "tools/rotation_supervisor.py", "--execute"],
        12: [str(root / ".venv/bin/python"), str(root / "tools/rotation_supervisor.py"), "--execute"],
        13: ["tmux", "new-session", "exec .venv/bin/python tools/rotation_supervisor.py"],
        14: ["python", "tools/rotation_supervisor.py", "--execute", "--restart-supervisor"],
        15: ["python", str(tmp_path / "other/tools/rotation_supervisor.py")],
        16: ["python3.14", "-u", "-B", "tools/rotation_supervisor.py", "--execute"],
        17: ["python", "main.py", "tools/rotation_supervisor.py"],
        18: ["python", "tools/wti_live.py", "--mode", "live"],
        19: ["python", "tools/rotation_supervisor.py.backup"],
        20: ["python", "tools/rotation_supervisor.py", "--rehearse"],
    }
    for pid, argv in commands.items():
        entry = proc / str(pid)
        entry.mkdir()
        (entry / "cmdline").write_bytes(b"\0".join(str(arg).encode() for arg in argv) + b"\0")
    original_resolve = Path.resolve

    def resolve(path, *args, **kwargs):
        return root if path.name == "cwd" else original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    assert rotation.supervisor_processes(root, proc) == [11, 12, 16]


@pytest.mark.parametrize("identity_changed", [False, True])
def test_terminate_uses_pidfd_and_rechecks_supervisor_identity(monkeypatch, tmp_path, identity_changed):
    events = []
    monkeypatch.setattr(rotation.os, "pidfd_open", lambda pid: events.append(("open", pid)) or 77, raising=False)
    monkeypatch.setattr(rotation.signal, "pidfd_send_signal", lambda *a: events.append(("signal", *a)), raising=False)
    monkeypatch.setattr(rotation.os, "close", lambda fd: events.append(("close", fd)))
    monkeypatch.setattr(rotation, "supervisor_processes", lambda root: [12] if identity_changed else [11])

    if identity_changed:
        with pytest.raises(RuntimeError, match="identity_changed"):
            rotation.terminate_supervisor(tmp_path, 11)
        assert not any(e[0] == "signal" for e in events)
    else:
        rotation.terminate_supervisor(tmp_path, 11)
        assert ("signal", 77, rotation.signal.SIGTERM) in events
    assert events[-1] == ("close", 77)


def _restart_environment(monkeypatch, root):
    (root / ".venv/bin").mkdir(parents=True)
    (root / ".venv/bin/python").touch()
    (root / ".env").touch()
    monkeypatch.setattr(rotation.sys, "platform", "linux")
    pids = [11]
    events = []
    monkeypatch.setattr(rotation, "supervisor_processes", lambda _: list(pids))

    def terminate(_root, pid):
        events.append(("terminate", pid))
        pids.remove(pid)

    monkeypatch.setattr(rotation, "terminate_supervisor", terminate)
    return pids, events


def _arguments():
    return [
        "--execute", "--restart-supervisor", "--confirm-automatic-rotation",
        "--i-understand-live-orders", "--confirm-initial-flat",
        "--continuous-gradient-rotation", "--confirm-continuous-gradient-rotation",
        "--lighter-market-id", "145", "--lighter-multiplier", "1",
    ]


def test_restart_replaces_only_supervisor_and_confirms_new_pid_without_changing_journal(
    monkeypatch, tmp_path,
):
    pids, events = _restart_environment(monkeypatch, tmp_path)
    state_path = tmp_path / "log/rotation_supervisor_state.json"
    worker_path = tmp_path / "log/wti_live_state.json"
    write_object_atomic(state_path, {
        "phase": "starting", "target_pair": "ETH", "launch_attempts": 1,
        "continuous_gradient_rotation": {"session_id": "persisted", "rounds_started": 7},
    })
    write_object_atomic(worker_path, {"pending_action": {"action_id": "must-not-repeat"}})
    before = state_path.read_bytes(), worker_path.read_bytes()

    def launch(command, *, check):
        assert command[:3] == ["tmux", "new-session", "-d"]
        assert "--restart-supervisor" not in command[-1]
        assert str(tmp_path / ".venv/bin/python") in command[-1]
        assert "--confirm-initial-flat" in command[-1]
        events.append(("launch", command))
        pids.append(22)
        write_object_atomic(tmp_path / "log/rotation_supervisor_health.json", {
            "pid": 22, "supervisor_id": "new-instance", "status": "running",
            "updated_at": datetime.now(timezone.utc).isoformat(), "last_error": None,
            "last_result": {"action": "recover_interrupted_eth_start_via_wti_session_bootstrap",
                            "snapshot": {"current_pair": "NONE", "target_pair": "NONE"}},
        })
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(rotation.subprocess, "run", launch)
    result = rotation.restart_supervisor(tmp_path, _arguments(), timeout_seconds=0)

    assert result["ready"] is True and result["pids"] == [22]
    assert result["supervisor_id"] == "new-instance"
    assert events[0] == ("terminate", 11) and len(events) == 2
    assert before == (state_path.read_bytes(), worker_path.read_bytes())


@pytest.mark.parametrize("failure", ["conflict", "not_stopped", "lock_held"])
def test_restart_never_launches_when_prior_supervisor_is_not_cleared(monkeypatch, tmp_path, failure):
    pids, events = _restart_environment(monkeypatch, tmp_path)
    monkeypatch.setattr(rotation.subprocess, "run", lambda *a, **kw: pytest.fail("duplicate supervisor"))
    if failure == "conflict":
        pids.append(12)
        with pytest.raises(RuntimeError, match="process_count_conflict"):
            rotation.restart_supervisor(tmp_path, _arguments(), timeout_seconds=0)
        assert not events
    elif failure == "not_stopped":
        monkeypatch.setattr(rotation, "terminate_supervisor", lambda *_: None)
        with pytest.raises(RuntimeError, match="did_not_stop"):
            rotation.restart_supervisor(tmp_path, _arguments(), timeout_seconds=0)
    else:
        pids.clear()
        with exclusive_lock(tmp_path / "log/rotation_supervisor.lock"):
            with pytest.raises(RuntimeError, match="already_running"):
                rotation.restart_supervisor(tmp_path, _arguments(), timeout_seconds=0)


@pytest.mark.parametrize("bad_health", ["stale", "wrong_pid", "blocked"])
def test_restart_requires_fresh_new_supervisor_health(monkeypatch, tmp_path, bad_health):
    pids, _events = _restart_environment(monkeypatch, tmp_path)

    def launch(*_args, **_kwargs):
        pids.append(22)
        at = datetime.now(timezone.utc)
        if bad_health == "stale":
            at -= timedelta(hours=1)
        write_object_atomic(tmp_path / "log/rotation_supervisor_health.json", {
            "pid": 11 if bad_health == "wrong_pid" else 22,
            "supervisor_id": "new-instance", "updated_at": at.isoformat(),
            "status": "blocked" if bad_health == "blocked" else "running",
            "last_error": "rotation_journal_blocked_manual_review_required" if bad_health == "blocked" else None,
        })

    monkeypatch.setattr(rotation.subprocess, "run", launch)
    result = rotation.restart_supervisor(tmp_path, _arguments(), timeout_seconds=0)
    assert result["ready"] is False
    assert result["error"]
    assert result["pids"] == [22]


def test_restart_option_requires_explicit_persistent_execution():
    common = ["--restart-supervisor", "--lighter-market-id", "145", "--lighter-multiplier", "1"]
    with pytest.raises(SystemExit):
        rotation.main(common)
    with pytest.raises(SystemExit):
        rotation.main([*common, "--execute", "--once", "--confirm-automatic-rotation", "--i-understand-live-orders"])


def test_health_identifies_supervisor_process_and_authorization(tmp_path):
    supervisor = rotation.RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=rotation.Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    supervisor._save_health({"status": "running"})
    health = json.loads(supervisor.health_path.read_text(encoding="utf-8"))
    assert health["pid"] == rotation.os.getpid()
    assert health["supervisor_id"] == supervisor.supervisor_id
    assert health["continuous_gradient_rotation"] is True
    assert health["confirm_initial_flat"] is True


def test_stop_request_completes_current_transition_and_releases_execution_lock(monkeypatch, tmp_path):
    supervisor = rotation.RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=rotation.Decimal("1"), confirm_initial_flat=False,
    )
    calls = []

    def transition():
        calls.append("transition")
        supervisor.request_stop(rotation.signal.SIGTERM, None)
        journal = rotation.initial_journal()
        journal.update({"phase": "starting", "target_pair": "ETH", "launch_attempts": 1})
        supervisor._save_journal(journal)
        return {"action": "start_initial_target"}

    monkeypatch.setattr(supervisor, "run_once", transition)
    monkeypatch.setattr(rotation.time, "sleep", lambda _: pytest.fail("must stop after transition"))
    assert supervisor.run(once=False, poll_seconds=5) == 0
    assert calls == ["transition"]
    saved = json.loads(supervisor.journal_path.read_text(encoding="utf-8"))
    assert saved["phase"] == "starting" and saved["launch_attempts"] == 1
    with exclusive_lock(tmp_path / "log/rotation_supervisor.lock"):
        pass
