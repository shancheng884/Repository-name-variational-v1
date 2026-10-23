#!/usr/bin/env python3
"""Linux WTI management takeover; live account checks belong to the runtime."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import subprocess
import time
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[1]
PORTS = (8766, 8767, 8768)


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"invalid_json_object:{path.name}")
    return value


def script_processes(root: Path, script_name: str, proc: Path = Path("/proc")) -> list[int]:
    target = root.resolve() / script_name
    found = []
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            argv = [os.fsdecode(arg) for arg in entry.joinpath("cmdline").read_bytes().rstrip(b"\0").split(b"\0")]
            if len(argv) < 2 or not re.fullmatch(r"python(?:\d+(?:\.\d+)*)?(?:\.exe)?", Path(argv[0]).name):
                continue
            arguments = argv[1:]
            while arguments and arguments[0] in {"-u", "-B", "-E", "-s", "-S", "-I", "-O", "-OO", "-q"}:
                arguments = arguments[1:]
            if not arguments or arguments[0].startswith("-"):
                continue
            script = Path(arguments[0])
            if not script.is_absolute():
                script = entry.joinpath("cwd").resolve(strict=True) / script
            if script.resolve() == target:
                found.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            continue
    return sorted(found)


def runtime_processes(root: Path, proc: Path = Path("/proc")) -> list[int]:
    return script_processes(root, "tools/wti_live.py", proc)


def validate_local_state(root: Path) -> dict:
    eth = read_json(root / "log/live_inventory_state.json")
    if (eth.get("asset") != "ETH" or eth.get("status") != "flat"
            or eth.get("open_lots") or eth.get("pending_actions")):
        raise RuntimeError("eth_local_state_not_flat")
    state = read_json(root / "log/wti_live_state.json")
    if state.get("status") not in {"flat", "open", "pending", "manual_review_required"}:
        raise RuntimeError("wti_local_state_invalid")
    if state["status"] == "flat" and (state.get("pending_action") or state.get("open_position")):
        raise RuntimeError("wti_flat_state_contains_exposure")
    if state["status"] != "flat" and not (state.get("pending_action") or state.get("open_position")):
        raise RuntimeError("wti_exposure_state_missing_action_or_position")
    return state


def require_free_ports() -> None:
    # Binding tests availability without making malformed WebSocket connections.
    sockets = []
    try:
        for port in PORTS:
            server = socket.socket()
            sockets.append(server)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("127.0.0.1", port))
    except OSError as exc:
        raise RuntimeError(f"forwarder_port_unavailable:{port}") from exc
    finally:
        for server in sockets:
            server.close()


def stop_old_runtime(root: Path, *, timeout: float = 30) -> None:
    pids = runtime_processes(root)
    if len(pids) > 1:
        raise RuntimeError(f"multiple_wti_runtimes:{pids}")
    if not pids:
        return
    pid = pids[0]
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = runtime_processes(root)
        if not remaining:
            return
        if remaining != [pid]:
            raise RuntimeError("wti_runtime_changed_during_shutdown")
        time.sleep(0.2)
    raise RuntimeError("old_wti_runtime_did_not_exit_no_force_kill")


def management_command(root: Path) -> list[str]:
    return [
        str(root / ".venv/bin/python"), "tools/wti_live.py", "--mode", "live",
        "--confirm-eth-flat", "--i-understand-live-orders", "--manage-existing-wti-position",
        "--expected-lighter-market-id", "145", "--entry-threshold-bps", "30",
        "--exit-target-bps", "1", "--roundtrip-cost-bps", "10",
        "--max-notional-usd", "20", "--max-slippage-bps", "8",
        "--max-hold-seconds", "21600", "--entry-cutoff-seconds", "1800",
    ]


def recovery_status(root: Path, *, previous_run: str | None) -> dict:
    state = read_json(root / "log/wti_live_state.json")
    health = read_json(root / "log/wti_live_risk_health.json")
    pids = runtime_processes(root)
    stamp = datetime.fromisoformat(str(health["updated_at"]).replace("Z", "+00:00"))
    age = (datetime.now(timezone.utc) - stamp).total_seconds()
    current = (len(pids) == 1 and health.get("pid") == pids[0]
               and health.get("run_id") != previous_run and -5 <= age <= 15
               and health.get("status") == "running")
    try:
        collector = read_json(root / "log/oil_swap/basis_samples/health.json")
        sample_at = datetime.fromisoformat(str(collector["last_sample_at"]).replace("Z", "+00:00"))
        sample_age = (datetime.now(timezone.utc) - sample_at).total_seconds()
    except (OSError, ValueError, KeyError):
        collector, sample_age = {}, None
    collecting = bool(
        current and collector.get("pid") == health.get("pid")
        and collector.get("status") == "running"
        and collector.get("samples_this_run", 0) >= 2
        and sample_age is not None and -5 <= sample_age <= 30
    )
    complete = (current and health.get("startup_reconciled") is True
                and health.get("manage_existing_only") is True
                and health.get("wti_entry_authorized") is False
                and state.get("status") in {"flat", "open"}
                and not state.get("pending_action") and collecting)
    return {
        "recovered": bool(complete), "current_runtime": bool(current),
        "pids": pids, "run_id": health.get("run_id"), "state": state.get("status"),
        "pending": bool(state.get("pending_action")),
        "collecting": collecting, "samples": collector.get("samples_this_run"),
        "market_status": collector.get("market_status"),
        "reason": state.get("manual_review_reason") or health.get("risk_reason"),
        "action": health.get("last_action"),
        "order_diagnostics": health.get("pending_order_diagnostics"),
    }


def execute_takeover(root: Path, *, wait_seconds: float = 90) -> dict:
    validate_local_state(root)
    if script_processes(root, "main.py"):
        raise RuntimeError("eth_runtime_still_running")
    old_health = root / "log/wti_live_risk_health.json"
    previous_run = read_json(old_health).get("run_id") if old_health.exists() else None
    subprocess.run(["tmux", "-V"], check=True, capture_output=True)
    command = management_command(root)
    if not Path(command[0]).is_file():
        raise RuntimeError("workspace_python_missing")
    if not (root / ".env").is_file():
        raise RuntimeError("workspace_env_missing")
    stop_old_runtime(root)
    validate_local_state(root)
    require_free_ports()
    # The old process owned the forwarder. A fresh account probe is only
    # possible after the new runtime starts it and reconnects the extension.
    session = f"wti-manage-{datetime.now(timezone.utc).strftime('%m%d%H%M%S')}-{os.getpid()}"
    log = root / "log" / f"{session}.log"
    shell_command = (
        f"cd {shlex.quote(str(root))} && set -a && . ./.env && set +a"
        f" && exec {shlex.join(command)} >> {shlex.quote(str(log))} 2>&1"
    )
    subprocess.run(["tmux", "new-session", "-d", "-s", session, shell_command], check=True)
    deadline = time.monotonic() + wait_seconds
    result = {}
    while True:
        try:
            result = recovery_status(root, previous_run=previous_run)
        except (OSError, ValueError, KeyError):
            result = {"recovered": False, "reason": "awaiting_current_runtime_health"}
        result.update({"session": session, "log": str(log)})
        if result.get("recovered") or time.monotonic() >= deadline:
            return result
        session_alive = subprocess.run(
            ["tmux", "has-session", "-t", session], capture_output=True,
        ).returncode == 0
        if not session_alive:
            return {**result, "recovered": False, "reason": "new_runtime_exited_check_log"}
        time.sleep(1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-eth-flat", action="store_true")
    parser.add_argument("--i-understand-live-orders", action="store_true")
    args = parser.parse_args(argv)
    try:
        if not args.execute:
            state = validate_local_state(ROOT)
            print(json.dumps({"pids": runtime_processes(ROOT), "state": state["status"], "orders_attempted": False}))
            return 0
        if not args.confirm_eth_flat or not args.i_understand_live_orders:
            raise RuntimeError("explicit_eth_flat_and_live_order_confirmation_required")
        import fcntl

        with (ROOT / "log/wti_takeover.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = execute_takeover(ROOT)
        print(json.dumps(result, ensure_ascii=True))
        return 0 if result["recovered"] else 2
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        print(json.dumps({"recovered": False, "error": str(exc)}, ensure_ascii=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
