#!/usr/bin/env python3
"""Fail-closed ETH/WTI session rotator. Default mode is strictly read-only."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import read_basis_samples  # noqa: E402
from tools.lib.market_sessions import OilSession  # noqa: E402
from tools.lib.pair_rotation import Pair, RotationAction, decide_rotation, desired_pair  # noqa: E402
from tools.lib.rotation_supervisor import (  # noqa: E402
    RotationPhase,
    exclusive_lock,
    initial_journal,
    read_object,
    validate_journal,
    verified_drain_completion,
    verified_flat_account_probe,
    write_object_atomic,
)
from tools.wti_live import (  # noqa: E402
    _session_window,
    readiness_for_new_entries,
)


ETH_STATE = ROOT / "log" / "live_inventory_state.json"
ETH_HEALTH = ROOT / "log" / "live_inventory_risk_health.json"
ETH_CONTROL = ROOT / "log" / "live_inventory_control.json"
WTI_STATE = ROOT / "log" / "wti_live_state.json"
WTI_HEALTH = ROOT / "log" / "wti_live_risk_health.json"
WTI_CONTROL = ROOT / "log" / "wti_live_control.json"
WTI_SAMPLES = ROOT / "log" / "oil_swap" / "basis_samples"
JOURNAL = ROOT / "log" / "rotation_supervisor_state.json"
HEALTH = ROOT / "log" / "rotation_supervisor_health.json"
PORTS = (8766, 8767, 8768)
HEARTBEAT_MAX_AGE_SECONDS = 20
ROTATION_POLL_SECONDS = 5
MAX_CONSECUTIVE_FAILURES = 3


def _age(value: Any, now: datetime) -> float | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return (now - parsed.astimezone(timezone.utc)).total_seconds()
    except (TypeError, ValueError):
        return None


def script_processes(root: Path, script: str, proc_root: Path = Path("/proc")) -> list[int]:
    """Find exact Python script processes; do not match the supervisor command itself."""
    if not proc_root.is_dir():
        return []
    target = (root / script).resolve()
    result: list[int] = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            argv = [os.fsdecode(part) for part in (entry / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")]
            if len(argv) < 2 or not Path(argv[0]).name.startswith("python"):
                continue
            arguments = argv[1:]
            while arguments and arguments[0] in {"-u", "-B", "-E", "-s", "-S", "-I", "-O", "-OO", "-q"}:
                arguments = arguments[1:]
            if not arguments or arguments[0].startswith("-"):
                continue
            script_arg = Path(arguments[0])
            if not script_arg.is_absolute():
                script_arg = (entry / "cwd").resolve() / script_arg
            if script_arg.resolve() == target:
                result.append(int(entry.name))
        except (OSError, ProcessLookupError, PermissionError):
            continue
    return sorted(result)


def _state_exposure(pair: Pair, state: dict[str, Any]) -> tuple[bool, bool, bool]:
    """Return (valid, exposure, pending), treating malformed state as unsafe."""
    if pair == Pair.ETH:
        valid = state.get("asset") == "ETH" and state.get("status") in {
            "flat", "open", "pending", "manual_review_required",
        }
        lots = state.get("open_lots")
        actions = state.get("pending_actions")
        if not isinstance(lots, list) or not isinstance(actions, list):
            return False, True, True
        exposure = bool(lots) or state.get("status") in {"open", "pending", "manual_review_required"}
        return valid, exposure, bool(actions)
    if pair == Pair.WTI:
        valid = state.get("asset") == "WTI" and state.get("status") in {
            "flat", "open", "pending", "manual_review_required",
        }
        pending = state.get("pending_action")
        position = state.get("open_position")
        if pending is not None and not isinstance(pending, dict):
            return False, True, True
        if position is not None and not isinstance(position, dict):
            return False, True, True
        exposure = position is not None or pending is not None or state.get("status") in {
            "open", "pending", "manual_review_required",
        }
        return valid, exposure, pending is not None
    return True, False, False


def _flat_state(pair: Pair, state: dict[str, Any], *, allow_missing: bool = False) -> bool:
    if not state and allow_missing:
        return True
    valid, exposure, pending = _state_exposure(pair, state)
    if not valid or exposure or pending:
        return False
    if pair == Pair.ETH:
        return state.get("status") == "flat" and not state.get("open_lots") and not state.get("pending_actions")
    if pair == Pair.WTI:
        return state.get("status") == "flat" and state.get("open_position") is None and state.get("pending_action") is None
    return True


def collect_snapshot(
    root: Path = ROOT,
    *,
    market_id: int = 145,
    multiplier: Decimal = Decimal("1"),
    now: datetime | None = None,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    eth_state = read_object(root / "log/live_inventory_state.json")
    eth_health = read_object(root / "log/live_inventory_risk_health.json")
    wti_state = read_object(root / "log/wti_live_state.json")
    wti_health = read_object(root / "log/wti_live_risk_health.json")
    collector = read_object(root / "log/oil_swap/basis_samples/health.json")
    rows = read_basis_samples(root / "log/oil_swap/basis_samples", limit=200000, asset_filter="WTI")
    rows.sort(key=lambda row: str(row.get("logged_at") or ""))
    readiness = readiness_for_new_entries(
        rows,
        market_id=market_id,
        multiplier=multiplier,
        now=now,
    )
    sample = rows[-1] if rows else {}
    session = _session_window(sample, now, 1800)
    eth_pids = script_processes(root, "main.py", proc_root)
    wti_pids = script_processes(root, "tools/wti_live.py", proc_root)
    collector_pids = script_processes(root, "tools/oil_swap_collector.py", proc_root)
    runtimes: dict[str, Any] = {}
    for name, pids, health in (
        ("ETH", eth_pids, eth_health),
        ("WTI", wti_pids, wti_health),
    ):
        age = _age(health.get("updated_at"), now)
        matched = (
            len(pids) == 1
            and health.get("pid") == pids[0]
            and health.get("status") == "running"
            and age is not None
            and -5 <= age <= HEARTBEAT_MAX_AGE_SECONDS
        )
        runtimes[name] = {
            "pids": pids,
            "matched_healthy": matched,
            "run_id": health.get("run_id"),
            "health_age_seconds": age,
            "health": health,
        }
    current = Pair.NONE
    if runtimes["ETH"]["matched_healthy"] and not wti_pids:
        current = Pair.ETH
    elif runtimes["WTI"]["matched_healthy"] and not eth_pids:
        current = Pair.WTI
    elif not eth_pids and not wti_pids:
        current = Pair.NONE
    eth_ready = (root / "live_config.json").is_file() and bool(eth_health.get("risk_action") != "block_all")
    wti_pilot = wti_state.get("gradient_pilot") or {}
    wti_pilot_available = wti_pilot.get("status") in {None, "available", "in_position", "entry_pending"}
    wti_ready = bool(readiness.get("production_ready") and wti_pilot_available)
    wti_gate_reasons = list(readiness.get("reasons", []))
    if readiness.get("production_ready") and not wti_pilot_available:
        wti_gate_reasons.append("persisted_gradient_pilot_budget_already_consumed")
    target, target_reason = desired_pair(
        session=session,
        wti_ready=wti_ready,
        eth_ready=eth_ready,
        current=current,
    )
    return {
        "observed_at": now.isoformat(),
        "current_pair": current.value,
        "target_pair": target.value,
        "target_reason": target_reason,
        "session": {
            "state": session.state.value,
            "allow_new_entries": session.allow_new_entries,
            "allow_position_management": session.allow_position_management,
            "reason": session.reason,
            "observed_at": session.observed_at,
            "closing_at": session.closing_at,
        },
        "readiness": readiness,
        "wti_pilot_status": wti_pilot.get("status"),
        "eth_ready": eth_ready,
        "wti_ready": wti_ready,
        "wti_gate_reasons": wti_gate_reasons,
        "collector_process_count": len(collector_pids),
        "runtimes": runtimes,
        "collector_pids": collector_pids,
        "states": {"ETH": eth_state, "WTI": wti_state},
        "collector": collector,
        "process_conflict": (
            bool(eth_pids and wti_pids)
            or len(eth_pids) > 1 or len(wti_pids) > 1
            or len(collector_pids) > 1
            or (bool(wti_pids) and bool(collector_pids))
            or (bool(eth_pids) and not runtimes["ETH"]["matched_healthy"])
            or (bool(wti_pids) and not runtimes["WTI"]["matched_healthy"])
        ),
    }


def rehearsal_plan(snapshot: dict[str, Any]) -> dict[str, Any]:
    current = Pair(snapshot["current_pair"])
    target = Pair(snapshot["target_pair"])
    state = snapshot["states"].get(current.value, {}) if current != Pair.NONE else {}
    valid, exposure, pending = _state_exposure(current, state) if current != Pair.NONE else (True, False, False)
    runtime = snapshot["runtimes"].get(current.value, {}) if current != Pair.NONE else {}
    session_state = snapshot.get("session", {}).get("state")
    from tools.lib.market_sessions import SessionWindow

    # Rehearsal only needs the verified session gates and does not execute the plan.
    session = SessionWindow(
        OilSession(session_state or OilSession.STATUS_UNKNOWN.value),
        "rehearsal", None,
        bool(snapshot.get("session", {}).get("allow_new_entries")),
        bool(snapshot.get("session", {}).get("allow_position_management")),
        str(snapshot.get("session", {}).get("reason") or "session_unknown"),
        snapshot.get("session", {}).get("observed_at"),
        exchange_open=(True if session_state in {"open", "entry_cutoff"} else False if session_state == "exchange_closed" else None),
    )
    proof = state.get("last_account_reconciliation") or {}
    active_orders_zero = proof.get("active_order_count") == 0 if current == Pair.WTI else None
    reconciled = runtime.get("health", {}).get("startup_reconciled") is True if current == Pair.WTI else None
    decision = decide_rotation(
        current=current,
        target=target,
        target_ready=bool(snapshot.get("wti_ready") if target == Pair.WTI else snapshot.get("eth_ready") if target == Pair.ETH else False),
        session=session,
        has_open_position=exposure,
        has_pending_action=pending,
        active_orders_zero=active_orders_zero,
        exchange_reconciled=reconciled,
    )
    reasons = []
    if snapshot.get("process_conflict"):
        reasons.append("multiple_or_conflicting_runtime_processes")
    if current != Pair.NONE and not runtime.get("matched_healthy"):
        reasons.append("current_runtime_identity_or_heartbeat_unverified")
    if current != Pair.NONE and not valid:
        reasons.append("current_local_state_invalid")
    if current == Pair.NONE and target != Pair.NONE:
        reasons.append("initial_both_account_flat_confirmation_required")
    if target == Pair.WTI and not snapshot.get("wti_ready"):
        reasons.extend(snapshot.get("wti_gate_reasons", snapshot.get("readiness", {}).get("reasons", [])))
    return {
        "mode": "read_only_rehearsal",
        "current_pair": current.value,
        "target_pair": target.value,
        "target_reason": snapshot.get("target_reason"),
        "decision": decision.action.value,
        "decision_reason": decision.reason,
        "allow_new_entries": decision.allow_new_entries,
        "execution_ready": not reasons and decision.action in {
            RotationAction.RUN_TARGET, RotationAction.MANAGE_CURRENT,
            RotationAction.DRAIN_CURRENT, RotationAction.SWITCH_TO_TARGET,
        },
        "blocking_reasons": list(dict.fromkeys(reasons)),
        "side_effects": False,
        "wti_production_ready": bool(snapshot.get("readiness", {}).get("production_ready")),
        "wti_readiness_reasons": snapshot.get("wti_gate_reasons", snapshot.get("readiness", {}).get("reasons", [])),
    }


class RotationSupervisor:
    def __init__(self, *, root: Path, market_id: int, multiplier: Decimal,
                 confirm_initial_flat: bool, dry_run: bool = False) -> None:
        self.root = root
        self.market_id = market_id
        self.multiplier = multiplier
        self.confirm_initial_flat = confirm_initial_flat
        self.dry_run = dry_run
        self.journal_path = root / "log/rotation_supervisor_state.json"
        self.health_path = root / "log/rotation_supervisor_health.json"

    def _save_journal(self, journal: dict[str, Any]) -> None:
        journal["updated_at"] = datetime.now(timezone.utc).isoformat()
        write_object_atomic(self.journal_path, journal)

    def _save_health(self, payload: dict[str, Any]) -> None:
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        write_object_atomic(self.health_path, payload)

    def _forwarder_available(self) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", 8768), timeout=2):
                return True
        except OSError:
            return False

    def _launch_oil_collector(self, session_name: str) -> None:
        if os.name == "nt":
            raise RuntimeError("automatic_rotation_execution_requires_linux")
        python = str(self.root / ".venv" / "bin" / "python")
        if not (self.root / ".env").is_file() or not Path(python).is_file():
            raise RuntimeError("rotation_runtime_environment_missing")
        log_path = self.root / "log" / f"{session_name}.log"
        command = [
            python, "tools/oil_swap_collector.py", "--reuse-forwarder",
            "--forwarder-command-port", "8768", "--output-dir", "log/oil_swap",
            "--notional-usd", "20", "--interval-seconds", "10",
        ]
        shell = (
            f"cd {shlex.quote(str(self.root))} && set -a && . ./.env && set +a && "
            f"exec {shlex.join(command)} >> {shlex.quote(str(log_path))} 2>&1"
        )
        subprocess.run(["tmux", "new-session", "-d", "-s", session_name, shell], check=True)

    def _ensure_oil_collector(self, journal: dict[str, Any], snapshot: dict[str, Any]) -> str:
        pids = snapshot.get("collector_pids") or []
        if len(pids) > 1:
            return "collector_process_count_conflict"
        if pids:
            collector = snapshot.get("collector") or {}
            age = _age(collector.get("updated_at"), datetime.now(timezone.utc))
            if (
                collector.get("pid") == pids[0]
                and collector.get("status") == "running"
                and age is not None
                and -5 <= age <= HEARTBEAT_MAX_AGE_SECONDS
            ):
                if journal.get("collector_launch_attempts"):
                    journal["collector_launch_attempts"] = 0
                    self._save_journal(journal)
                return "collector_running"
            return "collector_process_unverified"
        session_name = journal.get("collector_tmux_session")
        if self._tmux_session_exists(session_name):
            return "await_collector_process"
        if not self._forwarder_available():
            return "await_eth_forwarder"
        attempts = int(journal.get("collector_launch_attempts") or 0)
        if attempts >= 2:
            return "collector_start_retries_exhausted"
        session_name = str(session_name or (
            f"rotation-wti-collector-{datetime.now(timezone.utc).strftime('%m%d%H%M%S')}"
        ))
        journal.update({
            "collector_tmux_session": session_name,
            "collector_launch_attempts": attempts + 1,
        })
        self._save_journal(journal)
        self._launch_oil_collector(session_name)
        return "collector_started"

    def _start_command(self, pair: Pair, *, recovery: bool = False) -> list[str]:
        python = str(self.root / ".venv" / "bin" / "python")
        if pair == Pair.ETH:
            command = [
                python, "tools/live.py", "--asset", "ETH", "--v4-live",
                "--v4-real-gradient", "--v4-elastic-capacity",
                "--v4-bidirectional", "--v4-continuous",
            ]
            if recovery:
                command.extend(["--resume-open-position", "--drain-after-flat"])
            return command
        if pair == Pair.WTI:
            command = [
                python, "tools/wti_live.py", "--mode", "live", "--confirm-eth-flat",
                "--i-understand-live-orders", "--expected-lighter-market-id", str(self.market_id),
                "--entry-threshold-bps", "30", "--exit-target-bps", "1",
                "--roundtrip-cost-bps", "10", "--max-notional-usd", "20",
                "--max-slippage-bps", "8", "--max-hold-seconds", "21600",
                "--entry-cutoff-seconds", "1800",
            ]
            if recovery:
                command.extend(["--manage-existing-wti-position"])
            else:
                command.extend([
                    "--gradient-live", "--confirm-bounded-gradient-pilot",
                    "--gradient-second-tier-bps", "45",
                    "--gradient-layer-notional-usd", "10",
                    "--keep-collecting-after-pilot",
                ])
            return command
        raise RuntimeError("rotation_target_invalid")

    def _launch(self, pair: Pair, *, recovery: bool = False, session_name: str) -> str:
        if os.name == "nt":
            raise RuntimeError("automatic_rotation_execution_requires_linux")
        if not (self.root / ".env").is_file() or not (self.root / ".venv/bin/python").is_file():
            raise RuntimeError("rotation_runtime_environment_missing")
        if pair == Pair.WTI:
            for port in PORTS:
                with socket.socket() as sock:
                    sock.bind(("127.0.0.1", port))
        session = session_name
        log_path = self.root / "log" / f"{session}.log"
        command = self._start_command(pair, recovery=recovery)
        shell = (
            f"cd {shlex.quote(str(self.root))} && set -a && . ./.env && set +a && "
            f"exec {shlex.join(command)} >> {shlex.quote(str(log_path))} 2>&1"
        )
        subprocess.run(["tmux", "new-session", "-d", "-s", session, shell], check=True)
        return session

    @staticmethod
    def _tmux_session_exists(session_name: str | None) -> bool:
        if not session_name or os.name == "nt":
            return False
        return subprocess.run(
            ["tmux", "has-session", "-t", session_name], capture_output=True,
        ).returncode == 0

    def _write_drain(self, pair: Pair, runtime: dict[str, Any], journal: dict[str, Any]) -> None:
        pid = runtime["pids"][0]
        run_id = str(runtime["run_id"])
        request_id = journal.get("request_id") or uuid.uuid4().hex
        path = self.root / ("log/live_inventory_control.json" if pair == Pair.ETH else "log/wti_live_control.json")
        existing = read_object(path)
        if existing and existing.get("status") in {"requested", "draining"}:
            if existing.get("target_pid") != pid or existing.get("target_run_id") != run_id:
                raise RuntimeError("active_drain_request_belongs_to_different_runtime")
            request_id = existing.get("request_id") or request_id
        request = {
            "schema_version": 1, "action": "drain_after_flat", "status": "requested",
            "asset": pair.value, "target_pid": pid, "target_run_id": run_id,
            "request_id": request_id, "requested_at": datetime.now(timezone.utc).isoformat(),
        }
        journal.update({
            "phase": RotationPhase.DRAINING.value,
            "current_pair": pair.value,
            "target_pair": journal["target_pair"],
            "request_id": request_id,
            "source_pid": pid,
            "source_run_id": run_id,
            "target_run_id_before_start": None,
            "launch_attempts": 0,
        })
        self._save_journal(journal)
        write_object_atomic(path, request)

    def _stop_flat_wti_after_readonly_probe(
        self, runtime: dict[str, Any], state: dict[str, Any], journal: dict[str, Any],
    ) -> bool:
        """Closed-session fallback: stop only after independent WTI positions/orders proof."""
        if not _flat_state(Pair.WTI, state):
            return False
        health = runtime.get("health") or {}
        if health.get("manage_existing_only") is not True or health.get("wti_entry_authorized") is not False:
            return False
        last_probe_age = _age(journal.get("last_flat_probe_at"), datetime.now(timezone.utc))
        if last_probe_age is not None and last_probe_age < 20:
            return False
        journal["last_flat_probe_at"] = datetime.now(timezone.utc).isoformat()
        self._save_journal(journal)
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(self.root / "tools/wti_account_probe.py"),
                    "--expected-market-id",
                    str(self.market_id),
                ],
                capture_output=True, text=True, timeout=30, check=False,
            )
            result = json.loads(completed.stdout) if completed.returncode == 0 else {}
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            return False
        if not verified_flat_account_probe(
            result,
            expected_market_id=self.market_id,
            now=datetime.now(timezone.utc),
        ):
            return False
        request_id = str(journal.get("request_id") or "")
        pid = int(journal.get("source_pid") or -1)
        run_id = str(journal.get("source_run_id") or "")
        if not request_id or pid not in runtime.get("pids", []) or run_id != runtime.get("run_id"):
            return False
        current_health = read_object(self.root / "log/wti_live_risk_health.json")
        current_health_age = _age(current_health.get("updated_at"), datetime.now(timezone.utc))
        if (
            script_processes(self.root, "tools/wti_live.py") != [pid]
            or current_health.get("pid") != pid
            or current_health.get("run_id") != run_id
            or current_health.get("status") != "running"
            or current_health_age is None
            or not -5 <= current_health_age <= HEARTBEAT_MAX_AGE_SECONDS
        ):
            return False
        control_path = self.root / "log/wti_live_control.json"
        control = read_object(control_path)
        if (
            control.get("action") != "drain_after_flat"
            or control.get("status") not in {"requested", "draining"}
            or control.get("request_id") != request_id
            or control.get("target_pid") != pid
            or control.get("target_run_id") != run_id
        ):
            return False
        proof = {
            **control,
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "completion_source": "read_only_wti_account_probe",
            "variational_position_qty": "0",
            "lighter_position_qty": "0",
            "variational_active_orders": 0,
            "lighter_active_orders": 0,
            "active_order_count": 0,
            "account_probe_observed_at": result.get("observed_at"),
        }
        write_object_atomic(control_path, proof)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        return True

    def _start_target(self, pair: Pair, journal: dict[str, Any], snapshot: dict[str, Any], *, recovery: bool = False, allow_missing_flat: bool = False) -> None:
        state = snapshot["states"][pair.value]
        if recovery:
            other = Pair.WTI if pair == Pair.ETH else Pair.ETH
            if not _flat_state(other, snapshot["states"][other.value]):
                raise RuntimeError("recovery_refused_other_asset_not_flat")
        elif not _flat_state(pair, state, allow_missing=allow_missing_flat):
            raise RuntimeError("target_local_state_not_flat")
        if not recovery and pair == Pair.WTI:
            if not snapshot["readiness"].get("production_ready"):
                raise RuntimeError("wti_production_readiness_not_met")
            pilot = state.get("gradient_pilot") or {}
            if pilot.get("status") in {"completed", "exhausted"}:
                raise RuntimeError("persisted_gradient_pilot_budget_already_consumed")
        phase = RotationPhase.RECOVERING if recovery else RotationPhase.STARTING
        session_name = journal.get("tmux_session") or f"rotation-{pair.value.lower()}-{datetime.now(timezone.utc).strftime('%m%d%H%M%S')}"
        journal.update({
            "phase": phase.value,
            "current_pair": "NONE",
            "target_pair": pair.value,
            "request_id": journal.get("request_id"),
            "target_run_id_before_start": snapshot["runtimes"][pair.value]["run_id"],
            "launch_attempts": int(journal.get("launch_attempts") or 0) + 1,
            "tmux_session": session_name,
        })
        self._save_journal(journal)
        if not self._tmux_session_exists(session_name):
            self._launch(pair, recovery=recovery, session_name=session_name)

    def run_once(self) -> dict[str, Any]:
        snapshot = collect_snapshot(
            self.root, market_id=self.market_id, multiplier=self.multiplier,
        )
        journal = validate_journal(read_object(self.journal_path))
        if journal["phase"] == RotationPhase.BLOCKED.value:
            raise RuntimeError("rotation_journal_blocked_manual_review_required")
        plan = rehearsal_plan(snapshot)
        output: dict[str, Any] = {"snapshot": {
            key: value for key, value in snapshot.items()
            if key not in {"states", "runtimes", "collector", "collector_pids"}
        }, "plan": plan, "phase": journal["phase"], "action": "observe"}
        if self.dry_run:
            return {**output, "side_effects": False}
        if snapshot["process_conflict"]:
            if journal["phase"] in {RotationPhase.STARTING.value, RotationPhase.RECOVERING.value}:
                target_name = str(journal.get("target_pair") or "")
                target_runtime = snapshot["runtimes"].get(target_name, {})
                other_name = "WTI" if target_name == "ETH" else "ETH"
                other_runtime = snapshot["runtimes"].get(other_name, {})
                if (len(target_runtime.get("pids") or []) == 1
                        and not other_runtime.get("pids")
                        and not target_runtime.get("matched_healthy")):
                    output["action"] = "await_starting_runtime_health"
                    return output
            raise RuntimeError("multiple_or_conflicting_runtime_processes")
        if snapshot["current_pair"] == Pair.NONE.value:
            exposed = [pair for pair in (Pair.ETH, Pair.WTI)
                       if _state_exposure(pair, snapshot["states"][pair.value])[1]]
            if exposed:
                if len(exposed) != 1:
                    raise RuntimeError("both_assets_have_exposure_manual_review_required")
                pair = exposed[0]
                if journal["phase"] != RotationPhase.RECOVERING.value:
                    self._start_target(pair, journal, snapshot, recovery=True)
                    output.update({"action": "restart_management_only", "pair": pair.value})
                elif not self._tmux_session_exists(journal.get("tmux_session")):
                    if int(journal.get("launch_attempts") or 0) >= 2:
                        raise RuntimeError("management_recovery_retries_exhausted_manual_review_required")
                    self._start_target(pair, journal, snapshot, recovery=True)
                    output.update({"action": "retry_management_only_start", "pair": pair.value})
                else:
                    output["action"] = "await_management_recovery_runtime"
                return output
            if journal["phase"] == RotationPhase.DRAINING.value:
                source = Pair(journal["current_pair"])
                source_state = snapshot["states"].get(source.value, {})
                control_path = self.root / ("log/live_inventory_control.json" if source == Pair.ETH else "log/wti_live_control.json")
                control = read_object(control_path)
                ack = verified_drain_completion(
                    control,
                    request_id=str(journal.get("request_id") or ""),
                    source_pid=int(journal.get("source_pid") or -1),
                    source_run_id=str(journal.get("source_run_id") or ""),
                )
                if not ack or not _flat_state(source, source_state):
                    output.update({"action": "wait_for_verified_drain", "source": source.value})
                    return output
                target = Pair(journal["target_pair"])
                if target == Pair.NONE:
                    journal.update({"phase": RotationPhase.STABLE.value, "current_pair": "NONE", "request_id": None})
                    self._save_journal(journal)
                    output["action"] = "drain_complete_no_ready_target"
                    return output
                if target == Pair.WTI and snapshot.get("collector_pids"):
                    output["action"] = "wait_for_standalone_collector_exit"
                    return output
                if snapshot["target_pair"] != target.value:
                    output.update({"action": "target_changed_replan", "previous_target": target.value})
                    journal.update({"phase": RotationPhase.STABLE.value, "current_pair": "NONE", "request_id": None})
                    self._save_journal(journal)
                    return output
                self._start_target(target, journal, snapshot, allow_missing_flat=True)
                output.update({"action": "start_target", "pair": target.value})
                return output
            target = Pair(snapshot["target_pair"])
            if target == Pair.NONE:
                output["action"] = "wait_for_verified_session_and_readiness"
                return output
            if not self.confirm_initial_flat:
                raise RuntimeError("explicit_initial_both_accounts_flat_confirmation_required")
            if not all(_flat_state(
                pair,
                snapshot["states"][pair.value],
                allow_missing=True,
            ) for pair in (Pair.ETH, Pair.WTI)):
                raise RuntimeError("initial_rotation_requires_both_local_states_flat")
            if journal["phase"] == RotationPhase.STARTING.value:
                if int(journal.get("launch_attempts") or 0) >= 2:
                    raise RuntimeError("target_start_retries_exhausted_manual_review_required")
                if self._tmux_session_exists(journal.get("tmux_session")):
                    output["action"] = "await_starting_tmux_session"
                    return output
            if target == Pair.WTI and snapshot.get("collector_pids"):
                output["action"] = "wait_for_standalone_collector_exit"
                return output
            self._start_target(target, journal, snapshot, allow_missing_flat=True)
            output.update({"action": "start_initial_target", "pair": target.value})
            return output

        current = Pair(snapshot["current_pair"])
        runtime = snapshot["runtimes"][current.value]
        if not runtime["matched_healthy"]:
            raise RuntimeError("runtime_identity_or_heartbeat_unverified_do_not_restart")
        if journal["phase"] == RotationPhase.RECOVERING.value:
            health = runtime["health"]
            if health.get("startup_reconciled") is True and health.get("manage_existing_only") is True and health.get("wti_entry_authorized") is False:
                journal.update({"phase": RotationPhase.STABLE.value, "current_pair": current.value, "target_pair": current.value})
                self._save_journal(journal)
                output.update({"action": "management_recovery_adopted", "pair": current.value})
            else:
                output["action"] = "wait_for_strict_management_reconciliation"
            return output
        if journal["phase"] == RotationPhase.DRAINING.value:
            if current.value != journal.get("current_pair") or runtime["run_id"] != journal.get("source_run_id") or runtime["pids"][0] != journal.get("source_pid"):
                raise RuntimeError("draining_runtime_identity_changed_manual_review_required")
            control_path = self.root / ("log/live_inventory_control.json" if current == Pair.ETH else "log/wti_live_control.json")
            control = read_object(control_path)
            if verified_drain_completion(
                control,
                request_id=str(journal.get("request_id") or ""),
                source_pid=int(journal.get("source_pid") or -1),
                source_run_id=str(journal.get("source_run_id") or ""),
            ):
                output["action"] = "wait_for_drained_runtime_exit"
                return output
            if current == Pair.WTI and self._stop_flat_wti_after_readonly_probe(
                runtime, snapshot["states"]["WTI"], journal,
            ):
                output["action"] = "stop_verified_flat_wti_runtime"
                return output
            output.update({"action": "wait_for_drain", "source": current.value, "target": journal.get("target_pair")})
            return output
        if journal["phase"] == RotationPhase.STARTING.value:
            if current.value != journal.get("target_pair"):
                raise RuntimeError("starting_runtime_pair_mismatch_manual_review_required")
            health = runtime["health"]
            if health.get("run_id") == journal.get("target_run_id_before_start"):
                output["action"] = "wait_for_new_runtime_identity"
                return output
            reconciled = health.get("startup_reconciled") is True if current == Pair.WTI else health.get("account_risk_reason") is not None or health.get("risk_reason") is not None
            if not reconciled:
                output["action"] = "wait_for_startup_reconciliation"
                return output
            journal.update({"phase": RotationPhase.STABLE.value, "current_pair": current.value, "target_pair": current.value, "request_id": None, "launch_attempts": 0})
            self._save_journal(journal)
            output.update({"action": "target_runtime_adopted", "pair": current.value})
            return output

        target = Pair(snapshot["target_pair"])
        if target == current:
            journal.update({"phase": RotationPhase.STABLE.value, "current_pair": current.value, "target_pair": current.value})
            self._save_journal(journal)
            collector_action = self._ensure_oil_collector(journal, snapshot) if current == Pair.ETH else "not_required_for_current_pair"
            output.update({"action": "continue_current_pair", "pair": current.value, "collector": collector_action})
            return output
        current_state = snapshot["states"][current.value]
        valid, _exposure, _pending = _state_exposure(current, current_state)
        if not valid:
            raise RuntimeError("current_local_state_invalid_manual_review_required")
        if current == Pair.WTI and not snapshot["session"]["allow_position_management"] and _state_exposure(current, current_state)[1]:
            # The drain request blocks entries; the WTI runtime itself waits for a manageable exchange session.
            pass
        journal.update({"current_pair": current.value, "target_pair": target.value})
        self._write_drain(current, runtime, journal)
        output.update({"action": "request_safe_drain", "source": current.value, "target": target.value})
        return output

    def run(self, *, once: bool, poll_seconds: float) -> int:
        lock_path = self.root / "log/rotation_supervisor.lock"
        try:
            with exclusive_lock(lock_path):
                while True:
                    try:
                        result = self.run_once()
                        journal = validate_journal(read_object(self.journal_path))
                        if journal.get("consecutive_failures"):
                            journal["consecutive_failures"] = 0
                            journal.pop("last_error", None)
                            self._save_journal(journal)
                        self._save_health({"status": "running", "last_result": result, "last_error": None})
                        print(json.dumps(result, ensure_ascii=True, sort_keys=True), flush=True)
                    except Exception as exc:
                        journal = validate_journal(read_object(self.journal_path))
                        failure_count = int(journal.get("consecutive_failures") or 0) + 1
                        error = f"{type(exc).__name__}:{exc}"
                        journal.update({
                            "consecutive_failures": failure_count,
                            "last_error": error,
                        })
                        blocked = failure_count >= MAX_CONSECUTIVE_FAILURES
                        if blocked:
                            journal["phase"] = RotationPhase.BLOCKED.value
                        self._save_journal(journal)
                        self._save_health({
                            "status": "blocked" if blocked else "retrying",
                            "last_error": error,
                            "consecutive_failures": failure_count,
                            "phase": journal["phase"],
                        })
                        print(json.dumps({
                            "action": "blocked" if blocked else "retry_after_error",
                            "reason": error,
                            "consecutive_failures": failure_count,
                            "side_effects": False,
                        }, ensure_ascii=True), flush=True)
                        if once or blocked:
                            return 2
                    if once:
                        return 0
                    time.sleep(poll_seconds)
        except RuntimeError as exc:
            print(json.dumps({"action": "blocked", "reason": str(exc), "side_effects": False}, ensure_ascii=True))
            return 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--rehearse", action="store_true", help="Read-only snapshot and rotation rehearsal (default).")
    mode.add_argument("--execute", action="store_true", help="Run the automatic live rotation supervisor.")
    parser.add_argument("--confirm-automatic-rotation", action="store_true")
    parser.add_argument("--i-understand-live-orders", action="store_true")
    parser.add_argument("--confirm-initial-flat", action="store_true", help="Confirm both exchange accounts and all orders were manually checked flat before first supervised start.")
    parser.add_argument("--lighter-market-id", type=int, required=True)
    parser.add_argument("--lighter-multiplier", type=Decimal, required=True)
    parser.add_argument("--poll-seconds", type=float, default=ROTATION_POLL_SECONDS)
    parser.add_argument("--once", action="store_true", help="Execute one supervisor transition then exit.")
    args = parser.parse_args(argv)
    if args.lighter_market_id < 0 or not args.lighter_multiplier.is_finite() or args.lighter_multiplier <= 0:
        parser.error("Lighter WTI market identity must be positive and explicit")
    if args.poll_seconds < 1 or args.poll_seconds > 60:
        parser.error("poll interval must be between 1 and 60 seconds")
    if args.execute and not (args.confirm_automatic_rotation and args.i_understand_live_orders):
        parser.error("execute requires explicit automatic-rotation and live-order confirmations")
    supervisor = RotationSupervisor(
        root=ROOT,
        market_id=args.lighter_market_id,
        multiplier=args.lighter_multiplier,
        confirm_initial_flat=args.confirm_initial_flat,
        dry_run=not args.execute,
    )
    if not args.execute:
        try:
            snapshot = collect_snapshot(ROOT, market_id=args.lighter_market_id, multiplier=args.lighter_multiplier)
            print(json.dumps({"snapshot": {
                key: value for key, value in snapshot.items()
                if key not in {"states", "runtimes", "collector", "collector_pids"}
            }, "plan": rehearsal_plan(snapshot), "side_effects": False}, ensure_ascii=True, indent=2, sort_keys=True))
            return 0
        except Exception as exc:
            print(json.dumps({"mode": "read_only_rehearsal", "error": f"{type(exc).__name__}:{exc}", "side_effects": False}, ensure_ascii=True))
            return 2
    return supervisor.run(once=args.once, poll_seconds=args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
