#!/usr/bin/env python3
"""Fail-closed ETH/WTI session rotator. Default mode is strictly read-only."""

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
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
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
ACCOUNT_PROOF_MAX_AGE_SECONDS = 20
ROTATION_POLL_SECONDS = 5
MAX_CONSECUTIVE_FAILURES = 3
BOUNDED_TEST_MAX_ROUNDTRIPS = 1
BOUNDED_TEST_MAX_LAYERS = 2
BOUNDED_TEST_LAYER_NOTIONAL_USD = Decimal("10")
BOUNDED_TEST_MAX_NOTIONAL_USD = Decimal("20")
CONTINUOUS_ROTATION_MAX_LAYERS = 2
CONTINUOUS_ROTATION_LAYER_NOTIONAL_USD = Decimal("10")
CONTINUOUS_ROTATION_MAX_NOTIONAL_USD = Decimal("20")


def _age(value: Any, now: datetime) -> float | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return (now - parsed.astimezone(timezone.utc)).total_seconds()
    except (TypeError, ValueError):
        return None


def _wti_account_proof(
    state: dict[str, Any],
    health: dict[str, Any],
    *,
    market_id: int,
    now: datetime,
) -> dict[str, Any]:
    proof = state.get("last_account_reconciliation")
    proof = proof if isinstance(proof, dict) else {}
    reasons: list[str] = []
    age = _age(proof.get("observed_at"), now)
    if health.get("startup_reconciled") is not True:
        reasons.append("wti_startup_reconciliation_not_confirmed")
    if proof.get("verified") is not True:
        reasons.append("wti_account_read_not_verified")
    if proof.get("runtime_pid") != health.get("pid") or not health.get("pid"):
        reasons.append("wti_account_proof_pid_mismatch")
    if proof.get("runtime_run_id") != health.get("run_id") or not health.get("run_id"):
        reasons.append("wti_account_proof_run_id_mismatch")
    if proof.get("market_id") != market_id:
        reasons.append("wti_account_proof_market_id_mismatch")
    if age is None or not -5 <= age <= ACCOUNT_PROOF_MAX_AGE_SECONDS:
        reasons.append("wti_account_proof_stale_or_invalid")
    try:
        var_qty = Decimal(str(proof.get("variational_qty")))
        lighter_qty = Decimal(str(proof.get("lighter_qty")))
        active_count = proof.get("active_order_count")
        quantities_valid = var_qty.is_finite() and lighter_qty.is_finite()
        active_count_valid = (
            isinstance(active_count, int) and not isinstance(active_count, bool)
            and active_count >= 0
        )
    except (ArithmeticError, TypeError, ValueError):
        quantities_valid = False
        active_count_valid = False
        active_count = None
    if not quantities_valid:
        reasons.append("wti_account_quantities_unverified")
    if not active_count_valid:
        reasons.append("wti_active_order_count_unverified")
    try:
        if (
            state.get("asset") == "WTI"
            and state.get("status") == "flat"
            and state.get("open_position") is None
            and state.get("pending_action") is None
        ):
            expected_var_qty = Decimal("0")
            expected_lighter_qty = Decimal("0")
        elif (
            state.get("asset") == "WTI"
            and state.get("status") == "open"
            and state.get("pending_action") is None
            and isinstance(state.get("open_position"), dict)
        ):
            position = state["open_position"]
            expected_var_qty = Decimal(str(position["expected_var_qty"]))
            expected_lighter_qty = Decimal(str(position["expected_lighter_qty"]))
        else:
            expected_var_qty = expected_lighter_qty = None
        local_quantities_valid = (
            expected_var_qty is not None
            and expected_lighter_qty is not None
            and expected_var_qty.is_finite()
            and expected_lighter_qty.is_finite()
        )
    except (ArithmeticError, KeyError, TypeError, ValueError):
        expected_var_qty = expected_lighter_qty = None
        local_quantities_valid = False
    if not local_quantities_valid:
        reasons.append("wti_local_state_not_reconcilable")
    elif quantities_valid and (
        var_qty != expected_var_qty or lighter_qty != expected_lighter_qty
    ):
        reasons.append("wti_local_exchange_position_mismatch")
    active_orders_zero = active_count == 0 if active_count_valid else None
    if active_orders_zero is False:
        reasons.append("wti_active_orders_present")
    return {
        "verified": not any(reason != "wti_active_orders_present" for reason in reasons),
        "active_orders_zero": active_orders_zero,
        "age_seconds": age,
        "market_id": proof.get("market_id"),
        "variational_qty": proof.get("variational_qty"),
        "lighter_qty": proof.get("lighter_qty"),
        "active_order_count": active_count,
        "reasons": reasons,
    }


def _eth_account_proof(health: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    age = _age(health.get("account_snapshot_observed_at"), now)
    usable = health.get("variational_account_snapshot_usable") is True
    fresh = age is not None and -5 <= age <= 60
    reasons = []
    if not usable:
        reasons.append("eth_account_snapshot_unusable")
    if not fresh:
        reasons.append("eth_account_snapshot_stale_or_invalid")
    return {
        "verified": usable and fresh,
        "active_orders_zero": None,
        "age_seconds": age,
        "reasons": reasons,
        "active_orders_checked_on_drain": True,
    }


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
    bounded_rotation_test: bool = False,
    rotation_test_status: str | None = None,
    continuous_gradient_rotation: bool = False,
    continuous_rotation_status: str | None = None,
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
    session = _session_window(sample, now, 1800, feed_health=collector)
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
    production_wti_ready = bool(readiness.get("production_ready") and wti_pilot_available)
    test_status = rotation_test_status
    rotation_auth: dict[str, Any] = {}
    if test_status is None:
        rotation_journal = read_object(root / "log/rotation_supervisor_state.json")
        test_auth = rotation_journal.get("bounded_rotation_test")
        if isinstance(test_auth, dict):
            rotation_auth = test_auth
            test_status = str(test_auth.get("status") or "")
    else:
        rotation_auth = read_object(root / "log/rotation_supervisor_state.json").get(
            "bounded_rotation_test", {}
        )
        rotation_auth = rotation_auth if isinstance(rotation_auth, dict) else {}
    test_budget_available = test_status not in {"completed", "consumed", "blocked"}
    test_authorized = test_status in {"authorized", "active"}
    sample_age = _age(sample.get("logged_at"), now)
    sample_identity_ok = (
        sample.get("sample_quality") == "valid"
        and sample.get("asset") == "WTI"
        and sample_age is not None
        and -5 <= sample_age <= 120
        and str(sample.get("lighter_market_id")) == str(market_id)
    )
    try:
        sample_multiplier_ok = Decimal(str(sample.get("lighter_multiplier"))) == multiplier
    except (ArithmeticError, TypeError, ValueError):
        sample_multiplier_ok = False
    test_gate_reasons: list[str] = []
    if not bounded_rotation_test:
        test_gate_reasons.append("bounded_rotation_test_not_explicitly_selected")
    if not test_budget_available:
        test_gate_reasons.append("bounded_rotation_test_budget_already_consumed")
    if not sample_identity_ok:
        test_gate_reasons.append("bounded_rotation_test_requires_fresh_valid_wti_sample")
    if not sample_multiplier_ok:
        test_gate_reasons.append("bounded_rotation_test_lighter_multiplier_mismatch")
    if not wti_pilot_available:
        test_gate_reasons.append("persisted_gradient_pilot_budget_already_consumed")
    if wti_pilot:
        expected_pilot_config = {
            "entry_threshold_bps": "30",
            "exit_target_bps": "1",
            "roundtrip_cost_bps": "10",
            "max_notional_usd": "20",
            "max_slippage_bps": "8",
            "max_hold_seconds": 21600.0,
            "entry_cutoff_seconds": 1800.0,
            "tier_thresholds_bps": ["30", "45"],
            "layer_notional_usd": "10",
            "max_layers": 2,
            "max_completed_roundtrips": 1,
        }
        existing_config = wti_pilot.get("config") or {}
        identity_matches = (
            wti_pilot.get("market_id") == market_id
            and str(wti_pilot.get("multiplier")) == str(multiplier)
            and wti_pilot.get("max_layers") == BOUNDED_TEST_MAX_LAYERS
            and all(existing_config.get(key) == value for key, value in expected_pilot_config.items())
        )
        if bounded_rotation_test:
            test_id = rotation_auth.get("test_id")
            identity_matches = identity_matches and bool(test_id) and wti_pilot.get("rotation_test_id") == test_id
        if not identity_matches:
            test_gate_reasons.append("persisted_gradient_pilot_configuration_mismatch")
    bounded_test_ready = bounded_rotation_test and not test_gate_reasons
    rotation_journal = read_object(root / "log/rotation_supervisor_state.json")
    continuous_auth = rotation_journal.get("continuous_gradient_rotation") or {}
    if not isinstance(continuous_auth, dict):
        continuous_auth = {}
    if continuous_rotation_status is not None:
        continuous_auth["status"] = continuous_rotation_status
    continuous_authorized = continuous_auth.get("status") in {"authorized", "active"}
    continuous_sample_reasons: list[str] = []
    if not continuous_gradient_rotation:
        continuous_sample_reasons.append("continuous_gradient_rotation_not_explicitly_selected")
    if not continuous_authorized:
        continuous_sample_reasons.append("continuous_gradient_rotation_not_authorized")
    if (
        continuous_auth.get("market_id") != market_id
        or str(continuous_auth.get("multiplier")) != str(multiplier)
        or continuous_auth.get("max_layers") != CONTINUOUS_ROTATION_MAX_LAYERS
        or continuous_auth.get("layer_notional_usd") != str(CONTINUOUS_ROTATION_LAYER_NOTIONAL_USD)
        or continuous_auth.get("max_notional_usd") != str(CONTINUOUS_ROTATION_MAX_NOTIONAL_USD)
        or continuous_auth.get("entry_threshold_bps") != "30"
        or continuous_auth.get("second_tier_bps") != "45"
        or continuous_auth.get("exit_target_bps") != "1"
        or continuous_auth.get("roundtrip_cost_bps") != "10"
        or continuous_auth.get("max_slippage_bps") != "8"
        or continuous_auth.get("max_hold_seconds") != 21600
        or continuous_auth.get("entry_cutoff_seconds") != 1800
    ):
        continuous_sample_reasons.append("continuous_gradient_rotation_configuration_mismatch")
    if not sample_identity_ok:
        continuous_sample_reasons.append("continuous_gradient_rotation_requires_fresh_valid_wti_sample")
    if not sample_multiplier_ok:
        continuous_sample_reasons.append("continuous_gradient_rotation_lighter_multiplier_mismatch")
    continuous_id = continuous_auth.get("session_id")
    continuous_expected_config = {
        "entry_threshold_bps": "30",
        "exit_target_bps": "1",
        "roundtrip_cost_bps": "10",
        "max_notional_usd": "20",
        "max_slippage_bps": "8",
        "max_hold_seconds": 21600.0,
        "entry_cutoff_seconds": 1800.0,
        "tier_thresholds_bps": ["30", "45"],
        "layer_notional_usd": "10",
        "max_layers": 2,
        "max_completed_roundtrips": 1,
    }
    pilot_config = wti_pilot.get("config") or {}
    pilot_config_matches = (
        wti_pilot.get("market_id") == market_id
        and str(wti_pilot.get("multiplier")) == str(multiplier)
        and wti_pilot.get("max_layers") == CONTINUOUS_ROTATION_MAX_LAYERS
        and all(pilot_config.get(key) == value for key, value in continuous_expected_config.items())
    ) if wti_pilot else True
    pilot_current_session = bool(
        wti_pilot
        and wti_pilot.get("rotation_test_id") == continuous_id
        and wti_pilot.get("continuous_rotation") is True
        and isinstance(wti_pilot.get("round_number"), int)
        and wti_pilot.get("round_number") >= 1
    )
    flat_resolved_wti = bool(
        wti_state.get("status") == "flat"
        and wti_state.get("open_position") is None
        and wti_state.get("pending_action") is None
    )
    completed_predecessor = bool(
        wti_pilot
        and wti_pilot.get("status") == "completed"
        and wti_pilot.get("completed_roundtrips") == 1
        and flat_resolved_wti
    )
    if wti_pilot and (not pilot_config_matches or not (pilot_current_session or completed_predecessor)):
        continuous_sample_reasons.append("continuous_gradient_pilot_identity_mismatch")
    continuous_pilot_eligible = bool(
        wti_pilot_available or (continuous_authorized and (not wti_pilot or completed_predecessor))
    )
    if not continuous_pilot_eligible:
        continuous_sample_reasons.append("continuous_gradient_pilot_not_rearmable")
    continuous_rotation_ready = continuous_gradient_rotation and not continuous_sample_reasons
    wti_ready = bool(
        (wti_pilot_available or continuous_pilot_eligible)
        and (
            production_wti_ready
            or (bounded_test_ready and session.allow_new_entries)
            or (continuous_rotation_ready and session.allow_new_entries)
        )
    )
    wti_gate_reasons = list(readiness.get("reasons", []))
    if not wti_pilot_available:
        wti_gate_reasons.append("persisted_gradient_pilot_budget_already_consumed")
    if bounded_rotation_test:
        wti_gate_reasons.extend(test_gate_reasons)
        if bounded_test_ready and not session.allow_new_entries:
            wti_gate_reasons.append("bounded_rotation_test_requires_verified_open_entry_window")
    if continuous_gradient_rotation:
        wti_gate_reasons.extend(continuous_sample_reasons)
        if continuous_rotation_ready and not session.allow_new_entries:
            wti_gate_reasons.append("continuous_gradient_rotation_requires_verified_open_entry_window")
    target, target_reason = desired_pair(
        session=session,
        wti_ready=wti_ready,
        eth_ready=eth_ready,
        current=current,
    )
    return {
        "observed_at": now.isoformat(),
        "lighter_market_id": market_id,
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
        "bounded_rotation_test": bounded_rotation_test,
        "rotation_test_status": test_status,
        "bounded_test_authorized": test_authorized,
        "wti_production_gate_ready": production_wti_ready,
        "wti_bounded_test_gate_ready": bounded_test_ready,
        "wti_bounded_test_gate_reasons": list(dict.fromkeys(test_gate_reasons)),
        "rotation_test_id": rotation_auth.get("test_id"),
        "continuous_gradient_rotation": continuous_gradient_rotation,
        "continuous_rotation_status": continuous_auth.get("status"),
        "continuous_rotation_session_id": continuous_id,
        "continuous_rotation_rounds_completed": continuous_auth.get("rounds_completed"),
        "continuous_gradient_gate_ready": continuous_rotation_ready,
        "continuous_gradient_gate_reasons": list(dict.fromkeys(continuous_sample_reasons)),
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


def rehearsal_plan(snapshot: dict[str, Any], *, session_scenario: str = "live") -> dict[str, Any]:
    current = Pair(snapshot["current_pair"])
    target = Pair(snapshot["target_pair"])
    state = snapshot["states"].get(current.value, {}) if current != Pair.NONE else {}
    valid, exposure, pending = _state_exposure(current, state) if current != Pair.NONE else (True, False, False)
    runtime = snapshot["runtimes"].get(current.value, {}) if current != Pair.NONE else {}
    session_data = snapshot.get("session", {})
    session_state = session_data.get("state")
    from tools.lib.market_sessions import SessionWindow

    if session_scenario not in {"live", "open", "closed"}:
        raise ValueError("session_scenario_invalid")
    target_reason = snapshot.get("target_reason")
    if session_scenario != "live":
        now = datetime.now(timezone.utc)
        if session_scenario == "open":
            session_state = OilSession.OPEN.value
            allow_entries = bool(
                snapshot.get("wti_production_gate_ready")
                or snapshot.get("wti_bounded_test_gate_ready")
            )
            closing_at = (now.replace(microsecond=0) + timedelta(hours=2)).isoformat()
            session = SessionWindow(
                OilSession.OPEN, "simulated", None, allow_entries, True,
                "read_only_simulated_open_session", now.isoformat(),
                exchange_open=True, closing_at=closing_at,
            )
        else:
            session_state = OilSession.EXCHANGE_CLOSED.value
            session = SessionWindow(
                OilSession.EXCHANGE_CLOSED, "simulated", None, False, False,
                "read_only_simulated_closed_session", now.isoformat(),
                exchange_open=False,
            )
        target, target_reason = desired_pair(
            session=session,
            wti_ready=bool(
                snapshot.get("wti_production_gate_ready")
                or snapshot.get("wti_bounded_test_gate_ready")
            ),
            eth_ready=bool(snapshot.get("eth_ready")),
            current=current,
        )
    else:
        session = SessionWindow(
            OilSession(session_state or OilSession.STATUS_UNKNOWN.value),
            "rehearsal", None,
            bool(session_data.get("allow_new_entries")),
            bool(session_data.get("allow_position_management")),
            str(session_data.get("reason") or "session_unknown"),
            session_data.get("observed_at"),
            exchange_open=(True if session_state in {"open", "entry_cutoff"} else False if session_state == "exchange_closed" else None),
        )
    if current == Pair.WTI:
        account_proof = _wti_account_proof(
            state, runtime.get("health", {}),
            market_id=int(snapshot.get("lighter_market_id") or 145),
            now=datetime.now(timezone.utc),
        )
        active_orders_zero = account_proof.get("active_orders_zero")
        reconciled = account_proof.get("verified") is True
    elif current == Pair.ETH:
        account_proof = _eth_account_proof(runtime.get("health", {}), now=datetime.now(timezone.utc))
        active_orders_zero = None
        reconciled = account_proof.get("verified") is True
    else:
        account_proof = {"verified": False, "active_orders_zero": None, "reasons": ["initial_account_probe_required"]}
        active_orders_zero = None
        reconciled = None
    target_ready = bool(
        snapshot.get("wti_ready") if target == Pair.WTI
        else snapshot.get("eth_ready") if target == Pair.ETH else False
    )
    if session_scenario == "open" and target == Pair.WTI:
        target_ready = bool(
            snapshot.get("wti_production_gate_ready")
            or (
                snapshot.get("bounded_rotation_test")
                and snapshot.get("wti_bounded_test_gate_ready")
                and session.allow_new_entries
            )
            or (
                snapshot.get("continuous_gradient_rotation")
                and snapshot.get("continuous_gradient_gate_ready")
                and session.allow_new_entries
            )
        )
    decision = decide_rotation(
        current=current,
        target=target,
        target_ready=target_ready,
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
    if target == Pair.WTI and not target_ready:
        reasons.extend(snapshot.get("wti_gate_reasons", snapshot.get("readiness", {}).get("reasons", [])))
    if (
        target == Pair.WTI
        and snapshot.get("bounded_rotation_test")
        and not snapshot.get("bounded_test_authorized")
    ):
        reasons.append("bounded_rotation_test_authorization_not_persisted")
    if target == Pair.WTI and snapshot.get("continuous_gradient_rotation") and not snapshot.get(
        "continuous_gradient_gate_ready"
    ):
        reasons.extend(snapshot.get("continuous_gradient_gate_reasons", []))
    reasons.extend(account_proof.get("reasons", []))
    return {
        "mode": "read_only_rehearsal",
        "current_pair": current.value,
        "target_pair": target.value,
        "target_reason": target_reason,
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
        "account_proof": account_proof,
        "bounded_rotation_test": bool(snapshot.get("bounded_rotation_test")),
        "bounded_test_ready": bool(snapshot.get("wti_bounded_test_gate_ready")),
        "continuous_gradient_rotation": bool(snapshot.get("continuous_gradient_rotation")),
        "continuous_gradient_ready": bool(snapshot.get("continuous_gradient_gate_ready")),
        "session_scenario": session_scenario,
    }


class RotationSupervisor:
    def __init__(self, *, root: Path, market_id: int, multiplier: Decimal,
                 confirm_initial_flat: bool, dry_run: bool = False,
                 bounded_rotation_test: bool = False,
                 continuous_gradient_rotation: bool = False) -> None:
        self.root = root
        self.market_id = market_id
        self.multiplier = multiplier
        self.confirm_initial_flat = confirm_initial_flat
        self.dry_run = dry_run
        self.bounded_rotation_test = bounded_rotation_test
        self.continuous_gradient_rotation = continuous_gradient_rotation
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

    def _start_command(
        self,
        pair: Pair,
        *,
        recovery: bool = False,
        bounded_test: bool = False,
        rotation_test_id: str | None = None,
        continuous_rotation: bool = False,
        continuous_rotation_id: str | None = None,
    ) -> list[str]:
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
                if bounded_test:
                    if not rotation_test_id or not re.fullmatch(r"[0-9a-f]{32}", rotation_test_id):
                        raise RuntimeError("bounded_rotation_test_id_missing_or_invalid")
                    command.extend(["--allow-unqualified-data-pilot", "--rotation-test-id", rotation_test_id])
                if continuous_rotation:
                    if not continuous_rotation_id or not re.fullmatch(r"[0-9a-f]{32}", continuous_rotation_id):
                        raise RuntimeError("continuous_gradient_rotation_id_missing_or_invalid")
                    command.extend([
                        "--allow-unqualified-data-pilot",
                        "--continuous-gradient-rotation",
                        "--confirm-continuous-gradient-rotation",
                        "--rotation-test-id", continuous_rotation_id,
                    ])
            return command
        raise RuntimeError("rotation_target_invalid")

    def _launch(
        self,
        pair: Pair,
        *,
        recovery: bool = False,
        session_name: str,
        bounded_test: bool = False,
        rotation_test_id: str | None = None,
        continuous_rotation: bool = False,
        continuous_rotation_id: str | None = None,
    ) -> str:
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
        command = self._start_command(
            pair, recovery=recovery, bounded_test=bounded_test,
            rotation_test_id=rotation_test_id,
            continuous_rotation=continuous_rotation,
            continuous_rotation_id=continuous_rotation_id,
        )
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
            if self.bounded_rotation_test:
                test = journal.get("bounded_rotation_test") or {}
                if test.get("status") not in {"authorized", "active"}:
                    raise RuntimeError("bounded_rotation_test_authorization_unavailable")
                if not snapshot.get("wti_bounded_test_gate_ready") or not snapshot.get("session", {}).get("allow_new_entries"):
                    raise RuntimeError("bounded_rotation_test_current_market_gate_not_met")
            elif self.continuous_gradient_rotation:
                session = journal.get("continuous_gradient_rotation") or {}
                if session.get("status") not in {"authorized", "active"}:
                    raise RuntimeError("continuous_gradient_rotation_authorization_unavailable")
                if not snapshot.get("continuous_gradient_gate_ready") or not snapshot.get("session", {}).get("allow_new_entries"):
                    raise RuntimeError("continuous_gradient_rotation_current_market_gate_not_met")
            elif not snapshot["readiness"].get("production_ready"):
                raise RuntimeError("wti_production_readiness_not_met")
            pilot = state.get("gradient_pilot") or {}
            if pilot.get("status") == "exhausted" or (
                pilot.get("status") == "completed" and not self.continuous_gradient_rotation
            ):
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
            if self.bounded_rotation_test and pair == Pair.WTI and not recovery:
                test = journal.get("bounded_rotation_test") or {}
                self._launch(
                    pair, recovery=recovery, session_name=session_name,
                    bounded_test=True, rotation_test_id=str(test.get("test_id") or ""),
                )
            elif self.continuous_gradient_rotation and pair == Pair.WTI and not recovery:
                session = journal.get("continuous_gradient_rotation") or {}
                self._launch(
                    pair, recovery=False, session_name=session_name,
                    continuous_rotation=True,
                    continuous_rotation_id=str(session.get("session_id") or ""),
                )
            else:
                self._launch(pair, recovery=recovery, session_name=session_name)

    def _prepare_bounded_rotation_test(self, journal: dict[str, Any]) -> dict[str, Any] | None:
        if not self.bounded_rotation_test:
            return None
        test = journal.get("bounded_rotation_test")
        if test is None:
            test = {
                "schema_version": 1,
                "test_id": uuid.uuid4().hex,
                "status": "authorized",
                "market_id": self.market_id,
                "multiplier": str(self.multiplier),
                "max_completed_roundtrips": BOUNDED_TEST_MAX_ROUNDTRIPS,
                "max_layers": BOUNDED_TEST_MAX_LAYERS,
                "layer_notional_usd": str(BOUNDED_TEST_LAYER_NOTIONAL_USD),
                "max_notional_usd": str(BOUNDED_TEST_MAX_NOTIONAL_USD),
                "entry_threshold_bps": "30",
                "second_tier_bps": "45",
                "exit_target_bps": "1",
                "roundtrip_cost_bps": "10",
                "max_slippage_bps": "8",
                "max_hold_seconds": 21600,
                "entry_cutoff_seconds": 1800,
                "authorized_at": datetime.now(timezone.utc).isoformat(),
                "pilot_id": None,
            }
            journal["bounded_rotation_test"] = test
            self._save_journal(journal)
        if (
            not isinstance(test, dict)
            or test.get("schema_version") != 1
            or not isinstance(test.get("test_id"), str)
            or not re.fullmatch(r"[0-9a-f]{32}", test.get("test_id", ""))
            or test.get("max_completed_roundtrips") != BOUNDED_TEST_MAX_ROUNDTRIPS
            or test.get("max_layers") != BOUNDED_TEST_MAX_LAYERS
            or test.get("layer_notional_usd") != str(BOUNDED_TEST_LAYER_NOTIONAL_USD)
            or test.get("max_notional_usd") != str(BOUNDED_TEST_MAX_NOTIONAL_USD)
            or test.get("market_id") != self.market_id
            or test.get("multiplier") != str(self.multiplier)
            or test.get("entry_threshold_bps") != "30"
            or test.get("second_tier_bps") != "45"
            or test.get("exit_target_bps") != "1"
            or test.get("roundtrip_cost_bps") != "10"
            or test.get("max_slippage_bps") != "8"
            or test.get("max_hold_seconds") != 21600
            or test.get("entry_cutoff_seconds") != 1800
        ):
            raise RuntimeError("bounded_rotation_test_journal_invalid")
        state = read_object(self.root / "log/wti_live_state.json")
        pilot = state.get("gradient_pilot")
        if isinstance(pilot, dict):
            if pilot.get("rotation_test_id") != test["test_id"]:
                raise RuntimeError("bounded_rotation_test_pilot_identity_mismatch_manual_review_required")
            if test.get("pilot_id") not in {None, pilot.get("pilot_id")}:
                raise RuntimeError("bounded_rotation_test_pilot_id_changed_manual_review_required")
            test["pilot_id"] = pilot.get("pilot_id")
            if pilot.get("status") in {"completed", "exhausted"}:
                test.update({
                    "status": "completed",
                    "completed_at": pilot.get("completed_at") or datetime.now(timezone.utc).isoformat(),
                    "completed_roundtrips": pilot.get("completed_roundtrips"),
                })
            else:
                test["status"] = "active"
            self._save_journal(journal)
        elif test.get("pilot_id") and journal.get("phase") != RotationPhase.STARTING.value:
            raise RuntimeError("bounded_rotation_test_pilot_state_missing_manual_review_required")
        if test.get("status") == "completed":
            return test
        if test.get("status") not in {"authorized", "active"}:
            raise RuntimeError("bounded_rotation_test_not_reusable")
        return test

    def _prepare_continuous_gradient_rotation(
        self, journal: dict[str, Any]
    ) -> dict[str, Any] | None:
        if not self.continuous_gradient_rotation:
            return None
        session = journal.get("continuous_gradient_rotation")
        if session is None:
            session = {
                "schema_version": 1,
                "session_id": uuid.uuid4().hex,
                "status": "authorized",
                "market_id": self.market_id,
                "multiplier": str(self.multiplier),
                "entry_threshold_bps": "30",
                "second_tier_bps": "45",
                "exit_target_bps": "1",
                "roundtrip_cost_bps": "10",
                "layer_notional_usd": str(CONTINUOUS_ROTATION_LAYER_NOTIONAL_USD),
                "max_layers": CONTINUOUS_ROTATION_MAX_LAYERS,
                "max_notional_usd": str(CONTINUOUS_ROTATION_MAX_NOTIONAL_USD),
                "max_slippage_bps": "8",
                "max_hold_seconds": 21600,
                "entry_cutoff_seconds": 1800,
                "max_rounds": None,
                "rounds_completed": 0,
                "rounds_started": 0,
                "authorized_at": datetime.now(timezone.utc).isoformat(),
                "pilot_id": None,
            }
            journal["continuous_gradient_rotation"] = session
            self._save_journal(journal)
        expected = {
            "schema_version": 1,
            "market_id": self.market_id,
            "multiplier": str(self.multiplier),
            "entry_threshold_bps": "30",
            "second_tier_bps": "45",
            "exit_target_bps": "1",
            "roundtrip_cost_bps": "10",
            "layer_notional_usd": str(CONTINUOUS_ROTATION_LAYER_NOTIONAL_USD),
            "max_layers": CONTINUOUS_ROTATION_MAX_LAYERS,
            "max_notional_usd": str(CONTINUOUS_ROTATION_MAX_NOTIONAL_USD),
            "max_slippage_bps": "8",
            "max_hold_seconds": 21600,
            "entry_cutoff_seconds": 1800,
            "max_rounds": None,
        }
        if (
            not isinstance(session, dict)
            or not isinstance(session.get("session_id"), str)
            or not re.fullmatch(r"[0-9a-f]{32}", session.get("session_id", ""))
            or any(session.get(key) != value for key, value in expected.items())
            or session.get("status") not in {"authorized", "active"}
        ):
            raise RuntimeError("continuous_gradient_rotation_journal_invalid")
        state = read_object(self.root / "log/wti_live_state.json")
        state_session = state.get("gradient_rotation_session") or {}
        pilot = state.get("gradient_pilot")
        if state_session:
            if (
                state_session.get("session_id") != session["session_id"]
                or state_session.get("market_id") != self.market_id
                or str(state_session.get("multiplier")) != str(self.multiplier)
                or state_session.get("status") != "active"
            ):
                raise RuntimeError("continuous_gradient_state_identity_mismatch_manual_review_required")
            session["rounds_completed"] = state_session.get("rounds_completed", 0)
            session["rounds_started"] = state_session.get("rounds_started", 0)
        if isinstance(pilot, dict):
            same_session = (
                pilot.get("rotation_test_id") == session["session_id"]
                and pilot.get("continuous_rotation") is True
            )
            if same_session:
                if pilot.get("status") == "exhausted":
                    raise RuntimeError("continuous_gradient_round_exhausted_manual_review_required")
                session["pilot_id"] = pilot.get("pilot_id")
                session["status"] = "active"
            elif pilot.get("status") != "completed":
                raise RuntimeError("existing_gradient_pilot_not_terminal_manual_review_required")
        journal["continuous_gradient_rotation"] = session
        self._save_journal(journal)
        return session

    def run_once(self) -> dict[str, Any]:
        journal = validate_journal(read_object(self.journal_path))
        if journal["phase"] == RotationPhase.BLOCKED.value:
            raise RuntimeError("rotation_journal_blocked_manual_review_required")
        test_authorization = self._prepare_bounded_rotation_test(journal)
        continuous_authorization = self._prepare_continuous_gradient_rotation(journal)
        snapshot = collect_snapshot(
            self.root,
            market_id=self.market_id,
            multiplier=self.multiplier,
            bounded_rotation_test=self.bounded_rotation_test,
            rotation_test_status=(
                None if test_authorization is None else str(test_authorization["status"])
            ),
            continuous_gradient_rotation=self.continuous_gradient_rotation,
            continuous_rotation_status=(
                None if continuous_authorization is None
                else str(continuous_authorization["status"])
            ),
        )
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
            if (
                self.continuous_gradient_rotation
                and current == Pair.WTI
                and snapshot.get("continuous_gradient_gate_ready") is True
                and snapshot.get("session", {}).get("allow_new_entries") is True
            ):
                wti_state = snapshot["states"]["WTI"]
                health = runtime["health"]
                pilot = wti_state.get("gradient_pilot")
                session_id = (journal.get("continuous_gradient_rotation") or {}).get("session_id")
                current_round = (
                    isinstance(pilot, dict)
                    and pilot.get("rotation_test_id") == session_id
                    and pilot.get("continuous_rotation") is True
                )
                active_round = current_round and pilot.get("status") in {
                    "available", "entry_pending", "in_position",
                }
                if active_round:
                    journal.update({
                        "phase": RotationPhase.STABLE.value,
                        "current_pair": current.value,
                        "target_pair": current.value,
                    })
                    self._save_journal(journal)
                    output.update({
                        "action": "continue_continuous_gradient_rotation",
                        "pair": current.value,
                        "round_number": pilot.get("round_number"),
                    })
                    return output
                if wti_state.get("pending_action") is not None:
                    output.update({"action": "hold_current_wti_pending_reconciliation"})
                    return output
                pilot_matches = pilot is None or (
                    isinstance(pilot, dict)
                    and pilot.get("status") == "completed"
                    and pilot.get("completed_roundtrips") == 1
                    and _flat_state(Pair.WTI, wti_state)
                )
                management_only = (
                    health.get("manage_existing_only") is True
                    and health.get("wti_entry_authorized") is False
                )
                if management_only and not _flat_state(Pair.WTI, wti_state):
                    output.update({"action": "continue_management_only_until_flat", "pair": current.value})
                    return output
                proof = _wti_account_proof(
                    wti_state, health,
                    market_id=self.market_id, now=datetime.now(timezone.utc),
                )
                if (
                    proof.get("verified")
                    and proof.get("active_orders_zero") is True
                    and pilot_matches
                    and _flat_state(Pair.WTI, wti_state)
                ):
                    journal.update({
                        "current_pair": current.value,
                        "target_pair": current.value,
                    })
                    self._write_drain(current, runtime, journal)
                    output.update({
                        "action": "request_continuous_rotation_runtime_start",
                        "source": current.value,
                        "target": current.value,
                    })
                    return output
                output.update({"action": "hold_current_wti_account_or_round_proof_unverified"})
                return output
            if (
                self.bounded_rotation_test
                and current == Pair.WTI
                and snapshot.get("wti_bounded_test_gate_ready") is True
                and snapshot.get("session", {}).get("allow_new_entries") is True
            ):
                wti_state = snapshot["states"]["WTI"]
                health = runtime["health"]
                pilot = wti_state.get("gradient_pilot")
                test_id = (journal.get("bounded_rotation_test") or {}).get("test_id")
                already_bound = (
                    isinstance(pilot, dict)
                    and pilot.get("rotation_test_id") == test_id
                    and pilot.get("status") in {"available", "entry_pending", "in_position"}
                )
                if already_bound:
                    journal.update({"phase": RotationPhase.STABLE.value, "current_pair": current.value, "target_pair": current.value})
                    self._save_journal(journal)
                    output.update({"action": "continue_bounded_test_runtime", "pair": current.value})
                    return output
                if wti_state.get("pending_action") is not None:
                    output.update({"action": "hold_current_wti_pending_reconciliation"})
                    return output
                proof = _wti_account_proof(
                    wti_state, health,
                    market_id=self.market_id, now=datetime.now(timezone.utc),
                )
                pilot_matches = pilot is None or (
                    isinstance(pilot, dict)
                    and pilot.get("status") == "available"
                    and pilot.get("rotation_test_id") == test_id
                )
                management_only = (
                    health.get("manage_existing_only") is True
                    and health.get("wti_entry_authorized") is False
                )
                if management_only and not _flat_state(Pair.WTI, wti_state):
                    output.update({"action": "continue_management_only_until_flat", "pair": current.value})
                    return output
                if (
                    proof.get("verified")
                    and proof.get("active_orders_zero") is True
                    and pilot_matches
                    and (not management_only or _flat_state(Pair.WTI, wti_state))
                ):
                    journal.update({"current_pair": current.value, "target_pair": current.value})
                    self._write_drain(current, runtime, journal)
                    output.update({
                        "action": "request_bounded_test_runtime_upgrade" if management_only else "request_safe_drain_for_bounded_test",
                        "source": current.value,
                        "target": current.value,
                    })
                    return output
                output.update({"action": "hold_current_wti_account_or_pilot_proof_unverified"})
                return output
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
    parser.add_argument("--bounded-rotation-test", action="store_true", help="Limit the WTI side to one persisted two-layer round trip.")
    parser.add_argument("--confirm-bounded-rotation-test", action="store_true")
    parser.add_argument("--continuous-gradient-rotation", action="store_true", help="Keep authorizing sequential two-layer WTI rounds under one persistent rotation session.")
    parser.add_argument("--confirm-continuous-gradient-rotation", action="store_true")
    parser.add_argument("--rehearse-session", choices=("live", "open", "closed"), default="live", help="Read-only session scenario; open/closed do not change exchange state.")
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
    if args.execute and args.bounded_rotation_test and not args.confirm_bounded_rotation_test:
        parser.error("bounded rotation test execution requires --confirm-bounded-rotation-test")
    if args.confirm_bounded_rotation_test and not args.bounded_rotation_test:
        parser.error("bounded-test confirmation requires --bounded-rotation-test")
    if args.bounded_rotation_test and args.continuous_gradient_rotation:
        parser.error("bounded and continuous gradient rotation modes are mutually exclusive")
    if args.execute and args.continuous_gradient_rotation and not args.confirm_continuous_gradient_rotation:
        parser.error("continuous gradient rotation requires --confirm-continuous-gradient-rotation")
    if args.confirm_continuous_gradient_rotation and not args.continuous_gradient_rotation:
        parser.error("continuous-rotation confirmation requires --continuous-gradient-rotation")
    if args.execute and args.rehearse_session != "live":
        parser.error("simulated session scenarios are read-only")
    supervisor = RotationSupervisor(
        root=ROOT,
        market_id=args.lighter_market_id,
        multiplier=args.lighter_multiplier,
        confirm_initial_flat=args.confirm_initial_flat,
        dry_run=not args.execute,
        bounded_rotation_test=args.bounded_rotation_test,
        continuous_gradient_rotation=args.continuous_gradient_rotation,
    )
    if not args.execute:
        try:
            snapshot = collect_snapshot(
                ROOT,
                market_id=args.lighter_market_id,
                multiplier=args.lighter_multiplier,
                bounded_rotation_test=args.bounded_rotation_test,
                continuous_gradient_rotation=args.continuous_gradient_rotation,
            )
            print(json.dumps({"snapshot": {
                key: value for key, value in snapshot.items()
                if key not in {"states", "runtimes", "collector", "collector_pids"}
            }, "plan": rehearsal_plan(snapshot, session_scenario=args.rehearse_session), "side_effects": False}, ensure_ascii=True, indent=2, sort_keys=True))
            return 0
        except Exception as exc:
            print(json.dumps({"mode": "read_only_rehearsal", "error": f"{type(exc).__name__}:{exc}", "side_effects": False}, ensure_ascii=True))
            return 2
    return supervisor.run(once=args.once, poll_seconds=args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
