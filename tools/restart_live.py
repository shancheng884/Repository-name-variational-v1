#!/usr/bin/env python3
"""Restart the live strategy with open positions, with checks at every step.

The runtime performs its own strict two-venue reconciliation on resume; this
script refuses to declare success unless that verdict matches the local state.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.restart_guards import (  # noqa: E402
    assess_health,
    assess_state,
    build_launch_command,
    classify_reconcile,
    find_startup_reconcile,
    tail_events,
)

FORWARDER_PORTS = (8766, 8767, 8768)


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def find_pids(pattern: str) -> list[int]:
    result = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, check=False)
    return [int(item) for item in result.stdout.split() if item.isdigit()]


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), 1.0):
            return True
    except OSError:
        return False


def health_age_seconds(health: dict) -> float | None:
    stamp = health.get("updated_at")
    if not stamp:
        return None
    try:
        parsed = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (datetime.now(timezone.utc) - parsed).total_seconds()


def emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="actually stop and restart the live process; without it only the pre-flight runs",
    )
    parser.add_argument("--max-health-age", type=float, default=90.0)
    parser.add_argument("--stop-timeout", type=float, default=30.0)
    parser.add_argument("--wait-seconds", type=float, default=120.0)
    args = parser.parse_args()

    asset = args.asset.upper()
    state_path = ROOT / "log" / "live_inventory_state.json"
    health_path = ROOT / "log" / "live_inventory_risk_health.json"
    metrics_path = ROOT / "log" / "order_metrics.jsonl"

    state = read_json(state_path)
    health = read_json(health_path)
    state_check = assess_state(state, asset=asset)
    health_check = assess_health(
        health, now=datetime.now(timezone.utc), max_age_seconds=args.max_health_age
    )
    main_pids = find_pids("^" + str(ROOT) + "/\\.venv/bin/python main\\.py ")

    reasons = list(state_check["reasons"]) + list(health_check["reasons"])
    if len(main_pids) != 1:
        reasons.append("main_process_count:" + str(len(main_pids)))

    preflight = {
        "ok": not reasons,
        "stage": "preflight",
        "reasons": reasons,
        "asset": asset,
        "state": state_check,
        "health": health_check,
        "pids": main_pids,
    }
    if reasons:
        emit(preflight)
        return 2

    resume = state_check["status"] == "open"
    stamp = datetime.now(timezone.utc).strftime("%m%d%H%M%S")
    session = "mainnet-" + asset.lower() + "-" + stamp
    log_path = ROOT / "log" / (session + ".log")
    command = build_launch_command(
        repo=ROOT, asset=asset, resume=resume, session=session, log_path=log_path
    )

    if not args.execute:
        emit({**preflight, "stage": "preview", "resume": resume, "session": session, "command": command})
        return 0

    old_run_id = health_check.get("run_id")
    dead_pid = main_pids[0]
    os.kill(dead_pid, signal.SIGTERM)
    deadline = time.time() + args.stop_timeout
    while time.time() < deadline:
        if not Path("/proc/" + str(dead_pid)).exists():
            break
        time.sleep(1)
    if Path("/proc/" + str(dead_pid)).exists():
        emit({**preflight, "stage": "stop", "ok": False, "reasons": ["main_process_did_not_exit"]})
        return 3

    for pid in find_pids("\\.venv/bin/python tools/live\\.py "):
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass

    state_after = read_json(state_path)
    after_check = assess_state(state_after, asset=asset)
    drift = []
    if after_check["signature"] != state_check["signature"]:
        drift.append("lot_signature_changed")
    if after_check["status"] != state_check["status"]:
        drift.append("status_changed:" + str(state_check["status"]) + "->" + str(after_check["status"]))
    if drift:
        emit({**preflight, "stage": "post_stop", "ok": False, "reasons": drift, "state": after_check})
        return 4

    busy = [port for port in FORWARDER_PORTS if port_open(port)]
    if busy:
        emit({**preflight, "stage": "post_stop", "ok": False, "reasons": ["forwarder_port_busy:" + str(busy)]})
        return 5

    subprocess.run(["tmux", "new-session", "-d", "-s", session, command], check=False)

    new_run_id = None
    deadline = time.time() + args.wait_seconds
    while time.time() < deadline:
        current = read_json(health_path)
        candidate = current.get("run_id")
        age = health_age_seconds(current)
        if candidate and candidate != old_run_id and age is not None and abs(age) <= 30:
            new_run_id = candidate
            break
        time.sleep(2)

    if not new_run_id:
        emit(
            {
                **preflight,
                "stage": "start",
                "ok": False,
                "reasons": ["new_run_health_missing"],
                "session": session,
                "log": str(log_path),
            }
        )
        return 6

    event = find_startup_reconcile(tail_events(metrics_path), run_id=new_run_id)
    reconcile = classify_reconcile(event, expected=state_check["expected_qty"])
    positions_ok = bool(reconcile.get("positions", {}).get("ok"))
    ok = bool(reconcile["ok"]) and positions_ok

    emit(
        {
            **preflight,
            "stage": "running",
            "ok": ok,
            "reasons": [] if ok else reconcile["reasons"] + reconcile.get("positions", {}).get("reasons", []),
            "resume": resume,
            "session": session,
            "log": str(log_path),
            "previous_run_id": old_run_id,
            "run_id": new_run_id,
            "reconcile": reconcile,
        }
    )
    return 0 if ok else 7


if __name__ == "__main__":
    raise SystemExit(main())
