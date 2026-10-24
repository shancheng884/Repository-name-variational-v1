import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import rotation_supervisor as rotation
from tools import wti_live
from tools.lib.pair_rotation import Pair
from tools.lib.rotation_supervisor import initial_journal, verified_drain_completion, write_object_atomic


def _closed_feed(now):
    return {
        "market_status": "closed", "market_status_feed_connected": True,
        "market_status_feed_heartbeat_at": now.isoformat(),
    }


@pytest.mark.parametrize("fault", [None, "open", "stale", "disconnected"])
def test_handoff_obtains_closed_status_without_the_old_runtime_or_forwarder(monkeypatch, tmp_path, fault):
    now = datetime.now(timezone.utc)
    (tmp_path / "live_config.json").write_text("{}")
    feed = _closed_feed(now)
    if fault == "open":
        feed["market_status"] = "open"
    if fault == "stale":
        feed["market_status_feed_heartbeat_at"] = (now - timedelta(seconds=31)).isoformat()
    if fault == "disconnected":
        feed["market_status_feed_connected"] = False
    monkeypatch.setattr(rotation, "probe_exchange_session_status", lambda: feed)
    monkeypatch.setattr(rotation, "script_processes", lambda *a, **kw: [])
    monkeypatch.setattr(rotation, "read_basis_samples", lambda *a, **kw: [])
    monkeypatch.setattr(rotation, "readiness_for_new_entries", lambda *a, **kw: {"production_ready": False})

    snapshot = rotation.collect_snapshot(tmp_path, now=now, probe_exchange_session=True)

    assert snapshot["session_probe"] == feed
    assert snapshot["session"]["state"] == ("exchange_closed" if fault is None else "status_unknown")
    assert snapshot["target_pair"] == ("ETH" if fault is None else "NONE")
    assert snapshot["session"]["allow_new_entries"] is False


def test_readonly_snapshot_does_not_probe_unless_requested(monkeypatch, tmp_path):
    monkeypatch.setattr(rotation, "probe_exchange_session_status", lambda: pytest.fail("unexpected network read"))
    monkeypatch.setattr(rotation, "script_processes", lambda *a, **kw: [])
    monkeypatch.setattr(rotation, "read_basis_samples", lambda *a, **kw: [])
    monkeypatch.setattr(rotation, "readiness_for_new_entries", lambda *a, **kw: {})
    assert rotation.collect_snapshot(tmp_path)["session_probe"] is None


def test_existing_runtime_does_not_need_handoff_probe(monkeypatch, tmp_path):
    monkeypatch.setattr(rotation, "probe_exchange_session_status", lambda: pytest.fail("unexpected network read"))
    monkeypatch.setattr(rotation, "script_processes", lambda root, script, *a: [11] if script == "main.py" else [])
    monkeypatch.setattr(rotation, "read_basis_samples", lambda *a, **kw: [])
    monkeypatch.setattr(rotation, "readiness_for_new_entries", lambda *a, **kw: {})
    assert rotation.collect_snapshot(tmp_path, probe_exchange_session=True)["session_probe"] is None


def test_session_probe_closes_its_public_connection_and_retains_observation(monkeypatch):
    class Feed:
        connected = False
        status = None
        heartbeat_at = None
        error = None
        stop = False
        cleaned = False

        async def run(self):
            self.connected = True
            self.status = "closed"
            self.heartbeat_at = datetime.now(timezone.utc)
            try:
                await asyncio.Event().wait()
            finally:
                self.connected = False
                self.cleaned = True

        async def wait_for_status(self, timeout):
            await asyncio.sleep(0)
            return self.status is not None

    feed = Feed()
    monkeypatch.setattr(rotation, "VariationalMarketStatusFeed", lambda: feed)
    result = rotation.probe_exchange_session_status()
    assert result["market_status"] == "closed"
    assert result["market_status_feed_connected"] is True
    assert feed.cleaned and feed.stop


def _legacy_receipt():
    now = datetime(2026, 10, 24, 13, tzinfo=timezone.utc)
    return {
        "schema_version": 1, "asset": "WTI", "action": "drain_after_flat",
        "status": "completed", "request_id": "req", "target_pid": 123,
        "target_run_id": "wti-run", "active_order_count": 0,
        "variational_position_qty": "0", "lighter_position_qty": "0",
        "completed_at": now.isoformat(),
        "reconciliation_observed_at": (now - timedelta(seconds=1)).isoformat(),
    }


@pytest.mark.parametrize("fault", [None, "positive_orders", "bool_count", "partial_venue_fields",
                                  "positive_venue_orders", "old_at_completion", "missing_time",
                                  "naive_time", "nonflat", "wrong_request"])
def test_legacy_wti_receipt_requires_exact_identity_and_verified_zero_count(fault):
    receipt = _legacy_receipt()
    if fault == "positive_orders":
        receipt["active_order_count"] = 1
    if fault == "bool_count":
        receipt["active_order_count"] = False
    if fault == "partial_venue_fields":
        receipt["variational_active_orders"] = 0
    if fault == "positive_venue_orders":
        receipt.update({"variational_active_orders": 0, "lighter_active_orders": 1})
    if fault == "old_at_completion":
        receipt["reconciliation_observed_at"] = "2026-10-24T12:59:39+00:00"
    if fault == "missing_time":
        receipt.pop("reconciliation_observed_at")
    if fault == "naive_time":
        receipt["reconciliation_observed_at"] = "2026-10-24T12:59:59"
    if fault == "nonflat":
        receipt["lighter_position_qty"] = "0.00001"
    if fault == "wrong_request":
        receipt["request_id"] = "other"
    assert verified_drain_completion(
        receipt, request_id="req", source_pid=123, source_run_id="wti-run",
    ) is (fault is None)


def _snapshot():
    now = datetime.now(timezone.utc)
    return {
        "current_pair": "WTI", "target_pair": "ETH", "target_reason": "exchange_closed",
        "session": {"state": "exchange_closed", "allow_new_entries": False,
                    "allow_position_management": False, "observed_at": now.isoformat()},
        "readiness": {"production_ready": False, "reasons": []},
        "eth_ready": True, "wti_ready": False, "lighter_market_id": 145,
        "process_conflict": False, "collector_pids": [], "collector": {},
        "runtimes": {
            "ETH": {"pids": [], "matched_healthy": False, "run_id": "old-eth", "health": {}},
            "WTI": {"pids": [rotation.os.getpid()], "matched_healthy": True, "run_id": "wti-run",
                    "health": {"startup_reconciled": True}},
        },
        "states": {
            "ETH": {"asset": "ETH", "status": "flat", "open_lots": [], "pending_actions": []},
            "WTI": {"asset": "WTI", "status": "flat", "open_position": None, "pending_action": None,
                    "last_account_reconciliation": {
                        "verified": True, "observed_at": now.isoformat(), "market_id": 145,
                        "variational_qty": "0", "lighter_qty": "0", "active_order_count": 0,
                    }},
        },
    }


@pytest.mark.parametrize("legacy", [False, True])
def test_real_wti_runtime_receipt_is_consumed_then_eth_starts_once(monkeypatch, tmp_path, legacy):
    collect_snapshot = rotation.collect_snapshot
    snapshot = _snapshot()
    monkeypatch.setattr(rotation, "collect_snapshot", lambda *a, **kw: snapshot)
    supervisor = rotation.RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    launched = []
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda _: bool(launched))
    monkeypatch.setattr(supervisor, "_launch", lambda pair, **kw: launched.append((pair, kw)))
    assert supervisor.run_once()["action"] == "request_safe_drain"
    control_path = tmp_path / "log/wti_live_control.json"
    runtime = object.__new__(wti_live.WTILiveRuntime)
    runtime.args = SimpleNamespace(control_path=control_path, expected_lighter_market_id=145)
    runtime.run_id = "wti-run"
    runtime.rotation_drain_request = None
    runtime.startup_reconciled = True
    runtime.request_stop = lambda: setattr(runtime, "stop", True)
    assert runtime._poll_rotation_drain()
    assert runtime._complete_rotation_drain_if_verified_flat(snapshot["states"]["WTI"])
    if legacy:
        receipt = json.loads(control_path.read_text())
        receipt.pop("variational_active_orders")
        receipt.pop("lighter_active_orders")
        write_object_atomic(control_path, receipt)
    (tmp_path / "live_config.json").write_text("{}")
    write_object_atomic(tmp_path / "log/live_inventory_state.json", snapshot["states"]["ETH"])
    write_object_atomic(tmp_path / "log/wti_live_state.json", snapshot["states"]["WTI"])
    write_object_atomic(tmp_path / "log/oil_swap/basis_samples/health.json", {
        "status": "stopped", "market_status": "unknown", "market_status_feed_connected": False,
    })
    monkeypatch.setattr(rotation, "script_processes", lambda *a, **kw: [])
    monkeypatch.setattr(rotation, "read_basis_samples", lambda *a, **kw: [])
    monkeypatch.setattr(rotation, "readiness_for_new_entries", lambda *a, **kw: {"production_ready": False})
    monkeypatch.setattr(rotation, "probe_exchange_session_status", lambda: _closed_feed(datetime.now(timezone.utc)))
    after_exit = collect_snapshot(tmp_path, probe_exchange_session=True)
    assert after_exit["current_pair"] == "NONE" and after_exit["target_pair"] == "ETH"
    assert after_exit["session"]["state"] == "exchange_closed"
    snapshot.clear()
    snapshot.update(after_exit)
    assert supervisor.run_once()["action"] == "start_target"
    assert supervisor.run_once()["action"] == "await_starting_tmux_session"
    assert runtime.stop
    assert len(launched) == 1 and launched[0][0] == Pair.ETH


@pytest.mark.parametrize("reopened", [False, True])
def test_completed_drain_waits_for_real_status_without_restarting_bootstrap_on_probe_failure(
    monkeypatch, tmp_path, reopened,
):
    snapshot = _snapshot()
    snapshot.update({"current_pair": "NONE", "target_pair": "NONE"})
    snapshot["runtimes"]["WTI"].update({"pids": [], "matched_healthy": False})
    snapshot["session"]["state"] = "status_unknown"
    snapshot["session_probe"] = {"market_status": "unknown", "market_status_feed_connected": False}
    if reopened:
        snapshot["session_probe"] = {
            "market_status": "open", "market_status_feed_connected": True,
            "market_status_feed_heartbeat_at": datetime.now(timezone.utc).isoformat(),
            "source": "variational_market_status_websocket",
        }
    monkeypatch.setattr(rotation, "collect_snapshot", lambda *a, **kw: snapshot)
    supervisor = rotation.RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    journal = initial_journal()
    journal.update({"phase": "draining", "current_pair": "WTI", "target_pair": "ETH",
                    "request_id": "req", "source_pid": 123, "source_run_id": "wti-run"})
    write_object_atomic(supervisor.journal_path, journal)
    write_object_atomic(tmp_path / "log/wti_live_control.json", _legacy_receipt())
    launches = []
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda _: bool(launches))
    monkeypatch.setattr(supervisor, "_launch", lambda pair, **kw: launches.append((pair, kw)))
    first = supervisor.run_once()
    second = supervisor.run_once()
    if reopened:
        assert first["action"] == "target_changed_replan"
        assert second["action"] == "start_wti_session_bootstrap_management_only"
        assert len(launches) == 1 and launches[0][1]["recovery"] is True
    else:
        assert first["action"] == second["action"] == "wait_for_live_handoff_session_confirmation"
        saved = json.loads(supervisor.journal_path.read_text())
        assert saved["phase"] == "draining" and saved["request_id"] == "req"
        assert not launches


@pytest.mark.parametrize("fault", ["wrong_pid", "not_read_only", "orders_unknown", "stale"])
def test_collector_is_not_stopped_without_fresh_readonly_identity(monkeypatch, tmp_path, fault):
    snapshot = _snapshot()
    snapshot["collector_pids"] = [789]
    health = {"pid": 789, "run_id": "collector", "execution_mode": "collect_only",
              "orders_attempted": False, "updated_at": datetime.now(timezone.utc).isoformat()}
    snapshot["collector"] = health
    if fault == "wrong_pid":
        health["pid"] = 790
    if fault == "not_read_only":
        health["execution_mode"] = "live"
    if fault == "orders_unknown":
        health.pop("orders_attempted")
    if fault == "stale":
        health["updated_at"] = (datetime.now(timezone.utc) - timedelta(seconds=21)).isoformat()
    monkeypatch.setattr(rotation.os, "pidfd_open", lambda _: pytest.fail("must not stop this process"), raising=False)
    supervisor = rotation.RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"), confirm_initial_flat=True,
    )
    assert supervisor._stop_collector_after_verified_drain({}, snapshot) == "wait_for_standalone_collector_identity"


def test_verified_eth_drain_stops_readonly_collector_before_wti_start(monkeypatch, tmp_path):
    snapshot = _snapshot()
    snapshot.update({"current_pair": "NONE", "target_pair": "WTI",
                     "continuous_gradient_gate_ready": True})
    snapshot["session"].update({"state": "open", "allow_new_entries": True})
    snapshot["runtimes"]["WTI"].update({"pids": [], "matched_healthy": False})
    snapshot["collector_pids"] = [789]
    snapshot["collector"] = {
        "pid": 789, "run_id": "collector-run", "status": "running",
        "execution_mode": "collect_only", "orders_attempted": False,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    monkeypatch.setattr(rotation, "collect_snapshot", lambda *a, **kw: snapshot)
    monkeypatch.setattr(rotation, "script_processes", lambda root, script: [789] if script.endswith("oil_swap_collector.py") else [])
    monkeypatch.setattr(rotation.os, "pidfd_open", lambda _: 77, raising=False)
    signals = []
    monkeypatch.setattr(rotation.signal, "pidfd_send_signal", lambda *args: signals.append(args), raising=False)
    monkeypatch.setattr(rotation.os, "close", lambda _: None)
    supervisor = rotation.RotationSupervisor(
        root=tmp_path, market_id=145, multiplier=Decimal("1"),
        confirm_initial_flat=True, continuous_gradient_rotation=True,
    )
    journal = initial_journal()
    journal.update({"phase": "draining", "current_pair": "ETH", "target_pair": "WTI",
                    "source_pid": 123, "source_run_id": "eth-run", "request_id": "req"})
    write_object_atomic(supervisor.journal_path, journal)
    control = {**_legacy_receipt(), "asset": "ETH", "target_run_id": "eth-run",
               "variational_active_orders": 0, "lighter_active_orders": 0}
    write_object_atomic(tmp_path / "log/live_inventory_control.json", control)
    launched = []
    monkeypatch.setattr(supervisor, "_tmux_session_exists", lambda _: bool(launched))
    monkeypatch.setattr(supervisor, "_launch", lambda pair, **kw: launched.append((pair, kw)))

    assert supervisor.run_once()["action"] == "stop_standalone_collector_for_wti_handoff"
    assert supervisor.run_once()["action"] == "wait_for_standalone_collector_exit"
    assert signals == [(77, rotation.signal.SIGTERM)] and not launched
    snapshot["collector_pids"] = []
    assert supervisor.run_once()["action"] == "start_target"
    assert supervisor.run_once()["action"] == "await_starting_tmux_session"
    assert len(launched) == 1 and launched[0][0] == Pair.WTI
    assert launched[0][1]["continuous_rotation"] is True
