from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from main import VariationalToLighterRuntime
from tools.risk_wakeup_watchdog import (
    Incident,
    RiskWakeupWatchdog,
    WatchdogConfig,
    evaluate_incidents,
)
from tools.lib.wakeup_notifiers import NotificationResult


def config(**overrides) -> WatchdogConfig:
    values = {
        "enabled": True,
        "alert_when_flat_strategy_stopped": True,
        "poll_seconds": 5,
        "risk_heartbeat_max_age_seconds": 45,
        "pending_action_max_age_seconds": 30,
        "data_unavailable_critical_seconds": 300,
        "max_phone_attempts_per_incident": 3,
    }
    values.update(overrides)
    return WatchdogConfig(**values)


class FakeBark:
    enabled = True
    config_path = None

    def __init__(self):
        self.sent = []
        self.failures_remaining = 0

    def send(self, **kwargs):
        self.sent.append(kwargs)
        if self.failures_remaining:
            self.failures_remaining -= 1
            return NotificationResult(False, "temporary_failure")
        return NotificationResult(True, "sent")


class FakeFeishu:
    enabled = True
    config_path = None

    def __init__(self):
        self.messages = []
        self.phones = []
        self.message_failures_remaining = 0
        self.phone_failures_remaining = 0

    def send_message(self, **kwargs):
        self.messages.append(kwargs)
        if self.message_failures_remaining:
            self.message_failures_remaining -= 1
            return NotificationResult(False, "temporary_message_failure")
        return NotificationResult(
            True,
            "sent",
            f"message-{len(self.messages)}",
        )

    def phone_urgent(self, message_id):
        self.phones.append(message_id)
        if self.phone_failures_remaining:
            self.phone_failures_remaining -= 1
            return NotificationResult(False, "temporary_phone_failure")
        return NotificationResult(True, "sent")


class FakeTelegram:
    enabled = True

    def __init__(self):
        self.sent = []

    def send_now(self, text):
        self.sent.append(text)
        return True, "sent"


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def test_open_position_and_stopped_strategy_is_critical() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={"status": "open", "asset": "ETH", "open_lots": [{"lot_id": 1}]},
        risk_health={"updated_at": now.isoformat(), "risk_action": "normal"},
        events=[],
        strategy_running=False,
        config=config(),
        now=now,
    )

    assert any(
        item.key == "critical_account_risk"
        and item.severity == "critical"
        and "主策略已停止" in item.message
        for item in incidents
    )


def test_flat_stopped_strategy_is_warning_only() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={"updated_at": now.isoformat(), "risk_action": "normal"},
        events=[],
        strategy_running=False,
        config=config(),
        now=now,
    )

    assert [(item.key, item.severity) for item in incidents] == [
        ("strategy_stopped_flat", "warning")
    ]


def test_force_reduce_margin_and_position_mismatch_are_critical() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={
            "status": "manual_review_required",
            "asset": "ETH",
            "open_lots": [{"lot_id": 1}],
            "manual_review_reason": "startup_reconcile_exchange_position_mismatch",
        },
        risk_health={
            "updated_at": now.isoformat(),
            "risk_action": "force_reduce",
            "risk_reason": "maintenance_margin_usage_reduce",
            "max_maintenance_margin_usage_pct": "81",
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    critical = next(item for item in incidents if item.severity == "critical")
    assert critical.key == "critical_account_risk"
    assert "startup_reconcile_exchange_position_mismatch" in critical.message
    assert "maintenance_margin_usage_reduce" in critical.message


def build_watchdog(tmp_path, *, current, bark=None, feishu=None):
    return RiskWakeupWatchdog(
        config=config(channel_retry_seconds=10),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        watchdog_control_path=tmp_path / "control.json",
        bark=bark or FakeBark(),
        feishu=feishu or FakeFeishu(),
        telegram=FakeTelegram(),
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )


def test_critical_incident_immediately_sends_bark_message_and_phone(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    watchdog.run_once(synthetic=incident)

    assert len(bark.sent) == 1
    assert bark.sent[0]["critical"] is True
    assert len(feishu.messages) == 1
    assert feishu.phones == ["message-1"]


def test_failed_channel_retries_without_repeating_successful_channels(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    bark.failures_remaining = 1
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    current[0] += timedelta(seconds=11)
    watchdog.run_once(synthetic=incident)

    assert len(bark.sent) == 2
    assert len(feishu.messages) == 1
    assert len(feishu.phones) == 1


def test_new_critical_reason_realerts_all_channels(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    first = Incident("critical", "critical", "risk", "reason one", ("ETH", "one"))
    second = Incident("critical", "critical", "risk", "reason two", ("ETH", "two"))

    watchdog.run_once(synthetic=first)
    current[0] += timedelta(seconds=5)
    watchdog.run_once(synthetic=second)

    assert len(bark.sent) == 2
    assert len(feishu.messages) == 2
    assert len(feishu.phones) == 2


def test_recovery_sends_non_phone_recovery_notifications(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    watchdog.run_once()

    assert len(bark.sent) == 2
    assert bark.sent[-1]["critical"] is False
    assert len(feishu.messages) == 2
    assert len(feishu.phones) == 1


def test_persistent_account_data_loss_with_position_escalates(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    state_path = tmp_path / "state.json"
    risk_path = tmp_path / "risk.json"
    write_json(
        state_path,
        {"status": "open", "asset": "ETH", "open_lots": [{"lot_id": 1}]},
    )
    write_json(
        risk_path,
        {
            "updated_at": current[0].isoformat(),
            "risk_action": "block_entry",
            "risk_reason": "variational_account_snapshot_stale",
            "open_lots_total": 1,
        },
    )
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )

    first = watchdog.run_once()
    assert first[0].severity == "warning"
    assert len(bark.sent) == 1
    assert bark.sent[0]["critical"] is False
    assert len(feishu.phones) == 0

    current[0] += timedelta(seconds=301)
    write_json(
        risk_path,
        {
            "updated_at": current[0].isoformat(),
            "risk_action": "block_entry",
            "risk_reason": "variational_account_snapshot_stale",
            "open_lots_total": 1,
        },
    )
    promoted = watchdog.run_once()
    assert promoted[0].severity == "critical"
    assert bark.sent[-1]["critical"] is True
    assert len(feishu.phones) == 1


def test_heartbeat_only_mode_suppresses_strategy_incidents(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    write_json(tmp_path / "control.json", {"monitor_strategy": False})
    write_json(
        tmp_path / "state.json",
        {"status": "manual_review_required", "open_lots": [{"lot_id": 1}]},
    )

    incidents = watchdog.run_once()
    health = json.loads((tmp_path / "health.json").read_text())

    assert incidents == []
    assert bark.sent == []
    assert feishu.phones == []
    assert health["mode"] == "heartbeat_only"


def test_main_risk_loop_writes_sanitized_heartbeat(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_risk_health_file = tmp_path / "risk.json"
    runtime.live_inventory_open_lots = [{"qty": "0.01"}]
    runtime.live_inventory_run_id = "run-1"
    runtime.pending_live_inventory_actions_payload = lambda: []
    runtime.live_inventory_state_asset = lambda: "ETH"

    asyncio.run(
        runtime.write_live_inventory_risk_health(
            {
                "risk_action": "normal",
                "risk_reason": "account_risk_normal",
                "variational_equity_usd": "100",
                "lighter_equity_usd": "100",
            }
        )
    )
    body = json.loads(runtime.live_inventory_risk_health_file.read_text())

    assert body["status"] == "open"
    assert body["expected_open_qty"] == "0.01"
    assert body["open_lots_total"] == 1
    assert body["risk_action"] == "normal"
    assert "private" not in json.dumps(body).lower()
