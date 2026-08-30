from __future__ import annotations

import asyncio
import json
from datetime import datetime, time as clock_time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from main import VariationalToLighterRuntime
from tools.risk_wakeup_watchdog import (
    Incident,
    RiskWakeupWatchdog,
    WatchdogConfig,
    evaluate_incidents,
    is_night_window,
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
        "voice_escalation_seconds": 120,
        "voice_repeat_seconds": 900,
        "max_voice_calls_per_incident": 3,
        "voice_only_at_night": True,
        "night_start": clock_time(23, 0),
        "night_end": clock_time(8, 0),
    }
    values.update(overrides)
    return WatchdogConfig(**values)


class FakePushover:
    enabled = True

    def __init__(self):
        self.sent = []
        self.cancelled = []
        self.acknowledged = False
        self.expired = False

    def send(self, **kwargs):
        self.sent.append(kwargs)
        return NotificationResult(True, "sent", "receipt-1")

    def receipt_status(self, receipt):
        return (
            True,
            self.acknowledged,
            "expired" if self.expired else "checked",
        )

    def cancel(self, receipt):
        self.cancelled.append(receipt)
        return NotificationResult(True, "cancelled")


class FakeVoice:
    enabled = True

    def __init__(self):
        self.sent = []

    def send(self, params):
        self.sent.append(params)
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


def test_pushover_repeats_then_voice_escalates_only_when_unacknowledged(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    pushover = FakePushover()
    voice = FakeVoice()
    telegram = FakeTelegram()
    watchdog = RiskWakeupWatchdog(
        config=config(),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        pushover=pushover,
        voice=voice,
        telegram=telegram,
        clock=lambda: current[0],
        strategy_check=lambda _pid: False,
    )
    incident = Incident(
        key="test",
        severity="critical",
        title="test",
        message="test",
        voice_params=("ETH", "test"),
    )

    watchdog.run_once(synthetic=incident)
    assert len(pushover.sent) == 1
    assert pushover.sent[0]["emergency"] is True
    assert voice.sent == []

    current[0] += timedelta(seconds=121)
    watchdog.run_once(synthetic=incident)
    assert voice.sent == [["ETH", "test"]]

    current[0] += timedelta(seconds=901)
    pushover.acknowledged = True
    watchdog.run_once(synthetic=incident)
    assert len(voice.sent) == 1

    current[0] += timedelta(seconds=1)
    watchdog.run_once(synthetic=None)
    assert pushover.cancelled == []
    assert any("已恢复" in text for text in telegram.sent)


def test_recovery_cancels_unacknowledged_emergency(tmp_path) -> None:
    now = datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)
    pushover = FakePushover()
    watchdog = RiskWakeupWatchdog(
        config=config(),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        pushover=pushover,
        voice=FakeVoice(),
        telegram=FakeTelegram(),
        clock=lambda: now,
        strategy_check=lambda _pid: True,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    write_json(tmp_path / "state.json", {"status": "flat", "open_lots": []})
    write_json(
        tmp_path / "risk.json",
        {"updated_at": now.isoformat(), "risk_action": "normal"},
    )
    watchdog.run_once()

    assert pushover.cancelled == ["receipt-1"]


def test_expired_pushover_emergency_is_reissued(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    pushover = FakePushover()
    watchdog = RiskWakeupWatchdog(
        config=config(),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        pushover=pushover,
        voice=FakeVoice(),
        telegram=FakeTelegram(),
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    pushover.expired = True
    current[0] += timedelta(seconds=61)
    watchdog.run_once(synthetic=incident)

    assert len(pushover.sent) == 2


def test_new_critical_reason_realerts_after_previous_acknowledgement(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    pushover = FakePushover()
    watchdog = RiskWakeupWatchdog(
        config=config(),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        pushover=pushover,
        voice=FakeVoice(),
        telegram=FakeTelegram(),
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )
    first = Incident("critical", "critical", "risk", "reason one", ("ETH", "one"))
    second = Incident("critical", "critical", "risk", "reason two", ("ETH", "two"))

    watchdog.run_once(synthetic=first)
    pushover.acknowledged = True
    current[0] += timedelta(seconds=5)
    watchdog.run_once(synthetic=first)
    assert len(pushover.sent) == 1

    pushover.acknowledged = False
    current[0] += timedelta(seconds=5)
    watchdog.run_once(synthetic=second)

    assert len(pushover.sent) == 2


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
    pushover = FakePushover()
    voice = FakeVoice()
    watchdog = RiskWakeupWatchdog(
        config=config(),
        state_path=state_path,
        risk_health_path=risk_path,
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        pushover=pushover,
        voice=voice,
        telegram=FakeTelegram(),
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )

    first = watchdog.run_once()
    assert first[0].severity == "warning"
    assert pushover.sent == []

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
    assert len(pushover.sent) == 1
    assert voice.sent == []

    current[0] += timedelta(seconds=121)
    write_json(
        risk_path,
        {
            "updated_at": current[0].isoformat(),
            "risk_action": "block_entry",
            "risk_reason": "variational_account_snapshot_stale",
            "open_lots_total": 1,
        },
    )
    watchdog.run_once()
    assert voice.sent == [["ETH", "持仓期间账户数据持续失联"]]


def test_night_window_crosses_midnight() -> None:
    assert is_night_window(
        datetime(2026, 8, 30, 16, 30, tzinfo=timezone.utc),
        start=clock_time(23, 0),
        end=clock_time(8, 0),
    )
    assert not is_night_window(
        datetime(2026, 8, 30, 5, 0, tzinfo=timezone.utc),
        start=clock_time(23, 0),
        end=clock_time(8, 0),
    )


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
