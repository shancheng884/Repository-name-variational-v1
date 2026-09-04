from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from tools.lib.risk_wakeup_remote import (
    RemoteHeartbeatPublisher,
    build_heartbeat_payload,
    json_bytes,
    signature_for,
    verify_signature,
)
from tools.lib.wakeup_notifiers import NotificationResult
from tools.risk_wakeup_backup import (
    BackupAlertMonitor,
    BackupConfig,
    make_heartbeat_handler,
)
from http.server import ThreadingHTTPServer


class FakeNotifier:
    def __init__(self) -> None:
        self.enabled = True
        self.config_path = None
        self.calls: list[tuple[str, str]] = []

    def send(self, *, title: str, message: str, critical: bool) -> NotificationResult:
        self.calls.append(("bark", message))
        return NotificationResult(True, "sent")

    def send_message(self, *, title: str, message: str) -> NotificationResult:
        self.calls.append(("message", message))
        return NotificationResult(True, "sent", "msg-1")

    def phone_urgent(self, message_id: str) -> NotificationResult:
        self.calls.append(("phone", message_id))
        return NotificationResult(True, "sent")

    def send_now(self, message: str) -> tuple[bool, str]:
        self.calls.append(("telegram", message))
        return True, "sent"


def _config() -> BackupConfig:
    config = BackupConfig()
    config.enabled = True
    config.token = "test-token"
    config.expected_node_id = "vps-a"
    return config


def test_signature_rejects_tampering_and_old_timestamp() -> None:
    body = b'{"ok":true}'
    timestamp = "1000"
    signature = signature_for(token="secret", timestamp=timestamp, body=body)

    assert (
        verify_signature(
            token="secret",
            node_id="vps-a",
            received_node_id="vps-a",
            timestamp=timestamp,
            signature=signature,
            body=body,
            now=1000,
        )
        is None
    )
    assert (
        verify_signature(
            token="secret",
            node_id="vps-a",
            received_node_id="vps-a",
            timestamp=timestamp,
            signature=signature,
            body=b'{"ok":false}',
            now=1000,
        )
        == "invalid_signature"
    )
    assert (
        verify_signature(
            token="secret",
            node_id="vps-a",
            received_node_id="vps-a",
            timestamp=timestamp,
            signature=signature,
            body=body,
            now=1200,
        )
        == "timestamp_out_of_window"
    )


def test_heartbeat_payload_contains_only_operational_summary() -> None:
    payload = build_heartbeat_payload(
        node_id="vps-a",
        state={
            "asset": "ETH",
            "status": "open",
            "open_lots": [{"qty": "0.0081"}],
            "secret": "must-not-leave-a",
        },
        risk_health={
            "updated_at": "2026-09-02T00:00:00+00:00",
            "open_lots_total": 1,
            "pending_actions_total": 0,
            "risk_action": "normal",
            "risk_reason": "account_risk_normal",
        },
        strategy_running=True,
        watchdog_memory={"active_incidents": {}},
        alert_control={
            "notifications_enabled": False,
            "silenced_until": "2026-09-04T02:00:00+00:00",
            "reason": "operator_silence",
        },
    )

    encoded = json.dumps(payload, ensure_ascii=False)
    assert payload["strategy"]["open_lots_total"] == 1
    assert payload["alert_control"]["notifications_enabled"] is False
    assert "must-not-leave-a" not in encoded
    assert "0.0081" not in encoded


def test_http_handler_accepts_signed_heartbeat(tmp_path: Path) -> None:
    config = _config()
    heartbeat_path = tmp_path / "heartbeat.json"
    handler = make_heartbeat_handler(
        config=config,
        heartbeat_path=heartbeat_path,
        clock=lambda: datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload = {"schema_version": 1, "node_id": "vps-a", "sent_at": "now"}
        body = json_bytes(payload)
        timestamp = str(int(time.time()))
        request = Request(
            f"http://127.0.0.1:{server.server_port}/v1/risk-heartbeat",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(body)),
                "X-Risk-Node-Id": "vps-a",
                "X-Risk-Timestamp": timestamp,
                "X-Risk-Signature": signature_for(
                    token=config.token,
                    timestamp=timestamp,
                    body=body,
                ),
            },
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            assert response.status == 200
        saved = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        assert saved["payload"]["node_id"] == "vps-a"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_backup_waits_for_delivery_grace_then_sends_only_failed_channels(
    tmp_path: Path,
) -> None:
    config = _config()
    now = datetime(2026, 9, 2, 0, 0, 30, tzinfo=timezone.utc)
    heartbeat = {
        "schema_version": 1,
        "received_at": now.isoformat(),
        "node_id": "vps-a",
        "payload": {
            "schema_version": 1,
            "node_id": "vps-a",
            "watchdog": {
                "active_incidents": [
                    {
                        "key": "critical_account_risk",
                        "severity": "critical",
                        "title": "critical",
                        "message": "test failure",
                        "incident_signature": "sig-1",
                        "bark_status": "bark_http_400",
                        "feishu_message_status": "sent",
                        "feishu_phone_status": "sent",
                    }
                ]
            },
        },
    }
    heartbeat_path = tmp_path / "heartbeat.json"
    heartbeat_path.write_text(json.dumps(heartbeat), encoding="utf-8")
    bark = FakeNotifier()
    feishu = FakeNotifier()
    telegram = FakeNotifier()
    monitor = BackupAlertMonitor(
        config=config,
        heartbeat_path=heartbeat_path,
        state_path=tmp_path / "state.json",
        bark=bark,
        feishu=feishu,
        telegram=telegram,
        clock=lambda: now,
    )

    assert monitor.run_once() == []
    assert bark.calls == []
    monitor.clock = lambda: now + timedelta(seconds=16)
    reported = monitor.run_once()

    assert reported == ["remote_delivery:critical_account_risk:sig-1"]
    assert [kind for kind, _ in bark.calls] == ["bark"]
    assert feishu.calls == []
    assert telegram.calls == []


def test_backup_does_not_redeliver_acknowledged_incident(tmp_path: Path) -> None:
    config = _config()
    now = datetime(2026, 9, 2, 0, 0, 30, tzinfo=timezone.utc)
    heartbeat = {
        "schema_version": 1,
        "received_at": now.isoformat(),
        "node_id": "vps-a",
        "payload": {
            "schema_version": 1,
            "node_id": "vps-a",
            "watchdog": {
                "active_incidents": [
                    {
                        "key": "critical_account_risk",
                        "severity": "critical",
                        "title": "critical",
                        "message": "acknowledged failure",
                        "incident_signature": "sig-ack",
                        "acknowledged_at": now.isoformat(),
                        "bark_status": "bark_http_400",
                        "feishu_message_status": "sent",
                        "feishu_phone_status": "sent",
                    }
                ]
            },
        },
    }
    heartbeat_path = tmp_path / "heartbeat.json"
    heartbeat_path.write_text(json.dumps(heartbeat), encoding="utf-8")
    bark = FakeNotifier()
    monitor = BackupAlertMonitor(
        config=config,
        heartbeat_path=heartbeat_path,
        state_path=tmp_path / "state.json",
        bark=bark,
        feishu=FakeNotifier(),
        telegram=FakeNotifier(),
        clock=lambda: now + timedelta(seconds=16),
    )

    assert monitor.run_once() == []
    assert bark.calls == []


def test_backup_does_not_repeat_feishu_message_when_phone_retry_fails(
    tmp_path: Path,
) -> None:
    class PhoneFailureNotifier(FakeNotifier):
        def phone_urgent(self, message_id: str) -> NotificationResult:
            self.calls.append(("phone", message_id))
            return NotificationResult(False, "phone_failed")

    config = _config()
    config.delivery_grace_seconds = 0
    config.channel_retry_seconds = 10
    config.max_phone_attempts = 2
    now = datetime(2026, 9, 2, 0, 0, 30, tzinfo=timezone.utc)
    heartbeat = {
        "schema_version": 1,
        "received_at": now.isoformat(),
        "node_id": "vps-a",
        "payload": {
            "schema_version": 1,
            "node_id": "vps-a",
            "watchdog": {
                "active_incidents": [
                    {
                        "key": "critical_account_risk",
                        "severity": "critical",
                        "title": "critical",
                        "message": "phone failure",
                        "incident_signature": "sig-phone",
                        "bark_status": "sent",
                        "feishu_message_status": "failed",
                        "feishu_phone_status": "failed",
                    }
                ]
            },
        },
    }
    heartbeat_path = tmp_path / "heartbeat.json"
    heartbeat_path.write_text(json.dumps(heartbeat), encoding="utf-8")
    feishu = PhoneFailureNotifier()
    monitor = BackupAlertMonitor(
        config=config,
        heartbeat_path=heartbeat_path,
        state_path=tmp_path / "state.json",
        alert_control_path=tmp_path / "alert-control.json",
        bark=FakeNotifier(),
        feishu=feishu,
        telegram=FakeNotifier(),
        clock=lambda: now,
    )

    monitor.run_once()
    monitor.clock = lambda: now + timedelta(seconds=1)
    monitor.run_once()

    assert [kind for kind, _ in feishu.calls] == ["message", "phone"]


def test_backup_suppresses_stale_heartbeat_during_remote_silence(
    tmp_path: Path,
) -> None:
    config = _config()
    now = datetime(2026, 9, 2, 0, 2, 0, tzinfo=timezone.utc)
    heartbeat_path = tmp_path / "heartbeat.json"
    heartbeat_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "received_at": (now - timedelta(seconds=120)).isoformat(),
                "node_id": "vps-a",
                "payload": {
                    "schema_version": 1,
                    "node_id": "vps-a",
                    "alert_control": {
                        "notifications_enabled": False,
                        "silenced_until": (now + timedelta(minutes=5)).isoformat(),
                        "reason": "planned_maintenance",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    bark = FakeNotifier()
    monitor = BackupAlertMonitor(
        config=config,
        heartbeat_path=heartbeat_path,
        state_path=tmp_path / "state.json",
        alert_control_path=tmp_path / "alert-control.json",
        bark=bark,
        feishu=FakeNotifier(),
        telegram=FakeNotifier(),
        clock=lambda: now,
    )

    assert monitor.run_once() == []
    assert bark.calls == []
