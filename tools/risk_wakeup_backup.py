from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import stat
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.risk_wakeup_remote import (  # noqa: E402
    MAX_HEARTBEAT_BYTES,
    REMOTE_HEARTBEAT_PATH,
    verify_signature,
)
from tools.lib.runtime_files import read_json, write_json_atomic  # noqa: E402
from tools.lib.telegram_notifier import TelegramNotifier  # noqa: E402
from tools.lib.wakeup_notifiers import (  # noqa: E402
    BarkNotifier,
    FeishuUrgentNotifier,
    private_config_error,
)


REMOTE_HEARTBEAT_PATH_FILE = ROOT / "log" / "risk_wakeup_remote_heartbeat.json"
BACKUP_STATE_PATH = ROOT / "log" / "risk_wakeup_backup_state.json"
DEFAULT_PORT = 8769


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


class BackupConfig:
    def __init__(self) -> None:
        self.enabled = env_bool("RISK_WAKEUP_BACKUP_ENABLED", False)
        self.bind = os.getenv("RISK_WAKEUP_BACKUP_BIND", "0.0.0.0").strip()
        self.port = max(1, env_int("RISK_WAKEUP_BACKUP_PORT", DEFAULT_PORT))
        self.token = os.getenv("RISK_WAKEUP_BACKUP_TOKEN", "").strip()
        self.expected_node_id = os.getenv(
            "RISK_WAKEUP_BACKUP_NODE_ID", "vps-a"
        ).strip()
        self.max_age_seconds = max(
            15.0,
            env_float("RISK_WAKEUP_BACKUP_MAX_AGE_SECONDS", 45.0),
        )
        self.delivery_grace_seconds = max(
            5.0,
            env_float("RISK_WAKEUP_BACKUP_DELIVERY_GRACE_SECONDS", 15.0),
        )
        self.poll_seconds = max(
            1.0,
            env_float("RISK_WAKEUP_BACKUP_POLL_SECONDS", 3.0),
        )
        self.channel_retry_seconds = max(
            5.0,
            env_float("RISK_WAKEUP_BACKUP_CHANNEL_RETRY_SECONDS", 10.0),
        )
        self.max_phone_attempts = max(
            1,
            env_int("RISK_WAKEUP_MAX_PHONE_ATTEMPTS", 3),
        )


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: Mapping[str, Any]) -> None:
    raw = json.dumps(body, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(raw)))
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.wfile.write(raw)


def make_heartbeat_handler(
    *,
    config: BackupConfig,
    heartbeat_path: Path,
    clock: Callable[[], datetime] = utc_now,
) -> type[BaseHTTPRequestHandler]:
    class HeartbeatHandler(BaseHTTPRequestHandler):
        server_version = "RiskWakeupBackup/1"

        def do_POST(self) -> None:  # noqa: N802
            if self.path != REMOTE_HEARTBEAT_PATH:
                _json_response(self, 404, {"ok": False, "error": "not_found"})
                return
            try:
                content_length = int(self.headers.get("Content-Length") or "-1")
            except ValueError:
                content_length = -1
            if content_length < 1 or content_length > MAX_HEARTBEAT_BYTES:
                _json_response(self, 413, {"ok": False, "error": "invalid_body_size"})
                return
            body = self.rfile.read(content_length)
            error = verify_signature(
                token=config.token,
                node_id=config.expected_node_id,
                received_node_id=self.headers.get("X-Risk-Node-Id", ""),
                timestamp=self.headers.get("X-Risk-Timestamp", ""),
                signature=self.headers.get("X-Risk-Signature", ""),
                body=body,
            )
            if error:
                _json_response(self, 401, {"ok": False, "error": error})
                return
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                _json_response(self, 400, {"ok": False, "error": "invalid_json"})
                return
            if not isinstance(payload, dict) or payload.get("schema_version") != 1:
                _json_response(self, 400, {"ok": False, "error": "invalid_payload"})
                return
            if payload.get("node_id") != config.expected_node_id:
                _json_response(self, 401, {"ok": False, "error": "unexpected_payload_node"})
                return
            envelope = {
                "schema_version": 1,
                "received_at": iso_time(clock()),
                "remote_address": self.client_address[0],
                "node_id": config.expected_node_id,
                "payload": payload,
            }
            try:
                write_json_atomic(heartbeat_path, envelope)
            except OSError:
                _json_response(self, 500, {"ok": False, "error": "heartbeat_store_failed"})
                return
            _json_response(self, 200, {"ok": True, "received_at": envelope["received_at"]})

        def log_message(self, format: str, *args: Any) -> None:
            logging.getLogger("risk_wakeup_backup.http").debug(format, *args)

    return HeartbeatHandler


def _failed_channels(incident: Mapping[str, Any]) -> set[str]:
    failed: set[str] = set()
    if str(incident.get("bark_status") or "") != "sent":
        failed.add("bark")
    if (
        str(incident.get("feishu_message_status") or "") != "sent"
        or str(incident.get("feishu_phone_status") or "") != "sent"
    ):
        failed.add("feishu")
    return failed


class BackupAlertMonitor:
    def __init__(
        self,
        *,
        config: BackupConfig,
        heartbeat_path: Path = REMOTE_HEARTBEAT_PATH_FILE,
        state_path: Path = BACKUP_STATE_PATH,
        bark: BarkNotifier | None = None,
        feishu: FeishuUrgentNotifier | None = None,
        telegram: TelegramNotifier | None = None,
        clock: Callable[[], datetime] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        dry_run: bool = False,
    ) -> None:
        self.config = config
        self.heartbeat_path = heartbeat_path
        self.state_path = state_path
        self.bark = bark or BarkNotifier.from_config()
        self.feishu = feishu or FeishuUrgentNotifier.from_config()
        self.telegram = telegram or TelegramNotifier.from_env(
            logger=logging.getLogger("risk_wakeup_backup.telegram")
        )
        self.clock = clock
        self.monotonic = monotonic
        self.dry_run = dry_run
        self.memory = read_json(state_path)
        if not isinstance(self.memory.get("active_incidents"), dict):
            self.memory["active_incidents"] = {}
        self.memory.setdefault("seen_heartbeat", False)

    def configuration_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.config.enabled:
            errors.append("backup_disabled")
        if not self.config.token:
            errors.append("backup_token_missing")
        if not self.config.expected_node_id:
            errors.append("backup_node_id_missing")
        for label, notifier in (("bark", self.bark), ("feishu", self.feishu)):
            if not notifier.enabled:
                errors.append(f"{label}_not_configured")
            config_path = getattr(notifier, "config_path", None)
            if config_path is not None:
                problem = private_config_error(config_path)
                if problem:
                    errors.append(f"{label}_config_{problem}")
        env_path = ROOT / ".env"
        if not env_path.exists():
            errors.append("env_file_missing")
        elif os.name != "nt":
            try:
                permissions = stat.S_IMODE(env_path.stat().st_mode)
            except OSError:
                errors.append("env_file_permissions_unavailable")
            else:
                if permissions & 0o077:
                    errors.append("env_file_permissions_not_600")
        return errors

    def _persist(self, now: datetime) -> None:
        self.memory["schema_version"] = 1
        self.memory["updated_at"] = iso_time(now)
        write_json_atomic(self.state_path, self.memory)

    def _record_message(self, record: Mapping[str, Any]) -> str:
        return (
            f"A 节点 {record.get('node_id', self.config.expected_node_id)}："
            f"{record.get('message') or record.get('title') or '远程风险事件'}"
        )[:1800]

    def _retry_due(self, record: Mapping[str, Any], key: str, now: datetime) -> bool:
        attempted_at = parse_time(record.get(key))
        return attempted_at is None or (
            now - attempted_at
        ).total_seconds() >= self.config.channel_retry_seconds

    def _send_telegram(self, record: dict[str, Any], now: datetime) -> None:
        if record.get("telegram_status") == "sent":
            return
        if not self._retry_due(record, "last_telegram_attempt_at", now):
            return
        record["last_telegram_attempt_at"] = iso_time(now)
        if self.dry_run or not self.telegram.enabled:
            record["telegram_status"] = "not_configured" if not self.telegram.enabled else "dry_run"
            return
        ok, detail = self.telegram.send_now(
            "[VPS B 备用报警]\n" + self._record_message(record)
        )
        record["telegram_attempts"] = int(record.get("telegram_attempts") or 0) + 1
        record["telegram_status"] = "sent" if ok else detail

    def _send_bark(self, record: dict[str, Any], now: datetime) -> None:
        if record.get("bark_status") == "sent":
            return
        if not self._retry_due(record, "last_bark_attempt_at", now):
            return
        record["last_bark_attempt_at"] = iso_time(now)
        if self.dry_run:
            record["bark_status"] = "dry_run"
            return
        result = self.bark.send(
            title="VPS B 备用风险报警",
            message=self._record_message(record),
            critical=True,
        )
        record["bark_attempts"] = int(record.get("bark_attempts") or 0) + 1
        record["bark_status"] = result.detail

    def _send_feishu(self, record: dict[str, Any], now: datetime) -> None:
        if record.get("feishu_status") == "sent":
            return
        if not self._retry_due(record, "last_feishu_attempt_at", now):
            return
        record["last_feishu_attempt_at"] = iso_time(now)
        if self.dry_run:
            record["feishu_status"] = "dry_run"
            return
        result = self.feishu.send_message(
            title="VPS B 备用风险报警",
            message=self._record_message(record),
        )
        record["feishu_message_attempts"] = int(
            record.get("feishu_message_attempts") or 0
        ) + 1
        record["feishu_message_status"] = result.detail
        if not result.ok or not result.receipt:
            return
        phone_attempts = int(record.get("feishu_phone_attempts") or 0)
        if phone_attempts >= self.config.max_phone_attempts:
            return
        phone = self.feishu.phone_urgent(result.receipt)
        record["feishu_phone_attempts"] = phone_attempts + 1
        record["feishu_phone_status"] = phone.detail
        if phone.ok:
            record["feishu_status"] = "sent"

    def _deliver(
        self,
        record: dict[str, Any],
        *,
        channels: set[str],
        now: datetime | None = None,
    ) -> None:
        current = now or self.clock()
        if "bark" in channels:
            self._send_bark(record, current)
        if "feishu" in channels:
            self._send_feishu(record, current)
        if "telegram" in channels:
            self._send_telegram(record, current)

    def _desired_incidents(self, now: datetime) -> dict[str, dict[str, Any]]:
        envelope = read_json(self.heartbeat_path)
        if not envelope:
            if self.memory.get("seen_heartbeat"):
                return {
                    "remote_heartbeat_stale": {
                        "node_id": self.config.expected_node_id,
                        "title": "VPS A 心跳数据丢失",
                        "message": (
                            "VPS B 已经接收过 VPS A 的心跳，但本地心跳记录已丢失；"
                            "请立即检查 VPS B 磁盘和服务状态。"
                        ),
                        "channels": ["bark", "feishu", "telegram"],
                        "stale": True,
                    }
                }
            return {}
        received_at = parse_time(envelope.get("received_at"))
        if received_at is None:
            return {}
        self.memory["seen_heartbeat"] = True
        age = max(0.0, (now - received_at).total_seconds())
        if age > self.config.max_age_seconds:
            return {
                "remote_heartbeat_stale": {
                    "node_id": self.config.expected_node_id,
                    "title": "VPS A 心跳中断",
                    "message": (
                        f"VPS A 最近一次签名心跳已超过 {age:.0f} 秒未收到，"
                        "请立即检查 A 的网络、策略和本地看门狗。"
                    ),
                    "channels": ["bark", "feishu", "telegram"],
                    "stale": True,
                }
            }

        payload = envelope.get("payload")
        if not isinstance(payload, Mapping):
            return {}
        desired: dict[str, dict[str, Any]] = {}
        watchdog = payload.get("watchdog")
        active = watchdog.get("active_incidents") if isinstance(watchdog, Mapping) else []
        if isinstance(active, list):
            for incident in active:
                if not isinstance(incident, Mapping):
                    continue
                if str(incident.get("severity") or "") != "critical":
                    continue
                if incident.get("acknowledged_at"):
                    continue
                channels = _failed_channels(incident)
                if not channels:
                    continue
                key = str(incident.get("key") or "remote_critical")
                signature = str(incident.get("incident_signature") or "")
                if not signature:
                    signature = hashlib.sha256(
                        json.dumps(incident, sort_keys=True).encode("utf-8")
                    ).hexdigest()[:16]
                incident_key = f"remote_delivery:{key}:{signature}"
                desired[incident_key] = {
                    "node_id": self.config.expected_node_id,
                    "title": incident.get("title") or "A 节点严重事件通知失败",
                    "message": incident.get("message") or "A 节点严重事件存在未送达通知。",
                    "channels": sorted(channels),
                    "stale": False,
                }
        return desired

    def _recover(self, key: str, record: Mapping[str, Any], now: datetime) -> None:
        message = f"[VPS B 备用报警已恢复]\n事件：{key}\n恢复时间：{iso_time(now)}"
        record_copy = dict(record)
        record_copy["message"] = message
        self._send_telegram(record_copy, now)
        if self.dry_run:
            return
        self.bark.send(title="VPS B 备用报警已恢复", message=message, critical=False)
        self.feishu.send_message(title="VPS B 备用报警已恢复", message=message)

    def run_once(self) -> list[str]:
        now = self.clock()
        desired = self._desired_incidents(now)
        active = self.memory["active_incidents"]
        current_keys = set(desired)
        reported: list[str] = []
        for key, incident in desired.items():
            record = active.get(key)
            if not isinstance(record, dict):
                record = {
                    **incident,
                    "first_seen_at": iso_time(now),
                    "last_seen_at": iso_time(now),
                }
                active[key] = record
            else:
                record.update(
                    {
                        "message": incident["message"],
                        "channels": sorted(incident["channels"]),
                        "last_seen_at": iso_time(now),
                    }
                )
            first_seen = parse_time(record.get("first_seen_at")) or now
            if incident.get("stale") or (
                now - first_seen
            ).total_seconds() >= self.config.delivery_grace_seconds:
                self._deliver(
                    record,
                    channels=set(incident["channels"]),
                    now=now,
                )
                reported.append(key)
        for key in list(active):
            if key in current_keys:
                continue
            record = active.pop(key)
            if isinstance(record, Mapping):
                self._recover(key, record, now)
        self._persist(now)
        return reported


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VPS B remote risk wakeup backup")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test-alert", action="store_true")
    return parser


def _print_check(monitor: BackupAlertMonitor) -> int:
    errors = monitor.configuration_errors()
    print(f"backup_enabled={monitor.config.enabled}")
    print(f"backup_bind={monitor.config.bind}")
    print(f"backup_port={monitor.config.port}")
    print(f"expected_node_id={monitor.config.expected_node_id}")
    print(f"bark_configured={monitor.bark.enabled}")
    print(f"feishu_configured={monitor.feishu.enabled}")
    print(f"telegram_configured={monitor.telegram.enabled}")
    print(f"configuration_ready={not errors}")
    for error in errors:
        print(f"configuration_error={error}")
    return 0 if not errors else 1


def main() -> int:
    load_dotenv(ROOT / ".env")
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    config = BackupConfig()
    monitor = BackupAlertMonitor(config=config, dry_run=args.dry_run)
    if args.check:
        return _print_check(monitor)
    if args.test_alert:
        record = {
            "node_id": config.expected_node_id,
            "title": "VPS B 备用报警测试",
            "message": "这是 VPS B 备用报警端到端测试，不代表真实账户风险。",
            "channels": {"bark", "feishu", "telegram"},
        }
        monitor._deliver(record, channels=set(record["channels"]))
        print("backup_test=DRY_RUN" if args.dry_run else "backup_test=PASS")
        print(f"bark_status={record.get('bark_status')}")
        print(f"feishu_status={record.get('feishu_status')}")
        print(f"feishu_phone_status={record.get('feishu_phone_status')}")
        return 0 if args.dry_run or (
            record.get("bark_status") == "sent"
            and record.get("feishu_status") == "sent"
        ) else 1
    if args.once:
        incidents = monitor.run_once()
        print(f"backup_incidents={len(incidents)}")
        for incident in incidents:
            print(f"incident={incident}")
        return 0
    errors = monitor.configuration_errors()
    if errors:
        for error in errors:
            print(f"backup_configuration_error={error}")
        return 2

    heartbeat_path = REMOTE_HEARTBEAT_PATH_FILE
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    handler = make_heartbeat_handler(config=config, heartbeat_path=heartbeat_path)
    server = ThreadingHTTPServer((config.bind, config.port), handler)
    server.daemon_threads = True
    server_thread = threading.Thread(
        target=server.serve_forever,
        name="risk-wakeup-backup-http",
        daemon=True,
    )
    server_thread.start()
    print(f"backup_server=listening:{config.bind}:{config.port}")
    stop = threading.Event()

    def stop_handler(_signum: int, _frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    try:
        while not stop.is_set():
            try:
                monitor.run_once()
            except Exception:
                logging.getLogger("risk_wakeup_backup").exception(
                    "backup_monitor_cycle_failed"
                )
            stop.wait(config.poll_seconds)
    finally:
        server.shutdown()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
