from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import stat
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, time as clock_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import read_json, write_json_atomic
from tools.lib.telegram_notifier import TelegramNotifier
from tools.lib.wakeup_notifiers import PushoverNotifier, TencentVoiceNotifier


STATE_PATH = ROOT / "log" / "live_inventory_state.json"
RISK_HEALTH_PATH = ROOT / "log" / "live_inventory_risk_health.json"
ORDER_METRICS_PATH = ROOT / "log" / "order_metrics.jsonl"
WATCHDOG_STATE_PATH = ROOT / "log" / "risk_wakeup_watchdog_state.json"
WATCHDOG_HEALTH_PATH = ROOT / "log" / "risk_wakeup_watchdog_health.json"


CRITICAL_MANUAL_REVIEW_MARKERS = (
    "mismatch",
    "position",
    "submit_failed",
    "hedge_failed",
    "hedge_not_started",
    "pending",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        result = datetime.fromisoformat(text)
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except ValueError:
        return None


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


def parse_clock(value: str, default: clock_time) -> clock_time:
    try:
        hour, minute = value.strip().split(":", 1)
        return clock_time(hour=int(hour), minute=int(minute))
    except (AttributeError, TypeError, ValueError):
        return default


def beijing_timezone() -> timezone:
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("Asia/Shanghai")
    except Exception:
        return timezone(timedelta(hours=8), name="Asia/Shanghai")


def is_night_window(
    now: datetime,
    *,
    start: clock_time,
    end: clock_time,
) -> bool:
    current = now.astimezone(beijing_timezone()).time().replace(tzinfo=None)
    if start == end:
        return True
    if start < end:
        return start <= current < end
    return current >= start or current < end


def process_is_strategy(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    command_path = Path("/proc") / str(pid) / "cmdline"
    try:
        command = command_path.read_bytes().replace(b"\x00", b" ").decode(
            "utf-8",
            errors="replace",
        )
    except OSError:
        return False
    return "main.py" in command and "python" in command.lower()


@dataclass(frozen=True)
class Incident:
    key: str
    severity: str
    title: str
    message: str
    voice_params: tuple[str, ...]


@dataclass(frozen=True)
class WatchdogConfig:
    enabled: bool
    alert_when_flat_strategy_stopped: bool
    poll_seconds: float
    risk_heartbeat_max_age_seconds: float
    pending_action_max_age_seconds: float
    data_unavailable_critical_seconds: float
    voice_escalation_seconds: float
    voice_repeat_seconds: float
    max_voice_calls_per_incident: int
    voice_only_at_night: bool
    night_start: clock_time
    night_end: clock_time

    @classmethod
    def from_env(cls) -> "WatchdogConfig":
        return cls(
            enabled=env_bool("RISK_WAKEUP_ENABLED", False),
            alert_when_flat_strategy_stopped=env_bool(
                "RISK_WAKEUP_ALERT_WHEN_FLAT_STRATEGY_STOPPED",
                True,
            ),
            poll_seconds=max(1.0, env_float("RISK_WAKEUP_POLL_SECONDS", 5.0)),
            risk_heartbeat_max_age_seconds=max(
                15.0,
                env_float("RISK_WAKEUP_HEARTBEAT_MAX_AGE_SECONDS", 45.0),
            ),
            pending_action_max_age_seconds=max(
                5.0,
                env_float("RISK_WAKEUP_PENDING_MAX_AGE_SECONDS", 30.0),
            ),
            data_unavailable_critical_seconds=max(
                60.0,
                env_float(
                    "RISK_WAKEUP_DATA_UNAVAILABLE_CRITICAL_SECONDS",
                    300.0,
                ),
            ),
            voice_escalation_seconds=max(
                0.0,
                env_float("RISK_WAKEUP_VOICE_ESCALATION_SECONDS", 120.0),
            ),
            voice_repeat_seconds=max(
                60.0,
                env_float("RISK_WAKEUP_VOICE_REPEAT_SECONDS", 900.0),
            ),
            max_voice_calls_per_incident=max(
                1,
                env_int("RISK_WAKEUP_MAX_VOICE_CALLS", 3),
            ),
            voice_only_at_night=env_bool(
                "RISK_WAKEUP_VOICE_ONLY_AT_NIGHT",
                True,
            ),
            night_start=parse_clock(
                os.getenv("RISK_WAKEUP_NIGHT_START", "23:00"),
                clock_time(23, 0),
            ),
            night_end=parse_clock(
                os.getenv("RISK_WAKEUP_NIGHT_END", "08:00"),
                clock_time(8, 0),
            ),
        )


class JsonlFollower:
    def __init__(self, path: Path, *, initial_bytes: int = 2_000_000) -> None:
        self.path = path
        self.initial_bytes = max(4096, initial_bytes)
        self.offset: int | None = None
        self.rows: deque[dict[str, Any]] = deque(maxlen=500)

    def read(self) -> list[dict[str, Any]]:
        try:
            size = self.path.stat().st_size
        except OSError:
            return list(self.rows)
        if self.offset is None or size < self.offset:
            start = max(0, size - self.initial_bytes)
        else:
            start = self.offset
        try:
            with self.path.open("rb") as handle:
                handle.seek(start)
                data = handle.read()
        except OSError:
            return list(self.rows)
        self.offset = start + len(data)
        lines = data.splitlines()
        if start > 0 and lines:
            lines = lines[1:]
        for line in lines:
            try:
                value = json.loads(line.decode("utf-8", errors="replace"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                self.rows.append(value)
        return list(self.rows)


def _pending_age_seconds(item: dict[str, Any], now: datetime) -> float | None:
    for key in (
        "submitted_at",
        "created_at",
        "record_created_at",
        "last_updated_at",
    ):
        started = parse_time(item.get(key))
        if started is not None:
            return max(0.0, (now - started).total_seconds())
    return None


def evaluate_incidents(
    *,
    state: dict[str, Any],
    risk_health: dict[str, Any],
    events: list[dict[str, Any]],
    strategy_running: bool,
    config: WatchdogConfig,
    now: datetime,
) -> list[Incident]:
    incidents: list[Incident] = []
    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    pending = (
        state.get("pending_actions")
        if isinstance(state.get("pending_actions"), list)
        else []
    )
    try:
        heartbeat_open_lots = int(risk_health.get("open_lots_total") or 0)
    except (TypeError, ValueError):
        heartbeat_open_lots = 0
    try:
        heartbeat_pending = int(risk_health.get("pending_actions_total") or 0)
    except (TypeError, ValueError):
        heartbeat_pending = 0
    open_lot_count = max(len(open_lots), heartbeat_open_lots)
    pending_count = max(len(pending), heartbeat_pending)
    exposure = bool(open_lot_count or pending_count)
    asset = str(state.get("asset") or risk_health.get("asset") or "ETH")
    status = str(state.get("status") or "unknown")
    reason = str(
        state.get("manual_review_reason")
        or state.get("last_blocked_reason")
        or "unknown"
    )
    lot_text = f"未平子单 {open_lot_count}，待确认动作 {pending_count}"

    if (
        not exposure
        and not strategy_running
        and config.alert_when_flat_strategy_stopped
    ):
        incidents.append(
            Incident(
                key="strategy_stopped_flat",
                severity="warning",
                title="套利策略已停止",
                message=f"{asset}：当前空仓，但主策略进程未运行。",
                voice_params=(asset, "策略已停止"),
            )
        )

    if exposure and not strategy_running:
        incidents.append(
            Incident(
                key="strategy_stopped_with_exposure",
                severity="critical",
                title="套利策略停止但仍有仓位",
                message=f"{asset}：主策略已停止；{lot_text}。请立即检查双边仓位。",
                voice_params=(asset, "策略停止且仍有仓位"),
            )
        )

    if status == "manual_review_required":
        critical = exposure or any(
            marker in reason for marker in CRITICAL_MANUAL_REVIEW_MARKERS
        )
        incidents.append(
            Incident(
                key=f"manual_review:{reason}",
                severity="critical" if critical else "warning",
                title="套利账户需要人工核对",
                message=f"{asset}：原因 {reason}；{lot_text}。",
                voice_params=(asset, "双边账户需要人工核对"),
            )
        )

    if pending_count:
        ages = [
            age
            for age in (_pending_age_seconds(item, now) for item in pending)
            if age is not None
        ]
        state_updated_at = parse_time(state.get("updated_at"))
        oldest = max(ages) if ages else (
            max(0.0, (now - state_updated_at).total_seconds())
            if state_updated_at is not None
            else None
        )
        if (oldest is not None and oldest >= config.pending_action_max_age_seconds) or not strategy_running:
            incidents.append(
                Incident(
                    key="pending_action_stale",
                    severity="critical",
                    title="套利双边成交确认超时",
                    message=(
                        f"{asset}：{pending_count} 个动作未完成，"
                        f"最长等待 {oldest:.0f} 秒。"
                        if oldest is not None
                        else f"{asset}：{pending_count} 个动作未完成。"
                    ),
                    voice_params=(asset, "双边成交确认超时"),
                )
            )

    health_at = parse_time(risk_health.get("updated_at"))
    health_age = (
        max(0.0, (now - health_at).total_seconds())
        if health_at is not None
        else None
    )
    if exposure and (
        health_age is None
        or health_age > config.risk_heartbeat_max_age_seconds
    ):
        age_text = "不可用" if health_age is None else f"{health_age:.0f} 秒"
        incidents.append(
            Incident(
                key="risk_heartbeat_stale_with_exposure",
                severity="critical",
                title="套利账户风险监控失联",
                message=f"{asset}：风险心跳年龄 {age_text}；{lot_text}。",
                voice_params=(asset, "账户风险监控失联"),
            )
        )

    action = str(risk_health.get("risk_action") or "normal")
    risk_reason = str(risk_health.get("risk_reason") or "account_risk_normal")
    if action in {"force_reduce", "emergency_exit"}:
        margin = risk_health.get("max_maintenance_margin_usage_pct")
        leverage = risk_health.get("max_projected_venue_leverage")
        incidents.append(
            Incident(
                key=f"account_risk:{action}:{risk_reason}",
                severity="critical",
                title="套利账户触发强风险动作",
                message=(
                    f"{asset}：动作 {action}，原因 {risk_reason}，"
                    f"最高保证金使用率 {margin or '-'}%，"
                    f"最高单边杠杆 {leverage or '-'}x。"
                ),
                voice_params=(asset, "保证金风险已触发强制处理"),
            )
        )
    elif action in {"warning", "block_entry"}:
        data_visibility_risk = exposure and risk_reason in {
            "variational_account_snapshot_stale",
            "account_equity_unavailable",
        }
        incidents.append(
            Incident(
                key=(
                    f"data_visibility:{risk_reason}"
                    if data_visibility_risk
                    else f"account_risk:{action}:{risk_reason}"
                ),
                severity="warning",
                title="套利账户风险提醒",
                message=f"{asset}：动作 {action}，原因 {risk_reason}。",
                voice_params=(asset, "账户风险提醒"),
            )
        )

    if exposure:
        latest_manual_review = next(
            (
                row
                for row in reversed(events)
                if row.get("event") == "live_inventory_manual_review_required"
            ),
            None,
        )
        latest_reconcile = next(
            (
                row
                for row in reversed(events)
                if row.get("event") == "live_inventory_startup_reconcile_ok"
            ),
            None,
        )
        manual_at = parse_time(
            latest_manual_review.get("logged_at") if latest_manual_review else None
        )
        reconcile_at = parse_time(
            latest_reconcile.get("logged_at") if latest_reconcile else None
        )
        if latest_manual_review and manual_at is not None and (
            reconcile_at is None or manual_at > reconcile_at
        ):
            event_reason = str(latest_manual_review.get("reason") or "unknown")
            incidents.append(
                Incident(
                    key=f"unreconciled_manual_review:{event_reason}",
                    severity="critical",
                    title="套利双边状态尚未重新核对",
                    message=f"{asset}：故障 {event_reason} 后尚无成功双边核对；{lot_text}。",
                    voice_params=(asset, "双边仓位尚未重新核对"),
                )
            )

    critical = [item for item in incidents if item.severity == "critical"]
    if critical:
        messages = list(dict.fromkeys(item.message for item in critical))
        incidents = [item for item in incidents if item.severity != "critical"]
        incidents.append(
            Incident(
                key="critical_account_risk",
                severity="critical",
                title="Var/Lighter 账户紧急风险",
                message="\n".join(messages),
                voice_params=(asset, "账户出现紧急风险，请立即检查"),
            )
        )

    unique: dict[str, Incident] = {}
    for incident in incidents:
        unique[incident.key] = incident
    return list(unique.values())


class RiskWakeupWatchdog:
    def __init__(
        self,
        *,
        config: WatchdogConfig,
        state_path: Path = STATE_PATH,
        risk_health_path: Path = RISK_HEALTH_PATH,
        metrics_path: Path = ORDER_METRICS_PATH,
        watchdog_state_path: Path = WATCHDOG_STATE_PATH,
        watchdog_health_path: Path = WATCHDOG_HEALTH_PATH,
        pushover: PushoverNotifier | None = None,
        voice: TencentVoiceNotifier | None = None,
        telegram: TelegramNotifier | None = None,
        clock: Callable[[], datetime] = utc_now,
        strategy_check: Callable[[int | None], bool] = process_is_strategy,
        dry_run: bool = False,
    ) -> None:
        self.config = config
        self.state_path = state_path
        self.risk_health_path = risk_health_path
        self.watchdog_state_path = watchdog_state_path
        self.watchdog_health_path = watchdog_health_path
        self.pushover = pushover or PushoverNotifier.from_env()
        self.voice = voice or TencentVoiceNotifier.from_env()
        self.telegram = telegram or TelegramNotifier.from_env(
            logger=logging.getLogger("risk_wakeup_watchdog.telegram")
        )
        self.clock = clock
        self.strategy_check = strategy_check
        self.dry_run = dry_run
        self.metrics = JsonlFollower(metrics_path)
        self.memory = read_json(watchdog_state_path)
        if not isinstance(self.memory.get("active_incidents"), dict):
            self.memory["active_incidents"] = {}
        self.stop_requested = False

    def configuration_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.pushover.enabled:
            errors.append("pushover_not_configured")
        if not self.voice.enabled:
            errors.append("tencent_voice_not_configured")
        elif not self.voice.sdk_available:
            errors.append("tencentcloud_sdk_missing")
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

    def _notify_telegram(self, message: str) -> None:
        if self.dry_run or not self.telegram.enabled:
            return
        self.telegram.send_now(message)

    def _persist(self, *, now: datetime) -> None:
        self.memory["schema_version"] = 1
        self.memory["updated_at"] = iso_time(now)
        write_json_atomic(self.watchdog_state_path, self.memory)

    def _write_health(
        self,
        *,
        now: datetime,
        strategy_running: bool,
        incidents: list[Incident],
    ) -> None:
        write_json_atomic(
            self.watchdog_health_path,
            {
                "schema_version": 1,
                "updated_at": iso_time(now),
                "status": "running" if self.config.enabled else "disabled",
                "strategy_running": strategy_running,
                "active_incidents": [item.key for item in incidents],
                "critical_incidents": sum(
                    item.severity == "critical" for item in incidents
                ),
                "pushover_configured": self.pushover.enabled,
                "tencent_voice_configured": self.voice.enabled,
                "telegram_configured": self.telegram.enabled,
                "dry_run": self.dry_run,
            },
        )

    def _send_new_incident(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        self._notify_telegram(
            "\n".join(
                [
                    "[Var/Lighter] 夜间风险监控",
                    f"级别：{'紧急' if incident.severity == 'critical' else '提醒'}",
                    incident.message,
                ]
            )
        )
        if incident.severity != "critical" or self.dry_run:
            return
        self._send_pushover_emergency(incident, record, now=now, force=True)

    def _send_pushover_emergency(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
        force: bool = False,
    ) -> None:
        if self.dry_run or record.get("pushover_receipt"):
            return
        last_attempt = parse_time(record.get("last_pushover_attempt_at"))
        retry_seconds = max(30, int(getattr(self.pushover, "retry_seconds", 60)))
        if (
            not force
            and last_attempt is not None
            and (now - last_attempt).total_seconds() < retry_seconds
        ):
            return
        record["last_pushover_attempt_at"] = iso_time(now)
        record["pushover_attempts"] = int(record.get("pushover_attempts") or 0) + 1
        result = self.pushover.send(
            title=incident.title,
            message=incident.message,
            emergency=True,
        )
        record["pushover_status"] = result.detail
        if result.ok:
            record["pushover_sent_at"] = iso_time(now)
            record["pushover_receipt"] = result.receipt

    def _maybe_escalate_voice(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
        force: bool = False,
    ) -> None:
        if incident.severity != "critical" or self.dry_run:
            return
        self._send_pushover_emergency(incident, record, now=now)
        receipt = str(record.get("pushover_receipt") or "")
        if receipt and not record.get("pushover_acknowledged_at"):
            ok, acknowledged, detail = self.pushover.receipt_status(receipt)
            record["pushover_receipt_status"] = detail
            if ok and acknowledged:
                record["pushover_acknowledged_at"] = iso_time(now)
                return
            if ok and detail == "expired":
                record["pushover_receipt"] = None
                record["pushover_expired_at"] = iso_time(now)
                self._send_pushover_emergency(
                    incident,
                    record,
                    now=now,
                    force=True,
                )
        if record.get("pushover_acknowledged_at"):
            return
        first_seen = (
            parse_time(record.get("critical_since_at"))
            or parse_time(record.get("first_seen_at"))
            or now
        )
        if not force and (now - first_seen).total_seconds() < self.config.voice_escalation_seconds:
            return
        if (
            self.config.voice_only_at_night
            and not force
            and not is_night_window(
                now,
                start=self.config.night_start,
                end=self.config.night_end,
            )
        ):
            return
        calls = int(record.get("voice_calls") or 0)
        attempts = int(record.get("voice_attempts") or 0)
        if attempts >= self.config.max_voice_calls_per_incident:
            return
        last_call = parse_time(record.get("last_voice_call_at"))
        if (
            not force
            and last_call is not None
            and (now - last_call).total_seconds() < self.config.voice_repeat_seconds
        ):
            return
        last_attempt = parse_time(record.get("last_voice_attempt_at"))
        if (
            not force
            and last_call is None
            and last_attempt is not None
            and (now - last_attempt).total_seconds() < 60.0
        ):
            return
        record["voice_attempts"] = attempts + 1
        record["last_voice_attempt_at"] = iso_time(now)
        result = self.voice.send(list(incident.voice_params))
        record["voice_status"] = result.detail
        if result.ok:
            record["voice_calls"] = calls + 1
            record["last_voice_call_at"] = iso_time(now)

    def _recover_incident(
        self,
        key: str,
        record: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        receipt = str(record.get("pushover_receipt") or "")
        if receipt and not record.get("pushover_acknowledged_at") and not self.dry_run:
            self.pushover.cancel(receipt)
        self._notify_telegram(
            "\n".join(
                [
                    "[Var/Lighter] 夜间风险已恢复",
                    f"事件：{key}",
                    f"恢复时间：{iso_time(now)}",
                ]
            )
        )

    def run_once(
        self,
        *,
        synthetic: Incident | None = None,
        force_voice: bool = False,
    ) -> list[Incident]:
        now = self.clock()
        state = read_json(self.state_path)
        risk_health = read_json(self.risk_health_path)
        events = self.metrics.read()
        pid_value = risk_health.get("pid")
        try:
            pid = int(pid_value) if pid_value not in (None, "") else None
        except (TypeError, ValueError):
            pid = None
        strategy_running = self.strategy_check(pid)
        incidents = (
            [synthetic]
            if synthetic is not None
            else evaluate_incidents(
                state=state,
                risk_health=risk_health,
                events=events,
                strategy_running=strategy_running,
                config=self.config,
                now=now,
            )
        )
        active = self.memory["active_incidents"]
        if not any(item.severity == "critical" for item in incidents):
            promoted: list[Incident] = []
            for incident in incidents:
                record = active.get(incident.key)
                first_seen = parse_time(
                    record.get("first_seen_at")
                    if isinstance(record, dict)
                    else None
                )
                if (
                    incident.severity == "warning"
                    and incident.key.startswith("data_visibility:")
                    and first_seen is not None
                    and (now - first_seen).total_seconds()
                    >= self.config.data_unavailable_critical_seconds
                ):
                    incident = Incident(
                        key=incident.key,
                        severity="critical",
                        title="持仓期间双边账户数据持续失联",
                        message=incident.message
                        + " 数据持续不可用，请立即检查双边账户。",
                        voice_params=(
                            incident.voice_params[0]
                            if incident.voice_params
                            else "ETH",
                            "持仓期间账户数据持续失联",
                        ),
                    )
                promoted.append(incident)
            incidents = promoted
        current_keys = {item.key for item in incidents}
        for incident in incidents:
            record = active.get(incident.key)
            if not isinstance(record, dict):
                record = {
                    "first_seen_at": iso_time(now),
                    "severity": incident.severity,
                    "title": incident.title,
                    "voice_calls": 0,
                }
                active[incident.key] = record
                self._send_new_incident(incident, record, now=now)
            signature = hashlib.sha256(
                f"{incident.severity}\n{incident.title}\n{incident.message}".encode(
                    "utf-8"
                )
            ).hexdigest()[:16]
            previous_signature = record.get("incident_signature")
            if (
                incident.severity == "critical"
                and previous_signature
                and previous_signature != signature
            ):
                self._notify_telegram(
                    "\n".join(
                        [
                            "[Var/Lighter] 紧急风险原因已变化",
                            incident.message,
                        ]
                    )
                )
                if record.get("pushover_acknowledged_at"):
                    for key in (
                        "pushover_receipt",
                        "pushover_acknowledged_at",
                        "pushover_sent_at",
                        "last_pushover_attempt_at",
                        "last_voice_attempt_at",
                        "last_voice_call_at",
                    ):
                        record.pop(key, None)
                    record["pushover_attempts"] = 0
                    record["voice_attempts"] = 0
                    record["voice_calls"] = 0
                    record["critical_since_at"] = iso_time(now)
            if (
                incident.severity == "critical"
                and record.get("severity") != "critical"
            ):
                record["critical_since_at"] = iso_time(now)
            record["severity"] = incident.severity
            record["incident_signature"] = signature
            record["title"] = incident.title
            record["last_seen_at"] = iso_time(now)
            self._maybe_escalate_voice(
                incident,
                record,
                now=now,
                force=force_voice,
            )
        for key in list(active):
            if key in current_keys:
                continue
            record = active.pop(key)
            if isinstance(record, dict):
                self._recover_incident(key, record, now=now)
        self._persist(now=now)
        self._write_health(
            now=now,
            strategy_running=strategy_running,
            incidents=incidents,
        )
        return incidents

    def run_forever(self) -> None:
        while not self.stop_requested:
            try:
                self.run_once()
            except Exception as exc:
                logging.getLogger("risk_wakeup_watchdog").exception(
                    "watchdog_cycle_failed error=%s",
                    type(exc).__name__,
                )
            time.sleep(self.config.poll_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Independent Var/Lighter account-risk wakeup watchdog",
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test-alert", action="store_true")
    parser.add_argument("--include-voice", action="store_true")
    parser.add_argument(
        "--wait-for-ack-seconds",
        type=float,
        default=0.0,
        help="wait for a real Pushover acknowledgement during a test",
    )
    return parser


def wait_for_test_acknowledgement(
    watchdog: RiskWakeupWatchdog,
    incident: Incident,
    *,
    timeout_seconds: float,
    poll_seconds: float = 5.0,
    monotonic_fn: Callable[[], float] = time.monotonic,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> bool:
    timeout = max(0.0, timeout_seconds)
    deadline = monotonic_fn() + timeout
    while True:
        record = watchdog.memory["active_incidents"].get(incident.key, {})
        if record.get("pushover_acknowledged_at"):
            return True
        remaining = deadline - monotonic_fn()
        if remaining <= 0:
            return False
        sleep_fn(min(max(0.1, poll_seconds), remaining))
        watchdog.run_once(synthetic=incident, force_voice=False)


def main() -> int:
    load_dotenv(ROOT / ".env")
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    config = WatchdogConfig.from_env()
    watchdog = RiskWakeupWatchdog(config=config, dry_run=args.dry_run)
    if args.check:
        errors = watchdog.configuration_errors()
        print(f"watchdog_enabled={config.enabled}")
        print(f"pushover_configured={watchdog.pushover.enabled}")
        print(f"tencent_voice_configured={watchdog.voice.enabled}")
        print(f"tencent_voice_sdk_available={watchdog.voice.sdk_available}")
        print(f"telegram_configured={watchdog.telegram.enabled}")
        print(f"configuration_ready={not errors}")
        for error in errors:
            print(f"configuration_error={error}")
        return 0 if not errors else 1
    if not config.enabled and not args.test_alert and not args.dry_run:
        print("watchdog_disabled set RISK_WAKEUP_ENABLED=true")
        return 2
    if args.test_alert:
        wait_for_ack_seconds = max(0.0, args.wait_for_ack_seconds)
        incident = Incident(
            key=f"end_to_end_test:{int(time.time())}",
            severity="critical",
            title="套利夜间叫醒测试",
            message="ETH：这是端到端测试，不代表真实账户风险。",
            voice_params=("ETH", "夜间风险叫醒测试"),
        )
        watchdog.run_once(
            synthetic=incident,
            force_voice=args.include_voice and wait_for_ack_seconds <= 0,
        )
        if args.dry_run:
            print("test_alert=DRY_RUN")
            return 0
        acknowledgement_passed = False
        if wait_for_ack_seconds > 0:
            acknowledgement_passed = wait_for_test_acknowledgement(
                watchdog,
                incident,
                timeout_seconds=wait_for_ack_seconds,
            )
            print(
                "pushover_ack_test="
                + ("PASS" if acknowledgement_passed else "TIMEOUT")
            )
            if not acknowledgement_passed and args.include_voice:
                watchdog.run_once(synthetic=incident, force_voice=True)
        record = watchdog.memory["active_incidents"].get(incident.key, {})
        pushover_passed = record.get("pushover_status") == "sent"
        print(
            "pushover_test="
            + ("PASS" if pushover_passed else "FAIL")
            + f" detail={record.get('pushover_status') or 'not_attempted'}"
        )
        voice_passed = True
        if args.include_voice:
            voice_status = str(record.get("voice_status") or "not_attempted")
            if wait_for_ack_seconds > 0 and acknowledgement_passed:
                voice_passed = voice_status == "not_attempted"
                print(
                    "tencent_voice_suppression_test="
                    + ("PASS" if voice_passed else "FAIL")
                    + f" detail={voice_status}"
                )
            else:
                voice_passed = voice_status == "sent" or voice_status.startswith("sent:")
                print(
                    "tencent_voice_test="
                    + ("PASS" if voice_passed else "FAIL")
                    + f" detail={voice_status}"
                )
        acknowledgement_result = (
            acknowledgement_passed
            if wait_for_ack_seconds > 0 and not args.include_voice
            else True
        )
        return (
            0
            if pushover_passed and voice_passed and acknowledgement_result
            else 1
        )
    if args.once:
        incidents = watchdog.run_once()
        print(f"incidents={len(incidents)}")
        for incident in incidents:
            print(f"{incident.severity} {incident.key}")
        return 0

    configuration_errors = watchdog.configuration_errors()
    if configuration_errors:
        for error in configuration_errors:
            print(f"watchdog_configuration_error={error}")
        return 2

    def stop_handler(_signum: int, _frame: Any) -> None:
        watchdog.stop_requested = True

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    watchdog.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
