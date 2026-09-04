from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Mapping


REMOTE_HEARTBEAT_PATH = "/v1/risk-heartbeat"
MAX_HEARTBEAT_BYTES = 64 * 1024


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def signature_for(*, token: str, timestamp: str, body: bytes) -> str:
    message = timestamp.encode("ascii") + b"\n" + body
    return hmac.new(token.encode("utf-8"), message, hashlib.sha256).hexdigest()


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def verify_signature(
    *,
    token: str,
    node_id: str,
    received_node_id: str,
    timestamp: str,
    signature: str,
    body: bytes,
    now: float | None = None,
    max_clock_skew_seconds: float = 90.0,
) -> str | None:
    if not token:
        return "missing_server_token"
    if not node_id or received_node_id != node_id:
        return "unexpected_node_id"
    try:
        timestamp_value = float(timestamp)
    except (TypeError, ValueError):
        return "invalid_timestamp"
    current = time.time() if now is None else now
    if abs(current - timestamp_value) > max(1.0, max_clock_skew_seconds):
        return "timestamp_out_of_window"
    expected = signature_for(token=token, timestamp=timestamp, body=body)
    if not hmac.compare_digest(expected, str(signature or "")):
        return "invalid_signature"
    return None


def _safe_text(value: Any, *, limit: int = 500) -> str:
    return str(value or "")[:limit]


def _safe_count(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def build_heartbeat_payload(
    *,
    node_id: str,
    state: Mapping[str, Any],
    risk_health: Mapping[str, Any],
    strategy_running: bool,
    watchdog_memory: Mapping[str, Any],
    sent_at: datetime | None = None,
) -> dict[str, Any]:
    incidents: list[dict[str, Any]] = []
    active = watchdog_memory.get("active_incidents")
    if isinstance(active, Mapping):
        for key, value in sorted(active.items(), key=lambda item: str(item[0])):
            if not isinstance(value, Mapping):
                continue
            incidents.append(
                {
                    "key": _safe_text(key, limit=200),
                    "severity": _safe_text(value.get("severity"), limit=20),
                    "title": _safe_text(value.get("title"), limit=120),
                    "message": _safe_text(value.get("message"), limit=1000),
                    "first_seen_at": _safe_text(value.get("first_seen_at"), limit=80),
                    "last_seen_at": _safe_text(value.get("last_seen_at"), limit=80),
                    "incident_signature": _safe_text(
                        value.get("incident_signature"), limit=32
                    ),
                    "acknowledged_at": _safe_text(
                        value.get("acknowledged_at"), limit=80
                    ),
                    "bark_status": _safe_text(value.get("bark_status"), limit=80),
                    "feishu_message_status": _safe_text(
                        value.get("feishu_message_status"), limit=80
                    ),
                    "feishu_phone_status": _safe_text(
                        value.get("feishu_phone_status"), limit=80
                    ),
                }
            )

    return {
        "schema_version": 1,
        "node_id": _safe_text(node_id, limit=80),
        "sent_at": iso_time(sent_at or utc_now()),
        "strategy": {
            "running": bool(strategy_running),
            "asset": _safe_text(state.get("asset") or risk_health.get("asset"), limit=20),
            "status": _safe_text(state.get("status"), limit=40),
            "open_lots_total": _safe_count(
                risk_health.get("open_lots_total")
                if risk_health.get("open_lots_total") is not None
                else len(state.get("open_lots") or [])
                if isinstance(state.get("open_lots"), list)
                else 0
            ),
            "pending_actions_total": _safe_count(
                risk_health.get("pending_actions_total")
                if risk_health.get("pending_actions_total") is not None
                else len(state.get("pending_actions") or [])
                if isinstance(state.get("pending_actions"), list)
                else 0
            ),
            "manual_review_reason": _safe_text(
                state.get("manual_review_reason"), limit=200
            ),
        },
        "risk": {
            "updated_at": _safe_text(risk_health.get("updated_at"), limit=80),
            "risk_action": _safe_text(risk_health.get("risk_action"), limit=40),
            "risk_reason": _safe_text(risk_health.get("risk_reason"), limit=160),
        },
        "watchdog": {
            "updated_at": _safe_text(watchdog_memory.get("updated_at"), limit=80),
            "active_incidents": incidents,
        },
    }


class RemoteHeartbeatPublisher:
    """Non-blocking A-to-B heartbeat sender; failures never affect trading."""

    def __init__(
        self,
        *,
        endpoint: str,
        token: str,
        node_id: str = "vps-a",
        interval_seconds: float = 10.0,
        timeout_seconds: float = 4.0,
        urlopen: Callable[..., Any] = urllib.request.urlopen,
        monotonic: Callable[[], float] = time.monotonic,
        logger: logging.Logger | None = None,
    ) -> None:
        self.endpoint = endpoint.strip()
        self.token = token.strip()
        self.node_id = node_id.strip() or "vps-a"
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.urlopen = urlopen
        self.monotonic = monotonic
        self.logger = logger or logging.getLogger("risk_wakeup_remote")
        self._last_started = 0.0
        self._inflight = False
        self._lock = threading.Lock()

    @classmethod
    def from_env(
        cls,
        *,
        logger: logging.Logger | None = None,
    ) -> "RemoteHeartbeatPublisher":
        return cls(
            endpoint=os.getenv("RISK_WAKEUP_BACKUP_URL", ""),
            token=os.getenv("RISK_WAKEUP_BACKUP_TOKEN", ""),
            node_id=os.getenv("RISK_WAKEUP_BACKUP_NODE_ID", "vps-a"),
            interval_seconds=_env_float(
                "RISK_WAKEUP_BACKUP_INTERVAL_SECONDS", 10.0
            ),
            timeout_seconds=_env_float("RISK_WAKEUP_BACKUP_TIMEOUT_SECONDS", 4.0),
            logger=logger,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint and self.token)

    def publish(self, payload: Mapping[str, Any]) -> bool:
        if not self.enabled:
            return False
        now = self.monotonic()
        with self._lock:
            if self._inflight or now - self._last_started < self.interval_seconds:
                return False
            self._last_started = now
            self._inflight = True
        body = json_bytes(payload)
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "X-Risk-Node-Id": self.node_id,
            "X-Risk-Timestamp": timestamp,
            "X-Risk-Signature": signature_for(
                token=self.token,
                timestamp=timestamp,
                body=body,
            ),
        }
        thread = threading.Thread(
            target=self._send,
            args=(body, headers),
            name="risk-wakeup-remote-heartbeat",
            daemon=True,
        )
        thread.start()
        return True

    def _send(self, body: bytes, headers: Mapping[str, str]) -> None:
        try:
            request = urllib.request.Request(
                self.endpoint,
                data=body,
                headers=dict(headers),
                method="POST",
            )
            with self.urlopen(request, timeout=self.timeout_seconds) as response:
                status_value = getattr(response, "status", None)
                if status_value is None:
                    status_value = response.getcode()
                status = int(status_value)
                if not 200 <= status < 300:
                    self.logger.warning("remote_heartbeat_http_status=%s", status)
        except Exception as exc:
            self.logger.warning(
                "remote_heartbeat_send_failed error=%s",
                type(exc).__name__,
            )
        finally:
            with self._lock:
                self._inflight = False
