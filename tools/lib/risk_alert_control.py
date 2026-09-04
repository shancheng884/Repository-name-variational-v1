from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from tools.lib.runtime_files import read_json, write_json_atomic


CONTROL_SCHEMA_VERSION = 1


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


def read_alert_control(path: Path) -> dict[str, Any]:
    value = read_json(path)
    return value if isinstance(value, dict) else {}


def suppression_reason(
    control: dict[str, Any], *, now: datetime | None = None
) -> str | None:
    current = now or utc_now()
    if control.get("notifications_enabled") is False:
        raw_until_values = [
            control.get("silenced_until"),
            control.get("maintenance_until"),
        ]
        provided_until_values = [value for value in raw_until_values if value]
        if any(parse_time(value) is None for value in provided_until_values):
            return str(control.get("reason") or "operator_silence")
        until_values = [parse_time(value) for value in provided_until_values]
        if any(until is not None and until > current for until in until_values):
            return str(control.get("reason") or "operator_silence")
        # A timed silence expires automatically; a control file without an
        # expiry remains an explicit permanent operator silence.
        if not any(until is not None for until in until_values):
            return str(control.get("reason") or "operator_silence")
        return None
    for field, reason in (
        ("silenced_until", "operator_silence"),
        ("maintenance_until", "planned_maintenance"),
    ):
        until = parse_time(control.get(field))
        if until is not None and until > current:
            return reason
    return None


def notifications_allowed(
    control: dict[str, Any], *, now: datetime | None = None
) -> bool:
    return suppression_reason(control, now=now) is None


def write_alert_control(
    path: Path,
    *,
    notifications_enabled: bool,
    silenced_until: datetime | None = None,
    maintenance_until: datetime | None = None,
    reason: str | None = None,
    updated_by: str = "operator",
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or utc_now()
    payload = {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "notifications_enabled": bool(notifications_enabled),
        "silenced_until": iso_time(silenced_until) if silenced_until else None,
        "maintenance_until": (
            iso_time(maintenance_until) if maintenance_until else None
        ),
        "reason": reason or ("operator_silence" if not notifications_enabled else ""),
        "updated_by": updated_by,
        "updated_at": iso_time(current),
    }
    write_json_atomic(path, payload)
    return payload


def silence_alerts(
    path: Path,
    *,
    minutes: float,
    reason: str = "operator_silence",
    maintenance: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or utc_now()
    until = current + timedelta(minutes=max(1.0, float(minutes)))
    return write_alert_control(
        path,
        notifications_enabled=False,
        silenced_until=until,
        maintenance_until=until if maintenance else None,
        reason=reason,
        now=current,
    )


def resume_alerts(path: Path, *, now: datetime | None = None) -> dict[str, Any]:
    return write_alert_control(
        path,
        notifications_enabled=True,
        silenced_until=None,
        maintenance_until=None,
        reason="",
        now=now,
    )
