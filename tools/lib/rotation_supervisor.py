"""Fail-closed state and journal helpers for automatic ETH/WTI handoff."""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterator


class RotationPhase(StrEnum):
    STABLE = "stable"
    DRAINING = "draining"
    STARTING = "starting"
    RECOVERING = "recovering"
    BLOCKED = "blocked"


JOURNAL_SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"rotation_state_unreadable:{path.name}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"rotation_state_not_object:{path.name}")
    return value


def write_object_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=True, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def initial_journal() -> dict[str, Any]:
    return {
        "schema_version": JOURNAL_SCHEMA_VERSION,
        "phase": RotationPhase.STABLE.value,
        "current_pair": "NONE",
        "target_pair": "NONE",
        "request_id": None,
        "source_pid": None,
        "source_run_id": None,
        "target_run_id_before_start": None,
        "launch_attempts": 0,
        "collector_launch_attempts": 0,
        "consecutive_failures": 0,
        "updated_at": utc_now(),
    }


def validate_journal(value: dict[str, Any]) -> dict[str, Any]:
    if not value:
        return initial_journal()
    if (
        value.get("schema_version") != JOURNAL_SCHEMA_VERSION
        or value.get("phase") not in {phase.value for phase in RotationPhase}
        or value.get("current_pair") not in {"NONE", "ETH", "WTI"}
        or value.get("target_pair") not in {"NONE", "ETH", "WTI"}
    ):
        raise RuntimeError("rotation_journal_schema_invalid")
    value.setdefault("collector_launch_attempts", 0)
    value.setdefault("consecutive_failures", 0)
    return value


def verified_drain_completion(
    control: dict[str, Any],
    *,
    request_id: str,
    source_pid: int,
    source_run_id: str,
) -> bool:
    """Only the matching runtime's explicit dual-flat, zero-order ack completes a handoff."""
    if (
        control.get("status") != "completed"
        or control.get("request_id") != request_id
        or control.get("target_pid") != source_pid
        or control.get("target_run_id") != source_run_id
        or control.get("variational_active_orders") != 0
        or control.get("lighter_active_orders") != 0
    ):
        return False
    try:
        variational_qty = Decimal(str(control.get("variational_position_qty")))
        lighter_qty = Decimal(str(control.get("lighter_position_qty")))
        return (
            variational_qty.is_finite()
            and lighter_qty.is_finite()
            and variational_qty == 0
            and lighter_qty == 0
        )
    except (InvalidOperation, TypeError, ValueError):
        return False


def verified_flat_account_probe(
    report: dict[str, Any],
    *,
    expected_market_id: int,
    now: datetime | None = None,
    max_age_seconds: float = 15.0,
) -> bool:
    """Validate a fresh, read-only, market-bound dual-flat account snapshot."""
    now = now or datetime.now(timezone.utc)
    if (
        report.get("status") != "verified_read"
        or report.get("mode") != "read_only"
        or report.get("orders_attempted") is not False
        or report.get("execution_enabled") is not False
    ):
        return False
    market = report.get("lighter_market")
    snapshot = report.get("snapshot")
    if not isinstance(market, dict) or not isinstance(snapshot, dict):
        return False
    try:
        market_id = int(market.get("market_id"))
        variational_qty = Decimal(str(snapshot.get("variational_qty")))
        lighter_qty = Decimal(str(snapshot.get("lighter_qty")))
        observed_at = datetime.fromisoformat(
            str(report.get("observed_at")).replace("Z", "+00:00")
        )
        read_window_ms = Decimal(str(report.get("read_window_ms")))
        active_order_count = int(snapshot.get("active_order_count"))
    except (TypeError, ValueError, InvalidOperation, OverflowError):
        return False
    if (
        observed_at.tzinfo is None
        or observed_at.utcoffset() is None
        or market_id != expected_market_id
        or not variational_qty.is_finite()
        or not lighter_qty.is_finite()
        or variational_qty != 0
        or lighter_qty != 0
        or active_order_count != 0
        or report.get("active_order_count") != 0
        or not read_window_ms.is_finite()
        or read_window_ms < 0
        or read_window_ms > Decimal(str(max_age_seconds * 1000))
    ):
        return False
    age = (now - observed_at.astimezone(timezone.utc)).total_seconds()
    return -5.0 <= age <= max_age_seconds


@contextmanager
def exclusive_lock(path: Path) -> Iterator[None]:
    """Serialize supervisors across restarts/platforms without deleting lock files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            if path.stat().st_size == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    except (BlockingIOError, OSError) as exc:
        raise RuntimeError("rotation_supervisor_already_running") from exc
    finally:
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        handle.close()
