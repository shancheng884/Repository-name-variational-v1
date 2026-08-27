from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


PNL_BASELINE_FILE_NAME = "pnl_reporting_baseline.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_pnl_baseline(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def write_pnl_baseline(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "updated_at": utc_now(),
        **value,
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def new_pnl_baseline(
    *,
    asset: str,
    realized_pnl_usd: Any,
    completed_cycles: Any,
    started_at: str | None = None,
) -> dict[str, Any]:
    timestamp = started_at or utc_now()
    return {
        "asset": asset.upper(),
        "started_at": timestamp,
        "realized_pnl_baseline_usd": str(realized_pnl_usd or "0"),
        "completed_cycles_baseline": int(completed_cycles or 0),
        "confirmed_pnl_usd": "0",
        "tracked_completed_cycles": 0,
        "counted_cycle_keys": [],
        "account_baseline_equity_usd": None,
        "account_baseline_at": None,
        "external_cashflow_usd": "0",
        "external_cashflow_events": [],
    }


def set_pnl_account_baseline(
    path: Path,
    *,
    combined_equity_usd: Any,
    captured_at: str,
) -> dict[str, Any] | None:
    baseline = load_pnl_baseline(path)
    if baseline is None:
        return None
    if baseline.get("account_baseline_equity_usd") not in (None, ""):
        return baseline
    updated = {
        **baseline,
        "account_baseline_equity_usd": str(combined_equity_usd),
        "account_baseline_at": captured_at,
    }
    write_pnl_baseline(path, updated)
    return load_pnl_baseline(path)


def record_pnl_cycle(
    path: Path,
    *,
    run_id: str,
    asset: str,
    lot_id: Any,
    actual_pnl_usd: Any,
) -> dict[str, Any] | None:
    baseline = load_pnl_baseline(path)
    if baseline is None:
        return None
    cycle_key = f"{run_id}:{asset.upper()}:{lot_id}"
    counted = [str(value) for value in baseline.get("counted_cycle_keys", [])]
    if cycle_key in counted:
        return baseline
    try:
        cumulative = Decimal(str(baseline.get("confirmed_pnl_usd") or "0"))
        cycle_pnl = Decimal(str(actual_pnl_usd))
    except (InvalidOperation, TypeError, ValueError):
        return baseline
    updated = {
        **baseline,
        "confirmed_pnl_usd": str(cumulative + cycle_pnl),
        "tracked_completed_cycles": int(
            baseline.get("tracked_completed_cycles") or 0
        )
        + 1,
        "counted_cycle_keys": [*counted, cycle_key][-1000:],
    }
    write_pnl_baseline(path, updated)
    return load_pnl_baseline(path)


def record_external_cashflow(
    path: Path,
    *,
    amount_usd: Any,
    observed_at: str,
    reason: str,
) -> dict[str, Any] | None:
    baseline = load_pnl_baseline(path)
    if baseline is None:
        return None
    try:
        amount = Decimal(str(amount_usd))
        cumulative = Decimal(str(baseline.get("external_cashflow_usd") or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return baseline
    events = [
        value
        for value in list(baseline.get("external_cashflow_events") or [])
        if isinstance(value, dict)
    ]
    updated = {
        **baseline,
        "external_cashflow_usd": str(cumulative + amount),
        "external_cashflow_events": [
            *events,
            {
                "amount_usd": str(amount),
                "observed_at": observed_at,
                "reason": reason,
            },
        ][-100:],
    }
    write_pnl_baseline(path, updated)
    return load_pnl_baseline(path)
