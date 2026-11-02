from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

POSITION_TOLERANCE_ABS = Decimal("0.0005")
POSITION_TOLERANCE_REL = Decimal("0.01")
RECONCILED_STATUSES = {
    "both_exchanges_flat",
    "both_exchanges_matched",
    "positions_matched",
}
STARTUP_RECONCILE_EVENT = "live_inventory_startup_reconcile_ok"


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _age_seconds(stamp: Any, *, now: datetime) -> float | None:
    if not stamp:
        return None
    try:
        parsed = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (now - parsed).total_seconds()


def lot_signature(lots: Iterable[dict[str, Any]]) -> list[tuple[str, str]]:
    signature = []
    for lot in lots or []:
        if not isinstance(lot, dict):
            continue
        signature.append(
            (
                str(lot.get("lot_id")),
                str(_decimal(lot.get("qty")) or lot.get("qty")),
            )
        )
    return sorted(signature)


def expected_qty(lots: Iterable[dict[str, Any]]) -> Decimal:
    total = Decimal("0")
    for lot in lots or []:
        if not isinstance(lot, dict):
            continue
        qty = _decimal(lot.get("qty"))
        if qty is not None:
            total += abs(qty)
    return total


def assess_state(
    state: dict[str, Any],
    *,
    asset: str,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(state, dict) or not state:
        return {"ok": False, "reasons": ["state_unavailable"]}

    state_asset = str(state.get("asset") or "").upper()
    if state_asset != asset.upper():
        reasons.append(f"asset_mismatch:{state_asset or 'missing'}")

    status = str(state.get("status") or "")
    if status not in {"open", "flat"}:
        reasons.append(f"state_status_unexpected:{status or 'missing'}")

    lots = state.get("open_lots") or []
    pending = state.get("pending_actions") or []
    manual = state.get("manual_review_reason")
    if pending:
        reasons.append(f"pending_actions:{len(pending)}")
    if manual:
        reasons.append(f"manual_review:{manual}")
    if status == "open" and not lots:
        reasons.append("state_open_without_lots")
    if status == "flat" and lots:
        reasons.append("state_flat_with_lots")

    return {
        "ok": not reasons,
        "reasons": reasons,
        "asset": state_asset,
        "status": status,
        "lots": len(lots),
        "pending": len(pending),
        "expected_qty": str(expected_qty(lots)),
        "signature": lot_signature(lots),
    }


def assess_health(
    health: dict[str, Any],
    *,
    now: datetime,
    max_age_seconds: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(health, dict) or not health:
        return {"ok": False, "reasons": ["health_unavailable"]}

    age = _age_seconds(health.get("updated_at"), now=now)
    if age is None:
        reasons.append("health_timestamp_unreadable")
    elif age > max_age_seconds:
        reasons.append(f"health_stale:{round(age, 1)}s")
    elif age < -max_age_seconds:
        reasons.append(f"health_clock_ahead:{round(age, 1)}s")

    manual = health.get("manual_review_reason")
    if manual:
        reasons.append(f"health_manual_review:{manual}")
    if not health.get("risk_action"):
        reasons.append("health_risk_action_missing")

    return {
        "ok": not reasons,
        "reasons": reasons,
        "age_seconds": None if age is None else round(age, 1),
        "risk_action": health.get("risk_action"),
        "risk_reason": health.get("risk_reason"),
        "run_id": health.get("run_id"),
    }


def summarize_positions(
    *,
    variational_qty: Any,
    lighter_qty: Any,
    expected: Any,
) -> dict[str, Any]:
    var = _decimal(variational_qty)
    lig = _decimal(lighter_qty)
    exp = _decimal(expected)
    reasons: list[str] = []

    if var is None or lig is None or exp is None:
        return {
            "ok": False,
            "reasons": ["position_values_unreadable"],
            "variational_qty": variational_qty,
            "lighter_qty": lighter_qty,
            "expected_qty": expected,
        }

    tolerance = max(POSITION_TOLERANCE_ABS, exp * POSITION_TOLERANCE_REL)
    if abs(abs(var) - exp) > tolerance:
        reasons.append(f"variational_qty_mismatch:{var}")
    if abs(abs(lig) - exp) > tolerance:
        reasons.append(f"lighter_qty_mismatch:{lig}")
    if abs(abs(var) - abs(lig)) > tolerance:
        reasons.append(f"venues_imbalanced:var={var},lighter={lig}")

    return {
        "ok": not reasons,
        "reasons": reasons,
        "variational_qty": str(var),
        "lighter_qty": str(lig),
        "expected_qty": str(exp),
        "tolerance": str(tolerance),
    }


def tail_events(path: Path, *, limit: int = 4000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows[-limit:]


def find_startup_reconcile(rows: Iterable[dict[str, Any]], *, run_id: str) -> dict[str, Any] | None:
    found = None
    for row in rows:
        if row.get("event") != STARTUP_RECONCILE_EVENT:
            continue
        if run_id and str(row.get("run_id")) != run_id:
            continue
        found = row
    return found


def classify_reconcile(event: dict[str, Any] | None, *, expected: Any) -> dict[str, Any]:
    if not event:
        return {"ok": False, "reasons": ["startup_reconcile_event_missing"]}
    status = str(event.get("status") or "")
    if status not in RECONCILED_STATUSES:
        return {"ok": False, "reasons": [f"startup_reconcile_status:{status or 'missing'}"], "status": status}
    return {
        "ok": True,
        "reasons": [],
        "status": status,
        "positions": summarize_positions(
            variational_qty=event.get("variational_position_qty"),
            lighter_qty=event.get("lighter_position_qty"),
            expected=expected,
        ),
    }


def build_launch_command(
    *,
    repo: Path,
    asset: str,
    resume: bool,
    session: str,
    log_path: Path,
) -> str:
    flags = [
        "--asset",
        asset.upper(),
        "--v4-live",
        "--v4-real-gradient",
        "--v4-elastic-capacity",
        "--v4-bidirectional",
        "--v4-continuous",
    ]
    if resume:
        flags.append("--resume-open-position")
    joined = " ".join(flags)
    return (
        f"cd {repo} && set -a && source .env && set +a && "
        f"exec .venv/bin/python tools/live.py {joined} >> {log_path} 2>&1"
    )
