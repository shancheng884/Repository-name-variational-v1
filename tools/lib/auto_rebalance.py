from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2
ACTIVE_STATES = {
    "pause_add",
    "preflight",
    "submit_withdrawal",
    "confirm_wallet_credit",
    "submit_deposit",
    "confirm_venue_credit",
    "refresh_risk",
}
TERMINAL_STATES = {"complete", "manual_review"}
TRANSITIONS = {
    "pause_add": {"preflight", "manual_review"},
    "preflight": {"submit_withdrawal", "manual_review"},
    "submit_withdrawal": {"confirm_wallet_credit", "manual_review"},
    "confirm_wallet_credit": {"submit_deposit", "manual_review"},
    "submit_deposit": {"confirm_venue_credit", "manual_review"},
    "confirm_venue_credit": {"refresh_risk", "manual_review"},
    "refresh_risk": {"complete", "pause_add", "manual_review"},
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_rebalance_plan(
    *,
    variational_equity_usd: Decimal,
    lighter_equity_usd: Decimal,
    block_ratio: Decimal = Decimal("0.74"),
    target_imbalance_pct: Decimal = Decimal("6.5"),
) -> dict[str, Any]:
    if variational_equity_usd <= 0 or lighter_equity_usd <= 0:
        raise ValueError("venue equity must be positive")
    smaller = min(variational_equity_usd, lighter_equity_usd)
    larger = max(variational_equity_usd, lighter_equity_usd)
    ratio = smaller / larger
    required = ratio < block_ratio
    source = "variational" if variational_equity_usd > lighter_equity_usd else "lighter"
    destination = "lighter" if source == "variational" else "variational"
    difference = abs(variational_equity_usd - lighter_equity_usd)
    target_difference = (
        (variational_equity_usd + lighter_equity_usd)
        * target_imbalance_pct
        / Decimal("200")
    )
    amount = max(Decimal("0"), (difference - target_difference) / Decimal("2"))
    return {
        "required": required,
        "equity_balance_ratio": str(ratio),
        "source_venue": source if required else None,
        "destination_venue": destination if required else None,
        "amount_usd": str(amount if required else Decimal("0")),
        "target_imbalance_pct": str(target_imbalance_pct),
    }


def new_rebalance_request(plan: dict[str, Any]) -> dict[str, Any]:
    if not plan.get("required"):
        raise ValueError("rebalance plan is not required")
    timestamp = utc_now()
    return {
        "schema_version": SCHEMA_VERSION,
        "request_id": uuid.uuid4().hex,
        "idempotency_key": uuid.uuid4().hex,
        "state": "pause_add",
        "created_at": timestamp,
        "updated_at": timestamp,
        "withdrawal_id": None,
        "wallet_credit_tx_hash": None,
        "deposit_id": None,
        "destination_credit_id": None,
        "failure_reason": None,
        **plan,
    }


def validate_rebalance_preflight(
    request: dict[str, Any],
    *,
    source_available_usd: Decimal,
    source_maintenance_margin_usd: Decimal,
    source_safety_buffer_usd: Decimal,
    estimated_transfer_fee_usd: Decimal = Decimal("0"),
    minimum_transfer_usd: Decimal = Decimal("1"),
) -> dict[str, Any]:
    amount = Decimal(str(request.get("amount_usd") or "0"))
    required_after_transfer = (
        source_maintenance_margin_usd + source_safety_buffer_usd
    )
    post_transfer_available = (
        source_available_usd - amount - estimated_transfer_fee_usd
    )
    reasons: list[str] = []
    if amount < minimum_transfer_usd:
        reasons.append("transfer_amount_below_minimum")
    if post_transfer_available < required_after_transfer:
        reasons.append("source_margin_buffer_would_be_insufficient")
    return {
        "ready": not reasons,
        "reasons": reasons,
        "amount_usd": str(amount),
        "source_available_usd": str(source_available_usd),
        "source_maintenance_margin_usd": str(source_maintenance_margin_usd),
        "source_safety_buffer_usd": str(source_safety_buffer_usd),
        "estimated_transfer_fee_usd": str(estimated_transfer_fee_usd),
        "post_transfer_available_usd": str(post_transfer_available),
        "required_post_transfer_available_usd": str(required_after_transfer),
    }


def risk_recovered_after_rebalance(
    *,
    risk_action: str,
    wallet_credit_confirmed: bool,
    destination_credit_confirmed: bool,
    refreshed_equities: bool,
) -> bool:
    return bool(
        wallet_credit_confirmed
        and destination_credit_confirmed
        and refreshed_equities
        and risk_action in {"normal", "warning"}
    )


def create_rebalance_request_if_idle(
    path: Path,
    plan: dict[str, Any],
) -> dict[str, Any]:
    active = load_active_rebalance(path)
    if active is not None:
        return active
    request = new_rebalance_request(plan)
    write_rebalance_request(path, request)
    return request


def transition_rebalance_request(
    request: dict[str, Any],
    state: str,
    **updates: Any,
) -> dict[str, Any]:
    current = str(request.get("state") or "")
    if state not in TRANSITIONS.get(current, set()):
        raise ValueError(f"invalid rebalance transition: {current}->{state}")
    if current in TERMINAL_STATES:
        raise ValueError("terminal rebalance request cannot transition")
    return {**request, **updates, "state": state, "updated_at": utc_now()}


def load_active_rebalance(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("state") not in ACTIVE_STATES:
        return None
    return value


def write_rebalance_request(path: Path, request: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(request, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
