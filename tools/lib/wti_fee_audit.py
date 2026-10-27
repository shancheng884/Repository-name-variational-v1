"""Read-only, exact-order fee audit for the latest closed WTI position."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable


Lookup = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]


def _decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


async def audit_latest_closed_position_fees(
    state: dict[str, Any], lookup_order: Lookup,
) -> dict[str, Any]:
    """Recheck every persisted entry-layer and exit fill; never mutates state."""
    position = state.get("last_closed_position")
    if not isinstance(position, dict):
        return {
            "status": "unverified",
            "reason": "closed_position_not_found",
            "orders_attempted": False,
            "state_modified": False,
            "fees_usd": None,
            "funding_status": "unverified",
            "legs": [],
        }
    executions = state.get("execution_history")
    by_id = {
        str(item.get("action_id")): item
        for item in executions if isinstance(item, dict) and item.get("action_id")
    } if isinstance(executions, list) else {}
    layers = position.get("layers")
    entry_ids = [
        str(layer.get("entry_execution_id"))
        for layer in layers
        if isinstance(layer, dict) and layer.get("entry_execution_id")
    ] if isinstance(layers, list) else []
    if not entry_ids:
        fallback_id = position.get("last_entry_execution_id") or position.get("entry_execution_id")
        if fallback_id:
            entry_ids = [str(fallback_id)]
    entry_ids = list(dict.fromkeys(entry_ids))
    leg_specs = [
        (f"entry_layer_{index}", action_id, "open")
        for index, action_id in enumerate(entry_ids, start=1)
    ]
    leg_specs.append(("exit", position.get("exit_execution_id"), "close"))
    expected_leg_count = len(leg_specs) * 2
    legs: list[dict[str, Any]] = []
    fee_total = Decimal("0")
    all_confirmed = True
    for phase, action_id, expected_kind in leg_specs:
        execution = by_id.get(str(action_id or ""))
        if not isinstance(execution, dict) or execution.get("kind") != expected_kind:
            all_confirmed = False
            legs.append({
                "phase": phase,
                "status": "unverified",
                "reason": "persisted_execution_identity_missing",
            })
            continue
        execution_legs = execution.get("legs")
        if not isinstance(execution_legs, dict):
            execution_legs = {}
        for venue in ("variational", "lighter"):
            leg = execution_legs.get(venue)
            request = leg.get("request") if isinstance(leg, dict) else None
            stored = leg.get("result") if isinstance(leg, dict) else None
            client_order_id = request.get("client_order_id") if isinstance(request, dict) else None
            if not client_order_id or not isinstance(stored, dict):
                all_confirmed = False
                legs.append({
                    "phase": phase, "venue": venue,
                    "status": "unverified",
                    "reason": "persisted_order_identity_missing",
                })
                continue
            try:
                result = await lookup_order(str(client_order_id), dict(request))
            except Exception as exc:
                result = {
                    "status": "unknown",
                    "lookup_reason": "fee_audit_lookup_failed",
                    "error_type": type(exc).__name__,
                }
            result = result if isinstance(result, dict) else {}
            expected_qty = _decimal(stored.get("filled_qty"))
            observed_qty = _decimal(result.get("filled_qty"))
            expected_price = _decimal(stored.get("average_price"))
            observed_price = _decimal(result.get("average_price"))
            identity_matches = (
                str(result.get("client_order_id") or "") == str(client_order_id)
                and result.get("status") == "filled"
                and expected_qty is not None and observed_qty == expected_qty
                and expected_price is not None and observed_price == expected_price
            )
            fee_evidence = result.get("fee_evidence")
            fee = _decimal(result.get("fee_usd"))
            confirmed = bool(
                identity_matches
                and isinstance(fee_evidence, dict)
                and fee_evidence.get("status") == "confirmed_usd"
                and fee_evidence.get("raw_fee_unit") == "USD"
                and fee is not None and fee >= 0
                and _decimal(fee_evidence.get("fee_usd")) == fee
            )
            if confirmed:
                fee_total += fee
            else:
                all_confirmed = False
            legs.append({
                "phase": phase,
                "venue": venue,
                "client_order_id": str(client_order_id),
                "native_order_id": result.get("native_order_id"),
                "status": "confirmed_usd" if confirmed else "unverified",
                "identity_matches_persisted_fill": identity_matches,
                "filled_qty": None if observed_qty is None else str(observed_qty),
                "average_price": None if observed_price is None else str(observed_price),
                "fee_usd": str(fee) if confirmed else None,
                "fee_evidence": fee_evidence if isinstance(fee_evidence, dict) else None,
                "lookup_reason": result.get("lookup_reason"),
                "error_type": result.get("error_type"),
            })
    return {
        "status": "confirmed" if all_confirmed and len(legs) == expected_leg_count else "unverified",
        "reason": None if all_confirmed and len(legs) == expected_leg_count else "one_or_more_exact_usd_fee_proofs_missing",
        "orders_attempted": False,
        "state_modified": False,
        "entry_execution_id": position.get("entry_execution_id"),
        "entry_execution_ids": entry_ids,
        "exit_execution_id": position.get("exit_execution_id"),
        "expected_order_leg_count": expected_leg_count,
        "audited_order_leg_count": len(legs),
        "fees_usd": str(fee_total) if all_confirmed and len(legs) == expected_leg_count else None,
        "funding_status": "unverified",
        "legs": legs,
    }
