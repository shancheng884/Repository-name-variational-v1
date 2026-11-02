#!/usr/bin/env python3
"""Read-only audit of the most recently closed WTI round trip."""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.wti_execution import WTIPairExecutor


def _decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def build_report(state: dict[str, Any]) -> dict[str, Any]:
    position = state.get("last_closed_position")
    if not isinstance(position, dict):
        return {"status": "no_closed_position", "orders_attempted": False}

    breakdown = WTIPairExecutor._execution_cost_evidence(
        position,
        execution_history=state.get("execution_history"),
        exit_execution_id=position.get("exit_execution_id"),
    )
    gross = _decimal(position.get("actual_gross_pnl_usd"))
    fees = _decimal(breakdown.get("actual_fees_usd"))
    if breakdown.get("all_fills_verified") and breakdown.get("all_fees_verified_usd"):
        status = "fills_and_fees_verified"
    elif breakdown.get("all_fills_verified"):
        status = "fills_verified_fees_incomplete"
    else:
        status = "partial_evidence"
    return {
        "status": status,
        "orders_attempted": False,
        "entry_execution_id": position.get("entry_execution_id"),
        "exit_execution_id": position.get("exit_execution_id"),
        "actual_gross_pnl_usd": str(gross) if gross is not None else None,
        "actual_fees_usd": format(fees.normalize(), "f") if fees is not None else None,
        "confirmed_fee_usd_partial": breakdown.get("confirmed_fee_usd_partial"),
        "pnl_after_confirmed_trading_fees_before_funding_usd": (
            format((gross - fees).normalize(), "f")
            if gross is not None and fees is not None else None
        ),
        "actual_net_pnl_usd": position.get("actual_net_pnl_usd"),
        "actual_net_pnl_status": position.get("actual_net_pnl_status"),
        "funding_cashflow_usd": position.get("funding_cashflow_usd"),
        "funding_cashflow_confirmed": position.get("funding_cashflow_confirmed"),
        "execution_cost_evidence": breakdown,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only WTI realized fill, slippage, and fee evidence audit."
    )
    parser.add_argument("--state-file", type=Path, default=Path("log/wti_live_state.json"))
    args = parser.parse_args(argv)
    try:
        state = json.loads(args.state_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({
            "status": "unavailable",
            "orders_attempted": False,
            "error": type(exc).__name__,
        }, sort_keys=True))
        return 2
    if not isinstance(state, dict):
        print(json.dumps({"status": "invalid_state", "orders_attempted": False}))
        return 2
    report = build_report(state)
    print(json.dumps(report, ensure_ascii=True, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
