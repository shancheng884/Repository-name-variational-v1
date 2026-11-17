"""Execution outcomes and cash P&L, including failed-entry unwinds."""
from __future__ import annotations

from decimal import Decimal


def execution_cashflows(legs):
    cash = Decimal("0")
    fees = Decimal("0")
    rebate = Decimal("0")
    missing = []
    positions = {}
    for index, (venue, side, receipt) in enumerate(legs):
        receipt = receipt or {}
        if receipt.get("status") == "rejected":
            continue
        try:
            qty = Decimal(str(receipt["filled_qty"]))
            price = Decimal(str(receipt["avg_price"]))
            if qty <= 0 or price <= 0:
                raise ValueError("invalid_fill")
        except (KeyError, ValueError, ArithmeticError):
            missing.append(f"{venue}:{index}:fill")
            continue
        cash += qty * price * (1 if side == "sell" else -1)
        positions[venue] = positions.get(venue, Decimal("0")) + qty * (1 if side == "buy" else -1)
        raw_fee = receipt.get("fee_usd")
        # Both Lighter accounts charge no trading fee in this deployment.
        if raw_fee is None and venue.startswith("lighter"):
            raw_fee = "0"
        try:
            fee = Decimal(str(raw_fee))
        except (ValueError, ArithmeticError):
            missing.append(f"{venue}:{index}:fee")
            continue
        fees += fee
        if venue == "entropy":
            rebate += fee * Decimal("1.1")
    for venue, qty in positions.items():
        if abs(qty) > Decimal("0.00000001"):
            missing.append(f"{venue}:unbalanced:{qty}")
    return {"gross_usd": str(cash), "fees_usd": str(fees),
            "net_usd": str(cash - fees), "rebate_usd": str(rebate),
            "complete": not missing, "missing": missing}


def closed_child_result(action, receipts, recovery, buy, sell):
    entry = {"status": "filled", "filled_qty": str(action["qty"])}
    fee_venue = "entropy" if "entropy" in (buy, sell) else buy
    legs = [(buy, "buy", {**entry, "avg_price": action.get("entry_buy_price"),
                           "fee_usd": action.get("entry_fee_usd") if fee_venue == buy else "0"}),
            (sell, "sell", {**entry, "avg_price": action.get("entry_sell_price"),
                             "fee_usd": action.get("entry_fee_usd") if fee_venue == sell else "0"})]
    legs.extend((venue, "sell" if venue == buy else "buy", receipt)
                for venue, receipt in receipts.items())
    legs.extend((row["venue"], row["side"], row.get("receipt"))
                for row in (recovery or {}).get("corrections", []))
    result = execution_cashflows(legs)
    result["scope"] = "child_roundtrip"
    notional = Decimal(str(action["notional_usd"]))
    if notional > 0:
        for key in ("gross", "net"):
            result[key + "_bps"] = float(Decimal(result[key + "_usd"]) / notional * 10000)
    return result


def failed_entry_result(receipts, recovery, buy, sell):
    legs = [(venue, "buy" if venue == buy else "sell", receipt)
            for venue, receipt in receipts.items()]
    legs.extend((row["venue"], row["side"], row.get("receipt"))
                for row in (recovery or {}).get("corrections", []))
    result = execution_cashflows(legs)
    result["complete"] = result["complete"] and bool((recovery or {}).get("ok"))
    result["scope"] = "failed_entry_and_rollback"
    return result


def hedged_entry_result(receipts, recovery, hedge_qty, buy, sell):
    retained = {}
    excess = []
    for venue, receipt in receipts.items():
        filled = Decimal(str(receipt["filled_qty"]))
        ratio = hedge_qty / filled
        fee = receipt.get("fee_usd")
        retained[venue] = {**receipt, "filled_qty": str(hedge_qty),
                           "fee_usd": str(Decimal(str(fee)) * ratio) if fee is not None else None}
        if filled > hedge_qty:
            excess.append((venue, "buy" if venue == buy else "sell",
                           {**receipt, "filled_qty": str(filled - hedge_qty),
                            "fee_usd": str(Decimal(str(fee)) * (1 - ratio)) if fee is not None else None}))
    excess.extend((row["venue"], row["side"], row.get("receipt"))
                  for row in (recovery or {}).get("corrections", []))
    pnl = execution_cashflows(excess)
    pnl["scope"] = "partial_fill_excess_unwind"
    return retained, pnl


def record_attempt(stats, receipts, *, opening, hedged, recovery=None, pnl=None):
    stats["entry_attempts" if opening else "exit_attempts"] += 1
    if opening:
        key = "unverified_entries" if hedged is None else ("hedged_entries" if hedged else "failed_entries")
        stats[key] = stats.get(key, 0) + 1
    for venue, receipt in receipts.items():
        row = stats["venues"].setdefault(venue, {"attempts": 0, "rejected": 0,
                                                 "filled": 0, "unknown": 0, "partial": 0})
        row["attempts"] += 1
        status = (receipt or {}).get("status")
        row[status if status in ("rejected", "filled") else "unknown"] += 1
        if status == "filled" and receipt.get("requested_qty") is not None:
            if Decimal(str(receipt["filled_qty"])) < Decimal(str(receipt["requested_qty"])):
                row["partial"] = row.get("partial", 0) + 1
    if recovery:
        stats["rollback_orders"] += len(recovery.get("corrections", []))
        stats["rollback_failures"] += int(not recovery.get("ok"))
    if pnl is not None:
        if pnl.get("complete"):
            stats["rollback_net_usd"] = str(Decimal(stats["rollback_net_usd"]) + Decimal(pnl["net_usd"]))
        else:
            stats["unresolved_cost_events"] += 1


def new_execution_stats(saved=None):
    result = {"entry_attempts": 0, "exit_attempts": 0, "hedged_entries": 0,
              "unverified_entries": 0,
              "failed_entries": 0, "rollback_orders": 0, "rollback_failures": 0,
              "rollback_net_usd": "0", "unresolved_cost_events": 0, "venues": {}}
    if saved:
        result.update(saved)
    return result
