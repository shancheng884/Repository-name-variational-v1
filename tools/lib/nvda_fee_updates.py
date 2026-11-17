"""Nonblocking completion of actual fees; no estimated zero fees."""
from __future__ import annotations

from decimal import Decimal
from .nvda_execution_stats import closed_child_result, failed_entry_result, hedged_entry_result


def enrich(receipts, executors, asset):
    for venue, receipt in list(receipts.items()):
        if not isinstance(receipt, dict):
            continue
        method = getattr(executors.get(venue), "enrich_receipt", None)
        if method:
            receipts[venue] = method(receipt, asset)
    return receipts


def fee_total(receipts):
    return sum((Decimal(str(r.get("fee_usd") or "0")) for r in receipts.values()), Decimal("0"))


def refresh_open_fees(engines, executors, asset):
    rebate = Decimal("0")
    for engine in engines.values():
        for pos in engine.open_tiers.values():
            changed = False
            for lot in engine._lots(pos):
                if not lot.get("entry_fee_pending"):
                    continue
                receipts = enrich(lot.get("entry_receipts") or {}, executors, asset)
                if receipts and all(v != "entropy" or r.get("fee_usd") is not None for v, r in receipts.items()):
                    old = Decimal(str(lot["entry_fee_usd"]))
                    actual = fee_total(receipts)
                    lot["entry_fee_usd"] = float(actual)
                    lot["entry_fee_pending"] = False
                    lot["entry_receipts"] = receipts
                    rebate += (actual - old) * Decimal("1.1")
                    changed = True
            if changed:
                engine._refresh(pos)
    return rebate


def finalize_costs(stats, executors, asset, engines):
    realized, rebate, events, remaining = Decimal("0"), Decimal("0"), [], []
    for item in stats.get("pending_costs", []):
        receipts = enrich(item["receipts"], executors, asset)
        recovery = item.get("recovery") or {}
        for row in recovery.get("corrections", []):
            row["receipt"] = enrich({row["venue"]: row["receipt"]}, executors, asset)[row["venue"]]
        buy, sell = item["buy"], item["sell"]
        if item["category"] == "closed":
            action = item["action"]
            entries = enrich(action.get("entry_receipts") or {}, executors, asset)
            if entries and all(v != "entropy" or r.get("fee_usd") is not None for v, r in entries.items()):
                action["entry_fee_usd"] = str(fee_total(entries) * Decimal(str(action.get("entry_fee_fraction", 1))))
            pnl = closed_child_result(action, receipts, recovery, buy, sell)
        elif item["category"] == "failed":
            pnl = failed_entry_result(receipts, recovery, buy, sell)
        else:
            _, pnl = hedged_entry_result(receipts, recovery, Decimal(item["hedge_qty"]), buy, sell)
        if not pnl["complete"]:
            remaining.append(item)
            continue
        realized += Decimal(pnl["net_usd"])
        rebate += Decimal(pnl["rebate_usd"]) - Decimal(str(item.get("rebate_credited", 0)))
        stats["unresolved_cost_events"] = max(0, stats["unresolved_cost_events"] - 1)
        if item["category"] != "closed":
            stats["rollback_net_usd"] = str(Decimal(stats["rollback_net_usd"]) + Decimal(pnl["net_usd"]))
        for engine in engines.values():
            for trade in engine.trades:
                if trade.get("execution_token") == item["execution_token"]:
                    trade.update(pnl)
        events.append({"event": "cost_finalized", "execution_token": item["execution_token"],
                       "category": item["category"], "pnl": pnl})
    stats["pending_costs"] = remaining
    return realized, rebate, events
