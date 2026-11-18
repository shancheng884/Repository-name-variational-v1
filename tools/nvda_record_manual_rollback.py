"""Reconcile a manually closed, isolated Entropy NVDA rollback. Never places orders."""
from __future__ import annotations

import argparse
import asyncio
import copy
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import shutil

from tools.lib.nvda_fee_updates import finalize_costs
from tools.market_capture.config import DEFAULT_ASSETS, load_environment
from tools.market_capture.runner import _acquire_lock
from tools.nvda_tier_live import ENGINE_STATE, ORDER_LOG, ROOT, build_executors, position_snapshot


def reconcile(state, token, oid, fills, positions):
    venues = {"entropy", "lighter_rh"}
    if (set(positions) != venues
            or any(not r["verified"] or r["active_orders"] != 0
                   or Decimal(r["qty"]) != 0 for r in positions.values())):
        raise ValueError("both NVDA accounts must be verified flat and order-free")
    if state["asset"] != "NVDA" or state["pair"] != "entropy>lighter_rh":
        raise ValueError("unexpected strategy")
    state = copy.deepcopy(state)
    engines = state["engines"]
    pending = engines["reverse"].get("pending_action") or {}
    stats = state["execution_stats"]
    matches = [r for r in stats.get("pending_costs", []) if r["execution_token"] == token]
    if not pending and not matches:
        return state, None
    if (any(e.get("open_tiers") for e in engines.values()) or engines["forward"].get("pending_action")
            or pending.get("action") != "open" or pending.get("token") != token or len(matches) != 1):
        raise ValueError("unexpected ledger or order reservation")
    item = matches[0]
    ent, rh = (item["receipts"][v] for v in ("entropy", "lighter_rh"))
    recovery = item["recovery"]
    if (item["category"] != "failed" or item["buy"] != "lighter_rh" or item["sell"] != "entropy"
            or rh["status"] != "rejected" or ent["status"] != "filled" or recovery["ok"]
            or any(r["receipt"]["status"] != "rejected" for r in recovery["corrections"])):
        raise ValueError("not an isolated uncorrected Entropy short")
    since = datetime.fromisoformat(ent["submitted_at"]).timestamp() * 1000
    rows = {str(r["tid"]): r for r in fills if str(r.get("oid")) == oid}
    if not rows or any(r.get("coin") != "io:NVDA" or r.get("side") != "B"
                       or r.get("dir") != "Close Short" or float(r["time"]) <= since
                       or r.get("feeToken") not in (None, "USDC") for r in rows.values()):
        raise ValueError("manual fill identity or direction does not match")
    qty = sum(Decimal(r["sz"]) for r in rows.values())
    if qty <= 0 or qty != Decimal(ent["filled_qty"]):
        raise ValueError("manual fill size does not match the failed entry")
    price = sum(Decimal(r["px"]) * Decimal(r["sz"]) for r in rows.values()) / qty
    fee = sum(Decimal(r["fee"]) for r in rows.values())
    receipt = {"status": "filled", "order_id": oid, "filled_qty": str(qty), "avg_price": str(price),
               "fee_usd": str(fee), "source_at": datetime.fromtimestamp(
                   max(float(r["time"]) for r in rows.values()) / 1000, timezone.utc).isoformat()}
    recovery["ok"] = True
    recovery["corrections"].append({"venue": "entropy", "side": "buy", "qty": str(qty), "receipt": receipt})
    # Leave any unrelated incomplete costs untouched.
    other = [r for r in stats["pending_costs"] if r is not item]
    stats["pending_costs"] = [item]
    realized, rebate, events = finalize_costs(stats, {}, None, {})
    if len(events) != 1:
        raise ValueError("manual rollback costs cannot be proven")
    stats["pending_costs"] = other
    state["realized_usd"] = str(Decimal(state["realized_usd"]) + realized)
    state["rebate_usd"] = str(Decimal(state["rebate_usd"]) + rebate)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    engines["reverse"]["pending_action"] = None
    stats["manually_reconciled_rollbacks"] = stats.get("manually_reconciled_rollbacks", 0) + 1
    return state, {**events[0], "ts": state["updated_at"], "manual_close_receipt": receipt}


async def run(token, oid, apply):
    guard = _acquire_lock(ROOT / "log/nvda_tier_live.lock")
    ex = {}
    try:
        state = json.loads(ENGINE_STATE.read_text())
        venues = ("entropy", "lighter_rh")
        ex = {v: e for v, e in build_executors(load_environment()).items() if v in venues}
        ex["entropy"]._enable_fill_stream = False
        for e in ex.values():
            await e.start()
        asset = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")
        e = ex["entropy"]
        item = next((r for r in state["execution_stats"].get("pending_costs", [])
                     if r["execution_token"] == token), None)
        fills = []
        if item:
            since = datetime.fromisoformat(item["receipts"]["entropy"]["submitted_at"])
            fills = await asyncio.to_thread(e.info.user_fills_by_time, e.account_address, int(since.timestamp()*1000))
        snap = await position_snapshot(ex, asset, venues)
        updated, event = reconcile(state, token, oid, fills, snap)
        if event and apply:
            backup = ENGINE_STATE.with_name(ENGINE_STATE.name + ".before-manual-" + oid)
            shutil.copy2(ENGINE_STATE, backup)
            tmp = ENGINE_STATE.with_suffix(".manual.tmp")
            tmp.write_text(json.dumps(updated) + "\n")
            os.replace(tmp, ENGINE_STATE)
            with ORDER_LOG.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event) + "\n")
        print(json.dumps({"applied": bool(event and apply), "positions": snap, "event": event}))
    finally:
        for e in ex.values():
            await e.close()
        guard.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", required=True)
    parser.add_argument("--close-order-id", required=True, type=int)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.token, str(args.close_order_id), args.apply))
