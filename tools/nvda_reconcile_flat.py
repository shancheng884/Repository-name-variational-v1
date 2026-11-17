#!/usr/bin/env python3
"""Repair a stale NVDA close ledger only with paired fill receipts and flat accounts.

Never sends, cancels or retries an order. Defaults to a read-only recovery plan.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.nvda_execution_stats import closed_child_result
from tools.market_capture.config import DEFAULT_ASSETS, load_environment
from tools.market_capture.runner import _acquire_lock
from tools.nvda_tier_live import build_executors, position_snapshot


def recovery_plan(saved, rows, positions):
    if saved.get("asset") != "NVDA":
        raise ValueError("only_nvda_recovery_supported")
    buy, sell = saved["pair"].split(">")
    if set(positions) != {buy, sell} or not all(
        r.get("verified") and r.get("active_orders") == 0 and Decimal(str(r["qty"])) == 0
        for r in positions.values()
    ):
        raise ValueError("both_accounts_must_be_verified_flat_without_orders")
    state = copy.deepcopy(saved)
    recovered = []
    for direction, engine in state["engines"].items():
        pending = engine.get("pending_action")
        if pending and pending.get("action") != "close":
            raise ValueError("unresolved_open_requires_order_identity_recovery")
        b, s = (buy, sell) if direction == "forward" else (sell, buy)
        for pos in engine.get("open_tiers") or []:
            children = pos.get("children") or [pos]
            if len(children) != 1:
                raise ValueError("multi_child_recovery_requires_individual_fill_matching")
            lot = children[0]
            matches = [row for row in rows
                       if row.get("event") in ("closed", "close_failed")
                       and row.get("direction") == direction
                       and row.get("tier") == pos["index"]
                       and datetime.fromisoformat(row["ts"]).timestamp() >= pos["opened_at"]
                       and all(row.get("receipts", {}).get(v, {}).get("status") == "filled"
                               and Decimal(str(row["receipts"][v].get("filled_qty", 0))) == Decimal(str(pos["qty"]))
                               for v in (b, s))]
            if not matches:
                raise ValueError("no_exact_paired_close_receipts")
            matched = matches[0]
            action = {**lot, "qty": pos["qty"], "notional_usd": pos["notional_usd"]}
            pnl = closed_child_result(action, matched["receipts"], {}, b, s)
            if not pnl["complete"]:
                raise ValueError("paired_close_cost_incomplete")
            identity = hashlib.sha256(json.dumps(matched["receipts"], sort_keys=True).encode()).hexdigest()
            state["realized_usd"] = str(Decimal(state["realized_usd"]) + Decimal(pnl["net_usd"]))
            fee = Decimal(str(matched["receipts"].get("entropy", {}).get("fee_usd") or "0"))
            state["rebate_usd"] = str(Decimal(state["rebate_usd"]) + fee * Decimal("1.1"))
            engine.setdefault("trades", []).append({"tier": pos["index"], "reason": "reconciled_close",
                                                     "notional_usd": pos["notional_usd"],
                                                     "hold_s": datetime.fromisoformat(matched["ts"]).timestamp() - pos["opened_at"],
                                                     **pnl})
            stats = state.setdefault("execution_stats", {})
            stats["reconciled_closes"] = stats.get("reconciled_closes", 0) + 1
            recovered.append({"event": "closed_reconciled", "pair": saved["pair"],
                              "direction": direction, "tier": pos["index"], "qty": str(pos["qty"]),
                              "pnl": pnl, "matched_close_at": matched["ts"], "recovery_id": identity,
                              "note": "Paired closing fills verified; both accounts flat. No order sent."})
        engine["open_tiers"] = []
        engine["pending_action"] = None
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    return state, recovered


async def run(apply):
    guard = _acquire_lock(ROOT / "log/nvda_tier_live.lock")
    path = ROOT / "log/nvda_tier_live_engine.json"
    journal = ROOT / "log/nvda_tier_orders.jsonl"
    saved = json.loads(path.read_text())
    venues = saved["pair"].split(">")
    asset = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")
    ex = {v: e for v, e in build_executors(load_environment()).items() if v in venues}
    try:
        for e in ex.values():
            await e.start()
        first = await position_snapshot(ex, asset, venues)
        rows = [json.loads(line) for line in journal.read_text().splitlines() if line.strip()]
        recovery_plan(saved, rows, first)
        await asyncio.sleep(.5)
        positions = await position_snapshot(ex, asset, venues)
        state, recovered = recovery_plan(saved, rows, positions)
        if apply and recovered:
            backup = path.with_name(path.name + ".before-flat-recovery-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
            shutil.copy2(path, backup)
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps(state) + "\n")
            os.replace(temp, path)
            with journal.open("a") as out:
                for row in recovered:
                    out.write(json.dumps({"ts": state["updated_at"], **row}) + "\n")
        print(json.dumps({"applied": bool(apply and recovered), "positions": positions,
                          "recovered": recovered, "realized_usd": state["realized_usd"]}))
    finally:
        for e in ex.values():
            await e.close()
        guard.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    asyncio.run(run(parser.parse_args().apply))
