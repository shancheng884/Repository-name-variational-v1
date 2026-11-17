#!/usr/bin/env python3
"""Read-only rejection, latency and unwind-cost report from the order journal."""
from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.nvda_execution_stats import failed_entry_result, new_execution_stats, record_attempt


def report(path):
    stats = new_execution_stats()
    known_net = Decimal("0")
    missing = []
    latency = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        event = row.get("event")
        receipts = row.get("receipts") or {}
        if event not in ("opened", "open_failed", "closed", "close_failed"):
            continue
        recovery = row.get("recovery") or row.get("settle")
        pnl = row.get("pnl")
        if event == "open_failed" and pnl is None:
            buy, sell = row["pair"].split(">")
            if row.get("direction") == "reverse":
                buy, sell = sell, buy
            pnl = failed_entry_result(receipts, recovery, buy, sell)
        record_attempt(stats, receipts, opening=event in ("opened", "open_failed"),
                       hedged=event != "open_failed", recovery=recovery,
                       pnl=pnl if event == "open_failed" else None)
        for result in (pnl, row.get("excess_pnl")):
            if result is None:
                continue
            if result.get("complete"):
                known_net += Decimal(str(result["net_usd"]))
            else:
                missing.append({"ts": row.get("ts"), "event": event,
                                "missing": result.get("missing") or ["legacy_or_unverified_cost"]})
        for venue, receipt in receipts.items():
            value = receipt.get("submit_to_ack_ms")
            if value is not None:
                latency.setdefault(venue, []).append(float(value))
    for venue, values in latency.items():
        values.sort()
        latency[venue] = {"n": len(values), "p50_ms": values[int((len(values) - 1) * .5)],
                          "p80_ms": values[int((len(values) - 1) * .8)]}
    return {"execution_stats": stats, "verified_net_usd": str(known_net),
            "missing_cost_events": missing, "ack_latency": latency,
            "note": "Only complete cashflows enter verified_net_usd; rebates are excluded."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-log", type=Path, default=ROOT / "log/nvda_tier_orders.jsonl")
    args = parser.parse_args()
    print(json.dumps(report(args.order_log), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
