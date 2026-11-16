#!/usr/bin/env python3
"""Per-leg fee ledger for reconciliation.

Every probe fill already carries the venue's own ``fee_usd``. This tool turns
those into a flat ledger (one row per leg) plus a summary, so the entropy 110%
rebate can be checked against reality later without re-deriving anything.
"""
from __future__ import annotations
import argparse, json
from collections import defaultdict
from decimal import Decimal as D
from pathlib import Path

BPS = D("10000")
REBATE = {"entropy": D("1.10")}


def rounds(path):
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or '"probe_round"' not in line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("status") == "filled":
                yield row


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", default="log/venue_probe/rounds.jsonl")
    parser.add_argument("--out", default="log/venue_probe/fee_ledger.jsonl")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    rows = []
    summary = defaultdict(lambda: {"legs": 0, "gross_usd": D("0"), "gross_bps": D("0"),
                                   "rebate_usd": D("0"), "net_usd": D("0")})
    for row in rounds(args.rounds):
        if args.run_id and row.get("run_id") != args.run_id:
            continue
        for side in ("entry", "exit"):
            for venue, leg in ((row.get(side) or {}).get("legs") or {}).items():
                fee = leg.get("fee_usd")
                if fee in (None, ""):
                    continue
                try:
                    fee_usd = D(str(fee))
                    qty = D(str(leg["filled_qty"]))
                    price = D(str(leg["avg_price"]))
                except (KeyError, TypeError, ValueError):
                    continue
                if qty <= 0 or price <= 0:
                    continue
                notional = qty * price
                fee_bps = fee_usd / notional * BPS
                rate = REBATE.get(venue, D("0"))
                rebate = fee_usd * rate
                net = fee_usd - rebate
                rows.append({
                    "run_id": row.get("run_id"), "round_id": row.get("round_id"),
                    "asset": row.get("asset"), "venue": venue, "side": side,
                    "client_order_id": leg.get("client_order_id"),
                    "filled_qty": str(qty), "avg_price": str(price),
                    "notional_usd": str(notional), "fee_usd": str(fee_usd),
                    "fee_bps": str(fee_bps), "rebate_rate": str(rate),
                    "rebate_usd": str(rebate), "net_fee_usd": str(net),
                    "source_at": leg.get("source_at"),
                })
                bucket = summary[venue]
                bucket["legs"] += 1
                bucket["gross_usd"] += fee_usd
                bucket["gross_bps"] += fee_bps
                bucket["rebate_usd"] += rebate
                bucket["net_usd"] += net

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + ("\n" if rows else ""))
    print("ledger rows %d -> %s" % (len(rows), out))
    print("%-16s %5s %12s %10s %12s %12s" % ("venue", "legs", "gross_usd", "gross_bps", "rebate_usd", "net_usd"))
    total_gross = D("0"); total_net = D("0")
    for venue in sorted(summary):
        item = summary[venue]
        avg_bps = item["gross_bps"] / item["legs"] if item["legs"] else D("0")
        print("%-16s %5d %12.6f %10.4f %12.6f %12.6f" % (
            venue, item["legs"], item["gross_usd"], avg_bps, item["rebate_usd"], item["net_usd"]))
        total_gross += item["gross_usd"]; total_net += item["net_usd"]
    print("%-16s %5s %12.6f %10s %12s %12.6f" % ("TOTAL", "", total_gross, "", "", total_net))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
