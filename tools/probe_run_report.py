#!/usr/bin/env python3
"""Summarise one probe run: triggers, fills, and what each direction actually banked."""
from __future__ import annotations
import argparse, json, statistics as st
from collections import defaultdict
from decimal import Decimal as D
from pathlib import Path

BPS = D("10000")


def load_rows(path, run_id):
    rows = []
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or '"probe_round"' not in line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("run_id") == run_id:
                rows.append(row)
    return rows


def leg_pnl(row):
    buy, sell = row.get("buy_venue"), row.get("sell_venue")
    entry = (row.get("entry") or {}).get("legs") or {}
    exit_ = (row.get("exit") or {}).get("legs") or {}
    try:
        qty = min(D(str(entry[buy]["filled_qty"])), D(str(entry[sell]["filled_qty"])))
        bp = D(str(entry[buy]["avg_price"])); sp = D(str(entry[sell]["avg_price"]))
        bxp = D(str(exit_[buy]["avg_price"])); sxp = D(str(exit_[sell]["avg_price"]))
    except (KeyError, TypeError):
        return None, None
    mid = (bp + sp) / 2
    net_bps = float(((sp - bp) - (sxp - bxp)) / mid * BPS)
    usd = (bxp - bp) * qty + (sp - sxp) * qty
    return net_bps, usd


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", default="log/venue_probe/state.json")
    parser.add_argument("--rounds", default="log/venue_probe/rounds.jsonl")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    state = json.loads(Path(args.state).read_text())
    run_id = args.run_id or state.get("run_id")
    rows = load_rows(args.rounds, run_id)
    print("run %s  phase=%s  cursor=%s/%s" % (run_id, state.get("phase"), state.get("cursor"), state.get("planned_rounds")))

    groups = defaultdict(list)
    for row in rows:
        groups[(row.get("asset"), row.get("buy_venue"), row.get("sell_venue"))].append(row)

    total_usd = D("0"); total_fee = D("0")
    print("")
    print("%-5s %-16s -> %-16s %6s %7s %6s %8s %9s %8s %6s" % (
        "asset", "buy", "sell", "tries", "filled", "warm", "net_p50", "usd", "fee_usd", "rev_s"))
    for key in sorted(groups):
        items = groups[key]
        filled = [r for r in items if r.get("status") == "filled"]
        warm = sum(1 for r in items if r.get("reason") == "baseline_warming_up")
        nets = []; usd = D("0"); holds = []; fee = D("0")
        for row in filled:
            net_bps, pnl = leg_pnl(row)
            if net_bps is None:
                continue
            nets.append(net_bps); usd += pnl
            hold = (row.get("exit") or {}).get("hold_seconds")
            if hold is not None:
                holds.append(hold)
            for side in ("entry", "exit"):
                for leg in ((row.get(side) or {}).get("legs") or {}).values():
                    if leg.get("fee_usd") not in (None, ""):
                        try:
                            fee += D(str(leg["fee_usd"]))
                        except Exception:
                            pass
        total_usd += usd; total_fee += fee
        print("%-5s %-16s -> %-16s %6d %7d %6d %8s %9.4f %8.4f %6s" % (
            key[0], key[1], key[2], len(items), len(filled), warm,
            "-" if not nets else "%.2f" % st.median(nets), usd, fee,
            "-" if not holds else "%.0f" % st.median(holds)))
    print("")
    print("total realised %.4f USD, gross fees %.4f USD, over %d fills" % (
        total_usd, total_fee, sum(1 for r in rows if r.get("status") == "filled")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
