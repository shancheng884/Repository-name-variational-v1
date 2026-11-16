#!/usr/bin/env python3
"""Paper-run the NVDA five-tier engine on captured quotes."""
from __future__ import annotations
import argparse, statistics as st, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec
from tools.nvda_tier_replay import build_series, fit_tiers, load_grid, parse_ts


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="log/market_capture")
    parser.add_argument("--asset", default="NVDA")
    parser.add_argument("--pairs", default="entropy>lighter_mainnet,entropy>lighter_rh")
    parser.add_argument("--since", default="2026-11-14T17:00Z")
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--slippage-bps", type=float, default=1.3)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--notional", type=float, default=20.0)
    args = parser.parse_args(argv)

    since = parse_ts(args.since).timestamp()
    grids = {}
    for spec in [p.strip() for p in args.pairs.split(",") if p.strip()]:
        buy, sell = spec.split(">")
        a = grids.get(buy) or load_grid(args.output_dir, args.asset, buy, since)
        b = grids.get(sell) or load_grid(args.output_dir, args.asset, sell, since)
        grids.setdefault(buy, a); grids.setdefault(sell, b)
        if not a or not b:
            print("%s: no data" % spec); continue
        series = build_series(a, b)
        if len(series) < 3000:
            print("%s: only %d samples" % (spec, len(series))); continue
        cost = st.median([c for _, _, c in series]) + args.slippage_bps
        base, thresholds, _ = fit_tiers(series, cost, args.window_minutes)
        cfg = TierConfig(asset=args.asset, buy_venue=buy, sell_venue=sell,
                         tiers=tuple(TierSpec(i, t) for i, t in enumerate(thresholds, start=1)),
                         notional_usd=args.notional, timeout_seconds=args.timeout,
                         slippage_bps=args.slippage_bps)
        ledger = PositionLedger()
        engine = TierEngine(config=cfg, ledger=ledger)
        window: list[float] = []
        blocked = 0
        hours = (series[-1][0] - series[0][0]) / 3600
        for stamp, edge, _ in series:
            dev = edge - base[int(stamp) // 60]
            window.append(dev)
            if len(window) > cfg.confirm_window:
                window.pop(0)
            exchange = lambda: {buy: ledger.net(args.asset, buy), sell: ledger.net(args.asset, sell)}
            actions = engine.on_sample(stamp=stamp, edge=edge, baseline=base[int(stamp) // 60],
                                       price_hint=abs(edge) if edge else 1.0,
                                       window=window, exchange=exchange)
            blocked += sum(1 for x in actions if x.get("action") == "blocked")
        last_edge = series[-1][1]
        for index, pos in list(engine.open_tiers.items()):
            engine.trades.append({"tier": index, "net_bps": (pos.entry_edge - last_edge) - cfg.slippage_bps,
                                  "hold_s": series[-1][0] - pos.opened_at, "reason": "open_at_end",
                                  "entry_dev": pos.entry_dev})
            engine.open_tiers.pop(index)
        print("")
        print("=== %s  %s -> %s  (%.1f h, cost %.2f bps, ledger end %s/%s) ===" % (
            args.asset, buy, sell, hours, cost,
            ledger.net(args.asset, buy), ledger.net(args.asset, sell)))
        print("%-5s %6s %7s %11s %9s %8s %8s" % ("tier", "fills", "win%", "median_net", "usd", "hold_s", "timeout"))
        total = 0.0
        for row in engine.summary():
            total += row["total_usd"]
            print("%-5d %6d %6.0f%% %11.2f %9.4f %8.0f %7.0f%%" % (
                row["tier"], row["fills"], row["win_rate"] * 100, row["median_net_bps"],
                row["total_usd"], row["median_hold_s"], row["timeout_share"] * 100))
        print("trades %d, total %.4f USD, blocked %d, open at end %d" % (
            len(engine.trades), total, blocked, len(engine.open_tiers)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
