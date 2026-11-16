#!/usr/bin/env python3
"""NVDA five-tier ladder: parameter fit and replay.

Same architecture as the ETH gradient (percentile ladder, tier spacing,
2-of-3 confirmation, per-tier reversion exit, per-tier timeout), refitted to
NVDA quotes captured from entropy / lighter_mainnet / lighter_rh.
"""
from __future__ import annotations
import argparse, gzip, json, statistics as st
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BPS = 10000.0
PERCENTILES = (97.5, 98.25, 99.0, 99.5, 99.8)


def parse_ts(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _rows(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    try:
        with opener(path, "rt", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except ValueError:
                    continue
    except (OSError, EOFError):
        return


def load_grid(output_dir, asset, venue, since):
    out = {}
    for path in sorted((Path(output_dir) / "quotes" / venue / asset).glob("*.jsonl*")):
        for row in _rows(path):
            at = parse_ts(row.get("recv_at"))
            if at is None or at.timestamp() < since:
                continue
            try:
                bid = float(row["bid"]); ask = float(row["ask"]); mid = float(row["mid"])
                buy = float(row.get("executable_buy") or ask)
                sell = float(row.get("executable_sell") or bid)
            except (KeyError, TypeError, ValueError):
                continue
            if bid <= 0 or ask <= 0 or ask < bid or mid <= 0:
                continue
            out[int(at.timestamp())] = (buy, sell, (ask - bid) / mid * BPS)
    return out


def build_series(a, b):
    common = sorted(set(a) & set(b))
    series = []
    for stamp in common:
        buy = a[stamp][0]; sell = b[stamp][1]
        if buy <= 0 or sell <= 0:
            continue
        mid = (buy + sell) / 2
        series.append((stamp, (sell - buy) / mid * BPS, a[stamp][2] + b[stamp][2]))
    return series


def rolling_median(series, window_minutes=30):
    per_min = defaultdict(list)
    for stamp, edge, _ in series:
        per_min[stamp // 60].append(edge)
    minutes = sorted(per_min)
    med = {m: st.median(v) for m, v in per_min.items()}
    span = max(1, window_minutes)
    base = {}
    for i, m in enumerate(minutes):
        base[m] = st.median([med[x] for x in minutes[max(0, i - span + 1): i + 1]])
    return base


def percentile(values, p):
    values = sorted(values)
    if not values:
        return 0.0
    k = (len(values) - 1) * p / 100.0
    lo = int(k); hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def fit_tiers(series, cost_bps, window_minutes=30):
    base = rolling_median(series, window_minutes)
    dev = sorted(edge - base[int(stamp) // 60] for stamp, edge, _ in series)
    tiers = [cost_bps + percentile(dev, p) for p in PERCENTILES]
    # enforce a spacing floor so the ladder can never collapse on itself
    spaced = [tiers[0]]
    for value in tiers[1:]:
        spaced.append(max(value, spaced[-1] + 0.25))
    return base, spaced, dev


def slot_caps(equity_buy, equity_sell, child_notional=20.0, max_multiple=5.0, tier_count=None):
    """Cumulative child slots per tier, mirroring main.py's gradient capacity."""
    if not equity_buy or not equity_sell or equity_buy <= 0 or equity_sell <= 0:
        return None
    smaller = min(equity_buy, equity_sell)
    hard = int(smaller * max_multiple / child_notional)
    count = tier_count or len(PERCENTILES)
    return [min(hard, int(smaller * k / child_notional)) for k in range(1, count + 1)]


def tier_increments(caps, child_notional=20.0):
    """Notional each tier adds on top of the previous cumulative target."""
    if not caps:
        return None
    targets = [c * child_notional for c in caps]
    return [targets[0]] + [targets[i] - targets[i - 1] for i in range(1, len(targets))]


def replay(series, base, tiers, *, notional=20.0, tier_notional=None, slippage_bps=1.3, timeout_s=900,
           confirm_window=3, confirm_min=2):
    open_tiers = {}
    trades = []
    for index, (stamp, edge, _) in enumerate(series):
        dev = edge - base[int(stamp) // 60]
        # exits first
        for tier in list(open_tiers):
            pos = open_tiers[tier]
            if edge <= pos["frozen"] or stamp - pos["opened_at"] >= timeout_s:
                net = (pos["entry_edge"] - edge) - slippage_bps
                trades.append({"tier": tier, "net_bps": net, "hold_s": stamp - pos["opened_at"],
                               "reason": "reverted" if edge <= pos["frozen"] else "timeout",
                               "entry_dev": pos["entry_dev"], "entry_edge": pos["entry_edge"],
                               "notional": pos["notional"]})
                open_tiers.pop(tier)
        # entries: 2-of-3 confirmation like the ETH gradient
        if index + 1 < confirm_window:
            continue
        window = [series[i][1] - base[int(series[i][0]) // 60] for i in range(index - confirm_window + 1, index + 1)]
        for tier_index, threshold in enumerate(tiers, start=1):
            if tier_index in open_tiers:
                continue
            if sum(1 for value in window if value >= threshold) < confirm_min:
                continue
            size = tier_notional[tier_index - 1] if tier_notional else notional
            open_tiers[tier_index] = {"entry_edge": edge, "entry_dev": dev,
                                      "frozen": base[int(stamp) // 60], "opened_at": stamp,
                                      "notional": size}
    for tier, pos in open_tiers.items():
        net = -slippage_bps
        trades.append({"tier": tier, "net_bps": net, "hold_s": 0, "reason": "open_at_end",
                       "entry_dev": pos["entry_dev"], "entry_edge": pos["entry_edge"],
                       "notional": pos["notional"]})
    return trades


def summarise(trades, notional=20.0):
    by_tier = defaultdict(list)
    for trade in trades:
        by_tier[trade["tier"]].append(trade)
    rows = []
    for tier in sorted(by_tier):
        items = by_tier[tier]
        nets = [t["net_bps"] for t in items]
        usd = sum(t["net_bps"] / BPS * t.get("notional", notional) for t in items)
        rows.append({
            "tier": tier, "fills": len(items),
            "win_rate": sum(1 for n in nets if n > 0) / len(nets),
            "median_net_bps": st.median(nets),
            "total_usd": usd,
            "median_hold_s": st.median([t["hold_s"] for t in items]),
            "timeout_share": sum(1 for t in items if t["reason"] == "timeout") / len(items),
        })
    return rows


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
    parser.add_argument("--child-notional", type=float, default=20.0)
    parser.add_argument("--equity-buy", type=float, default=None)
    parser.add_argument("--equity-sell", type=float, default=None)
    parser.add_argument("--max-equity-multiple", type=float, default=5.0)
    args = parser.parse_args(argv)

    since = parse_ts(args.since).timestamp()
    for spec in [p.strip() for p in args.pairs.split(",") if p.strip()]:
        buy, sell = spec.split(">")
        a = load_grid(args.output_dir, args.asset, buy, since)
        b = load_grid(args.output_dir, args.asset, sell, since)
        if not a or not b:
            print("%s: no data" % spec); continue
        series = build_series(a, b)
        if len(series) < 3000:
            print("%s: only %d samples" % (spec, len(series))); continue
        cost = st.median([c for _, _, c in series]) + args.slippage_bps
        base, tiers, dev = fit_tiers(series, cost, args.window_minutes)
        hours = (series[-1][0] - series[0][0]) / 3600
        print("")
        print("=== %s  %s -> %s  (%.1f h, cost %.2f bps) ===" % (args.asset, buy, sell, hours, cost))
        print("%-6s %-10s %-10s" % ("tier", "threshold", "pctile"))
        for i, (t, p) in enumerate(zip(tiers, PERCENTILES), start=1):
            print("%-6d %-10.2f p%.2f" % (i, t, p))
        caps = slot_caps(args.equity_buy, args.equity_sell, args.child_notional,
                         args.max_equity_multiple, len(tiers))
        increments = tier_increments(caps, args.child_notional)
        if increments:
            print("sizing: cumulative %s USD, per tier %s USD (child %.0fU, cap %.0fx)"
                  % ([round(sum(increments[:i + 1]), 1) for i in range(len(increments))],
                     [round(v, 1) for v in increments], args.child_notional, args.max_equity_multiple))
        trades = replay(series, base, tiers, notional=args.notional, tier_notional=increments,
                        slippage_bps=args.slippage_bps, timeout_s=args.timeout)
        print("trades %d over %.1f h (%.1f/day)" % (len(trades), hours, len(trades) / hours * 24 if hours else 0))
        print("%-5s %6s %8s %12s %10s %9s %8s" % ("tier", "fills", "win%", "median_net", "usd", "hold_s", "timeout"))
        for row in summarise(trades, args.notional):
            print("%-5d %6d %7.0f%% %12.2f %10.4f %9.0f %7.0f%%" % (
                row["tier"], row["fills"], row["win_rate"] * 100, row["median_net_bps"],
                row["total_usd"], row["median_hold_s"], row["timeout_share"] * 100))
        total = sum(r["total_usd"] for r in summarise(trades, args.notional))
        print("total %.4f USD over %d trades" % (total, len(trades)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
