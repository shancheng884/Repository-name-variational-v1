#!/usr/bin/env python3
"""Replay captured quotes and score how often a cross-venue spread dislocation
would pay for the round-trip spread plus slippage.

For every ordered venue pair and asset this reports:

* ``cost``      - median round-trip spread plus the measured slippage allowance
* ``med_vol``   - standard deviation of the rolling median itself (how much the
                  "normal" level drifts, which is what makes a fixed threshold
                  dangerous)
* ``dev_p90/99``- deviation from that rolling median
* ``ev/day``    - dislocations per day that clear ``cost + target``
* ``hit%``      - share of time spent above the trigger
* ``med_peak``  - median peak dislocation of those episodes
* ``bps/day``   - median (peak - cost) times episodes per day

Venues whose spread is structurally broken (p90 above --max-spread-bps) are
dropped, because a 100bps quote spike is not an opportunity.
"""
from __future__ import annotations

import argparse
import gzip
import json
import statistics as st
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ASSETS = "AAPL,NVDA,SNDK,CRCL,QQQ"
DEFAULT_VENUES = "lighter_mainnet,lighter_rh,variational,entropy"


def percentile(values, p):
    values = sorted(values)
    return values[int(p * (len(values) - 1))] if values else float("nan")


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


def load_quotes(output_dir, asset, venue, since, until=None):
    """Last quote per second, filtered to [since, until)."""
    out = {}
    for path in sorted((Path(output_dir) / "quotes" / venue / asset).glob("*.jsonl*")):
        for row in _rows(path):
            at = parse_ts(row.get("recv_at"))
            if at is None:
                continue
            stamp = at.timestamp()
            if stamp < since or (until is not None and stamp >= until):
                continue
            try:
                bid = float(row["bid"]); ask = float(row["ask"]); mid = float(row["mid"])
                buy = float(row.get("executable_buy") or ask)
                sell = float(row.get("executable_sell") or bid)
            except (KeyError, TypeError, ValueError):
                continue
            if bid <= 0 or ask <= 0 or ask < bid or mid <= 0:
                continue
            out[int(stamp)] = (buy, sell, (ask - bid) / mid * 10000.0)
    return out


def analyse_pair(samples, *, window_minutes=30, slippage_bps=1.3, target_bps=1.0, gap_seconds=60,
                 hours=None, revert_bps=0.0, max_hold_seconds=900, overhead_seconds=30):
    """Score one ordered pair.

    ``samples`` is an iterable of ``(timestamp, edge_bps, spread_cost_bps)`` in
    time order.  Everything is computed against a trailing rolling median of the
    per-minute medians, because the tradable object is the deviation from the
    pair's own normal level, not the raw level.
    """
    samples = list(samples)
    if not samples:
        return None
    cost = st.median([c for _, _, c in samples]) + slippage_bps
    per_minute = defaultdict(list)
    for stamp, edge, _ in samples:
        per_minute[int(stamp) // 60].append(edge)
    minutes = sorted(per_minute)
    minute_median = {m: st.median(v) for m, v in per_minute.items()}
    span = max(1, int(window_minutes))
    baseline = {}
    for index, minute in enumerate(minutes):
        window = [minute_median[m] for m in minutes[max(0, index - span + 1): index + 1]]
        baseline[minute] = st.median(window)
    medians = [baseline[m] for m in minutes]
    median_vol = st.pstdev(medians) if len(medians) > 2 else 0.0

    trigger = cost + target_bps
    raw_edges = [edge for _, edge, _ in samples]
    deviations = [(stamp, edge - baseline[int(stamp) // 60]) for stamp, edge, _ in samples]
    events = []
    current = None
    last = None
    for index, (stamp, dev) in enumerate(deviations):
        if dev >= trigger:
            if current is None or (last is not None and stamp - last > gap_seconds):
                if current:
                    events.append(current)
                current = {"start": index, "peak": dev, "entry": dev,
                           "base": baseline[int(stamp) // 60]}
            else:
                current["peak"] = max(current["peak"], dev)
            last = stamp
        elif current is not None and last is not None and stamp - last > gap_seconds:
            events.append(current)
            current = None
    if current:
        events.append(current)

    # How long until the dislocation is gone (and therefore how fast the bps can
    # actually be banked), not just how large it got.
    for event in events:
        start = event["start"]
        stamp0, dev0 = deviations[start]
        event["entry"] = dev0
        event["reverted_s"] = None
        # Reversion is measured against the baseline frozen at entry: the level
        # this pair sat at when we traded it. Measuring against a moving median
        # would let the baseline drift chase the price and fake a reversion.
        frozen = event.get("base", 0.0)
        for index in range(start + 1, len(deviations)):
            stamp, _dev = deviations[index]
            if stamp - stamp0 > max_hold_seconds:
                break
            if raw_edges[index] - frozen <= revert_bps:
                event["reverted_s"] = stamp - stamp0
                break

    if hours is None:
        hours = max((samples[-1][0] - samples[0][0]) / 3600.0, 1e-9)
    peaks = [event["peak"] for event in events]
    per_day = len(events) / hours * 24.0 if hours else 0.0
    median_peak = percentile(peaks, 0.5) if peaks else float("nan")
    net = (median_peak - cost) if peaks else float("nan")
    entries = [event["entry"] for event in events]
    reverts = [event["reverted_s"] for event in events if event["reverted_s"] is not None]
    median_entry = percentile(entries, 0.5) if entries else float("nan")
    net_entry = (median_entry - cost) if entries else float("nan")
    med_rev = percentile(reverts, 0.5) if reverts else None
    timeout_share = (1.0 - len(reverts) / len(events)) if events else None
    speed = None
    speed_day = None
    if reverts and med_rev and med_rev > 0 and net_entry == net_entry:
        speed = net_entry / (med_rev / 3600.0)
        serial = 86400.0 / (med_rev + max(1, overhead_seconds))
        speed_day = net_entry * serial
    return {
        "samples": len(samples),
        "hours": round(hours, 2),
        "cost_bps": round(cost, 3),
        "median_vol_bps": round(median_vol, 3),
        "dev_p50_bps": round(percentile([d for _, d in deviations], 0.5), 3),
        "dev_p90_bps": round(percentile([d for _, d in deviations], 0.9), 3),
        "dev_p99_bps": round(percentile([d for _, d in deviations], 0.99), 3),
        "events_per_day": round(per_day, 2),
        "hit_share": round(sum(1 for _, d in deviations if d >= trigger) / len(deviations), 4),
        "median_peak_bps": None if median_peak != median_peak else round(median_peak, 3),
        "net_per_event_bps": None if net != net else round(net, 3),
        "bps_per_day": None if net != net else round(net * per_day, 1),
        "entry_dev_bps": None if median_entry != median_entry else round(median_entry, 3),
        "net_at_entry_bps": None if net_entry != net_entry else round(net_entry, 3),
        "reversion_median_s": None if med_rev is None else round(med_rev, 1),
        "timeout_share": None if timeout_share is None else round(timeout_share, 4),
        "bps_per_hour_held": None if speed is None else round(speed, 1),
        "bps_per_day_serial": None if speed_day is None else round(speed_day, 1),
    }


def pair_samples(grid_a, grid_b, asset):
    common = sorted(set(grid_a) & set(grid_b))
    out = []
    for stamp in common:
        buy = grid_a[stamp][0]
        sell = grid_b[stamp][1]
        if buy <= 0 or sell <= 0:
            continue
        mid = (buy + sell) / 2
        out.append((stamp, (sell - buy) / mid * 10000.0, grid_a[stamp][2] + grid_b[stamp][2]))
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description="Replay captured quotes into opportunity statistics")
    parser.add_argument("--output-dir", default="log/market_capture")
    parser.add_argument("--from", dest="since", required=True, help="ISO timestamp, e.g. 2026-11-14T17:00Z")
    parser.add_argument("--to", dest="until", default=None)
    parser.add_argument("--assets", default=DEFAULT_ASSETS)
    parser.add_argument("--venues", default=DEFAULT_VENUES)
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--slippage-bps", type=float, default=1.3)
    parser.add_argument("--target-bps", type=float, default=1.0)
    parser.add_argument("--gap-seconds", type=int, default=60)
    parser.add_argument("--revert-bps", type=float, default=0.0, help="deviation level that counts as reverted")
    parser.add_argument("--max-hold-seconds", type=float, default=900.0, help="give up counting a reversion after this")
    parser.add_argument("--overhead-seconds", type=float, default=30.0, help="per-trade overhead used for serial throughput")
    parser.add_argument("--max-spread-bps", type=float, default=10.0, help="drop venues whose p90 spread exceeds this")
    parser.add_argument("--min-samples", type=int, default=1000)
    parser.add_argument("--json", default="")
    args = parser.parse_args(argv)

    since = parse_ts(args.since).timestamp()
    until = parse_ts(args.until).timestamp() if args.until else None
    assets = [a.strip().upper() for a in args.assets.split(",") if a.strip()]
    venues = [v.strip() for v in args.venues.split(",") if v.strip()]

    grids = {}
    for asset in assets:
        for venue in venues:
            grid = load_quotes(args.output_dir, asset, venue, since, until)
            if grid:
                grids[(asset, venue)] = grid

    broken = set()
    for (asset, venue), grid in grids.items():
        if percentile([r[2] for r in grid.values()], 0.9) > args.max_spread_bps:
            broken.add((asset, venue))

    rows = []
    for asset in assets:
        for buy in venues:
            for sell in venues:
                if buy == sell:
                    continue
                if (asset, buy) not in grids or (asset, sell) not in grids:
                    continue
                if (asset, buy) in broken or (asset, sell) in broken:
                    continue
                samples = pair_samples(grids[(asset, buy)], grids[(asset, sell)], asset)
                if len(samples) < args.min_samples:
                    continue
                stats = analyse_pair(samples, window_minutes=args.window_minutes,
                                     slippage_bps=args.slippage_bps, target_bps=args.target_bps,
                                     gap_seconds=args.gap_seconds, revert_bps=args.revert_bps,
                                     max_hold_seconds=args.max_hold_seconds,
                                     overhead_seconds=args.overhead_seconds)
                if stats is None:
                    continue
                stats.update({"asset": asset, "buy_venue": buy, "sell_venue": sell})
                rows.append(stats)

    rows.sort(key=lambda r: -(r["bps_per_day_serial"] if r["bps_per_day_serial"] is not None else -1))
    header = ("%-5s %-16s -> %-16s %6s %6s %6s %7s %7s %6s %7s %7s" %
              ("asset", "buy", "sell", "cost", "entry", "net", "rev_s", "timeout", "ev/day", "bps/h", "bps/day"))
    print(header)
    for row in rows:
        print("%-5s %-16s -> %-16s %6.2f %6s %6s %7s %6s %6.1f %7s %7s" % (
            row["asset"], row["buy_venue"], row["sell_venue"], row["cost_bps"],
            "-" if row["entry_dev_bps"] is None else "%.2f" % row["entry_dev_bps"],
            "-" if row["net_at_entry_bps"] is None else "%.2f" % row["net_at_entry_bps"],
            "-" if row["reversion_median_s"] is None else "%.0f" % row["reversion_median_s"],
            "-" if row["timeout_share"] is None else "%.0f%%" % (row["timeout_share"] * 100),
            row["events_per_day"],
            "-" if row["bps_per_hour_held"] is None else "%.1f" % row["bps_per_hour_held"],
            "-" if row["bps_per_day_serial"] is None else "%.1f" % row["bps_per_day_serial"]))
    if args.json:
        Path(args.json).write_text(json.dumps({
            "since": args.since, "until": args.until, "assets": assets, "venues": venues,
            "window_minutes": args.window_minutes, "slippage_bps": args.slippage_bps,
            "target_bps": args.target_bps, "max_spread_bps": args.max_spread_bps,
            "dropped_venues": sorted("%s:%s" % b for b in broken),
            "rows": rows,
        }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
