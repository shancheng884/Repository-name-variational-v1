from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.analyze_var_quote_sources import (  # noqa: E402
    DEFAULT_ENDPOINT,
    DIRECTION_LONG,
    DIRECTION_SHORT,
    decimal_to_str,
    edge_bps,
    request_var_quote,
    to_decimal,
)


@dataclass(slots=True)
class LighterSnapshot:
    logged_at: str
    asset: str
    bid: Decimal
    ask: Decimal
    buy_fill_price: Decimal | None
    sell_fill_price: Decimal | None


def latest_lighter_snapshot(path: Path, *, asset: str, latest: int) -> LighterSnapshot | None:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("event") != "market_sample":
                continue
            if str(row.get("asset") or "").upper() != asset.upper():
                continue
            rows.append(row)
    if latest > 0:
        rows = rows[-latest:]
    for row in reversed(rows):
        bid = to_decimal(row.get("lighter_bid"))
        ask = to_decimal(row.get("lighter_ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            continue
        return LighterSnapshot(
            logged_at=str(row.get("logged_at") or ""),
            asset=str(row.get("asset") or "").upper(),
            bid=bid,
            ask=ask,
            buy_fill_price=to_decimal(row.get("lighter_buy_fill_price")),
            sell_fill_price=to_decimal(row.get("lighter_sell_fill_price")),
        )
    return None


def percentile(values: list[Decimal], pct: float) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * pct))
    return ordered[index]


def build_result(snapshot: LighterSnapshot, quote_message: dict[str, Any], *, quote_ms: Decimal, samples: list[dict[str, Decimal]]) -> dict[str, Any]:
    result = quote_message.get("result") if isinstance(quote_message.get("result"), dict) else {}
    bid = to_decimal(result.get("bid"))
    ask = to_decimal(result.get("ask"))
    long_lighter_price = snapshot.sell_fill_price or snapshot.bid
    short_lighter_price = snapshot.buy_fill_price or snapshot.ask
    long_edge = edge_bps(direction=DIRECTION_LONG, var_price=ask, lighter_price=long_lighter_price) if ask is not None else None
    short_edge = edge_bps(direction=DIRECTION_SHORT, var_price=bid, lighter_price=short_lighter_price) if bid is not None else None
    if long_edge is not None and short_edge is not None:
        samples.append({"long": long_edge, "short": short_edge, "best": max(long_edge, short_edge)})
    best_edges = [sample["best"] for sample in samples]
    long_edges = [sample["long"] for sample in samples]
    short_edges = [sample["short"] for sample in samples]
    best_fresh_edge = max((value for value in (long_edge, short_edge) if value is not None), default=None)
    best_direction = None
    if best_fresh_edge is not None:
        best_direction = DIRECTION_LONG if long_edge == best_fresh_edge else DIRECTION_SHORT
    return {
        "asset": snapshot.asset,
        "sample_count": len(samples),
        "lighter_logged_at": snapshot.logged_at,
        "lighter_bid": decimal_to_str(snapshot.bid),
        "lighter_ask": decimal_to_str(snapshot.ask),
        "lighter_long_price": decimal_to_str(long_lighter_price),
        "lighter_short_price": decimal_to_str(short_lighter_price),
        "fresh_quote_ok": bool(quote_message.get("ok")),
        "fresh_quote_id": result.get("quoteId") or result.get("quote_id"),
        "fresh_quote_bid": decimal_to_str(bid),
        "fresh_quote_ask": decimal_to_str(ask),
        "fresh_quote_timestamp": result.get("quoteTimestamp") or result.get("quote_timestamp"),
        "fresh_quote_ms": decimal_to_str(quote_ms),
        "long_var_short_lighter_fresh_bps": decimal_to_str(long_edge),
        "short_var_long_lighter_fresh_bps": decimal_to_str(short_edge),
        "best_direction": best_direction,
        "best_fresh_edge_bps": decimal_to_str(best_fresh_edge),
        "best_fresh_edge_min_bps": decimal_to_str(min(best_edges)) if best_edges else None,
        "best_fresh_edge_median_bps": decimal_to_str(Decimal(str(statistics.median(best_edges)))) if best_edges else None,
        "best_fresh_edge_p90_bps": decimal_to_str(percentile(best_edges, 0.9)),
        "best_fresh_edge_max_bps": decimal_to_str(max(best_edges)) if best_edges else None,
        "long_fresh_edge_median_bps": decimal_to_str(Decimal(str(statistics.median(long_edges)))) if long_edges else None,
        "short_fresh_edge_median_bps": decimal_to_str(Decimal(str(statistics.median(short_edges)))) if short_edges else None,
    }


async def run_watch(args: argparse.Namespace) -> None:
    samples: list[dict[str, Decimal]] = []
    while True:
        snapshot = latest_lighter_snapshot(args.file, asset=args.asset, latest=args.latest)
        if snapshot is None:
            print("no_lighter_snapshot", flush=True)
            await asyncio.sleep(args.interval_seconds)
            continue
        started = time.perf_counter()
        quote = await request_var_quote(
            args.endpoint,
            asset=snapshot.asset,
            amount=Decimal(str(args.lot_notional_usd)),
            timeout_seconds=args.timeout_seconds,
        )
        quote_ms = Decimal(str((time.perf_counter() - started) * 1000))
        result = build_result(snapshot, quote, quote_ms=quote_ms, samples=samples)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
        if args.max_samples > 0 and len(samples) >= args.max_samples:
            return
        await asyncio.sleep(args.interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure executable Var fresh quote edge against the latest Lighter book snapshot.")
    parser.add_argument("--file", type=Path, default=Path("log/market_samples.jsonl"))
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--lot-notional-usd", type=float, default=20.0)
    parser.add_argument("--latest", type=int, default=1000)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-samples", type=int, default=0, help="Stop after this many successful fresh edge samples; 0 means run forever.")
    args = parser.parse_args()
    if args.lot_notional_usd <= 0:
        parser.error("--lot-notional-usd must be > 0")
    if args.latest <= 0:
        parser.error("--latest must be > 0")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be > 0")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0")
    if args.max_samples < 0:
        parser.error("--max-samples must be >= 0")
    asyncio.run(run_watch(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
