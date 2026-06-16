from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from inventory_engine import (  # noqa: E402
    DIRECTION_LONG_VAR_SHORT_LIGHTER,
    DIRECTION_SHORT_VAR_LONG_LIGHTER,
    INVENTORY_DIRECTIONS,
    PaperInventoryEngine,
)
from tools.analyze_fresh_quote_edges import latest_lighter_snapshot  # noqa: E402
from tools.analyze_var_quote_sources import DEFAULT_ENDPOINT, decimal_to_str, edge_bps, request_var_quote, to_decimal  # noqa: E402


@dataclass(slots=True)
class FreshInventorySample:
    logged_at: str
    asset: str
    var_bid: Decimal
    var_ask: Decimal
    lighter_bid: Decimal
    lighter_ask: Decimal
    lighter_buy_price: Decimal
    lighter_sell_price: Decimal
    long_edge_bps: Decimal
    short_edge_bps: Decimal
    quote_id: str | None
    quote_timestamp: str | None
    quote_ms: Decimal


def make_sample(*, snapshot: Any, quote_message: dict[str, Any], quote_ms: Decimal) -> FreshInventorySample | None:
    result = quote_message.get("result") if isinstance(quote_message.get("result"), dict) else {}
    bid = to_decimal(result.get("bid"))
    ask = to_decimal(result.get("ask"))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    lighter_buy_price = snapshot.buy_fill_price or snapshot.ask
    lighter_sell_price = snapshot.sell_fill_price or snapshot.bid
    long_edge = edge_bps(direction=DIRECTION_LONG_VAR_SHORT_LIGHTER, var_price=ask, lighter_price=lighter_sell_price)
    short_edge = edge_bps(direction=DIRECTION_SHORT_VAR_LONG_LIGHTER, var_price=bid, lighter_price=lighter_buy_price)
    return FreshInventorySample(
        logged_at=snapshot.logged_at,
        asset=snapshot.asset,
        var_bid=bid,
        var_ask=ask,
        lighter_bid=snapshot.bid,
        lighter_ask=snapshot.ask,
        lighter_buy_price=lighter_buy_price,
        lighter_sell_price=lighter_sell_price,
        long_edge_bps=long_edge,
        short_edge_bps=short_edge,
        quote_id=result.get("quoteId") or result.get("quote_id"),
        quote_timestamp=result.get("quoteTimestamp") or result.get("quote_timestamp"),
        quote_ms=quote_ms,
    )


def sample_prices(sample: FreshInventorySample, direction: str) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    if direction == DIRECTION_LONG_VAR_SHORT_LIGHTER:
        return sample.long_edge_bps, sample.var_ask, sample.lighter_sell_price, sample.var_bid, sample.lighter_buy_price
    if direction == DIRECTION_SHORT_VAR_LONG_LIGHTER:
        return sample.short_edge_bps, sample.var_bid, sample.lighter_buy_price, sample.var_ask, sample.lighter_sell_price
    raise ValueError(f"Unsupported direction: {direction}")


def event_to_json(event: Any) -> dict[str, Any]:
    row = asdict(event)
    for key, value in list(row.items()):
        if isinstance(value, Decimal):
            row[key] = decimal_to_str(value)
    return row


def state_row(*, sample: FreshInventorySample, engine: PaperInventoryEngine, sample_index: int, events: list[Any]) -> dict[str, Any]:
    best_edge = max(sample.long_edge_bps, sample.short_edge_bps)
    best_direction = DIRECTION_LONG_VAR_SHORT_LIGHTER if sample.long_edge_bps >= sample.short_edge_bps else DIRECTION_SHORT_VAR_LONG_LIGHTER
    return {
        "event": "fresh_quote_inventory_paper_state",
        "sample_index": sample_index,
        "asset": sample.asset,
        "logged_at": sample.logged_at,
        "quote_timestamp": sample.quote_timestamp,
        "quote_id": sample.quote_id,
        "quote_ms": decimal_to_str(sample.quote_ms),
        "var_bid": decimal_to_str(sample.var_bid),
        "var_ask": decimal_to_str(sample.var_ask),
        "lighter_bid": decimal_to_str(sample.lighter_bid),
        "lighter_ask": decimal_to_str(sample.lighter_ask),
        "long_edge_bps": decimal_to_str(sample.long_edge_bps),
        "short_edge_bps": decimal_to_str(sample.short_edge_bps),
        "best_direction": best_direction,
        "best_edge_bps": decimal_to_str(best_edge),
        "open_lots": engine.open_lots(),
        "open_long_lots": engine.open_lots(DIRECTION_LONG_VAR_SHORT_LIGHTER),
        "open_short_lots": engine.open_lots(DIRECTION_SHORT_VAR_LONG_LIGHTER),
        "realized_pnl_usd": decimal_to_str(engine.realized_pnl_usd),
        "actions": [event_to_json(event) for event in events],
    }


async def run(args: argparse.Namespace) -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal(str(args.lot_notional_usd)),
        max_lots=args.max_lots,
        max_total_lots=args.max_total_lots,
        entry_bps=Decimal(str(args.entry_bps)),
        exit_bps=Decimal(str(args.exit_bps)),
        min_hold_samples=args.min_hold_samples,
        latency_samples=0,
    )
    sample_index = 0
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
        sample = make_sample(snapshot=snapshot, quote_message=quote, quote_ms=quote_ms)
        if sample is None:
            print(json.dumps({"event": "fresh_quote_inventory_quote_failed", "ok": bool(quote.get("ok")), "result": quote.get("result")}, ensure_ascii=False, sort_keys=True), flush=True)
            await asyncio.sleep(args.interval_seconds)
            continue

        events = []
        for direction in INVENTORY_DIRECTIONS:
            edge, var_entry, lighter_entry, var_exit, lighter_exit = sample_prices(sample, direction)
            events.extend(
                engine.on_sample(
                    direction=direction,
                    edge_bps=edge,
                    var_entry_price=var_entry,
                    lighter_entry_price=lighter_entry,
                    var_exit_price=var_exit,
                    lighter_exit_price=lighter_exit,
                    logged_at=sample.logged_at,
                    sample_index=sample_index,
                )
            )
        print(json.dumps(state_row(sample=sample, engine=engine, sample_index=sample_index, events=events), ensure_ascii=False, sort_keys=True), flush=True)
        sample_index += 1
        if args.max_samples > 0 and sample_index >= args.max_samples:
            return
        await asyncio.sleep(args.interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Paper trade layered inventory using Var fresh quotes and latest Lighter book snapshots.")
    parser.add_argument("--file", type=Path, default=Path("log/market_samples.jsonl"))
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--lot-notional-usd", type=float, default=20.0)
    parser.add_argument("--entry-bps", type=float, default=3.0)
    parser.add_argument("--exit-bps", type=float, default=1.0)
    parser.add_argument("--max-lots", type=int, default=3)
    parser.add_argument("--max-total-lots", type=int, default=3)
    parser.add_argument("--min-hold-samples", type=int, default=3)
    parser.add_argument("--latest", type=int, default=1000)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-samples", type=int, default=0, help="Stop after this many samples; 0 means run forever.")
    args = parser.parse_args()
    if args.lot_notional_usd <= 0:
        parser.error("--lot-notional-usd must be > 0")
    if args.max_lots <= 0:
        parser.error("--max-lots must be > 0")
    if args.max_total_lots <= 0:
        parser.error("--max-total-lots must be > 0")
    if args.min_hold_samples < 0:
        parser.error("--min-hold-samples must be >= 0")
    if args.latest <= 0:
        parser.error("--latest must be > 0")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be > 0")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0")
    if args.max_samples < 0:
        parser.error("--max-samples must be >= 0")
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
