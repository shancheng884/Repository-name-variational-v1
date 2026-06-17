from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.analyze_var_quote_sources import decimal_to_str  # noqa: E402


LIGHTER_ORDER_BOOKS_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"


@dataclass(slots=True)
class MarketBook:
    asset: str
    market_id: int
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    offset: int = 0
    ready: bool = False


def utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def update_levels(book: MarketBook, side: str, levels: list[Any]) -> None:
    target = book.bids if side == "bids" else book.asks
    for level in levels:
        if isinstance(level, list) and len(level) >= 2:
            price = to_decimal(level[0])
            size = to_decimal(level[1])
        elif isinstance(level, dict):
            price = to_decimal(level.get("price"))
            size = to_decimal(level.get("size"))
        else:
            continue
        if price is None or size is None:
            continue
        if size > 0:
            target[price] = size
        else:
            target.pop(price, None)


def best_bid_ask(book: MarketBook) -> tuple[Decimal | None, Decimal | None]:
    bid = max(book.bids) if book.bids else None
    ask = min(book.asks) if book.asks else None
    return bid, ask


def estimate_fill_price(levels: dict[Decimal, Decimal], *, side: str, notional_usd: Decimal) -> Decimal | None:
    if notional_usd <= 0:
        return None
    ordered = sorted(levels.items(), reverse=(side.upper() == "SELL"))
    remaining_notional = notional_usd
    total_qty = Decimal("0")
    total_quote = Decimal("0")
    for price, size in ordered:
        if price <= 0 or size <= 0:
            continue
        level_notional = price * size
        take_notional = min(remaining_notional, level_notional)
        take_qty = take_notional / price
        total_qty += take_qty
        total_quote += take_notional
        remaining_notional -= take_notional
        if remaining_notional <= 0:
            break
    if total_qty <= 0 or remaining_notional > 0:
        return None
    return total_quote / total_qty


def parse_market_id(message: dict[str, Any]) -> int | None:
    for container_key in ("order_book", "order_book_diff"):
        container = message.get(container_key)
        if isinstance(container, dict):
            for key in ("market_id", "marketId", "id"):
                value = container.get(key)
                if value is not None:
                    with contextlib.suppress(Exception):
                        return int(value)
    channel = str(message.get("channel") or "")
    if "/" in channel:
        tail = channel.rsplit("/", 1)[-1]
        with contextlib.suppress(Exception):
            return int(tail)
    return None


def apply_order_book_message(books_by_id: dict[int, MarketBook], message: dict[str, Any]) -> None:
    market_id = parse_market_id(message)
    if market_id is None and len(books_by_id) == 1:
        market_id = next(iter(books_by_id))
    if market_id is None or market_id not in books_by_id:
        return
    book = books_by_id[market_id]
    order_book = message.get("order_book") if isinstance(message.get("order_book"), dict) else {}
    msg_type = str(message.get("type") or "")
    if msg_type == "subscribed/order_book":
        book.bids.clear()
        book.asks.clear()
        book.offset = int(order_book.get("offset", 0) or 0)
        update_levels(book, "bids", order_book.get("bids", []))
        update_levels(book, "asks", order_book.get("asks", []))
        book.ready = True
        return
    if msg_type == "update/order_book" and book.ready:
        new_offset = int(order_book.get("offset", 0) or 0)
        if new_offset <= book.offset:
            return
        update_levels(book, "bids", order_book.get("bids", []))
        update_levels(book, "asks", order_book.get("asks", []))
        book.offset = new_offset


def market_sample_row(book: MarketBook, *, notional_usd: Decimal) -> dict[str, Any] | None:
    bid, ask = best_bid_ask(book)
    if bid is None or ask is None:
        return None
    buy_fill = estimate_fill_price(book.asks, side="BUY", notional_usd=notional_usd) or ask
    sell_fill = estimate_fill_price(book.bids, side="SELL", notional_usd=notional_usd) or bid
    mid = (bid + ask) / Decimal("2")
    return {
        "event": "market_sample",
        "logged_at": utc_now(),
        "asset": book.asset,
        "source": "lighter_multi_asset_collector",
        "lighter_market_id": book.market_id,
        "lighter_bid": decimal_to_str(bid),
        "lighter_ask": decimal_to_str(ask),
        "lighter_buy_fill_price": decimal_to_str(buy_fill),
        "lighter_sell_fill_price": decimal_to_str(sell_fill),
        "lighter_mid": decimal_to_str(mid),
    }


def load_lighter_markets(assets: list[str]) -> dict[int, MarketBook]:
    wanted = {asset.upper() for asset in assets}
    with urllib.request.urlopen(LIGHTER_ORDER_BOOKS_URL, timeout=10) as response:
        data = json.loads(response.read().decode("utf-8"))
    books: dict[int, MarketBook] = {}
    for market in data.get("order_books", []):
        symbol = str(market.get("symbol") or "").upper()
        if symbol not in wanted:
            continue
        market_id = int(market["market_id"])
        books[market_id] = MarketBook(asset=symbol, market_id=market_id)
    missing = sorted(wanted - {book.asset for book in books.values()})
    if missing:
        raise RuntimeError(f"Missing Lighter markets: {missing}")
    return books


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")


async def collect_market(
    *,
    book: MarketBook,
    output: Path,
    notional_usd: Decimal,
    interval_seconds: float,
    url: str,
    write_lock: asyncio.Lock,
    stop_event: asyncio.Event,
    write_state: dict[str, int],
    max_samples: int,
) -> None:
    import websockets

    last_written = 0.0
    while not stop_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=30) as websocket:
                await websocket.send(json.dumps({"type": "subscribe", "channel": f"order_book/{book.market_id}"}))
                while True:
                    raw = await websocket.recv()
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    message = json.loads(raw)
                    if message.get("type") == "ping":
                        await websocket.send(json.dumps({"type": "pong"}))
                        continue
                    apply_order_book_message({book.market_id: book}, message)
                    now = time.monotonic()
                    if not book.ready or now - last_written < interval_seconds:
                        continue
                    row = market_sample_row(book, notional_usd=notional_usd)
                    if row is None:
                        continue
                    async with write_lock:
                        if max_samples > 0 and write_state["written"] >= max_samples:
                            stop_event.set()
                            return
                        append_jsonl(output, row)
                        write_state["written"] += 1
                        if max_samples > 0 and write_state["written"] >= max_samples:
                            stop_event.set()
                    print(json.dumps(row, ensure_ascii=False, sort_keys=True), flush=True)
                    last_written = now
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(
                json.dumps(
                    {"asset": book.asset, "event": "lighter_multi_asset_collector_reconnect", "error": str(exc)},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )
            await asyncio.sleep(1)


async def run(args: argparse.Namespace) -> None:
    assets = [asset.strip().upper() for asset in args.assets.split(",") if asset.strip()]
    books_by_id = load_lighter_markets(assets)
    url = f"{LIGHTER_WS_URL}?server_pings=true" if os.getenv("LIGHTER_WS_SERVER_PINGS", "").lower() in {"1", "true", "yes", "on"} else LIGHTER_WS_URL
    write_lock = asyncio.Lock()
    stop_event = asyncio.Event()
    write_state = {"written": 0}
    tasks = [
        asyncio.create_task(
            collect_market(
                book=book,
                output=args.output,
                notional_usd=Decimal(str(args.notional_usd)),
                interval_seconds=args.interval_seconds,
                url=url,
                write_lock=write_lock,
                stop_event=stop_event,
                write_state=write_state,
                max_samples=args.max_samples,
            )
        )
        for book in books_by_id.values()
    ]
    try:
        if args.max_samples > 0:
            await stop_event.wait()
            return
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect read-only Lighter market samples for multiple assets.")
    parser.add_argument("--assets", default="BTC,ETH,SOL")
    parser.add_argument("--output", type=Path, default=Path("log/market_samples.jsonl"))
    parser.add_argument("--notional-usd", type=float, default=20.0)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--max-samples", type=int, default=0, help="Total rows to write across all assets; 0 means run forever.")
    args = parser.parse_args()
    if not [asset for asset in args.assets.split(",") if asset.strip()]:
        parser.error("--assets must include at least one asset")
    if args.notional_usd <= 0:
        parser.error("--notional-usd must be > 0")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be > 0")
    if args.max_samples < 0:
        parser.error("--max-samples must be >= 0")
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
