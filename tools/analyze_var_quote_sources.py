from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import websockets


DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"
DEFAULT_ENDPOINT = "ws://127.0.0.1:8768"
DEFAULT_SLIPPAGE_BPS = Decimal("100")


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def edge_bps(*, direction: str, var_price: Decimal, lighter_price: Decimal) -> Decimal:
    if direction == DIRECTION_LONG:
        return ((lighter_price - var_price) / var_price) * Decimal("10000")
    if direction == DIRECTION_SHORT:
        return ((var_price - lighter_price) / var_price) * Decimal("10000")
    raise ValueError(f"unsupported direction: {direction}")


@dataclass(slots=True)
class Candidate:
    logged_at: str
    asset: str
    direction: str
    snapshot_edge_bps: Decimal
    snapshot_var_price: Decimal
    lighter_price: Decimal
    qty: Decimal
    lot_notional_usd: Decimal
    snapshot_var_timestamp: str | None
    snapshot_var_source_url: str | None
    snapshot_var_source_stream: str | None


def candidate_from_row(row: dict[str, Any], *, direction: str, lot_notional_usd: Decimal) -> Candidate | None:
    asset = str(row.get("asset") or "").upper()
    if not asset:
        return None
    if direction == DIRECTION_LONG:
        snapshot_edge = to_decimal(row.get("long_var_short_lighter_bps"))
        var_price = to_decimal(row.get("var_buy_price"))
        lighter_price = to_decimal(row.get("lighter_bid"))
    elif direction == DIRECTION_SHORT:
        snapshot_edge = to_decimal(row.get("short_var_long_lighter_bps"))
        var_price = to_decimal(row.get("var_sell_price"))
        lighter_price = to_decimal(row.get("lighter_ask"))
    else:
        raise ValueError(f"unsupported direction: {direction}")
    if snapshot_edge is None or var_price is None or lighter_price is None or var_price <= 0:
        return None
    notional_price = var_price * (Decimal("1") + DEFAULT_SLIPPAGE_BPS / Decimal("10000"))
    return Candidate(
        logged_at=str(row.get("logged_at") or ""),
        asset=asset,
        direction=direction,
        snapshot_edge_bps=snapshot_edge,
        snapshot_var_price=var_price,
        lighter_price=lighter_price,
        qty=lot_notional_usd / notional_price,
        lot_notional_usd=lot_notional_usd,
        snapshot_var_timestamp=str(row.get("var_timestamp") or row.get("entry_snapshot_var_timestamp") or "") or None,
        snapshot_var_source_url=str(row.get("var_source_url") or "") or None,
        snapshot_var_source_stream=str(row.get("var_source_stream") or "") or None,
    )


def latest_candidate(
    path: Path,
    *,
    asset: str,
    threshold_bps: Decimal,
    lot_notional_usd: Decimal,
    latest: int,
) -> Candidate | None:
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
        candidates = [
            candidate_from_row(row, direction=DIRECTION_LONG, lot_notional_usd=lot_notional_usd),
            candidate_from_row(row, direction=DIRECTION_SHORT, lot_notional_usd=lot_notional_usd),
        ]
        valid = [candidate for candidate in candidates if candidate is not None and candidate.snapshot_edge_bps >= threshold_bps]
        if valid:
            return max(valid, key=lambda candidate: candidate.snapshot_edge_bps)
    return None


async def request_var_quote(endpoint: str, *, asset: str, amount: Decimal, timeout_seconds: float) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    payload = {
        "type": "VAR_API_QUOTE",
        "requestId": request_id,
        "market": asset.upper(),
        "amount": decimal_to_str(amount),
    }
    async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20) as websocket:
        await websocket.send(json.dumps(payload, ensure_ascii=True))
        while True:
            raw = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
            message = json.loads(raw)
            if message.get("requestId") == request_id:
                return message


def analyze_candidate(candidate: Candidate, quote_message: dict[str, Any], *, quote_ms: Decimal) -> dict[str, Any]:
    result = quote_message.get("result") if isinstance(quote_message.get("result"), dict) else {}
    bid = to_decimal(result.get("bid"))
    ask = to_decimal(result.get("ask"))
    fresh_var_price = ask if candidate.direction == DIRECTION_LONG else bid
    fresh_edge = None
    if fresh_var_price is not None:
        fresh_edge = edge_bps(direction=candidate.direction, var_price=fresh_var_price, lighter_price=candidate.lighter_price)
    return {
        "asset": candidate.asset,
        "direction": candidate.direction,
        "candidate_logged_at": candidate.logged_at,
        "snapshot_var_timestamp": candidate.snapshot_var_timestamp,
        "snapshot_var_source_url": candidate.snapshot_var_source_url,
        "snapshot_var_source_stream": candidate.snapshot_var_source_stream,
        "snapshot_var_price": decimal_to_str(candidate.snapshot_var_price),
        "lighter_price": decimal_to_str(candidate.lighter_price),
        "qty": decimal_to_str(candidate.qty),
        "snapshot_edge_bps": decimal_to_str(candidate.snapshot_edge_bps),
        "fresh_quote_ok": bool(quote_message.get("ok")),
        "fresh_quote_id": result.get("quoteId") or result.get("quote_id"),
        "fresh_quote_bid": decimal_to_str(bid),
        "fresh_quote_ask": decimal_to_str(ask),
        "fresh_quote_timestamp": result.get("quoteTimestamp") or result.get("quote_timestamp"),
        "fresh_quote_ms": decimal_to_str(quote_ms),
        "fresh_var_price": decimal_to_str(fresh_var_price),
        "fresh_edge_bps": decimal_to_str(fresh_edge),
        "edge_loss_bps": decimal_to_str(candidate.snapshot_edge_bps - fresh_edge) if fresh_edge is not None else None,
    }


async def run_once(args: argparse.Namespace) -> dict[str, Any] | None:
    candidate = latest_candidate(
        args.file,
        asset=args.asset,
        threshold_bps=Decimal(str(args.threshold_bps)),
        lot_notional_usd=Decimal(str(args.lot_notional_usd)),
        latest=args.latest,
    )
    if candidate is None:
        print("no_candidate")
        return None
    started = time.perf_counter()
    quote = await request_var_quote(
        args.endpoint,
        asset=candidate.asset,
        amount=candidate.lot_notional_usd,
        timeout_seconds=args.timeout_seconds,
    )
    quote_ms = Decimal(str((time.perf_counter() - started) * 1000))
    result = analyze_candidate(candidate, quote, quote_ms=quote_ms)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


async def run_watch(args: argparse.Namespace) -> None:
    seen_keys: set[tuple[str, str]] = set()
    while True:
        candidate = latest_candidate(
            args.file,
            asset=args.asset,
            threshold_bps=Decimal(str(args.threshold_bps)),
            lot_notional_usd=Decimal(str(args.lot_notional_usd)),
            latest=args.latest,
        )
        key = (candidate.logged_at, candidate.direction) if candidate is not None else None
        if candidate is not None and key not in seen_keys:
            seen_keys.add(key)
            started = time.perf_counter()
            quote = await request_var_quote(
                args.endpoint,
                asset=candidate.asset,
                amount=candidate.lot_notional_usd,
                timeout_seconds=args.timeout_seconds,
            )
            quote_ms = Decimal(str((time.perf_counter() - started) * 1000))
            print(json.dumps(analyze_candidate(candidate, quote, quote_ms=quote_ms), ensure_ascii=False, sort_keys=True), flush=True)
        await asyncio.sleep(args.interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare Variational /prices snapshot edge with fresh indicative quote edge.")
    parser.add_argument("--file", type=Path, default=Path("log/market_samples.jsonl"))
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--threshold-bps", type=float, default=10.0)
    parser.add_argument("--lot-notional-usd", type=float, default=20.0)
    parser.add_argument("--latest", type=int, default=3000)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if args.threshold_bps < 0:
        parser.error("--threshold-bps must be >= 0")
    if args.lot_notional_usd <= 0:
        parser.error("--lot-notional-usd must be > 0")
    if args.latest <= 0:
        parser.error("--latest must be > 0")
    if args.watch:
        asyncio.run(run_watch(args))
    else:
        asyncio.run(run_once(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
