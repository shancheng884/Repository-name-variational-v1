from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from decimal import Decimal
from typing import Any

import websockets


async def request(endpoint: str, payload: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    payload = dict(payload)
    payload["requestId"] = request_id
    async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20) as websocket:
        await websocket.send(json.dumps(payload, ensure_ascii=True))
        while True:
            raw = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
            message = json.loads(raw)
            if message.get("requestId") == request_id:
                return message


def positive_decimal(parser: argparse.ArgumentParser, value: str, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception:
        parser.error(f"{name} must be a decimal number")
    if result <= 0:
        parser.error(f"{name} must be positive")
    return result


def build_payload(args: argparse.Namespace, market: str, amount: Decimal) -> dict[str, Any]:
    if args.action == "positions":
        return {"type": "VAR_API_POSITIONS", "account": args.account}
    if args.action == "orders":
        return {
            "type": "VAR_API_ORDERS",
            "account": args.account,
            "status": args.status,
            "instrument": args.instrument,
            "createdAtGte": args.created_at_gte,
            "limit": args.limit,
            "offset": args.offset,
            "orderBy": args.order_by,
            "order": args.order,
        }
    if args.action == "quote":
        return {
            "type": "VAR_API_QUOTE",
            "market": market,
            "amount": str(amount),
            "account": args.account,
        }
    return {
        "type": "VAR_API_ORDER",
        "side": str(args.side).upper(),
        "market": market,
        "amount": str(amount),
        "maxSlippage": args.max_slippage,
        "reduceOnly": args.reduce_only,
        "reuseQuoteId": args.reuse_quote_id,
        "account": args.account,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Call Variational page API through the Chrome command broker.")
    parser.add_argument("action", choices=("positions", "orders", "quote", "order"))
    parser.add_argument("--endpoint", default="ws://127.0.0.1:8768")
    parser.add_argument("--market", default="BTC")
    parser.add_argument("--amount", default="0", help="Base asset quantity, e.g. BTC amount.")
    parser.add_argument("--side", choices=("BUY", "SELL", "buy", "sell"))
    parser.add_argument("--max-slippage", type=float, default=0.005)
    parser.add_argument("--reduce-only", action="store_true")
    parser.add_argument("--reuse-quote-id")
    parser.add_argument("--account")
    parser.add_argument("--status", default="pending,canceled,cleared,rejected", help="Order status filter for action=orders.")
    parser.add_argument("--instrument", help="Instrument filter for action=orders, e.g. P-ETH-USDC-3600.")
    parser.add_argument("--created-at-gte", help="created_at_gte filter for action=orders.")
    parser.add_argument("--limit", type=int, default=20, help="Result limit for action=orders.")
    parser.add_argument("--offset", type=int, default=0, help="Result offset for action=orders.")
    parser.add_argument("--order-by", default="created_at", help="Sort field for action=orders.")
    parser.add_argument("--order", default="desc", choices=("asc", "desc"), help="Sort order for action=orders.")
    parser.add_argument("--confirm", action="store_true", help="Required for action=order.")
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    args = parser.parse_args()

    market = str(args.market).strip().upper()
    if not market:
        parser.error("--market is required")

    if args.action in {"quote", "order"}:
        amount = positive_decimal(parser, args.amount, "--amount")
    else:
        amount = Decimal("0")

    if args.action == "order":
        if not args.confirm:
            parser.error("action=order requires --confirm")
        if not args.side:
            parser.error("action=order requires --side")
    if args.max_slippage < 0:
        parser.error("--max-slippage must be >= 0")

    payload = build_payload(args, market, amount)

    result = asyncio.run(request(args.endpoint, payload, args.timeout_seconds))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
