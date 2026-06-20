from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from collections import Counter
from typing import Any


async def request(endpoint: str, payload: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
    import websockets

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


def extract_orders(response: dict[str, Any]) -> list[dict[str, Any]]:
    result = response.get("result") if isinstance(response.get("result"), dict) else response
    orders_payload = result.get("orders") if isinstance(result, dict) else None
    if isinstance(orders_payload, dict):
        rows = orders_payload.get("result")
    else:
        rows = result.get("result") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def order_asset(order: dict[str, Any]) -> str:
    instrument = order.get("instrument")
    if isinstance(instrument, dict):
        value = instrument.get("underlying") or instrument.get("symbol") or instrument.get("asset")
        if value:
            return str(value).upper()
    for key in ("asset", "market", "symbol"):
        value = order.get(key)
        if value:
            return str(value).upper()
    return "UNKNOWN"


def summarize_orders(orders: list[dict[str, Any]], *, recent_limit: int) -> dict[str, Any]:
    by_side_status: Counter[str] = Counter()
    by_side_clearing: Counter[str] = Counter()
    by_status: Counter[str] = Counter()
    by_clearing: Counter[str] = Counter()
    by_asset_side_clearing: Counter[str] = Counter()
    for order in orders:
        asset = order_asset(order)
        side = str(order.get("side") or "unknown").lower()
        status = str(order.get("status") or "unknown").lower()
        clearing_status = str(order.get("clearing_status") or "none").lower()
        by_status[status] += 1
        by_clearing[clearing_status] += 1
        by_side_status[f"{side}|{status}"] += 1
        by_side_clearing[f"{side}|{clearing_status}"] += 1
        by_asset_side_clearing[f"{asset}|{side}|{clearing_status}"] += 1

    recent = []
    for order in orders[:recent_limit]:
        recent.append(
            {
                "created_at": order.get("created_at"),
                "asset": order_asset(order),
                "side": order.get("side"),
                "status": order.get("status"),
                "clearing_status": order.get("clearing_status"),
                "qty": order.get("qty"),
                "price": order.get("price"),
                "rfq_id": order.get("rfq_id"),
                "order_id": order.get("order_id"),
                "execution_timestamp": order.get("execution_timestamp"),
                "cancel_reason": order.get("cancel_reason"),
                "failed_risk_checks": order.get("failed_risk_checks"),
            }
        )

    return {
        "total_orders": len(orders),
        "by_status": dict(sorted(by_status.items())),
        "by_clearing_status": dict(sorted(by_clearing.items())),
        "by_side_status": dict(sorted(by_side_status.items())),
        "by_side_clearing_status": dict(sorted(by_side_clearing.items())),
        "by_asset_side_clearing_status": dict(sorted(by_asset_side_clearing.items())),
        "recent_orders": recent,
    }


async def fetch_pages(args: argparse.Namespace) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for page in range(args.pages):
        offset = args.offset + page * args.limit
        payload = {
            "type": "VAR_API_ORDERS",
            "account": args.account,
            "status": args.status,
            "instrument": args.instrument,
            "createdAtGte": args.created_at_gte,
            "limit": args.limit,
            "offset": offset,
            "orderBy": args.order_by,
            "order": args.order,
        }
        response = await request(args.endpoint, payload, args.timeout_seconds)
        if not response.get("ok"):
            raise RuntimeError(json.dumps(response, ensure_ascii=False, sort_keys=True))
        page_orders = extract_orders(response)
        orders.extend(page_orders)
        if len(page_orders) < args.limit:
            break
    return orders


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize Variational orders through the Chrome command broker.")
    parser.add_argument("--endpoint", default="ws://127.0.0.1:8768")
    parser.add_argument("--account")
    parser.add_argument("--status", default="pending,canceled,cleared,rejected")
    parser.add_argument("--instrument", default="P-ETH-USDC-3600")
    parser.add_argument("--created-at-gte")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--order-by", default="created_at")
    parser.add_argument("--order", default="desc", choices=("asc", "desc"))
    parser.add_argument("--recent-limit", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    args = parser.parse_args()

    if args.limit <= 0:
        parser.error("--limit must be positive")
    if args.pages <= 0:
        parser.error("--pages must be positive")
    if args.recent_limit < 0:
        parser.error("--recent-limit must be >= 0")

    orders = asyncio.run(fetch_pages(args))
    summary = summarize_orders(orders, recent_limit=args.recent_limit)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
