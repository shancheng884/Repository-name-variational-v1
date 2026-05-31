from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from decimal import Decimal
from typing import Any

import websockets


async def place_order(
    endpoint: str,
    side: str,
    amount: str,
    confirm: bool,
    expected_min_btc_qty: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20) as websocket:
        await websocket.send(
            json.dumps(
                {
                    "type": "PLACE_ORDER",
                    "requestId": request_id,
                    "side": side.upper(),
                    "amount": amount,
                    "confirm": confirm,
                    "expectedMinBtcQty": expected_min_btc_qty,
                },
                ensure_ascii=True,
            )
        )
        while True:
            raw = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
            payload = json.loads(raw)
            if payload.get("requestId") == request_id:
                return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare and optionally click a Variational order submit button.")
    parser.add_argument("--endpoint", default="ws://127.0.0.1:8768")
    parser.add_argument("--side", choices=("BUY", "SELL", "buy", "sell"), required=True)
    parser.add_argument("--amount", required=True, help="Variational Size value, normally USD when the panel is in $ mode.")
    parser.add_argument("--max-amount", default="5", help="Safety cap for --amount. Default: 5")
    parser.add_argument(
        "--expected-min-btc-qty",
        default="0",
        help="If set above 0, require page Order Quantity to be at least this BTC amount before clicking.",
    )
    parser.add_argument("--confirm", action="store_true", help="Actually click the submit button. Without this, no click occurs.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    args = parser.parse_args()

    amount = Decimal(str(args.amount))
    max_amount = Decimal(str(args.max_amount))
    if amount <= 0:
        parser.error("--amount must be positive")
    if max_amount <= 0:
        parser.error("--max-amount must be positive")
    if amount > max_amount:
        parser.error(f"--amount {amount} exceeds --max-amount {max_amount}")
    expected_min_btc_qty = Decimal(str(args.expected_min_btc_qty))
    if expected_min_btc_qty < 0:
        parser.error("--expected-min-btc-qty must be non-negative")

    result = asyncio.run(
        place_order(
            args.endpoint,
            args.side,
            str(amount),
            args.confirm,
            str(expected_min_btc_qty),
            args.timeout_seconds,
        )
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
