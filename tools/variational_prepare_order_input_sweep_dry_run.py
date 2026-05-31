from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from typing import Any

import websockets


async def run_sweep(endpoint: str, side: str, amount: str, timeout_seconds: float) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20) as websocket:
        await websocket.send(
            json.dumps(
                {
                    "type": "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN",
                    "requestId": request_id,
                    "side": side.upper(),
                    "amount": amount,
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
    parser = argparse.ArgumentParser(description="Try visible Variational order inputs without submitting.")
    parser.add_argument("--endpoint", default="ws://127.0.0.1:8768")
    parser.add_argument("--side", choices=("BUY", "SELL", "buy", "sell"), default="BUY")
    parser.add_argument("--amount", default="20")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    args = parser.parse_args()

    result = asyncio.run(run_sweep(args.endpoint, args.side, args.amount, args.timeout_seconds))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
