from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from typing import Any

import websockets


async def run_probe(endpoint: str, timeout_seconds: float) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
    async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20) as websocket:
        await websocket.send(
            json.dumps(
                {
                    "type": "PAGE_PROBE",
                    "requestId": request_id,
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
    parser = argparse.ArgumentParser(description="Probe the Variational Chrome command channel.")
    parser.add_argument("--endpoint", default="ws://127.0.0.1:8768")
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    args = parser.parse_args()

    result = asyncio.run(run_probe(args.endpoint, args.timeout_seconds))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
