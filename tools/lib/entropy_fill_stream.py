"""Read-only account fill stream, matched by exchange order ID."""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import datetime, timezone
from decimal import Decimal
import json

import websockets


class EntropyFillStream:
    def __init__(self, url, address):
        self.url, self.address = url, address
        self.orders = OrderedDict()
        self.changed = asyncio.Event()
        self.task = None
        self.last_error = None
        self.connected = False
        self.messages = 0
        self.last_received_at = None

    def start(self):
        self.task = asyncio.create_task(self.run(), name="entropy-execution-fills")

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.connected = False

    def handle(self, message):
        if message.get("channel") != "userFills":
            return
        self.messages += 1
        self.last_received_at = datetime.now(timezone.utc).isoformat()
        for row in (message.get("data") or {}).get("fills", []):
            oid = str(row.get("oid") or "")
            if not oid:
                continue
            key = str(row.get("tid") or (row.get("hash"), row.get("time"), row.get("px"), row.get("sz")))
            fills = self.orders.setdefault(oid, {})
            fills[key] = dict(row)
            self.orders.move_to_end(oid)
            while len(self.orders) > 2048:
                self.orders.popitem(last=False)
        self.changed.set()

    def rows(self, oid, coin, expected_qty=None):
        rows = [r for r in self.orders.get(str(oid), {}).values() if r.get("coin") == coin]
        if not rows:
            return None
        try:
            quantities = [Decimal(str(r["sz"])) for r in rows]
            if any(not q.is_finite() or q <= 0 for q in quantities):
                return None
            total = sum(quantities, Decimal("0"))
        except (KeyError, ValueError, ArithmeticError):
            return None
        if expected_qty is not None and total < expected_qty:
            return None
        return rows

    async def wait(self, oid, coin, expected_qty):
        while True:
            self.changed.clear()
            rows = self.rows(oid, coin, expected_qty)
            if rows:
                return rows
            await self.changed.wait()

    async def run(self):
        backoff = .5
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    self.connected = True
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {
                        "type": "userFills", "user": self.address}}))
                    backoff = .5
                    async for raw in ws:
                        self.handle(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.last_error = type(exc).__name__
                await asyncio.sleep(backoff)
                backoff = min(10, backoff * 2)
