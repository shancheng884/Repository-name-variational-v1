from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
import asyncio
import json
import logging
import time
import uuid

import websockets

from ..audit import FillAuditor
from ..hub import MarketHub
from ..model import Fill, ReferenceQuote, decimal_text, parse_epoch, to_decimal
from ..storage import TapeWriter
from .base import VenueHealth


class VariationalPriceFeed:
    def __init__(
        self,
        *,
        ws_url: str,
        assets: tuple[Any, ...],
        writer: TapeWriter,
        hub: MarketHub,
        logger: logging.Logger,
    ) -> None:
        self.name = "variational"
        self.ws_url = ws_url
        self.assets = tuple(asset for asset in assets if asset.supports("variational"))
        self.writer = writer
        self.hub = hub
        self.logger = logger
        self.health = VenueHealth(self.name)
        self.channels: dict[str, str] = {
            f"instrument_price:P-RWA/{'ETF' if asset.var_kind == 'etf' else 'EQY'}-{asset.symbol('variational')}-USDC": asset.name
            for asset in self.assets
        }
        self.health.subscribed = list(self.channels)

    def _subscription(self) -> dict[str, Any]:
        return {
            "action": "subscribe",
            "instruments": [
                {
                    "underlying": asset.symbol("variational"),
                    "instrument_type": asset.var_instrument_type,
                    "settlement_asset": "USDC",
                    "kind": asset.var_kind,
                }
                for asset in self.assets
            ],
        }

    def _subscription_batches(self, size: int = 3) -> list[dict[str, Any]]:
        instruments = self._subscription()["instruments"]
        return [
            {"action": "subscribe", "instruments": instruments[index:index + size]}
            for index in range(0, len(instruments), size)
        ]

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=2 ** 23) as ws:
                    self.health.status = "running"
                    self.health.connected = True
                    for batch in self._subscription_batches():
                        await ws.send(json.dumps(batch))
                        await asyncio.sleep(0.2)
                    backoff = 1.0
                    async for raw in ws:
                        self.health.message()
                        message = json.loads(raw)
                        self.handle(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.connected = False
                self.health.status = "reconnecting"
                self.health.reconnects += 1
                self.health.error(f"{type(exc).__name__}:{exc}")
                self.logger.warning("variational_capture_reconnect error=%s", exc)
                if "429" in str(exc):
                    backoff = max(backoff, 30.0)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 300.0)

    def handle(self, message: dict[str, Any]) -> None:
        if not isinstance(message, dict):
            return
        if message.get("type") == "heartbeat":
            return
        if "error" in message and not message.get("channel"):
            self.health.error(str(message.get("error"))[:200])
            return
        channel = str(message.get("channel") or "")
        asset = self.channels.get(channel)
        if asset is None:
            return
        pricing = message.get("pricing") or {}
        price = to_decimal(pricing.get("price") or pricing.get("underlying_price"))
        if price is None or price <= 0:
            self.health.error(f"pricing_invalid:{channel}")
            return
        source_at = parse_epoch(pricing.get("timestamp") or message.get("timestamp"))
        quote = ReferenceQuote(
            venue=self.name,
            symbol=asset,
            mid=price,
            recv_at=datetime.now(timezone.utc),
            source_at=source_at,
        )
        self.hub.update_reference(quote, recv_mono=time.monotonic())
        self.health.last_quote_at = quote.recv_at


class VariationalFillFeed:
    def __init__(
        self,
        *,
        command_url: str,
        assets: tuple[Any, ...],
        writer: TapeWriter,
        auditor: FillAuditor,
        interval: float = 5.0,
        timeout_seconds: float = 12.0,
        accept_after: datetime | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.command_url = command_url
        self.assets = assets
        self.symbol_to_asset = {
            asset.symbol("variational"): asset.name
            for asset in assets if asset.supports("variational")
        }
        self.writer = writer
        self.auditor = auditor
        self.interval = max(2.0, interval)
        self.timeout_seconds = timeout_seconds
        self.accept_after = accept_after
        self.logger = logger or logging.getLogger(__name__)
        self.health = VenueHealth("variational:fills")
        self.seen: set[str] = set()

    async def run(self) -> None:
        from tools.lib.wti_readonly_adapter import VariationalForwarderClient

        while True:
            try:
                async with VariationalForwarderClient(self.command_url, timeout_seconds=self.timeout_seconds) as client:
                    self.health.status = "running"
                    self.health.connected = True
                    while True:
                        rows, error = await self._fetch(client)
                        if error:
                            self.health.error(error)
                            await asyncio.sleep(self.interval)
                            continue
                        for row in rows:
                            fill = self._normalize(row)
                            if fill is None or fill.order_id in self.seen:
                                continue
                            self.seen.add(fill.order_id or "")
                            self.auditor.submit(fill)
                            self.health.last_quote_at = fill.recv_at
                        self.health.message()
                        await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.connected = False
                self.health.status = "reconnecting"
                self.health.reconnects += 1
                self.health.error(f"{type(exc).__name__}:{exc}")
                await asyncio.sleep(10)

    async def _fetch(self, client: Any) -> tuple[list[dict[str, Any]], str | None]:
        since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        payload = {
            "type": "VAR_API_ORDERS",
            "requestId": uuid.uuid4().hex,
            "status": "pending,canceled,cleared,rejected",
            "createdAtGte": since,
            "limit": 100,
            "offset": 0,
            "orderBy": "created_at",
            "order": "desc",
            "requestTimeoutMs": int(self.timeout_seconds * 1000),
        }
        message = await client.request(payload)
        if not isinstance(message, dict) or message.get("ok") is not True:
            return [], f"variational_orders_failed:{str(message.get('error'))[:120]}"
        result = message.get("result")
        rows = _order_rows(result)
        if rows is None:
            return [], "variational_orders_shape_invalid"
        return rows, None

    def _normalize(self, row: dict[str, Any]) -> Fill | None:
        instrument = row.get("instrument") if isinstance(row.get("instrument"), dict) else {}
        symbol = str(
            instrument.get("underlying")
            or row.get("underlying")
            or row.get("symbol")
            or ""
        ).upper()
        asset = self.symbol_to_asset.get(symbol)
        if asset is None:
            return None
        status = str(row.get("status") or "").lower()
        clearing = str(row.get("clearing_status") or "").lower()
        if status not in {"cleared", "filled"} and "success" not in clearing:
            return None
        side = str(row.get("side") or "").lower()
        if side not in {"buy", "sell"}:
            return None
        qty = to_decimal(row.get("qty") or row.get("filled_qty") or row.get("size"))
        price = to_decimal(row.get("price") or row.get("average_price") or row.get("limit_price"))
        if qty is None or price is None or qty <= 0 or price <= 0:
            return None
        at = parse_epoch(row.get("execution_timestamp") or row.get("created_at"))
        if self.accept_after is not None and (at is None or at < self.accept_after):
            return None
        return Fill(
            venue="variational",
            symbol=asset,
            side=side,
            qty=qty,
            price=price,
            recv_at=datetime.now(timezone.utc),
            source_at=at,
            fee_usd=to_decimal(row.get("fee") or row.get("fee_usd")),
            order_id=str(row.get("order_id") or row.get("id") or "") or None,
            trade_id=str(row.get("trade_id") or row.get("order_id") or "") or None,
            status=status or "cleared",
            fill_quality="order_record",
        )


def _order_rows(payload: Any) -> list[dict[str, Any]] | None:
    current = payload
    for _ in range(10):
        if isinstance(current, list):
            return [row for row in current if isinstance(row, dict)]
        if not isinstance(current, dict):
            return None
        next_value = None
        for key in ("orders", "result", "data"):
            if key in current:
                next_value = current[key]
                break
        if next_value is None:
            return None
        current = next_value
    return None
