from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
import asyncio
import json
import logging
import time

import requests
import websockets

from ..audit import FillAuditor
from ..hub import MarketHub
from ..model import Fill, book_from_levels, decimal_text, parse_epoch, to_decimal
from ..storage import TapeWriter
from .base import VenueHealth


class EntropyBookFeed:
    def __init__(
        self,
        *,
        info_url: str,
        ws_url: str,
        dex: str,
        asset_symbols: dict[str, str],
        writer: TapeWriter,
        hub: MarketHub,
        logger: logging.Logger,
        max_levels: int = 50,
    ) -> None:
        self.name = "entropy"
        self.info_url = info_url
        self.ws_url = ws_url
        self.dex = dex
        self.asset_symbols = asset_symbols
        self.writer = writer
        self.hub = hub
        self.logger = logger
        self.max_levels = max_levels
        self.health = VenueHealth(self.name)
        self.coin_to_asset: dict[str, str] = {}
        self.universe: dict[str, dict[str, Any]] = {}

    async def load_universe(self) -> None:
        response = await asyncio.to_thread(
            requests.post, self.info_url, json={"type": "meta", "dex": self.dex}, timeout=15
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        universe = payload.get("universe") if isinstance(payload, dict) else None
        if not isinstance(universe, list):
            raise RuntimeError("entropy_meta_universe_missing")
        by_name = {str(row.get("name") or ""): row for row in universe if isinstance(row, dict)}
        for asset, coin in self.asset_symbols.items():
            row = by_name.get(coin)
            if row is None:
                self.health.error(f"market_not_found:{coin}")
                continue
            self.coin_to_asset[coin] = asset
            self.universe[coin] = dict(row)
        self.health.subscribed = [f"{asset}:{coin}" for coin, asset in self.coin_to_asset.items()]
        if not self.coin_to_asset:
            raise RuntimeError("entropy_no_supported_markets")

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    self.health.status = "running"
                    self.health.connected = True
                    for coin in self.coin_to_asset:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))
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
                self.logger.warning("entropy_capture_reconnect error=%s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    def handle(self, message: dict[str, Any]) -> None:
        channel = message.get("channel")
        data = message.get("data")
        if channel == "l2Book" and isinstance(data, dict):
            coin = str(data.get("coin") or "")
            asset = self.coin_to_asset.get(coin)
            if asset is None:
                return
            levels = data.get("levels") or [[], []]
            if not isinstance(levels, list) or len(levels) < 2:
                return
            book = book_from_levels(
                self.name, asset, levels[0][: self.max_levels], levels[1][: self.max_levels],
                recv_at=datetime.now(timezone.utc), source_at=parse_epoch(data.get("time")),
            )
            self.hub.update_book(book, recv_mono=time.monotonic())
            self.health.last_quote_at = book.recv_at
        elif channel == "trades" and isinstance(data, list):
            for trade in data:
                self._write_trade(trade)

    def _write_trade(self, trade: dict[str, Any]) -> None:
        coin = str(trade.get("coin") or "")
        asset = self.coin_to_asset.get(coin)
        if asset is None:
            return
        at = parse_epoch(trade.get("time"))
        self.writer.append(
            "market_trades", self.name, asset,
            {
                "schema": 1, "event": "market_trade",
                "venue": self.name, "symbol": asset,
                "trade_id": str(trade.get("tid") or trade.get("hash") or ""),
                "side": "buy" if trade.get("side") == "B" else "sell",
                "price": decimal_text(to_decimal(trade.get("px"))),
                "size": decimal_text(to_decimal(trade.get("sz"))),
                "source_at": at.isoformat() if at else None,
                "recv_at": datetime.now(timezone.utc).isoformat(),
            },
            at=at,
        )


class EntropyFillFeed:
    def __init__(
        self,
        *,
        info_url: str,
        dex: str,
        asset_symbols: dict[str, str],
        writer: TapeWriter,
        auditor: FillAuditor,
        account_address: str,
        interval: float = 2.0,
        accept_after: datetime | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.info_url = info_url
        self.dex = dex
        self.asset_symbols = asset_symbols
        self.writer = writer
        self.auditor = auditor
        self.account_address = account_address
        self.interval = max(1.0, interval)
        self.accept_after = accept_after
        self.logger = logger or logging.getLogger(__name__)
        self.health = VenueHealth("entropy:fills")
        self.seen: set[str] = set()

    @property
    def configured(self) -> bool:
        return bool(self.account_address.startswith("0x") and len(self.account_address) >= 42)

    async def run(self) -> None:
        if not self.configured:
            self.health.status = "disabled_missing_address"
            return
        self.health.status = "running"
        self.health.connected = True
        while True:
            try:
                rows = await asyncio.to_thread(self._fetch)
            except Exception as exc:
                self.health.error(f"user_fills:{type(exc).__name__}:{exc}")
                await asyncio.sleep(self.interval)
                continue
            for row in rows:
                fill = self._normalize(row)
                if fill is None or fill.trade_id in self.seen:
                    continue
                self.seen.add(fill.trade_id or "")
                self.auditor.submit(fill)
                self.health.last_quote_at = fill.recv_at
            self.health.message()
            await asyncio.sleep(self.interval)

    def _fetch(self) -> list[dict[str, Any]]:
        response = requests.post(
            self.info_url,
            json={"type": "userFills", "user": self.account_address, "aggregateByTime": False},
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()
        return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []

    def _normalize(self, row: dict[str, Any]) -> Fill | None:
        coin = str(row.get("coin") or "")
        asset = None
        for name, symbol in self.asset_symbols.items():
            if symbol == coin:
                asset = name
                break
        if asset is None:
            return None
        price = to_decimal(row.get("px"))
        qty = to_decimal(row.get("sz"))
        if price is None or qty is None or price <= 0 or qty <= 0:
            return None
        side = {"B": "buy", "A": "sell"}.get(str(row.get("side") or "").upper())
        if side is None:
            return None
        at = parse_epoch(row.get("time"))
        if self.accept_after is not None and (at is None or at < self.accept_after):
            return None
        return Fill(
            venue="entropy",
            symbol=asset,
            side=side,
            qty=qty,
            price=price,
            recv_at=datetime.now(timezone.utc),
            source_at=at,
            fee_usd=to_decimal(row.get("fee")),
            order_id=str(row.get("oid") or "") or None,
            trade_id=str(row.get("tid") or row.get("hash") or "") or None,
            fill_quality="trade_record",
        )
