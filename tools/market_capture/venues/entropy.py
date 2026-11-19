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

from ...lib.nvda_quote_alignment import quote_clock_issue
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
        fast_depth: bool = False,
        race_bbo: bool = False,
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
        self.fast_depth = fast_depth
        self.race_bbo = race_bbo
        self.health = VenueHealth(self.name)
        self.backup_health = VenueHealth(f"{self.name}:bbo_backup") if race_bbo else None
        self.bbo_wins = {"primary": 0, "backup": 0}
        self.bbo_drops = {"duplicate": 0, "older": 0, "conflict": 0, "invalid": 0}
        self._last_bbo: dict[str, tuple[datetime, tuple[Decimal, ...]]] = {}
        self.coin_to_asset: dict[str, str] = {}
        self.universe: dict[str, dict[str, Any]] = {}
        self.book_messages = 0
        self.bbo_messages = 0
        self.trade_messages = 0

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
        if self.race_bbo:
            await asyncio.gather(self._run_lane(primary=True), self._run_lane(primary=False))
        else:
            await self._run_lane(primary=True)

    def bbo_race_health(self, now: datetime) -> dict[str, Any]:
        assert self.backup_health is not None
        return {"backup": self.backup_health.public(now), "wins": dict(self.bbo_wins),
                "drops": dict(self.bbo_drops)}

    async def _run_lane(self, *, primary: bool) -> None:
        lane = "primary" if primary else "backup"
        health = self.health if primary else self.backup_health
        assert health is not None
        health.subscribed = list(self.health.subscribed)
        retry_floor = 5.0 if self.race_bbo else 1.0
        backoff = retry_floor
        while True:
            started = time.monotonic()
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    for coin in self.coin_to_asset:
                        if primary:
                            subscription = {"type": "l2Book", "coin": coin}
                            if self.fast_depth:
                                subscription["fast"] = True
                            await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "bbo", "coin": coin}}))
                        if primary:
                            await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))
                    health.status = "running"
                    health.connected = True
                    if not self.race_bbo:
                        backoff = retry_floor
                    async for raw in self._messages(ws, keepalive=not primary):
                        recv_at = datetime.now(timezone.utc)
                        health.message()
                        message = json.loads(raw)
                        if primary or message.get("channel") == "bbo":
                            self.handle(message, lane=lane, recv_at=recv_at)
                    raise ConnectionError("entropy_ws_closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                health.connected = False
                health.status = "reconnecting"
                health.reconnects += 1
                health.error(f"{type(exc).__name__}:{exc}")
                self.logger.warning("entropy_capture_reconnect lane=%s error=%s", lane, exc)
                if time.monotonic() - started >= 60.0:
                    backoff = retry_floor
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    @staticmethod
    async def _messages(ws: Any, *, keepalive: bool):
        if not keepalive:
            async for raw in ws:
                yield raw
            return
        while True:
            try:
                async with asyncio.timeout(40.0):
                    raw = await ws.recv()
            except TimeoutError:
                await ws.send('{"method":"ping"}')
                async with asyncio.timeout(15.0):
                    raw = await ws.recv()
            yield raw

    def _accept_bbo(self, coin: str, source_at: datetime | None, recv_at: datetime,
                    bid: Decimal, ask: Decimal,
                    bid_size: Decimal | None, ask_size: Decimal | None) -> bool:
        if (source_at is None or not 0 < bid <= ask or bid_size is None or ask_size is None
                or min(bid_size, ask_size) <= 0 or quote_clock_issue(
                    {"venue": self.name, "source_at": source_at, "recv_at": recv_at}, recv_at.timestamp())):
            self.bbo_drops["invalid"] += 1
            return False
        top = (bid, ask, bid_size, ask_size)
        previous = self._last_bbo.get(coin)
        if previous is not None and source_at <= previous[0]:
            kind = "older" if source_at < previous[0] else "duplicate" if top == previous[1] else "conflict"
            self.bbo_drops[kind] += 1
            return False
        self._last_bbo[coin] = (source_at, top)
        return True

    def handle(self, message: dict[str, Any], *, lane: str = "primary",
               recv_at: datetime | None = None) -> None:
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
            self.book_messages += 1
            self.health.last_quote_at = book.recv_at
        elif channel == "bbo" and isinstance(data, dict):
            coin = str(data.get("coin") or "")
            asset = self.coin_to_asset.get(coin)
            if asset is None:
                return
            top = data.get("bbo") or []
            if not isinstance(top, list) or len(top) < 2:
                return
            bid_row = top[0] if isinstance(top[0], dict) else {}
            ask_row = top[1] if isinstance(top[1], dict) else {}
            bid = to_decimal(bid_row.get("px"))
            ask = to_decimal(ask_row.get("px"))
            if bid is None or ask is None:
                return
            source_at = parse_epoch(data.get("time"))
            bid_size = to_decimal(bid_row.get("sz"))
            ask_size = to_decimal(ask_row.get("sz"))
            recv_at = recv_at or datetime.now(timezone.utc)
            if self.race_bbo:
                # Both readers run on this loop. Publish once without awaiting the other lane.
                if not self._accept_bbo(coin, source_at, recv_at, bid, ask, bid_size, ask_size):
                    return
                self.bbo_wins[lane] += 1
            self.hub.update_bbo(
                venue=self.name, symbol=asset, bid=bid, ask=ask, quote_kind="bbo",
                recv_at=recv_at, source_at=source_at,
                bid_size=bid_size, ask_size=ask_size,
            )
            self.bbo_messages += 1
            self.health.last_quote_at = recv_at
            if lane == "backup" and self.backup_health is not None:
                self.backup_health.last_quote_at = recv_at
        elif channel == "trades" and isinstance(data, list):
            for trade in data:
                self._write_trade(trade)

    def _write_trade(self, trade: dict[str, Any]) -> None:
        coin = str(trade.get("coin") or "")
        asset = self.coin_to_asset.get(coin)
        if asset is None:
            return
        at = parse_epoch(trade.get("time"))
        self.trade_messages += 1
        # The print is the only sound staleness proof: a trade through the
        # quoted top means the ladder we hold is already wrong.
        self.hub.update_trade(
            venue=self.name, symbol=asset, price=to_decimal(trade.get("px")),
            size=to_decimal(trade.get("sz")),
            side="buy" if trade.get("side") == "B" else "sell",
            at=at, recv_at=datetime.now(timezone.utc),
        )
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
