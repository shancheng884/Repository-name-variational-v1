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


class LighterBookFeed:
    def __init__(
        self,
        *,
        name: str,
        rest_url: str,
        ws_url: str,
        asset_symbols: dict[str, str],
        writer: TapeWriter,
        hub: MarketHub,
        logger: logging.Logger,
        max_levels: int = 50,
    ) -> None:
        self.name = name
        self.rest_url = rest_url.rstrip("/")
        self.ws_url = ws_url
        self.asset_symbols = asset_symbols
        self.writer = writer
        self.hub = hub
        self.logger = logger
        self.max_levels = max_levels
        self.health = VenueHealth(name)
        self.markets: dict[str, dict[str, Any]] = {}
        self.market_to_asset: dict[int, str] = {}
        self.books: dict[int, dict[str, Any]] = {}
        self.seen_trades: set[str] = set()

    async def load_markets(self) -> None:
        response = await asyncio.to_thread(
            requests.get, f"{self.rest_url}/api/v1/orderBooks", timeout=15
        )
        response.raise_for_status()
        rows = response.json().get("order_books") or []
        by_symbol = {str(row.get("symbol") or "").upper(): row for row in rows}
        for asset, venue_symbol in self.asset_symbols.items():
            row = by_symbol.get(venue_symbol.upper())
            if row is None:
                self.health.error(f"market_not_found:{venue_symbol}")
                continue
            market_id = int(row["market_id"])
            self.markets[asset] = dict(row)
            self.market_to_asset[market_id] = asset
            self.books[market_id] = {"bids": {}, "asks": {}, "nonce": None, "ready": False}
        self.health.subscribed = [f"{asset}:{row['market_id']}" for asset, row in self.markets.items()]
        if not self.markets:
            raise RuntimeError(f"{self.name}_no_supported_markets")

    @staticmethod
    def _levels(value: list[Any]) -> list[tuple[Decimal, Decimal]]:
        out = []
        for level in value or ():
            if isinstance(level, dict):
                price = to_decimal(level.get("price"))
                size = to_decimal(level.get("size"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price, size = to_decimal(level[0]), to_decimal(level[1])
            else:
                continue
            if price is not None and size is not None:
                out.append((price, size))
        return out

    @staticmethod
    def _apply(target: dict[Decimal, Decimal], levels: list[Any]) -> None:
        for price, size in LighterBookFeed._levels(levels):
            if size > 0:
                target[price] = size
            else:
                target.pop(price, None)

    def _handle(self, message: dict[str, Any]) -> bool:
        channel = str(message.get("channel") or "")
        if channel.startswith("trade:"):
            self._handle_trades(channel, message)
            return False
        market_id = None
        for separator in (":", "/"):
            if channel.startswith(f"order_book{separator}"):
                try:
                    market_id = int(channel.split(separator, 1)[1])
                except ValueError:
                    return False
                break
        if market_id not in self.books:
            return False
        payload = message.get("order_book") or {}
        state = self.books[market_id]
        message_type = str(message.get("type") or "")
        begin_nonce = payload.get("begin_nonce")
        nonce = payload.get("nonce")
        snapshot = not state["ready"] or message_type.startswith("subscribed/")
        if (
            not snapshot
            and begin_nonce is not None
            and state["nonce"] is not None
            and int(begin_nonce) != int(state["nonce"])
        ):
            state["ready"] = False
            state["bids"].clear()
            state["asks"].clear()
            self.health.error(f"sequence_gap:{market_id}:{begin_nonce}!={state['nonce']}")
            return True
        if snapshot:
            state["bids"].clear()
            state["asks"].clear()
        self._apply(state["bids"], payload.get("bids", []))
        self._apply(state["asks"], payload.get("asks", []))
        if nonce is not None:
            state["nonce"] = int(nonce)
        state["ready"] = bool(state["bids"] and state["asks"])
        if not state["ready"]:
            return False
        asset = self.market_to_asset[market_id]
        bids = sorted(state["bids"].items(), reverse=True)[: self.max_levels]
        asks = sorted(state["asks"].items())[: self.max_levels]
        source_at = parse_epoch(payload.get("last_updated_at") or message.get("timestamp"))
        book = book_from_levels(
            self.name, asset, bids, asks,
            recv_at=datetime.now(timezone.utc), source_at=source_at,
            sequence=state["nonce"], continuity=True,
        )
        self.hub.update_book(book, recv_mono=time.monotonic())
        self.health.last_quote_at = book.recv_at
        return False

    def _handle_trades(self, channel: str, message: dict[str, Any]) -> None:
        try:
            market_id = int(channel.split(":", 1)[1])
        except (IndexError, ValueError):
            return
        asset = self.market_to_asset.get(market_id)
        if asset is None:
            return
        rows = list(message.get("trades") or []) + list(message.get("liquidation_trades") or [])
        for row in rows:
            if not isinstance(row, dict):
                continue
            trade_id = str(row.get("trade_id") or row.get("trade_id_str") or "")
            if not trade_id or trade_id in self.seen_trades:
                continue
            self.seen_trades.add(trade_id)
            at = parse_epoch(row.get("timestamp") or message.get("timestamp"))
            # Feed the print into the hub so a ladder a trade has traded
            # through is provably stale, exactly as for the entropy feed.
            self.hub.update_trade(
                venue=self.name, symbol=asset,
                price=to_decimal(row.get("price")), size=to_decimal(row.get("size")),
                side=row.get("type") if isinstance(row.get("type"), str) else None,
                at=at, recv_at=datetime.now(timezone.utc),
            )
            self.writer.append(
                "market_trades", self.name, asset,
                {
                    "schema": 1, "event": "market_trade",
                    "venue": self.name, "symbol": asset,
                    "trade_id": trade_id,
                    "price": decimal_text(to_decimal(row.get("price"))),
                    "size": decimal_text(to_decimal(row.get("size"))),
                    "usd_amount": decimal_text(to_decimal(row.get("usd_amount"))),
                    "trade_type": row.get("type"),
                    "source_at": at.isoformat() if at else None,
                    "recv_at": datetime.now(timezone.utc).isoformat(),
                },
                at=at,
            )
        if len(self.seen_trades) > 100000:
            self.seen_trades = set(list(self.seen_trades)[-40000:])

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=25, ping_timeout=25, max_size=None) as ws:
                    self.health.status = "running"
                    self.health.connected = True
                    for market_id in self.books:
                        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))
                        await ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{market_id}"}))
                    backoff = 1.0
                    async for raw in ws:
                        self.health.message()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        message = json.loads(raw)
                        if message.get("type") == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                            continue
                        needs_resubscribe = self._handle(message)
                        if needs_resubscribe:
                            for market_id in self.books:
                                await ws.send(json.dumps({"type": "unsubscribe", "channel": f"order_book/{market_id}"}))
                            for market_id in self.books:
                                await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.connected = False
                self.health.status = "reconnecting"
                self.health.reconnects += 1
                self.health.error(f"{type(exc).__name__}:{exc}")
                self.logger.warning("lighter_capture_reconnect venue=%s error=%s", self.name, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)


class LighterTradePoller:
    def __init__(
        self,
        *,
        feed: LighterBookFeed,
        writer: TapeWriter,
        interval: float = 5.0,
    ) -> None:
        self.feed = feed
        self.writer = writer
        self.interval = max(1.0, interval)
        self.seen: set[str] = set()

    async def run(self) -> None:
        while True:
            for asset, market in self.feed.markets.items():
                try:
                    rows = await asyncio.to_thread(self._fetch, int(market["market_id"]))
                except Exception as exc:
                    self.feed.health.error(f"recent_trades:{type(exc).__name__}")
                    continue
                for row in rows:
                    trade_id = str(row.get("trade_id") or "")
                    if not trade_id or trade_id in self.seen:
                        continue
                    self.seen.add(trade_id)
                    at = parse_epoch(row.get("timestamp") or row.get("created_at"))
                    self.writer.append(
                        "market_trades", self.feed.name, asset,
                        {
                            "schema": 1, "event": "market_trade",
                            "venue": self.feed.name, "symbol": asset,
                            "trade_id": trade_id,
                            "price": decimal_text(to_decimal(row.get("price"))),
                            "size": decimal_text(to_decimal(row.get("size"))),
                            "usd_amount": decimal_text(to_decimal(row.get("usd_amount"))),
                            "source_at": at.isoformat() if at else None,
                            "recv_at": datetime.now(timezone.utc).isoformat(),
                        },
                        at=at,
                    )
                if len(self.seen) > 50000:
                    self.seen = set(list(self.seen)[-20000:])
            await asyncio.sleep(self.interval)

    def _fetch(self, market_id: int) -> list[dict[str, Any]]:
        response = requests.get(
            f"{self.feed.rest_url}/api/v1/recentTrades",
            params={"market_id": market_id, "limit": 100},
            timeout=10,
        )
        response.raise_for_status()
        return list(response.json().get("trades") or [])


class LighterFillFeed:
    def __init__(
        self,
        *,
        name: str,
        rest_url: str,
        asset_symbols: dict[str, str],
        markets: dict[str, dict[str, Any]],
        writer: TapeWriter,
        auditor: FillAuditor,
        account_index: int | None,
        api_key_index: int | None,
        private_key: str,
        interval: float = 2.0,
        accept_after: datetime | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.name = name
        self.rest_url = rest_url.rstrip("/")
        self.asset_symbols = asset_symbols
        self.markets = markets
        self.writer = writer
        self.auditor = auditor
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.private_key = private_key
        self.interval = max(1.0, interval)
        self.accept_after = accept_after
        self.logger = logger or logging.getLogger(__name__)
        self.health = VenueHealth(f"{name}:fills")
        self.seen: set[str] = set()
        self.market_ids = {int(row["market_id"]): asset for asset, row in markets.items()}

    @property
    def configured(self) -> bool:
        return bool(
            self.account_index is not None
            and self.api_key_index is not None
            and self.private_key
        )

    async def run(self) -> None:
        if not self.configured:
            self.health.status = "disabled_missing_credentials"
            return
        try:
            def _load_sdk():
                from lighter.api import OrderApi
                from lighter.signer_client import SignerClient
                return OrderApi, SignerClient

            OrderApi, SignerClient = await asyncio.to_thread(_load_sdk)
            # SignerClient builds an aiohttp connector and must be constructed
            # in the event-loop thread; construction itself is cheap.
            client = SignerClient(
                url=self.rest_url,
                account_index=int(self.account_index),
                api_private_keys={int(self.api_key_index): self.private_key},
            )
            api = OrderApi(client.api_client)
            self.health.status = "running"
            self.health.connected = True
            token = None
            token_at = 0.0
            while True:
                if token is None or time.monotonic() - token_at > 600:
                    token, error = await asyncio.to_thread(
                        client.create_auth_token_with_expiry,
                        api_key_index=int(self.api_key_index),
                    )
                    token_at = time.monotonic()
                    if error is not None or not token:
                        self.health.error("lighter_auth_token_unavailable")
                        token = None
                        await asyncio.sleep(self.interval)
                        continue
                try:
                    response = await api.trades(
                        sort_by="timestamp",
                        limit=100,
                        auth=token,
                        account_index=int(self.account_index),
                        sort_dir="desc",
                        _request_timeout=8.0,
                    )
                    rows = _rows(response)
                except Exception as exc:
                    self.health.error(f"trades:{type(exc).__name__}:{exc}")
                    token = None
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
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.health.connected = False
            self.health.status = "error"
            self.health.error(f"{type(exc).__name__}:{exc}")
            return
        finally:
            client = locals().get("client")
            close = getattr(client, "close", None)
            if callable(close):
                try:
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    pass

    def _normalize(self, row: dict[str, Any]) -> Fill | None:
        market_id = row.get("market_id")
        try:
            market_id = int(market_id)
        except (TypeError, ValueError):
            return None
        asset = self.market_ids.get(market_id)
        if asset is None:
            return None
        price = to_decimal(row.get("price"))
        qty = to_decimal(row.get("size", row.get("qty", row.get("base_amount"))))
        if price is None or qty is None or price <= 0 or qty <= 0:
            return None
        ask_account = row.get("ask_account_id", row.get("ask_account_index"))
        bid_account = row.get("bid_account_id", row.get("bid_account_index"))
        if str(ask_account) == str(self.account_index):
            side = "sell"
        elif str(bid_account) == str(self.account_index):
            side = "buy"
        else:
            return None
        at = parse_epoch(row.get("timestamp") or row.get("created_at") or row.get("trade_time"))
        if self.accept_after is not None and (at is None or at < self.accept_after):
            return None
        return Fill(
            venue=self.name,
            symbol=asset,
            side=side,
            qty=qty,
            price=price,
            recv_at=datetime.now(timezone.utc),
            source_at=at,
            fee_usd=to_decimal(row.get("fee") or row.get("fee_usd")),
            order_id=str(row.get("order_id") or row.get("ask_id") or row.get("bid_id") or "") or None,
            trade_id=str(row.get("trade_id") or row.get("trade_id_str") or "") or None,
            fill_quality="trade_record",
        )


def _rows(response: Any) -> list[dict[str, Any]]:
    if response is None:
        return []
    if isinstance(response, dict):
        for key in ("trades", "orders", "data"):
            value = response.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        return []
    for attr in ("trades", "orders", "data"):
        value = getattr(response, attr, None)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    if hasattr(response, "to_dict"):
        return _rows(response.to_dict())
    if hasattr(response, "model_dump"):
        return _rows(response.model_dump(mode="json"))
    return []
