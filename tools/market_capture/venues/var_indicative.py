from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any
import asyncio
import json
import logging
import time
import uuid

from ..hub import MarketHub
from ..model import ReferenceQuote, parse_epoch, to_decimal
from ..storage import TapeWriter
from .base import VenueHealth


class VariationalIndicativeFeed:
    """Round-robin exact-size indicative RFQ poller.

    The endpoint is the same one used by a Var order confirmation, so every
    response is recorded with its quote id, requested quantity, round-trip
    latency and the exchange rate-limit hint. No order is submitted here.
    """

    def __init__(
        self,
        *,
        command_url: str,
        assets: tuple[Any, ...],
        notional_usd: Decimal,
        writer: TapeWriter,
        hub: MarketHub,
        logger: logging.Logger,
        interval_seconds: float = 1.0,
        timeout_seconds: float = 4.0,
    ) -> None:
        self.name = "variational:indicative"
        self.command_url = command_url
        self.assets = tuple(asset for asset in assets if asset.supports("variational"))
        self.notional_usd = Decimal(str(notional_usd))
        self.writer = writer
        self.hub = hub
        self.logger = logger
        self.interval_seconds = max(0.2, float(interval_seconds))
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.health = VenueHealth(self.name)
        self.health.subscribed = [asset.name for asset in self.assets]
        self.requests = 0
        self.successes = 0
        self.errors = 0
        self.rate_limited = 0
        self.last_rate_limit_reset_ms: int | None = None
        self.last_quote_id: str | None = None

    @staticmethod
    def _amount_for_notional(mid: Decimal, notional_usd: Decimal) -> Decimal | None:
        if mid <= 0 or notional_usd <= 0:
            return None
        raw = notional_usd / mid
        return raw.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    def _payload(self, asset: Any, amount: Decimal) -> dict[str, Any]:
        return {
            "type": "VAR_API_QUOTE",
            "requestId": uuid.uuid4().hex,
            "market": asset.symbol("variational"),
            "amount": format(amount, "f"),
            "instrumentType": asset.var_instrument_type,
            "settlementAsset": "USDC",
            "kind": asset.var_kind,
            "fundingIntervalS": 0,
            "confirm": False,
            "requestTimeoutMs": int(self.timeout_seconds * 1000),
            "operationTimeoutMs": int(self.timeout_seconds * 1000),
        }

    async def run(self) -> None:
        from tools.lib.wti_readonly_adapter import VariationalForwarderClient

        while True:
            try:
                async with VariationalForwarderClient(self.command_url, timeout_seconds=self.timeout_seconds + 2.0) as client:
                    self.health.status = "running"
                    self.health.connected = True
                    while True:
                        limited = False
                        for asset in self.assets:
                            row = await self.poll_one(client, asset)
                            if row is not None and row.get("rate_limit_reset_ms"):
                                limited = True
                        await asyncio.sleep(self.interval_seconds * (2.0 if limited else 1.0))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.connected = False
                self.health.status = "reconnecting"
                self.health.reconnects += 1
                self.health.error(f"{type(exc).__name__}:{exc}")
                self.logger.warning("variational_indicative_reconnect error=%s", exc)
                await asyncio.sleep(min(10.0, self.interval_seconds * 5.0))

    def _reference_mid(self, asset: Any) -> Decimal | None:
        ref = self.hub.references.get(("variational", asset.name))
        return ref.mid if ref is not None else None

    async def poll_one(self, client: Any, asset: Any) -> dict[str, Any] | None:
        mid = self._reference_mid(asset)
        amount = self._amount_for_notional(mid, self.notional_usd) if mid is not None else None
        if amount is None:
            self.health.error(f"reference_mid_missing:{asset.name}")
            return None
        payload = self._payload(asset, amount)
        started = time.monotonic()
        self.requests += 1
        try:
            message = await client.request(payload)
        except Exception as exc:
            self.errors += 1
            self.health.error(f"quote:{type(exc).__name__}:{exc}")
            self.writer.append("var_quote_errors", self.name, asset.name, {
                "schema": 1, "event": "var_quote_error", "at": datetime.now(timezone.utc).isoformat(),
                "asset": asset.name, "amount": format(amount, "f"),
                "error": f"{type(exc).__name__}:{exc}"[:240],
            })
            return None
        elapsed_ms = (time.monotonic() - started) * 1000.0
        result = message.get("result") if isinstance(message, dict) else None
        if not isinstance(result, dict):
            result = message if isinstance(message, dict) else {}
        ok = bool(isinstance(message, dict) and message.get("ok") is True and result.get("ok") is True)
        bid = to_decimal(result.get("bid"))
        ask = to_decimal(result.get("ask"))
        quote_id = str(result.get("quoteId") or result.get("quote_id") or "") or None
        rate_limit = result.get("rateLimitResetMs")
        try:
            rate_limit = int(rate_limit) if rate_limit is not None else None
        except (TypeError, ValueError):
            rate_limit = None
        recv_at = datetime.now(timezone.utc)
        source_at = parse_epoch(result.get("quoteTimestamp") or result.get("timestamp")) or recv_at
        status = str(result.get("status") or "").lower()
        if rate_limit is not None:
            self.last_rate_limit_reset_ms = rate_limit
        if not ok:
            text = str(result.get("error") or (message.get("error") if isinstance(message, dict) else "") or "unknown")
            if "rate" in text.lower() or rate_limit:
                self.rate_limited += 1
            self.errors += 1
            self.health.error(f"quote_rejected:{asset.name}:{text[:160]}")
        elif bid is None or ask is None or ask <= bid:
            self.errors += 1
            self.health.error(f"quote_shape_invalid:{asset.name}")
        else:
            self.successes += 1
            self.last_quote_id = quote_id
            quote = ReferenceQuote(
                venue="variational",
                symbol=asset.name,
                mid=(bid + ask) / 2,
                recv_at=recv_at,
                source_at=source_at,
                bid=bid,
                ask=ask,
                bbo_recv_at=recv_at,
                bbo_source_at=source_at,
                quote_id=quote_id,
                quote_kind="variational_indicative",
                notional_usd=self.notional_usd,
                qty=amount,
                request_latency_ms=elapsed_ms,
                rate_limit_reset_ms=rate_limit,
            )
            self.hub.update_reference(quote, recv_mono=time.monotonic())
            self.health.last_quote_at = recv_at
            self.health.message()
        row = {
            "schema": 1,
            "event": "var_indicative_quote",
            "at": recv_at.isoformat(),
            "asset": asset.name,
            "market": asset.symbol("variational"),
            "instrument_type": asset.var_instrument_type,
            "kind": asset.var_kind,
            "notional_usd": str(self.notional_usd),
            "requested_qty": format(amount, "f"),
            "ok": ok,
            "status": status,
            "bid": str(bid) if bid is not None else None,
            "ask": str(ask) if ask is not None else None,
            "quote_id": quote_id,
            "quote_timestamp": source_at.isoformat() if source_at else None,
            "request_latency_ms": round(elapsed_ms, 3),
            "rate_limit_reset_ms": rate_limit,
            "request_id": payload["requestId"],
            "error": None if ok else str(result.get("error") or "")[:200] or None,
        }
        self.writer.append("var_indicative", self.name, asset.name, row, at=recv_at)
        return row

    def public_state(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "successes": self.successes,
            "errors": self.errors,
            "rate_limited": self.rate_limited,
            "last_rate_limit_reset_ms": self.last_rate_limit_reset_ms,
            "last_quote_id": self.last_quote_id,
        }
