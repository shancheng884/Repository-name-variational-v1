from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

ZERO = Decimal("0")
BPS = Decimal("10000")


def to_decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value is None or value == "":
        return default
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return number if number.is_finite() else default


def decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def parse_epoch(value: Any) -> datetime | None:
    """Parse ISO, seconds, milliseconds, microseconds or nanoseconds."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number <= 0:
            return None
        if number > 1e17:
            number /= 1e9
        elif number > 1e14:
            number /= 1e6
        elif number > 1e11:
            number /= 1e3
        try:
            return datetime.fromtimestamp(number, timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return parse_epoch(float(text))
        except (TypeError, ValueError):
            return None


def spread_bps(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2
    return (ask - bid) / mid * BPS if mid else None


def edge_bps(buy_price: Decimal | None, sell_price: Decimal | None) -> Decimal | None:
    """Return the executable edge from buying at buy_price and selling at sell_price."""
    if buy_price is None or sell_price is None or buy_price <= 0 or sell_price <= 0:
        return None
    mid = (buy_price + sell_price) / 2
    return (sell_price - buy_price) / mid * BPS if mid else None


def adverse_bps(side: str, entry: Decimal, mark: Decimal) -> Decimal:
    """Positive means the mark moved against the position."""
    if entry <= 0:
        return ZERO
    if side == "buy":
        return (entry - mark) / entry * BPS
    return (mark - entry) / entry * BPS


def _levels(value: Iterable[Any]) -> tuple[tuple[Decimal, Decimal], ...]:
    out: list[tuple[Decimal, Decimal]] = []
    for level in value or ():
        if isinstance(level, dict):
            price = to_decimal(level.get("price", level.get("px")))
            size = to_decimal(level.get("size", level.get("sz")))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price, size = to_decimal(level[0]), to_decimal(level[1])
        else:
            continue
        if price is not None and size is not None and price > 0 and size > 0:
            out.append((price, size))
    return tuple(out)


def executable_price(
    levels: Iterable[tuple[Decimal, Decimal]], side: str, notional: Decimal
) -> Decimal | None:
    """VWAP for a quote-currency notional, or None when visible depth is short."""
    if notional <= 0:
        return None
    remaining = notional
    base = ZERO
    quote = ZERO
    ordered = sorted(levels, key=lambda item: item[0], reverse=side == "sell")
    for price, size in ordered:
        available = price * size
        take = min(available, remaining)
        if take <= 0:
            continue
        base += take / price
        quote += take
        remaining -= take
        if remaining <= ZERO:
            return quote / base if base > 0 else None
    return None


@dataclass(frozen=True)
class Book:
    venue: str
    symbol: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    recv_at: datetime
    source_at: datetime | None = None
    sequence: int | None = None
    continuity: bool = True

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    @property
    def spread_bps(self) -> Decimal | None:
        return spread_bps(self.best_bid, self.best_ask)

    def executable(self, side: str, notional: Decimal) -> Decimal | None:
        return executable_price(self.asks if side == "buy" else self.bids, side, notional)

    def top(self, limit: int = 10) -> dict[str, list[list[str]]]:
        return {
            "bids": [[decimal_text(price), decimal_text(size)] for price, size in self.bids[:limit]],
            "asks": [[decimal_text(price), decimal_text(size)] for price, size in self.asks[:limit]],
        }

    def quote_row(self, *, notional: Decimal, recv_mono: float | None = None) -> dict[str, Any]:
        buy = self.executable("buy", notional)
        sell = self.executable("sell", notional)
        latency_ms = None
        if self.source_at is not None:
            latency_ms = (self.recv_at - self.source_at).total_seconds() * 1000.0
        return {
            "schema": 1,
            "event": "quote",
            "venue": self.venue,
            "symbol": self.symbol,
            "recv_at": to_iso(self.recv_at),
            "source_at": to_iso(self.source_at),
            "source_latency_ms": round(latency_ms, 3) if latency_ms is not None else None,
            "recv_mono": recv_mono,
            "book_available": True,
            "bid": decimal_text(self.best_bid),
            "ask": decimal_text(self.best_ask),
            "bid_size": decimal_text(self.bids[0][1]) if self.bids else None,
            "ask_size": decimal_text(self.asks[0][1]) if self.asks else None,
            "mid": decimal_text(self.mid),
            "spread_bps": decimal_text(self.spread_bps),
            "executable_buy": decimal_text(buy),
            "executable_sell": decimal_text(sell),
            "notional_usd": decimal_text(notional),
            "sequence": self.sequence,
            "continuity": self.continuity,
        }


@dataclass(frozen=True)
class ReferenceQuote:
    venue: str
    symbol: str
    mid: Decimal
    recv_at: datetime
    source_at: datetime | None = None
    sequence: int | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    bbo_recv_at: datetime | None = None
    bbo_source_at: datetime | None = None
    quote_id: str | None = None
    quote_kind: str = "price_stream"
    notional_usd: Decimal | None = None
    qty: Decimal | None = None
    request_latency_ms: float | None = None
    rate_limit_reset_ms: int | None = None

    @property
    def has_bbo(self) -> bool:
        return self.bid is not None and self.ask is not None and self.ask > self.bid

    def quote_row(self, *, recv_mono: float | None = None) -> dict[str, Any]:
        latency_ms = None
        if self.source_at is not None:
            latency_ms = (self.recv_at - self.source_at).total_seconds() * 1000.0
        bbo_recv_at = self.bbo_recv_at or (self.recv_at if self.has_bbo else None)
        bbo_source_at = self.bbo_source_at or (self.source_at if self.has_bbo else None)
        bbo_latency_ms = None
        if bbo_source_at is not None and bbo_recv_at is not None:
            bbo_latency_ms = (bbo_recv_at - bbo_source_at).total_seconds() * 1000.0
        executable_buy = self.ask if self.has_bbo else None
        executable_sell = self.bid if self.has_bbo else None
        return {
            "schema": 1,
            "event": "quote",
            "venue": self.venue,
            "symbol": self.symbol,
            "recv_at": to_iso(self.recv_at),
            "source_at": to_iso(self.source_at),
            "source_latency_ms": round(latency_ms, 3) if latency_ms is not None else None,
            "recv_mono": recv_mono,
            "book_available": False,
            "bid": decimal_text(self.bid),
            "ask": decimal_text(self.ask),
            "bid_size": None,
            "ask_size": None,
            "mid": decimal_text(self.mid),
            "spread_bps": decimal_text(spread_bps(self.bid, self.ask)),
            "executable_buy": decimal_text(executable_buy),
            "executable_sell": decimal_text(executable_sell),
            "notional_usd": decimal_text(self.notional_usd),
            "qty": decimal_text(self.qty),
            "sequence": self.sequence,
            "continuity": True,
            "quote_kind": self.quote_kind,
            "quote_id": self.quote_id,
            "bbo_recv_at": to_iso(bbo_recv_at),
            "bbo_source_at": to_iso(bbo_source_at),
            "bbo_source_latency_ms": round(bbo_latency_ms, 3) if bbo_latency_ms is not None else None,
            "request_latency_ms": round(self.request_latency_ms, 3) if self.request_latency_ms is not None else None,
            "rate_limit_reset_ms": self.rate_limit_reset_ms,
        }


@dataclass(frozen=True)
class Fill:
    venue: str
    symbol: str
    side: str
    qty: Decimal
    price: Decimal
    recv_at: datetime
    source_at: datetime | None = None
    fee_usd: Decimal | None = None
    order_id: str | None = None
    trade_id: str | None = None
    status: str = "filled"
    fill_quality: str = "trade_record"
    raw_ref: str | None = None
    client_order_id: str | None = None
    pair_id: str | None = None
    submit_at: datetime | None = None
    ack_at: datetime | None = None
    fill_at: datetime | None = None
    signal_at: datetime | None = None
    seen_mid: Decimal | None = None
    seen_executable: Decimal | None = None
    seen_quote_id: str | None = None

    @property
    def event_id(self) -> str:
        return ":".join(
            part for part in (
                self.venue,
                self.symbol,
                self.pair_id or "",
                self.trade_id or self.order_id or "",
                self.side,
                to_iso(self.source_at) or to_iso(self.recv_at) or "",
            ) if part
        )

    def row(self) -> dict[str, Any]:
        return {
            "schema": 1,
            "event": "fill",
            "event_id": self.event_id,
            "venue": self.venue,
            "symbol": self.symbol,
            "side": self.side,
            "qty": decimal_text(self.qty),
            "price": decimal_text(self.price),
            "notional_usd": decimal_text(self.qty * self.price),
            "fee_usd": decimal_text(self.fee_usd),
            "order_id": self.order_id,
            "trade_id": self.trade_id,
            "client_order_id": self.client_order_id,
            "pair_id": self.pair_id,
            "status": self.status,
            "fill_quality": self.fill_quality,
            "source_at": to_iso(self.source_at),
            "recv_at": to_iso(self.recv_at),
            "submit_at": to_iso(self.submit_at),
            "ack_at": to_iso(self.ack_at),
            "fill_at": to_iso(self.fill_at),
            "signal_at": to_iso(self.signal_at),
            "seen_mid": decimal_text(self.seen_mid),
            "seen_executable": decimal_text(self.seen_executable),
            "seen_quote_id": self.seen_quote_id,
            "raw_ref": self.raw_ref,
        }


def book_from_levels(
    venue: str,
    symbol: str,
    bids: Iterable[Any],
    asks: Iterable[Any],
    *,
    recv_at: datetime,
    source_at: datetime | None = None,
    sequence: int | None = None,
    continuity: bool = True,
) -> Book:
    return Book(
        venue=venue,
        symbol=symbol,
        bids=_levels(bids),
        asks=_levels(asks),
        recv_at=recv_at,
        source_at=source_at,
        sequence=sequence,
        continuity=continuity,
    )
