from __future__ import annotations

from collections import Counter, deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
import time

from .config import CaptureConfig
from .model import Book, ReferenceQuote, decimal_text, edge_bps, to_iso
from .storage import TapeWriter

BOOK_VENUES = ("lighter_mainnet", "lighter_rh", "entropy")
REFERENCE_HISTORY_SECONDS = 120.0


class MarketHub:
    def __init__(self, writer: TapeWriter, config: CaptureConfig) -> None:
        self.writer = writer
        self.config = config
        self.books: dict[tuple[str, str], Book] = {}
        self.references: dict[tuple[str, str], ReferenceQuote] = {}
        self._mid_quotes: dict[tuple[str, str], ReferenceQuote] = {}
        self._bbo_quotes: dict[tuple[str, str], ReferenceQuote] = {}
        self._history: dict[tuple[str, str], deque[tuple[datetime, dict[str, Any]]]] = {}
        self._last_book_write: dict[tuple[str, str], float] = {}
        self.quote_counts: Counter[tuple[str, str]] = Counter()
        self.book_counts: Counter[tuple[str, str]] = Counter()
        self.edge_counts: Counter[str] = Counter()

    @property
    def notional(self) -> Decimal:
        return self.config.notional_usd

    def update_book(self, book: Book, *, recv_mono: float | None = None) -> None:
        key = (book.venue, book.symbol)
        self.books[key] = book
        self.quote_counts[key] += 1
        self.writer.append(
            "quotes", book.venue, book.symbol,
            book.quote_row(notional=self.notional, recv_mono=recv_mono),
            at=book.recv_at,
        )
        self._remember(key, book.recv_at, self._book_snapshot(book))
        now = time.monotonic()
        last = self._last_book_write.get(key, 0.0)
        if now - last >= self.config.book_snapshot_seconds:
            self._last_book_write[key] = now
            self.book_counts[key] += 1
            top = book.top(self.config.max_book_levels)
            self.writer.append(
                "books", book.venue, book.symbol,
                {
                    "schema": 1,
                    "event": "book_snapshot",
                    "venue": book.venue,
                    "symbol": book.symbol,
                    "recv_at": to_iso(book.recv_at),
                    "source_at": to_iso(book.source_at),
                    "sequence": book.sequence,
                    "continuity": book.continuity,
                    "bids": top["bids"],
                    "asks": top["asks"],
                    "best_bid": decimal_text(book.best_bid),
                    "best_ask": decimal_text(book.best_ask),
                    "spread_bps": decimal_text(book.spread_bps),
                },
                at=book.recv_at,
            )

    def update_reference(
        self, quote: ReferenceQuote, *, recv_mono: float | None = None
    ) -> None:
        key = (quote.venue, quote.symbol)
        self.quote_counts[key] += 1
        self.writer.append(
            "quotes", quote.venue, quote.symbol,
            quote.quote_row(recv_mono=recv_mono),
            at=quote.recv_at,
        )
        if quote.has_bbo or quote.quote_kind == "variational_indicative":
            self._bbo_quotes[key] = quote
        else:
            self._mid_quotes[key] = quote
        merged = self._merge_reference(key)
        if merged is not None:
            self.references[key] = merged
            self._remember(key, merged.recv_at, self._reference_snapshot(key, merged))

    def _merge_reference(self, key: tuple[str, str]) -> ReferenceQuote | None:
        mid = self._mid_quotes.get(key)
        bbo = self._bbo_quotes.get(key)
        if mid is None and bbo is None:
            return None
        if bbo is None:
            return mid
        if mid is None or mid.venue != bbo.venue:
            return bbo
        return ReferenceQuote(
            venue=bbo.venue,
            symbol=bbo.symbol,
            mid=mid.mid,
            recv_at=max(mid.recv_at, bbo.recv_at),
            source_at=mid.source_at,
            sequence=mid.sequence,
            bid=bbo.bid,
            ask=bbo.ask,
            bbo_recv_at=bbo.bbo_recv_at or bbo.recv_at,
            bbo_source_at=bbo.bbo_source_at or bbo.source_at,
            quote_id=bbo.quote_id,
            quote_kind=bbo.quote_kind,
            notional_usd=bbo.notional_usd,
            qty=bbo.qty,
            request_latency_ms=bbo.request_latency_ms,
            rate_limit_reset_ms=bbo.rate_limit_reset_ms,
        )

    def _book_snapshot(self, book: Book) -> dict[str, Any]:
        return {
            "venue": book.venue,
            "symbol": book.symbol,
            "book_available": True,
            "bid": book.best_bid,
            "ask": book.best_ask,
            "mid": book.mid,
            "spread_bps": book.spread_bps,
            "executable_buy": book.executable("buy", self.notional),
            "executable_sell": book.executable("sell", self.notional),
            "recv_at": book.recv_at,
            "source_at": book.source_at,
            "sequence": book.sequence,
            "continuity": book.continuity,
            "quote_kind": "book",
        }

    def _reference_snapshot(self, key: tuple[str, str], ref: ReferenceQuote) -> dict[str, Any]:
        mid = self._mid_quotes.get(key)
        bbo = self._bbo_quotes.get(key)
        now = datetime.now(timezone.utc)
        mid_age = (now - mid.recv_at).total_seconds() if mid is not None else None
        bbo_age = (now - bbo.recv_at).total_seconds() if bbo is not None else None
        can_trade = (
            bbo is not None and bbo.has_bbo and bbo_age is not None
            and bbo_age <= 10.0 and mid_age is not None and mid_age <= 10.0
        )
        return {
            "venue": ref.venue,
            "symbol": ref.symbol,
            "book_available": False,
            "bid": bbo.bid if can_trade and bbo is not None else None,
            "ask": bbo.ask if can_trade and bbo is not None else None,
            "mid": ref.mid,
            "spread_bps": (bbo.ask - bbo.bid) / ((bbo.ask + bbo.bid) / 2) * Decimal("10000") if can_trade and bbo is not None else None,
            "executable_buy": bbo.ask if can_trade and bbo is not None else None,
            "executable_sell": bbo.bid if can_trade and bbo is not None else None,
            "recv_at": ref.recv_at,
            "bbo_recv_at": bbo.recv_at if bbo is not None else None,
            "source_at": ref.source_at,
            "bbo_source_at": bbo.source_at if bbo is not None else None,
            "age_s": round((bbo_age if can_trade else mid_age) or 0.0, 3),
            "mid_age_s": round(mid_age, 3) if mid_age is not None else None,
            "bbo_age_s": round(bbo_age, 3) if bbo_age is not None else None,
            "sequence": ref.sequence,
            "continuity": True,
            "quote_kind": ref.quote_kind,
            "quote_id": ref.quote_id,
            "notional_usd": ref.notional_usd,
            "qty": ref.qty,
            "request_latency_ms": ref.request_latency_ms,
            "rate_limit_reset_ms": ref.rate_limit_reset_ms,
        }

    def _remember(self, key: tuple[str, str], at: datetime, snapshot: dict[str, Any]) -> None:
        history = self._history.setdefault(key, deque(maxlen=5000))
        history.append((at, snapshot))
        cutoff = at.timestamp() - REFERENCE_HISTORY_SECONDS
        while history and history[0][0].timestamp() < cutoff:
            history.popleft()

    def latest(self, venue: str, symbol: str, *, max_age: float = 10.0) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        book = self.books.get((venue, symbol))
        if book is not None and (now - book.recv_at).total_seconds() <= max_age:
            snap = self._book_snapshot(book)
            snap["age_s"] = round((now - book.recv_at).total_seconds(), 3)
            snap["recv_at"] = book.recv_at.isoformat()
            snap["source_at"] = book.source_at.isoformat() if book.source_at else None
            return snap
        key = (venue, symbol)
        ref = self.references.get(key)
        if ref is None:
            return None
        snap = self._reference_snapshot(key, ref)
        if snap["age_s"] > max_age:
            return None
        snap["recv_at"] = ref.recv_at.isoformat()
        snap["source_at"] = ref.source_at.isoformat() if ref.source_at else None
        snap["bbo_recv_at"] = snap["bbo_recv_at"].isoformat() if snap["bbo_recv_at"] else None
        snap["bbo_source_at"] = snap["bbo_source_at"].isoformat() if snap["bbo_source_at"] else None
        return snap

    def as_of(self, venue: str, symbol: str, at: datetime | None) -> dict[str, Any] | None:
        if at is None:
            return None
        history = self._history.get((venue, symbol))
        if not history:
            return None
        target = at.astimezone(timezone.utc) if at.tzinfo else at.replace(tzinfo=timezone.utc)
        for ts, snap in reversed(history):
            if ts <= target:
                out = dict(snap)
                out["age_s"] = round((target - ts).total_seconds(), 3)
                out["recv_at"] = ts.isoformat()
                source_at = out.get("source_at")
                out["source_at"] = source_at.isoformat() if isinstance(source_at, datetime) else source_at
                bbo_recv_at = out.get("bbo_recv_at")
                out["bbo_recv_at"] = bbo_recv_at.isoformat() if isinstance(bbo_recv_at, datetime) else bbo_recv_at
                bbo_source_at = out.get("bbo_source_at")
                out["bbo_source_at"] = bbo_source_at.isoformat() if isinstance(bbo_source_at, datetime) else bbo_source_at
                return out
        return None

    def edge_rows(self, symbol: str) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat()
        snapshots = {}
        for venue in BOOK_VENUES + ("variational",):
            quote = self.latest(venue, symbol, max_age=10.0)
            if quote is not None:
                snapshots[venue] = quote
        rows: list[dict[str, Any]] = []
        venues = list(snapshots)
        for left_index, left_name in enumerate(venues):
            for right_name in venues[left_index + 1:]:
                left, right = snapshots[left_name], snapshots[right_name]
                for buy_name, sell_name, buy, sell in ((left_name, right_name, left, right), (right_name, left_name, right, left)):
                    buy_price, sell_price = buy.get("executable_buy"), sell.get("executable_sell")
                    mode = "executable"
                    if buy_price is None or sell_price is None:
                        buy_price, sell_price = buy.get("mid"), sell.get("mid")
                        mode = "reference"
                    if buy_price is None or sell_price is None:
                        continue
                    edge = edge_bps(Decimal(str(buy_price)), Decimal(str(sell_price)))
                    if edge is None:
                        continue
                    rows.append({
                        "schema": 1, "event": "edge", "symbol": symbol, "at": now,
                        "buy_venue": buy_name, "sell_venue": sell_name,
                        "buy_price": decimal_text(Decimal(str(buy_price))),
                        "sell_price": decimal_text(Decimal(str(sell_price))),
                        "edge_bps": decimal_text(edge), "mode": mode,
                        "buy_quote_kind": buy.get("quote_kind"),
                        "sell_quote_kind": sell.get("quote_kind"),
                        "buy_age_s": buy.get("age_s"), "sell_age_s": sell.get("age_s"),
                        "buy_quote_id": buy.get("quote_id"), "sell_quote_id": sell.get("quote_id"),
                    })
        for row in rows:
            self.writer.append("edges", "cross", symbol, row)
            self.edge_counts[symbol] += 1
        return rows

    def freshness(self) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        out = []
        for venue in ("variational",) + BOOK_VENUES:
            for asset in self.config.assets:
                if not asset.supports(venue):
                    continue
                symbol = asset.name
                key = (venue, symbol)
                last = self.books.get(key)
                ref = self.references.get(key)
                mid = self._mid_quotes.get(key)
                bbo = self._bbo_quotes.get(key)
                at = last.recv_at if last else (ref.recv_at if ref else None)
                out.append({
                    "venue": venue, "symbol": symbol,
                    "has_book": last is not None, "has_reference": ref is not None,
                    "has_bbo": bool(bbo and bbo.has_bbo),
                    "last_quote_age_s": round((now - at).total_seconds(), 3) if at else None,
                    "bbo_age_s": round((now - bbo.recv_at).total_seconds(), 3) if bbo else None,
                    "mid_age_s": round((now - mid.recv_at).total_seconds(), 3) if mid else None,
                    "quotes": self.quote_counts.get(key, 0),
                    "book_snapshots": self.book_counts.get(key, 0),
                })
        return out
