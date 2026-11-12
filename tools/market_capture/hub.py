from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
import time

from .config import CaptureConfig
from .model import Book, ReferenceQuote, decimal_text, edge_bps, to_iso
from .storage import TapeWriter

BOOK_VENUES = ("lighter_mainnet", "lighter_rh", "entropy")


class MarketHub:
    def __init__(self, writer: TapeWriter, config: CaptureConfig) -> None:
        self.writer = writer
        self.config = config
        self.books: dict[tuple[str, str], Book] = {}
        self.references: dict[tuple[str, str], ReferenceQuote] = {}
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
        self.references[key] = quote
        self.quote_counts[key] += 1
        self.writer.append(
            "quotes", quote.venue, quote.symbol,
            quote.quote_row(recv_mono=recv_mono),
            at=quote.recv_at,
        )

    def latest(self, venue: str, symbol: str, *, max_age: float = 10.0) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        book = self.books.get((venue, symbol))
        if book is not None and (now - book.recv_at).total_seconds() <= max_age:
            return {
                "venue": venue,
                "symbol": symbol,
                "book_available": True,
                "bid": book.best_bid,
                "ask": book.best_ask,
                "mid": book.mid,
                "spread_bps": book.spread_bps,
                "executable_buy": book.executable("buy", self.notional),
                "executable_sell": book.executable("sell", self.notional),
                "recv_at": book.recv_at.isoformat(),
                "source_at": book.source_at.isoformat() if book.source_at else None,
                "age_s": round((now - book.recv_at).total_seconds(), 3),
                "sequence": book.sequence,
                "continuity": book.continuity,
            }
        ref = self.references.get((venue, symbol))
        if ref is not None and (now - ref.recv_at).total_seconds() <= max_age:
            return {
                "venue": venue,
                "symbol": symbol,
                "book_available": False,
                "bid": None,
                "ask": None,
                "mid": ref.mid,
                "spread_bps": None,
                "executable_buy": None,
                "executable_sell": None,
                "recv_at": ref.recv_at.isoformat(),
                "source_at": ref.source_at.isoformat() if ref.source_at else None,
                "age_s": round((now - ref.recv_at).total_seconds(), 3),
                "sequence": ref.sequence,
                "continuity": True,
            }
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
                left_buy, left_sell = left["executable_buy"], left["executable_sell"]
                right_buy, right_sell = right["executable_buy"], right["executable_sell"]
                mode = "executable"
                if left_buy is None or right_sell is None:
                    left_buy = left["mid"]
                    right_sell = right["mid"]
                    mode = "reference"
                if left_buy is not None and right_sell is not None:
                    edge = edge_bps(left_buy, right_sell)
                    if edge is not None:
                        rows.append({
                            "schema": 1, "event": "edge", "symbol": symbol, "at": now,
                            "buy_venue": left_name, "sell_venue": right_name,
                            "buy_price": decimal_text(left_buy), "sell_price": decimal_text(right_sell),
                            "edge_bps": decimal_text(edge), "mode": mode,
                            "buy_age_s": left["age_s"], "sell_age_s": right["age_s"],
                        })
                edge = edge_bps(right_buy, left_sell)
                if edge is not None:
                    rows.append({
                        "schema": 1, "event": "edge", "symbol": symbol, "at": now,
                        "buy_venue": right_name, "sell_venue": left_name,
                        "buy_price": decimal_text(right_buy), "sell_price": decimal_text(left_sell),
                        "edge_bps": decimal_text(edge), "mode": mode,
                        "buy_age_s": right["age_s"], "sell_age_s": left["age_s"],
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
                symbol = asset.symbol(venue)
                key = (venue, symbol)
                last = self.books.get(key)
                ref = self.references.get(key)
                at = last.recv_at if last else (ref.recv_at if ref else None)
                out.append({
                    "venue": venue, "symbol": symbol,
                    "has_book": last is not None, "has_reference": ref is not None,
                    "last_quote_age_s": round((now - at).total_seconds(), 3) if at else None,
                    "quotes": self.quote_counts.get(key, 0),
                    "book_snapshots": self.book_counts.get(key, 0),
                })
        return out
