from __future__ import annotations

from collections import Counter, deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
import time

from .config import CaptureConfig
from .model import Book, ReferenceQuote, decimal_text, edge_bps, spread_bps, to_iso
from .storage import TapeWriter

def _dec(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        out = Decimal(str(value))
    except Exception:
        return None
    return out if out.is_finite() else None


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


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
        self._bbo_tops: dict[tuple[str, str], dict[str, Any]] = {}
        self._last_trades: dict[tuple[str, str], dict[str, Any]] = {}
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

    def update_bbo(
        self,
        *,
        venue: str,
        symbol: str,
        bid: Decimal,
        ask: Decimal,
        quote_kind: str = "bbo",
        recv_at: datetime | None = None,
        source_at: datetime | None = None,
        bid_size: Decimal | None = None,
        ask_size: Decimal | None = None,
        sequence: int | None = None,
    ) -> None:
        """Fast top-of-book channel.

        Hyperliquid publishes `l2Book` ladders every ~7-12s, but pushes `bbo` on
        every top-of-book change.  Without the fast channel every entropy round
        is priced off a ladder that is already seconds old.
        """
        now = recv_at or datetime.now(timezone.utc)
        key = (venue, symbol)
        self._bbo_tops[key] = {
            "bid": bid, "ask": ask, "bid_size": bid_size, "ask_size": ask_size,
            "recv_at": now, "source_at": source_at, "quote_kind": quote_kind,
            "sequence": sequence,
        }
        if bid is None or ask is None:
            return
        self.update_reference(
            ReferenceQuote(
                venue=venue, symbol=symbol, mid=(bid + ask) / 2, recv_at=now,
                source_at=source_at, sequence=sequence, bid=bid, ask=ask,
                bbo_recv_at=now, bbo_source_at=source_at, quote_kind=quote_kind,
                notional_usd=self.notional,
            )
        )

    def update_trade(
        self,
        *,
        venue: str,
        symbol: str,
        price: Decimal | None,
        size: Decimal | None = None,
        side: str | None = None,
        at: datetime | None = None,
        recv_at: datetime | None = None,
    ) -> None:
        """Remember the last print so a stale ladder can be proven stale."""
        if price is None:
            return
        self._last_trades[(venue, symbol)] = {
            "price": price, "size": size, "side": side,
            "at": at or datetime.now(timezone.utc),
            "recv_at": recv_at or datetime.now(timezone.utc),
        }

    def _trade_staleness(
        self,
        venue: str,
        symbol: str,
        bid: Decimal | None,
        ask: Decimal | None,
        quote_at: datetime | None,
        quote_source_at: datetime | None = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Prove a quote stale only when a later print traded through its top.

        A quiet book is not a stale book: if nothing has printed since the
        snapshot, the quoted top is still the live top.  A print above the ask
        (or below the bid) after the snapshot is proof the ladder moved.
        """
        if bid is None or ask is None or quote_at is None:
            return False, None
        trade = self._last_trades.get((venue, symbol))
        if not trade:
            return False, None
        at = trade.get("at")
        price = trade.get("price")
        received = trade.get("recv_at")
        if at is None or price is None:
            return False, None
        # The print has to have been delivered after we snapshotted, and its own
        # venue timestamp has to be at or after the snapshot's, or it is a
        # delayed print that cannot carry information about the current top.
        if received is not None and received <= quote_at:
            return False, None
        if quote_source_at is not None and at < quote_source_at:
            return False, None
        if price > ask:
            return True, {"reason": "trade_above_ask", "trade_price": decimal_text(price),
                          "trade_at": to_iso(at), "ask": decimal_text(ask)}
        if price < bid:
            return True, {"reason": "trade_below_bid", "trade_price": decimal_text(price),
                          "trade_at": to_iso(at), "bid": decimal_text(bid)}
        return False, None

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
        """Freshest tradeable quote: book ladder overlaid with the fast BBO.

        The book still supplies depth-aware executable prices; the BBO supplies
        the true top whenever it is newer.  `provably_stale` is set only when a
        later print traded through the quoted top, which is the sound way to
        call a quiet ladder wrong without inventing a staleness timer.
        """
        now = datetime.now(timezone.utc)
        key = (venue, symbol)
        book = self.books.get(key)
        book_age = round((now - book.recv_at).total_seconds(), 3) if book is not None else None
        bbo = self._bbo_tops.get(key)
        bbo_age = round((now - bbo["recv_at"]).total_seconds(), 3) if bbo is not None else None
        snap: dict[str, Any] | None = None
        if book is not None and book_age is not None and book_age <= max_age:
            snap = self._book_snapshot(book)
            snap["age_s"] = book_age
            snap["recv_at"] = book.recv_at.isoformat()
            snap["source_at"] = book.source_at.isoformat() if book.source_at else None
            snap["depth_source"] = "book"
        if bbo is not None and bbo_age is not None and bbo_age <= max_age:
            book_at = book.recv_at if book is not None else None
            if snap is None or book_at is None or bbo["recv_at"] >= book_at:
                if snap is None:
                    snap = {
                        "venue": venue, "symbol": symbol, "book_available": True,
                        "continuity": True, "sequence": bbo.get("sequence"),
                        "depth_source": "bbo_only",
                    }
                bid, ask = bbo.get("bid"), bbo.get("ask")
                snap["bid"] = bid
                snap["ask"] = ask
                if bid is not None and ask is not None:
                    snap["mid"] = (bid + ask) / 2
                    snap["spread_bps"] = spread_bps(bid, ask)
                    snap["executable_buy"] = ask
                    snap["executable_sell"] = bid
                snap["bid_size"] = bbo.get("bid_size")
                snap["ask_size"] = bbo.get("ask_size")
                snap["age_s"] = bbo_age
                snap["recv_at"] = bbo["recv_at"].isoformat()
                snap["source_at"] = bbo["source_at"].isoformat() if bbo.get("source_at") else snap.get("source_at")
                snap["quote_kind"] = "book+bbo" if book is not None else "bbo"
                if snap.get("depth_source") == "book":
                    snap["depth_source"] = "bbo_overlay"
        if snap is None:
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
        snap["book_age_s"] = book_age
        snap["bbo_age_s"] = bbo_age
        stale, evidence = self._trade_staleness(
            venue, symbol, _dec(snap.get("bid")), _dec(snap.get("ask")),
            _parse_iso(snap.get("recv_at")), _parse_iso(snap.get("source_at")),
        )
        snap["provably_stale"] = stale
        snap["staleness_evidence"] = evidence
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
