from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable
import asyncio

from .hub import MarketHub, BOOK_VENUES
from .model import Fill, adverse_bps, decimal_text, edge_bps, to_iso
from .storage import TapeWriter


def _bps(price: Decimal | None, reference: Decimal | None, *, buy: bool) -> Decimal | None:
    if price is None or reference is None or reference <= 0:
        return None
    return ((price - reference) if buy else (reference - price)) / reference * Decimal("10000")


class FillAuditor:
    """Build a causal chain around every observed own fill.

    Quote lookups are strictly as-of the exchange fill timestamp when that
    timestamp is available. Later quotes are used only in the explicit
    post-fill horizons, so the entry measurement cannot include look-ahead.
    """

    def __init__(
        self,
        hub: MarketHub,
        writer: TapeWriter,
        *,
        delays: tuple[float, ...] = (0.25, 0.5, 1.0, 3.0),
        stall_snapshot: Callable[[float, float], dict[str, Any]] | None = None,
    ) -> None:
        self.hub = hub
        self.writer = writer
        self.delays = tuple(sorted(delays))
        self.stall_snapshot = stall_snapshot or (lambda start, end: {})
        self.tasks: set[asyncio.Task[Any]] = set()
        self.audited = 0

    def submit(self, fill: Fill) -> None:
        self.writer.append("fills", fill.venue, fill.symbol, fill.row(), at=fill.recv_at)
        task = asyncio.create_task(self._audit(fill), name=f"fill-audit:{fill.event_id}")
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _audit(self, fill: Fill) -> None:
        chain = self.start_chain(fill)
        elapsed = 0.0
        for delay in self.delays:
            await asyncio.sleep(max(0.0, delay - elapsed))
            elapsed = delay
            self.capture_horizon(chain, fill, delay)
        chain["completed_at"] = datetime.now(timezone.utc).isoformat()
        chain["summary"] = self.summarize(chain)
        self.writer.append("chains", fill.venue, fill.symbol, chain, at=fill.recv_at)
        self.audited += 1

    def _quote_at(self, venue: str, symbol: str, at: datetime | None) -> dict[str, Any] | None:
        if at is not None:
            return self.hub.as_of(venue, symbol, at)
        return self.hub.latest(venue, symbol, max_age=10.0)

    def start_chain(self, fill: Fill) -> dict[str, Any]:
        at = fill.source_at or fill.recv_at
        own = self._quote_at(fill.venue, fill.symbol, at)
        alignment = "causal_as_of_fill" if fill.source_at is not None else "receive_time_only"
        fill_mid = Decimal(str(own["mid"])) if own and own.get("mid") is not None else fill.seen_mid
        fill_vs_mid = _bps(fill.price, fill_mid, buy=fill.side == "buy")
        seen_executable = fill.seen_executable
        if own is not None:
            seen_executable = (
                Decimal(str(own["ask"])) if fill.side == "buy" and own.get("ask") is not None
                else Decimal(str(own["bid"])) if fill.side == "sell" and own.get("bid") is not None
                else seen_executable
            )
        fill_vs_seen = _bps(fill.price, seen_executable, buy=fill.side == "buy")
        hedge = self.best_hedge(fill, at=at)
        return {
            "schema": 1,
            "event": "fill_chain",
            "chain_id": fill.event_id,
            "pair_id": fill.pair_id,
            "client_order_id": fill.client_order_id,
            "venue": fill.venue,
            "symbol": fill.symbol,
            "side": fill.side,
            "qty": decimal_text(fill.qty),
            "fill_price": decimal_text(fill.price),
            "fill_source_at": to_iso(fill.source_at),
            "fill_recv_at": to_iso(fill.recv_at),
            "fill_at": to_iso(fill.fill_at or fill.source_at),
            "submit_at": to_iso(fill.submit_at),
            "ack_at": to_iso(fill.ack_at),
            "signal_at": to_iso(fill.signal_at),
            "fee_usd": decimal_text(fill.fee_usd),
            "quote_alignment": alignment,
            "quote_at_fill": _jsonable(own),
            "fill_mid": decimal_text(fill_mid),
            "seen_executable": decimal_text(seen_executable),
            "seen_quote_id": fill.seen_quote_id or (own or {}).get("quote_id"),
            "fill_vs_mid_bps": decimal_text(fill_vs_mid),
            "fill_vs_seen_executable_bps": decimal_text(fill_vs_seen),
            "hedge_at_fill": hedge,
            "horizons": [],
            "stall": self.stall_snapshot(
                fill.recv_at.timestamp() - 1.0,
                fill.recv_at.timestamp() + max(self.delays) + 0.5,
            ),
        }

    def best_hedge(self, fill: Fill, *, at: datetime | None = None) -> dict[str, Any] | None:
        candidates = []
        for venue in BOOK_VENUES:
            if venue == fill.venue:
                continue
            quote = self._quote_at(venue, fill.symbol, at)
            if quote is None:
                continue
            if fill.side == "buy":
                price = quote.get("executable_sell")
                edge = edge_bps(fill.price, Decimal(str(price))) if price is not None else None
            else:
                price = quote.get("executable_buy")
                edge = edge_bps(Decimal(str(price)), fill.price) if price is not None else None
            if edge is None:
                continue
            candidates.append({
                "venue": venue,
                "price": decimal_text(Decimal(str(price))),
                "edge_bps": decimal_text(edge),
                "age_s": quote.get("age_s"),
                "quote_at": quote.get("recv_at"),
                "quote_kind": quote.get("quote_kind"),
            })
        if not candidates:
            return None
        return max(candidates, key=lambda row: Decimal(row["edge_bps"]))

    def capture_horizon(self, chain: dict[str, Any], fill: Fill, delay: float) -> None:
        points = []
        for venue in (fill.venue,) + tuple(v for v in BOOK_VENUES if v != fill.venue):
            quote = self.hub.latest(venue, fill.symbol, max_age=10.0)
            if quote is None or quote.get("mid") is None:
                continue
            mid = Decimal(str(quote["mid"]))
            adverse = adverse_bps(fill.side, fill.price, mid)
            hedge_price = quote.get("executable_sell" if fill.side == "buy" else "executable_buy")
            hedge_edge = None
            if hedge_price is not None:
                if fill.side == "buy":
                    hedge_edge = edge_bps(fill.price, Decimal(str(hedge_price)))
                else:
                    hedge_edge = edge_bps(Decimal(str(hedge_price)), fill.price)
            point = {
                "delay_s": delay,
                "venue": venue,
                "mid": decimal_text(mid),
                "adverse_bps": decimal_text(adverse),
                "hedge_edge_bps": decimal_text(hedge_edge),
                "age_s": quote.get("age_s"),
                "quote_at": quote.get("recv_at"),
                "quote_kind": quote.get("quote_kind"),
            }
            points.append(point)
            self.writer.append(
                "adverse", venue, fill.symbol,
                {
                    "schema": 1, "event": "fill_adverse", "chain_id": fill.event_id,
                    "pair_id": fill.pair_id, "fill_venue": fill.venue, "fill_side": fill.side,
                    "fill_price": decimal_text(fill.price), "delay_s": delay,
                    **point,
                },
            )
        chain["horizons"].append({"delay_s": delay, "points": points})

    @staticmethod
    def summarize(chain: dict[str, Any]) -> dict[str, Any]:
        worst: dict[str, Any] | None = None
        last: dict[str, Any] | None = None
        for horizon in chain.get("horizons", []):
            for point in horizon.get("points", []):
                if point["venue"] != chain["venue"]:
                    continue
                value = Decimal(str(point["adverse_bps"]))
                if worst is None or value > Decimal(str(worst["adverse_bps"])):
                    worst = {"delay_s": point["delay_s"], "adverse_bps": point["adverse_bps"]}
                last = {"delay_s": point["delay_s"], "adverse_bps": point["adverse_bps"]}
        hedge = chain.get("hedge_at_fill") or {}
        return {
            "quote_alignment": chain.get("quote_alignment"),
            "fill_vs_mid_bps": chain.get("fill_vs_mid_bps"),
            "fill_vs_seen_executable_bps": chain.get("fill_vs_seen_executable_bps"),
            "worst_adverse_bps": worst["adverse_bps"] if worst else None,
            "worst_adverse_delay_s": worst["delay_s"] if worst else None,
            "last_adverse_bps": last["adverse_bps"] if last else None,
            "hedge_venue": hedge.get("venue"),
            "hedge_edge_bps": hedge.get("edge_bps"),
        }

    async def close(self) -> None:
        for task in list(self.tasks):
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value
