from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable
import asyncio

from .hub import MarketHub, BOOK_VENUES
from .model import Fill, adverse_bps, decimal_text, edge_bps, to_iso
from .storage import TapeWriter


class FillAuditor:
    """Build a causal chain around every observed own fill.

    The chain records the quote seen at the fill, the fill price, the hedge
    venue's executable price, and adverse excursion at fixed horizons.
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

    def start_chain(self, fill: Fill) -> dict[str, Any]:
        own = self.hub.latest(fill.venue, fill.symbol, max_age=10.0)
        fill_mid = None
        if own is not None and own.get("mid") is not None:
            fill_mid = Decimal(str(own["mid"]))
        fill_vs_mid = None
        if fill_mid is not None and fill_mid > 0:
            if fill.side == "buy":
                fill_vs_mid = (fill.price - fill_mid) / fill_mid * Decimal("10000")
            else:
                fill_vs_mid = (fill_mid - fill.price) / fill_mid * Decimal("10000")
        hedge = self.best_hedge(fill, at_fill=True)
        return {
            "schema": 1,
            "event": "fill_chain",
            "chain_id": fill.event_id,
            "venue": fill.venue,
            "symbol": fill.symbol,
            "side": fill.side,
            "qty": decimal_text(fill.qty),
            "fill_price": decimal_text(fill.price),
            "fill_source_at": to_iso(fill.source_at),
            "fill_recv_at": to_iso(fill.recv_at),
            "fee_usd": decimal_text(fill.fee_usd),
            "own_quote_at_fill": _jsonable(own),
            "fill_vs_mid_bps": decimal_text(fill_vs_mid),
            "hedge_at_fill": hedge,
            "horizons": [],
            "stall": self.stall_snapshot(fill.recv_at.timestamp() - 1.0, fill.recv_at.timestamp() + max(self.delays) + 0.5),
        }

    def best_hedge(self, fill: Fill, *, at_fill: bool = True) -> dict[str, Any] | None:
        candidates = []
        for venue in BOOK_VENUES:
            if venue == fill.venue:
                continue
            quote = self.hub.latest(venue, fill.symbol, max_age=10.0)
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
            }
            points.append(point)
            self.writer.append(
                "adverse", venue, fill.symbol,
                {
                    "schema": 1, "event": "fill_adverse", "chain_id": fill.event_id,
                    "fill_venue": fill.venue, "fill_side": fill.side,
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
            "fill_vs_mid_bps": chain.get("fill_vs_mid_bps"),
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
