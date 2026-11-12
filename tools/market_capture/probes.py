from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable
import asyncio
import time

import requests

from .storage import TapeWriter


class LatencyProbe:
    def __init__(
        self,
        *,
        venue: str,
        target: str,
        request: Callable[[], Any],
        writer: TapeWriter,
        interval: float = 5.0,
    ) -> None:
        self.venue = venue
        self.target = target
        self.request = request
        self.writer = writer
        self.interval = max(1.0, interval)
        self.samples = 0
        self.last_ms: float | None = None
        self.last_error: str | None = None

    async def run(self) -> None:
        session = requests.Session()
        while True:
            started = time.perf_counter()
            status = None
            error = None
            try:
                response = await asyncio.to_thread(self.request, session)
                status = getattr(response, "status_code", 200)
                if status >= 400:
                    error = f"http_{status}"
            except Exception as exc:
                error = f"{type(exc).__name__}:{exc}"[:200]
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self.samples += 1
            self.last_ms = elapsed_ms
            if error:
                self.last_error = error
            row = {
                "schema": 1, "event": "latency_probe", "venue": self.venue,
                "target": self.target, "http_status": status,
                "rtt_ms": round(elapsed_ms, 3), "ok": error is None,
                "error": error, "at": datetime.now(timezone.utc).isoformat(),
            }
            self.writer.append("latency", self.venue, "probe", row)
            await asyncio.sleep(self.interval)


def lighter_probe(rest_url: str, market_id: int) -> Callable[[requests.Session], Any]:
    def call(session: requests.Session) -> Any:
        return session.get(
            f"{rest_url.rstrip('/')}/api/v1/recentTrades",
            params={"market_id": market_id, "limit": 1},
            timeout=6,
        )
    return call


def entropy_probe(info_url: str, dex: str) -> Callable[[requests.Session], Any]:
    def call(session: requests.Session) -> Any:
        return session.post(info_url, json={"type": "meta", "dex": dex}, timeout=6)
    return call


def variational_probe(asset: str = "NVDA") -> Callable[[requests.Session], Any]:
    def call(session: requests.Session) -> Any:
        return session.get(
            f"https://omni.variational.io/perpetual/{asset}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=6,
        )
    return call
