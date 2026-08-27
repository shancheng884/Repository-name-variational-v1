from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable


class RollingWindowRateLimiter:
    """Rolling-window limiter with capacity reserved for urgent risk exits."""

    def __init__(
        self,
        *,
        normal_limit: int,
        hard_limit: int,
        window_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if normal_limit <= 0 or hard_limit < normal_limit:
            raise ValueError("invalid rolling-window limits")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.normal_limit = normal_limit
        self.hard_limit = hard_limit
        self.window_seconds = float(window_seconds)
        self._clock = clock
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()
        self._blocked_until = 0.0

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()

    async def acquire(self, *, urgent: bool = False) -> float:
        """Wait for one token and return the time spent waiting in seconds."""
        started = self._clock()
        while True:
            async with self._lock:
                now = self._clock()
                self._prune(now)
                limit = self.hard_limit if urgent else self.normal_limit
                if now >= self._blocked_until and len(self._timestamps) < limit:
                    self._timestamps.append(now)
                    return max(0.0, now - started)
                waits = [max(0.0, self._blocked_until - now)]
                if len(self._timestamps) >= limit:
                    waits.append(
                        max(
                            0.001,
                            self._timestamps[len(self._timestamps) - limit]
                            + self.window_seconds
                            - now,
                        )
                    )
                wait_seconds = max(0.001, max(waits))
            await asyncio.sleep(wait_seconds)

    async def penalize(self, seconds: float) -> None:
        """Apply platform-requested backoff after a strike or rate error."""
        async with self._lock:
            self._blocked_until = max(
                self._blocked_until,
                self._clock() + max(0.0, float(seconds)),
            )

    def snapshot(self) -> dict[str, float | int]:
        now = self._clock()
        self._prune(now)
        return {
            "normal_limit": self.normal_limit,
            "hard_limit": self.hard_limit,
            "window_seconds": self.window_seconds,
            "used": len(self._timestamps),
            "reserved_remaining": max(
                0, self.hard_limit - max(self.normal_limit, len(self._timestamps))
            ),
            "backoff_seconds": max(0.0, self._blocked_until - now),
        }
