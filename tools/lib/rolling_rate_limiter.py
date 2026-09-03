from __future__ import annotations

import asyncio
import json
import math
import os
import time
from collections import deque
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows development hosts
    fcntl = None


class RollingWindowRateLimiter:
    """Rolling-window limiter with capacity reserved for urgent risk exits."""

    def __init__(
        self,
        *,
        normal_limit: int,
        hard_limit: int,
        window_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        state_path: str | os.PathLike[str] | None = None,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if normal_limit <= 0 or hard_limit < normal_limit:
            raise ValueError("invalid rolling-window limits")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.normal_limit = normal_limit
        self.hard_limit = hard_limit
        self.window_seconds = float(window_seconds)
        self._clock = clock
        self._wall_clock = wall_clock
        self._state_path = Path(state_path) if state_path is not None else None
        self._timestamps: deque[float] = deque()
        self._category_timestamps: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()
        self._blocked_until = 0.0
        if self._state_path is not None:
            with self._state_file_lock():
                self._load_state(self._clock())

    @contextmanager
    def _state_file_lock(self):
        """Serialize limiter state across strategy restarts/processes on POSIX."""
        if self._state_path is None:
            yield
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self._state_path.with_name(self._state_path.name + ".lock")
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _load_state(self, now: float) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if not isinstance(payload, dict):
            return
        wall_now = self._wall_clock()

        def restore(value: object) -> float | None:
            try:
                wall_timestamp = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(wall_timestamp):
                return None
            return now - (wall_now - wall_timestamp)

        timestamps_payload = payload.get("timestamps") or []
        if not isinstance(timestamps_payload, list):
            timestamps_payload = []
        timestamps = [
            restored
            for item in timestamps_payload
            if (restored := restore(item)) is not None
        ]
        self._timestamps = deque(timestamps)
        categories: dict[str, deque[float]] = {}
        categories_payload = payload.get("categories") or {}
        if not isinstance(categories_payload, dict):
            categories_payload = {}
        for category, values in categories_payload.items():
            if not isinstance(category, str) or not isinstance(values, list):
                continue
            restored_values = [
                restored
                for item in values
                if (restored := restore(item)) is not None
            ]
            if restored_values:
                categories[category] = deque(restored_values)
        self._category_timestamps = categories
        restored_blocked_until = restore(payload.get("blocked_until"))
        self._blocked_until = restored_blocked_until or 0.0
        self._prune(now)

    def _save_state(self, now: float) -> None:
        if self._state_path is None:
            return
        wall_now = self._wall_clock()

        def to_wall(timestamp: float) -> float:
            return wall_now - (now - timestamp)

        payload = {
            "schema_version": 1,
            "updated_at": wall_now,
            "timestamps": [to_wall(item) for item in self._timestamps],
            "categories": {
                category: [to_wall(item) for item in timestamps]
                for category, timestamps in self._category_timestamps.items()
            },
            "blocked_until": to_wall(self._blocked_until)
            if self._blocked_until > now
            else 0.0,
        }
        temporary = self._state_path.with_name(self._state_path.name + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, self._state_path)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()
        for category, timestamps in list(self._category_timestamps.items()):
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()
            if not timestamps:
                self._category_timestamps.pop(category, None)

    @staticmethod
    def _validate_cost(cost: int) -> int:
        if isinstance(cost, bool) or not isinstance(cost, int) or cost <= 0:
            raise ValueError("cost must be a positive integer")
        return cost

    async def acquire(
        self,
        *,
        urgent: bool = False,
        cost: int = 1,
        category: str | None = None,
        category_limit: int | None = None,
    ) -> float:
        """Wait for ``cost`` tokens and return the time spent waiting."""
        cost = self._validate_cost(cost)
        if category is not None and category_limit is None:
            raise ValueError("category_limit is required when category is set")
        if category_limit is not None and category_limit < cost:
            raise ValueError("category_limit must be at least cost")
        if cost > self.hard_limit:
            raise ValueError("cost must not exceed hard_limit")
        started = self._clock()
        while True:
            async with self._lock:
                with self._state_file_lock():
                    now = self._clock()
                    self._load_state(now)
                    self._prune(now)
                    limit = self.hard_limit if urgent else self.normal_limit
                    category_timestamps = (
                        self._category_timestamps.setdefault(category, deque())
                        if category is not None
                        else None
                    )
                    category_ready = (
                        category_timestamps is None
                        or category_limit is None
                        or len(category_timestamps) + cost <= category_limit
                    )
                    if (
                        now >= self._blocked_until
                        and len(self._timestamps) + cost <= limit
                        and category_ready
                    ):
                        self._timestamps.extend([now] * cost)
                        if category_timestamps is not None:
                            category_timestamps.extend([now] * cost)
                        self._save_state(now)
                        return max(0.0, now - started)
                    waits = [max(0.0, self._blocked_until - now)]
                    required_expirations = len(self._timestamps) + cost - limit
                    if required_expirations > 0:
                        waits.append(
                            max(
                                0.001,
                                self._timestamps[required_expirations - 1]
                                + self.window_seconds
                                - now,
                            )
                        )
                    if (
                        category_timestamps is not None
                        and category_limit is not None
                        and len(category_timestamps) + cost > category_limit
                    ):
                        required_expirations = (
                            len(category_timestamps) + cost - category_limit
                        )
                        waits.append(
                            max(
                                0.001,
                                category_timestamps[required_expirations - 1]
                                + self.window_seconds
                                - now,
                            )
                        )
                    wait_seconds = max(0.001, max(waits))
            await asyncio.sleep(wait_seconds)

    async def try_acquire(
        self,
        *,
        urgent: bool = False,
        limit: int | None = None,
        category: str | None = None,
        cost: int = 1,
    ) -> tuple[bool, float]:
        """Take ``cost`` tokens without waiting and return ``(acquired, wait_seconds)``.

        Background work uses this method so a saturated rolling window cannot
        stall the live decision loop. Without ``category``, ``limit`` applies
        to the shared window for backwards compatibility. With ``category``,
        ``limit`` applies only to that category while all categories still
        share the normal or hard rolling-window limit.
        """
        cost = self._validate_cost(cost)
        if cost > self.hard_limit:
            raise ValueError("cost must not exceed hard_limit")
        started = self._clock()
        async with self._lock:
            with self._state_file_lock():
                now = self._clock()
                self._load_state(now)
                self._prune(now)
                max_limit = self.hard_limit if urgent else self.normal_limit
                category_limit = None
                if limit is not None:
                    if limit <= 0:
                        raise ValueError("limit must be positive")
                    if category is None:
                        max_limit = min(max_limit, int(limit))
                    else:
                        category_limit = int(limit)
                if category_limit is not None and category_limit < cost:
                    raise ValueError("limit must be at least cost")
                category_timestamps = (
                    self._category_timestamps.setdefault(category, deque())
                    if category is not None
                    else None
                )
                if (
                    now < self._blocked_until
                    or len(self._timestamps) + cost > max_limit
                ):
                    return False, max(0.0, now - started)
                if (
                    category_timestamps is not None
                    and category_limit is not None
                    and len(category_timestamps) + cost > category_limit
                ):
                    return False, max(0.0, now - started)
                self._timestamps.extend([now] * cost)
                if category_timestamps is not None:
                    category_timestamps.extend([now] * cost)
                self._save_state(now)
                return True, max(0.0, now - started)

    async def penalize(self, seconds: float) -> None:
        """Apply platform-requested backoff after a strike or rate error."""
        async with self._lock:
            with self._state_file_lock():
                now = self._clock()
                self._load_state(now)
                self._blocked_until = max(
                    self._blocked_until,
                    now + max(0.0, float(seconds)),
                )
                self._save_state(now)

    def snapshot(self) -> dict[str, float | int]:
        with self._state_file_lock():
            now = self._clock()
            self._load_state(now)
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
