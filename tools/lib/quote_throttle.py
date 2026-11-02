from __future__ import annotations

import time
from collections import deque
from typing import Any, Callable, Sequence

DEFAULT_COOLDOWN_SECONDS = 60.0
DEFAULT_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 10.0, 20.0, 60.0)


class ExitQuoteThrottle:
    """Per-key cooldown for profit-exit RFQ requests.

    A successful quote blocks further requests for ``cooldown_seconds``; a failed
    one blocks for the next backoff step, so a tight server-side quota is not
    hammered by the exit loop. Only request pacing changes here - no exit
    decision is altered.
    """

    def __init__(
        self,
        *,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        backoff_seconds: Sequence[float] = DEFAULT_BACKOFF_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cooldown_seconds = float(cooldown_seconds)
        self.backoff_seconds = tuple(float(item) for item in backoff_seconds) or (0.0,)
        self._clock = clock
        self._next_allowed: dict[str, float] = {}
        self._failures: dict[str, int] = {}

    def _key(self, key: str) -> str:
        return str(key).upper()

    def allow(self, key: str) -> bool:
        name = self._key(key)
        return self._clock() >= self._next_allowed.get(name, 0.0)

    def remaining(self, key: str) -> float:
        name = self._key(key)
        return max(0.0, self._next_allowed.get(name, 0.0) - self._clock())

    def failures(self, key: str) -> int:
        return self._failures.get(self._key(key), 0)

    def record_success(self, key: str) -> float:
        name = self._key(key)
        self._failures[name] = 0
        wait = self.cooldown_seconds
        self._next_allowed[name] = self._clock() + wait
        return wait

    def record_failure(self, key: str) -> float:
        name = self._key(key)
        count = self._failures.get(name, 0) + 1
        self._failures[name] = count
        wait = self.backoff_seconds[min(count - 1, len(self.backoff_seconds) - 1)]
        self._next_allowed[name] = self._clock() + wait
        return wait

    def reset(self, key: str | None = None) -> None:
        if key is None:
            self._next_allowed.clear()
            self._failures.clear()
            return
        name = self._key(key)
        self._next_allowed.pop(name, None)
        self._failures.pop(name, None)

    def snapshot(self) -> dict[str, Any]:
        return {
            "cooldown_seconds": self.cooldown_seconds,
            "backoff_seconds": list(self.backoff_seconds),
            "keys": {
                name: {
                    "next_allowed_in_seconds": round(
                        max(0.0, until - self._clock()), 3
                    ),
                    "consecutive_failures": self._failures.get(name, 0),
                }
                for name, until in self._next_allowed.items()
            },
        }


DEFAULT_PASSIVE_MARGIN_BPS = 1.5
DEFAULT_PROBE_INTERVAL_SECONDS = 180.0


def should_probe_exit_rfq(
    *,
    passive_pnl_bps: float | None,
    threshold_bps: float | None,
    margin_bps: float = DEFAULT_PASSIVE_MARGIN_BPS,
    seconds_since_last_probe: float | None = None,
    probe_interval_seconds: float = DEFAULT_PROBE_INTERVAL_SECONDS,
) -> dict[str, Any]:
    """Decide whether spending an RFQ on the exit check is worth it."""

    if threshold_bps is None:
        return {"probe": True, "reason": "threshold_unknown"}
    if passive_pnl_bps is None:
        return {"probe": True, "reason": "passive_pnl_unavailable"}
    if float(passive_pnl_bps) >= float(threshold_bps) - float(margin_bps):
        return {"probe": True, "reason": "passive_near_threshold"}
    if (
        seconds_since_last_probe is None
        or float(seconds_since_last_probe) >= float(probe_interval_seconds)
    ):
        return {"probe": True, "reason": "heartbeat_probe"}
    return {"probe": False, "reason": "passive_pnl_far_below_threshold"}


class RfqAttemptLog:
    """Rolling record of profit-exit RFQ attempts, used as quota evidence."""

    def __init__(
        self,
        *,
        window_seconds: float = 600.0,
        maxlen: int = 400,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.window_seconds = float(window_seconds)
        self._clock = clock
        self._rows: deque[dict[str, Any]] = deque(maxlen=max(1, int(maxlen)))

    def record(self, *, outcome: str, waited_ms: float | None = None) -> None:
        self._rows.append(
            {
                "at": self._clock(),
                "outcome": str(outcome),
                "waited_ms": None if waited_ms is None else float(waited_ms),
            }
        )

    def _window_rows(self) -> list[dict[str, Any]]:
        cutoff = self._clock() - self.window_seconds
        return [row for row in self._rows if row["at"] >= cutoff]

    def summary(self) -> dict[str, Any]:
        rows = self._window_rows()
        counts: dict[str, int] = {}
        for row in rows:
            counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1
        successes = [row["at"] for row in rows if row["outcome"] == "success"]
        gaps = [b - a for a, b in zip(successes, successes[1:])]
        window_minutes = self.window_seconds / 60.0
        return {
            "window_seconds": self.window_seconds,
            "attempts": len(rows),
            "outcomes": counts,
            "successes_per_minute": (
                round(len(successes) / window_minutes, 3) if window_minutes else None
            ),
            "min_success_gap_seconds": round(min(gaps), 1) if gaps else None,
            "avg_success_gap_seconds": (
                round(sum(gaps) / len(gaps), 1) if gaps else None
            ),
        }
