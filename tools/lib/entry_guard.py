"""Entry guards that do not touch order mechanics.

Two independent guards decide whether the strategy may *open* a new lot:

* :class:`EventBlackout` - a static calendar of macro events (FOMC, CPI, NFP...)
  with a symmetric window around each event.  Inside the window the tape is
  driven by news flow, the Variational reference quote lags reality, and the
  measured basis is meaningless for a few minutes.  Renting those minutes is
  cheaper than carrying a fat permanent reserve.

* :class:`LatencyGuard` - a rolling average of recent submit round-trips.  When
  the current round-trip is more than ``threshold_multiplier`` times the recent
  average, a venue is probably congested (this is what a stalled exchange looks
  like before the API starts erroring).  Stop opening new lots until it heals.

Both guards only ever *block* entries.  Exits stay available at all times, so a
position can always be unwound while a guard is active.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence


def _parse_ts(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class EventWindow:
    start: datetime
    end: datetime
    label: str

    def contains(self, moment: datetime) -> bool:
        return self.start <= moment <= self.end


class EventBlackout:
    """Static event calendar with a symmetric pre/post window."""

    def __init__(self, windows: Iterable[EventWindow]):
        self.windows = sorted(windows, key=lambda window: window.start)

    @classmethod
    def from_payload(
        cls,
        payload: dict,
        *,
        default_window_minutes: int = 30,
    ) -> "EventBlackout":
        windows: list[EventWindow] = []
        for item in payload.get("events") or []:
            if not isinstance(item, dict):
                continue
            start = _parse_ts(item.get("at_utc") or "")
            if start is None:
                continue
            before = int(item.get("before_minutes", default_window_minutes))
            after = int(item.get("after_minutes", default_window_minutes))
            windows.append(
                EventWindow(
                    start=start - timedelta(minutes=max(0, before)),
                    end=start + timedelta(minutes=max(0, after)),
                    label=str(item.get("label") or "event"),
                )
            )
        return cls(windows)

    @classmethod
    def load(cls, path: str | Path, *, default_window_minutes: int = 30) -> "EventBlackout":
        candidate = Path(path)
        if not candidate.exists():
            return cls([])
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            return cls([])
        if not isinstance(payload, dict):
            return cls([])
        return cls.from_payload(payload, default_window_minutes=default_window_minutes)

    def active_window(self, moment: datetime | None = None) -> EventWindow | None:
        now = moment or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        for window in self.windows:
            if window.contains(now):
                return window
        return None

    def blocked(self, moment: datetime | None = None) -> bool:
        return self.active_window(moment) is not None


class LatencyGuard:
    """Rolling submit round-trip average with a degradation ratio."""

    def __init__(
        self,
        *,
        window_seconds: float = 24 * 3600,
        threshold_multiplier: float = 1.5,
        min_samples: int = 20,
    ):
        self.window_seconds = float(window_seconds)
        self.threshold_multiplier = float(threshold_multiplier)
        self.min_samples = int(min_samples)
        self._samples: deque[tuple[float, float]] = deque(maxlen=2000)

    def add(self, monotonic_ts: float, roundtrip_ms: float) -> None:
        if roundtrip_ms is None or roundtrip_ms <= 0:
            return
        self._samples.append((float(monotonic_ts), float(roundtrip_ms)))
        self._trim(float(monotonic_ts))

    def _trim(self, now_monotonic: float) -> None:
        horizon = now_monotonic - self.window_seconds
        while self._samples and self._samples[0][0] < horizon:
            self._samples.popleft()

    def average_ms(self, now_monotonic: float) -> float | None:
        self._trim(now_monotonic)
        if len(self._samples) < self.min_samples:
            return None
        total = sum(value for _, value in self._samples)
        return total / len(self._samples)

    def degraded(
        self,
        now_monotonic: float,
        current_ms: float,
    ) -> tuple[bool, float | None, float | None]:
        """Return ``(degraded, current, average)``."""
        self.add(now_monotonic, current_ms)
        average = self.average_ms(now_monotonic)
        if average is None or average <= 0:
            return False, current_ms, average
        return (
            current_ms > average * self.threshold_multiplier,
            current_ms,
            average,
        )
