from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable


def exchange_epoch_seconds(value: Any) -> float | None:
    """Normalize epoch seconds, milliseconds, microseconds, or ISO timestamps."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return None
            number = parsed.timestamp()
        except (ValueError, TypeError, OverflowError):
            return None
    if not math.isfinite(number) or number <= 0:
        return None
    while number >= 100_000_000_000:
        number /= 1000
    return number


def lighter_book_clock(message: dict[str, Any], *, received_epoch: float) -> dict[str, Any]:
    book = message.get("order_book") or {}
    raw_frame = message.get("timestamp")
    raw_state = book.get("last_updated_at", message.get("last_updated_at"))
    frame = exchange_epoch_seconds(raw_frame)
    state = exchange_epoch_seconds(raw_state)
    error = None
    if frame is None:
        error = "lighter_book_exchange_timestamp_missing_or_invalid"
    elif raw_state is not None and state is None:
        error = "lighter_book_state_timestamp_invalid"
    elif any(value > received_epoch + 0.1 for value in (frame, state) if value is not None):
        error = "lighter_book_exchange_timestamp_in_future"
    return {
        "exchange_frame_epoch": frame,
        "exchange_state_epoch": state,
        "exchange_frame_at": (
            datetime.fromtimestamp(frame, timezone.utc).isoformat() if frame is not None else None
        ),
        "exchange_state_at": (
            datetime.fromtimestamp(state, timezone.utc).isoformat() if state is not None else None
        ),
        "error": error,
    }


def shortfall_tail(values: Iterable[Decimal]) -> tuple[Decimal, Decimal, Decimal]:
    samples = [value for value in values if value.is_finite() and value >= 0]
    if not samples:
        return Decimal("0"), Decimal("0"), Decimal("0")
    ordered = sorted(samples)

    def percentile(p: int) -> Decimal:
        return ordered[min(len(ordered) - 1, (len(ordered) * p + 99) // 100 - 1)]

    # A newly observed tail loss must affect the next decision even when P95
    # excludes the single largest observation in a full rolling window.
    return percentile(80), percentile(95), max(percentile(95), samples[-1])


def advance_shortfall_reserve(
    previous: Decimal, observed_target: Decimal, *, decay_per_fill: Decimal = Decimal("0.25")
) -> Decimal:
    return max(observed_target, previous - decay_per_fill, Decimal("0"))
