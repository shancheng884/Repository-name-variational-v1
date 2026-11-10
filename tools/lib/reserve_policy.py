"""Dynamic execution-reserve policy for the Var/Lighter basis strategy.

The legacy code used fixed reserves (1.5 bps entry prior, 3.5 bps exit prior).
Measured slippage on this venue pair is far smaller (p90 ~0.35 bps), so those
numbers starved the strategy of entries in calm tape while still being too thin
when the tape gets violent.

This module derives both reserves from the current market noise (MAD of the
basis signal) with hard floors and caps: a quiet tape gets a thin reserve, a
violent tape automatically gets a thick one.  A short slippage-feedback hook
thickens the reserve again if recent fills keep coming in worse than expected.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Iterable, Sequence

# Entry leg: generous enough for queue position + one tick of adverse drift.
ENTRY_MAD_MULTIPLIER = Decimal("1.2")
ENTRY_FLOOR_BPS = Decimal("1.0")
ENTRY_CAP_BPS = Decimal("4.0")

# Exit leg moves less size and is unwound against a resting book.
EXIT_MAD_MULTIPLIER = Decimal("0.4")
EXIT_FLOOR_BPS = Decimal("0.4")
EXIT_CAP_BPS = Decimal("2.0")

# Slippage feedback: N consecutive fills worse than the trigger thicken reserves.
SLIPPAGE_FEEDBACK_TRIGGER_BPS = Decimal("3.0")
SLIPPAGE_FEEDBACK_WINDOW = 3
SLIPPAGE_FEEDBACK_MULTIPLIER = Decimal("1.5")

# Never allow a dynamic reserve to exceed this, even with feedback engaged.
MAX_RESERVE_BPS = Decimal("8.0")


def _to_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    if parsed.is_nan():
        return None
    return parsed


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    if value < low:
        return low
    if value > high:
        return high
    return value


def slippage_feedback_active(
    samples: Iterable[object],
    *,
    trigger_bps: Decimal = SLIPPAGE_FEEDBACK_TRIGGER_BPS,
    window: int = SLIPPAGE_FEEDBACK_WINDOW,
) -> bool:
    """True when the last ``window`` fills were all worse than ``trigger_bps``."""
    recent = []
    for sample in samples:
        parsed = _to_decimal(sample)
        if parsed is None:
            continue
        recent.append(abs(parsed))
    if len(recent) < window:
        return False
    tail = recent[-window:]
    return all(value >= trigger_bps for value in tail)


def dynamic_entry_reserve_bps(
    mad_bps: object,
    *,
    feedback_active: bool = False,
) -> Decimal:
    """Reserve applied to the entry threshold for the current market noise."""
    mad = _to_decimal(mad_bps)
    if mad is None or mad <= 0:
        base = ENTRY_FLOOR_BPS
    else:
        base = _clamp(mad * ENTRY_MAD_MULTIPLIER, ENTRY_FLOOR_BPS, ENTRY_CAP_BPS)
    if feedback_active:
        base = min(base * SLIPPAGE_FEEDBACK_MULTIPLIER, MAX_RESERVE_BPS)
    return base


def dynamic_exit_reserve_bps(
    mad_bps: object,
    *,
    feedback_active: bool = False,
) -> Decimal:
    """Reserve applied when deciding to unwind a lot."""
    mad = _to_decimal(mad_bps)
    if mad is None or mad <= 0:
        base = EXIT_FLOOR_BPS
    else:
        base = _clamp(mad * EXIT_MAD_MULTIPLIER, EXIT_FLOOR_BPS, EXIT_CAP_BPS)
    if feedback_active:
        base = min(base * SLIPPAGE_FEEDBACK_MULTIPLIER, MAX_RESERVE_BPS)
    return base


def combined_entry_cost_bps(
    mad_bps: object,
    *,
    exit_prior_bps: object,
    feedback_active: bool = False,
) -> tuple[Decimal, Decimal]:
    """Return ``(entry_reserve, exit_increment)`` for threshold construction.

    ``exit_increment`` is the part of the exit reserve that has to be charged on
    top of the legacy prior, matching how the strategy used to add
    ``exit_reserve - prior`` to the entry threshold.
    """
    entry = dynamic_entry_reserve_bps(mad_bps, feedback_active=feedback_active)
    exit_reserve = dynamic_exit_reserve_bps(mad_bps, feedback_active=feedback_active)
    prior = _to_decimal(exit_prior_bps) or Decimal("0")
    increment = exit_reserve - prior
    if increment < Decimal("0"):
        increment = Decimal("0")
    return entry, increment
