"""Allocation of the single profit-exit RFQ slot across open lots.

The Variational order budget allows roughly one quote request per minute while a
gradient position can hold a dozen lots that all want to exit at the same time.
Re-asking for the lot that was quoted a few seconds ago burns that slot and
leaves the other lots waiting, so the slot is handed out in priority order:

1. forced exits (account risk, stop) always win;
2. lots that have not been quoted inside the cooldown go before the rest;
3. a lot that waited longer than the starvation window is promoted anyway;
4. then the highest executable PnL, then the longest wait.

The module is pure arithmetic so the live loop and the audits share one rule.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

DEFAULT_ALLOCATION_COOLDOWN_SECONDS = 45.0
DEFAULT_STARVATION_SECONDS = 120.0


@dataclass(frozen=True)
class ExitCandidate:
    lot_id: str
    pnl_bps: Decimal | None = None
    waited_seconds: float = 0.0
    seconds_since_refresh: float | None = None
    forced: bool = False


def recently_refreshed(
    seconds_since_refresh: float | None,
    *,
    cooldown_seconds: float = DEFAULT_ALLOCATION_COOLDOWN_SECONDS,
) -> bool:
    if seconds_since_refresh is None:
        return False
    return float(seconds_since_refresh) < float(cooldown_seconds)


def exit_candidate_priority(
    *,
    pnl_bps: Any,
    waited_seconds: Any,
    recently_refreshed: bool,
    forced: bool = False,
    starvation_seconds: float = DEFAULT_STARVATION_SECONDS,
) -> tuple[int, int, float, float]:
    """Higher tuples win. ``main.py`` compares this instead of raw PnL."""

    waited = float(waited_seconds or 0.0)
    try:
        edge = float(pnl_bps) if pnl_bps is not None else float("-inf")
    except (TypeError, ValueError):
        edge = float("-inf")
    starved = waited >= float(starvation_seconds)
    return (
        1 if forced else 0,
        1 if (not recently_refreshed or starved) else 0,
        edge,
        waited,
    )


def rank_candidates(
    candidates: Iterable[ExitCandidate | dict[str, Any]],
    *,
    cooldown_seconds: float = DEFAULT_ALLOCATION_COOLDOWN_SECONDS,
    starvation_seconds: float = DEFAULT_STARVATION_SECONDS,
) -> list[ExitCandidate]:
    rows = [_coerce(item) for item in candidates]
    return sorted(
        rows,
        key=lambda item: exit_candidate_priority(
            pnl_bps=item.pnl_bps,
            waited_seconds=item.waited_seconds,
            recently_refreshed=recently_refreshed(
                item.seconds_since_refresh, cooldown_seconds=cooldown_seconds
            ),
            forced=item.forced,
            starvation_seconds=starvation_seconds,
        ),
        reverse=True,
    )


def allocated_lots(
    candidates: Iterable[ExitCandidate | dict[str, Any]],
    *,
    slots: int = 1,
    cooldown_seconds: float = DEFAULT_ALLOCATION_COOLDOWN_SECONDS,
    starvation_seconds: float = DEFAULT_STARVATION_SECONDS,
) -> list[str]:
    if slots < 0:
        return []
    ordered = rank_candidates(
        candidates,
        cooldown_seconds=cooldown_seconds,
        starvation_seconds=starvation_seconds,
    )
    return [item.lot_id for item in ordered[:slots]]


def _coerce(item: ExitCandidate | dict[str, Any]) -> ExitCandidate:
    if isinstance(item, ExitCandidate):
        return item
    return ExitCandidate(
        lot_id=str(item.get("lot_id") or ""),
        pnl_bps=item.get("pnl_bps"),
        waited_seconds=float(item.get("waited_seconds") or 0.0),
        seconds_since_refresh=item.get("seconds_since_refresh"),
        forced=bool(item.get("forced")),
    )