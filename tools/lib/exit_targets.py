"""Exit-target arithmetic shared by the live runtime and its tests."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

DEFAULT_PORTFOLIO_EXIT_TARGET_BPS = Decimal("3.00")
DEFAULT_LOT_EXIT_TARGET_BPS = Decimal("3.00")


def _bounded_target(configured: object | None, default: Decimal) -> Decimal:
    if configured is None:
        return default
    try:
        value = Decimal(str(configured))
    except (InvalidOperation, TypeError, ValueError):
        return default
    if not value.is_finite():
        return default
    return value if value > 0 else default


def lot_exit_target_bps(configured: object | None = None) -> Decimal:
    """Effective per-lot exit gate, aligned with the batch target."""

    return _bounded_target(configured, DEFAULT_LOT_EXIT_TARGET_BPS)


def cap_exit_threshold(raw: Decimal, target: Decimal | None) -> Decimal:
    """The adaptive per-lot gate may tighten, but never exceed the target.

    The formula adds a dynamic execution reserve on top of the profit target,
    which pushed the real gate to 4.5 bps while the batch exit aimed at 3.0.
    Capping keeps the time-decayed path (which can go lower) intact.
    """

    if target is None or target <= 0:
        return raw
    return min(raw, target)


def portfolio_exit_target_bps(configured: object | None) -> Decimal:
    """Aggregate executable PnL required before every open lot closes together.

    The aggregate already uses the depth-priced Lighter fill, so the target is
    net of slippage on that leg and does not need the per-lot reserve added on
    top. A missing or unreadable value falls back to the reviewed default.
    """

    return _bounded_target(configured, DEFAULT_PORTFOLIO_EXIT_TARGET_BPS)