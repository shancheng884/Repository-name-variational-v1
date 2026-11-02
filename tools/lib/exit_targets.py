"""Exit-target arithmetic shared by the live runtime and its tests."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

DEFAULT_PORTFOLIO_EXIT_TARGET_BPS = Decimal("3.00")


def portfolio_exit_target_bps(configured: object | None) -> Decimal:
    """Aggregate executable PnL required before every open lot closes together.

    The aggregate already uses the depth-priced Lighter fill, so the target is
    net of slippage on that leg and does not need the per-lot reserve added on
    top. A missing or unreadable value falls back to the reviewed default.
    """

    if configured is None:
        return DEFAULT_PORTFOLIO_EXIT_TARGET_BPS
    try:
        value = Decimal(str(configured))
    except (InvalidOperation, TypeError, ValueError):
        return DEFAULT_PORTFOLIO_EXIT_TARGET_BPS
    if not value.is_finite():
        return DEFAULT_PORTFOLIO_EXIT_TARGET_BPS
    return value if value > 0 else DEFAULT_PORTFOLIO_EXIT_TARGET_BPS