"""NVDA five-tier basis engine with net-position reconciliation.

Deliberately standalone: it does not import or modify main.py. The ETH
strategy keeps running untouched; this module is the NVDA-shaped equivalent of
its gradient, plus the reconciliation the shared entropy account needs.

Sizing follows the ETH / CL ladder verbatim: a fixed child order, cumulative
capacity of tier k equal to k x the smaller verified venue equity, hard capped
at max_equity_multiple x equity. Tier 1 opens 1x equity and tier 5 tops the
position out at 5x -- the same numbers main.py produces through
``v4_real_gradient_slot_caps``.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class TierSpec:
    index: int
    threshold_bps: float


@dataclass(frozen=True)
class TierConfig:
    asset: str
    buy_venue: str
    sell_venue: str
    tiers: tuple[TierSpec, ...]
    child_notional_usd: float = 20.0
    max_equity_multiple: float = 5.0
    equity_usd: dict[str, float | None] | None = None
    # Fallback size per tier when venue equity is not supplied (paper runs).
    notional_usd: float = 20.0
    timeout_seconds: float | None = None
    confirm_window: int = 3
    confirm_min: int = 2
    slippage_bps: float = 1.3

    def slot_caps(self) -> tuple[int, ...] | None:
        """Cumulative child slots per tier, mirroring main.py's 5x hard cap."""
        if not self.equity_usd:
            return None
        values = [
            self.equity_usd.get(self.buy_venue),
            self.equity_usd.get(self.sell_venue),
        ]
        if any(value is None or not (value > 0) for value in values):
            return None
        smaller = min(values)
        hard_slots = int(
            smaller * self.max_equity_multiple / self.child_notional_usd
        )
        return tuple(
            min(hard_slots, int(smaller * index / self.child_notional_usd))
            for index in range(1, len(self.tiers) + 1)
        )

    def tier_target_notional_usd(self, tier_index: int) -> float | None:
        """Cumulative notional the ladder should hold once this tier is open."""
        caps = self.slot_caps()
        if caps is None or tier_index <= 0:
            return None
        return caps[min(tier_index, len(caps)) - 1] * self.child_notional_usd


class PositionLedger:
    """Real position per (asset, venue), whatever strategy opened it."""

    def __init__(self) -> None:
        self._by_strategy: dict[tuple[str, str, str], Decimal] = defaultdict(lambda: Decimal("0"))

    def set(self, strategy: str, asset: str, venue: str, qty: Decimal | float) -> None:
        self._by_strategy[(strategy, asset, venue)] = Decimal(str(qty))

    def add(self, strategy: str, asset: str, venue: str, qty: Decimal | float) -> None:
        key = (strategy, asset, venue)
        self._by_strategy[key] = self._by_strategy.get(key, Decimal("0")) + Decimal(str(qty))

    def net(self, asset: str, venue: str) -> Decimal:
        total = Decimal("0")
        for (strat, sym, ven), qty in self._by_strategy.items():
            if sym == asset and ven == venue:
                total += qty
        return total

    def by_strategy(self, asset: str) -> dict[str, dict[str, Decimal]]:
        out: dict[str, dict[str, Decimal]] = defaultdict(dict)
        for (strat, sym, ven), qty in self._by_strategy.items():
            if sym == asset:
                out[strat][ven] = qty
        return out

    def reconcile(self, asset: str, venues: tuple[str, ...], exchange: dict[str, Decimal | float | None],
                  tolerance: Decimal = Decimal("0.0000001")) -> dict[str, Any]:
        diffs = {}
        ok = True
        for venue in venues:
            actual = exchange.get(venue)
            if actual is None:
                ok = False
                diffs[venue] = {"expected": str(self.net(asset, venue)), "actual": None, "error": "unreadable"}
                continue
            expected = self.net(asset, venue)
            actual_d = Decimal(str(actual))
            delta = actual_d - expected
            diffs[venue] = {"expected": str(expected), "actual": str(actual_d), "delta": str(delta)}
            if abs(delta) > tolerance:
                ok = False
        return {"ok": ok, "diffs": diffs}


@dataclass
class OpenTier:
    index: int
    entry_edge: float
    entry_dev: float
    frozen_baseline: float
    opened_at: float
    qty: float
    notional_usd: float = 0.0
    # A close is only booked once the venue confirms both legs; until then the
    # tier stays open here so a failed leg is retried instead of forgotten.
    closing: bool = False


@dataclass
class TierEngine:
    config: TierConfig
    ledger: PositionLedger
    strategy_id: str = "nvda-tier"
    open_tiers: dict[int, OpenTier] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)

    def _confirmed(self, window: list[float], threshold: float) -> bool:
        return sum(1 for value in window if value >= threshold) >= self.config.confirm_min

    def _open_notional(self) -> float:
        return sum(position.notional_usd for position in self.open_tiers.values())

    def _entry_notional(self, tier_index: int) -> float:
        """Increment needed to reach the cumulative tier capacity."""
        target = self.config.tier_target_notional_usd(tier_index)
        if target is None:
            return self.config.notional_usd
        return target - self._open_notional()

    def on_sample(self, *, stamp: float, edge: float, baseline: float, price_hint: float,
                  window: list[float], exchange: dict[str, Decimal | float | None],
                  allow_entry: bool = True) -> list[dict[str, Any]]:
        """Decide closes (always) and entries (only while this direction owns the book).

        The ETH gradient runs bidirectionally but locks the active direction
        until the book is fully flat, so the caller passes allow_entry=False
        when the opposite direction still holds a tier.
        """
        actions: list[dict[str, Any]] = []
        dev = edge - baseline

        for index in sorted(self.open_tiers):
            pos = self.open_tiers[index]
            if pos.closing:
                # Already waiting on a venue fill; do not queue a second close.
                continue
            reverted = edge <= pos.frozen_baseline
            # timeout_seconds=None keeps the tier open until it reverts.
            expired = (self.config.timeout_seconds is not None
                       and stamp - pos.opened_at >= self.config.timeout_seconds)
            if not (reverted or expired):
                continue
            net_bps = (pos.entry_edge - edge) - self.config.slippage_bps
            pos.closing = True
            actions.append({"action": "close", "tier": index,
                            "reason": "reverted" if reverted else "timeout",
                            "net_bps": net_bps, "qty": pos.qty,
                            "notional_usd": pos.notional_usd,
                            "entry_edge": pos.entry_edge, "exit_edge": edge,
                            "hold_s": stamp - pos.opened_at})

        # One reconciliation per decision step: the account is checked before the
        # batch of orders goes out, not after every child order we just queued.
        # The snapshot must be taken after our own closes, otherwise the books we
        # just updated look like a mismatch to a stale venue read.
        if callable(exchange):
            exchange = exchange()
        check = self.ledger.reconcile(self.config.asset,
                                      (self.config.buy_venue, self.config.sell_venue), exchange)
        if not check["ok"]:
            eligible = [t for t in self.config.tiers
                        if t.index not in self.open_tiers and self._confirmed(window, t.threshold_bps)]
            if eligible:
                actions.append({"action": "blocked", "tier": eligible[0].index,
                                "reason": "position_mismatch", "diffs": check["diffs"]})
            return actions

        if not allow_entry:
            return actions

        for tier in self.config.tiers:
            if tier.index in self.open_tiers:
                continue
            if not self._confirmed(window, tier.threshold_bps):
                continue
            increment = self._entry_notional(tier.index)
            if increment <= 0:
                # Cumulative capacity already reached; nothing left to add.
                continue
            qty = increment / price_hint if price_hint > 0 else 0.0
            if qty <= 0:
                continue
            self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, +qty)
            self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, -qty)
            self.open_tiers[tier.index] = OpenTier(index=tier.index, entry_edge=edge, entry_dev=dev,
                                                   frozen_baseline=baseline, opened_at=stamp, qty=qty,
                                                   notional_usd=increment)
            actions.append({"action": "open", "tier": tier.index, "dev_bps": dev, "qty": qty,
                            "notional_usd": increment})
        return actions

    def confirm_close(self, action: dict[str, Any]) -> dict[str, Any] | None:
        """Book a close only after the venue confirmed both legs filled."""
        index = int(action.get("tier"))
        pos = self.open_tiers.pop(index, None)
        if pos is None:
            return None
        trade = {"tier": index, "net_bps": float(action.get("net_bps") or 0.0),
                 "hold_s": float(action.get("hold_s") or 0.0),
                 "reason": action.get("reason") or "reverted",
                 "entry_dev": pos.entry_dev, "notional_usd": pos.notional_usd}
        self.trades.append(trade)
        self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, -pos.qty)
        self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, +pos.qty)
        return trade

    def cancel_close(self, index: int) -> None:
        """Release the close lock so the next sample can try again."""
        pos = self.open_tiers.get(int(index))
        if pos is not None:
            pos.closing = False

    def drop_tier(self, index: int) -> None:
        """Undo a tier whose paired order was never sent.

        The engine books a tier before the runner places it; when the order is
        refused (margin, leverage, a failed leg) both the tier and its ledger
        legs have to disappear or every later reconciliation sees a phantom
        position.
        """
        pos = self.open_tiers.pop(index, None)
        if pos is None:
            return
        self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, -pos.qty)
        self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, +pos.qty)

    def summary(self) -> list[dict[str, Any]]:
        by_tier: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for trade in self.trades:
            by_tier[trade["tier"]].append(trade)
        rows = []
        for index in sorted(by_tier):
            items = by_tier[index]
            nets = [i["net_bps"] for i in items]
            rows.append({
                "tier": index, "fills": len(items),
                "win_rate": sum(1 for n in nets if n > 0) / len(nets),
                "median_net_bps": sorted(nets)[len(nets) // 2],
                "total_usd": sum(i["net_bps"] / 10000.0 * i.get("notional_usd", 0.0) for i in items),
                "median_hold_s": sorted(i["hold_s"] for i in items)[len(items) // 2],
                "timeout_share": sum(1 for i in items if i["reason"] == "timeout") / len(items),
            })
        return rows