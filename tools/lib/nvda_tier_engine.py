"""NVDA five-tier basis engine with net-position reconciliation.

Deliberately standalone: it does not import or modify main.py. The ETH
strategy keeps running untouched; this module is the NVDA-shaped equivalent of
its gradient, plus the reconciliation the shared entropy account needs.
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
    notional_usd: float = 20.0
    timeout_seconds: float = 900.0
    confirm_window: int = 3
    confirm_min: int = 2
    slippage_bps: float = 1.3


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


@dataclass
class TierEngine:
    config: TierConfig
    ledger: PositionLedger
    strategy_id: str = "nvda-tier"
    open_tiers: dict[int, OpenTier] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)

    def _qty(self, price: float) -> float:
        return self.config.notional_usd / price if price > 0 else 0.0

    def _confirmed(self, window: list[float], threshold: float) -> bool:
        return sum(1 for value in window if value >= threshold) >= self.config.confirm_min

    def on_sample(self, *, stamp: float, edge: float, baseline: float, price_hint: float,
                  window: list[float], exchange: dict[str, Decimal | float | None]) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        dev = edge - baseline

        for index in sorted(self.open_tiers):
            pos = self.open_tiers[index]
            reverted = edge <= pos.frozen_baseline
            expired = stamp - pos.opened_at >= self.config.timeout_seconds
            if not (reverted or expired):
                continue
            net_bps = (pos.entry_edge - edge) - self.config.slippage_bps
            self.trades.append({"tier": index, "net_bps": net_bps, "hold_s": stamp - pos.opened_at,
                                "reason": "reverted" if reverted else "timeout", "entry_dev": pos.entry_dev})
            self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, -pos.qty)
            self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, +pos.qty)
            self.open_tiers.pop(index)
            actions.append({"action": "close", "tier": index,
                            "reason": "reverted" if reverted else "timeout", "net_bps": net_bps})

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

        for tier in self.config.tiers:
            if tier.index in self.open_tiers:
                continue
            if not self._confirmed(window, tier.threshold_bps):
                continue
            qty = self._qty(price_hint)
            if qty <= 0:
                continue
            self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, +qty)
            self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, -qty)
            self.open_tiers[tier.index] = OpenTier(index=tier.index, entry_edge=edge, entry_dev=dev,
                                                   frozen_baseline=baseline, opened_at=stamp, qty=qty)
            actions.append({"action": "open", "tier": tier.index, "dev_bps": dev, "qty": qty})
        return actions

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
                "total_usd": sum(n / 10000.0 * self.config.notional_usd for n in nets),
                "median_hold_s": sorted(i["hold_s"] for i in items)[len(items) // 2],
                "timeout_share": sum(1 for i in items if i["reason"] == "timeout") / len(items),
            })
        return rows
