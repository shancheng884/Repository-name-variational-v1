"""NVDA five-tier basis engine with net-position reconciliation.

Deliberately standalone: it does not import or modify main.py.  Sizing follows
the ETH / CL ladder verbatim: a fixed child order, cumulative capacity of tier k
equal to k x the smaller verified venue equity, hard capped at
max_equity_multiple x equity.

Execution contract:
* ``on_sample`` only *proposes* one entry per direction; nothing is booked until
  the runner has proved both legs filled and the venue positions match;
* ``confirm_open`` is the only place an entry reaches the ledger;
* a close is booked only after both closing legs were confirmed;
* every ledger lookup accepts either the asset name or the asset object, so a
  caller can no longer turn the expected position into zero by accident.
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


def asset_symbol(asset: Any) -> str:
    """Ledger key for an asset name or an asset definition object."""
    return getattr(asset, "name", asset)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    # When set, a position only closes after the spread has converged far
    # enough to pay the measured exit slippage and still keep the profit
    # target.  ``None`` keeps the legacy "back to the frozen anchor" exit.
    exit_cost_bps: float | None = None
    profit_target_bps: float = 1.0

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
        self._by_strategy[(strategy, asset_symbol(asset), venue)] = Decimal(str(qty))

    def add(self, strategy: str, asset: str, venue: str, qty: Decimal | float) -> None:
        key = (strategy, asset_symbol(asset), venue)
        self._by_strategy[key] = self._by_strategy.get(key, Decimal("0")) + Decimal(str(qty))

    def net(self, asset: str, venue: str) -> Decimal:
        symbol = asset_symbol(asset)
        total = Decimal("0")
        for (strat, sym, ven), qty in self._by_strategy.items():
            if sym == symbol and ven == venue:
                total += qty
        return total

    def by_strategy(self, asset: str) -> dict[str, dict[str, Decimal]]:
        symbol = asset_symbol(asset)
        out: dict[str, dict[str, Decimal]] = defaultdict(dict)
        for (strat, sym, ven), qty in self._by_strategy.items():
            if sym == symbol:
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
    entry_buy_price: float | None = None
    entry_sell_price: float | None = None
    entry_fee_usd: float = 0.0
    children: list[dict[str, Any]] = field(default_factory=list)
    signal_basis: str = "raw_v1"
    entry_fee_pending: bool = False
    entry_receipts: dict[str, Any] = field(default_factory=dict)

    def to_state(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "entry_edge": self.entry_edge,
            "entry_dev": self.entry_dev,
            "frozen_baseline": self.frozen_baseline,
            "opened_at": self.opened_at,
            "qty": self.qty,
            "notional_usd": self.notional_usd,
            "closing": False,
            "entry_buy_price": self.entry_buy_price,
            "entry_sell_price": self.entry_sell_price,
            "entry_fee_usd": self.entry_fee_usd,
            "children": self.children,
            "signal_basis": self.signal_basis,
            "entry_fee_pending": self.entry_fee_pending,
            "entry_receipts": self.entry_receipts,
        }

    @classmethod
    def from_state(cls, row: dict[str, Any]) -> "OpenTier":
        return cls(
            index=int(row["index"]),
            entry_edge=float(row.get("entry_edge") or 0.0),
            entry_dev=float(row.get("entry_dev") or 0.0),
            frozen_baseline=float(row.get("frozen_baseline") or 0.0),
            opened_at=float(row.get("opened_at") or time.time()),
            qty=float(row.get("qty") or 0.0),
            notional_usd=float(row.get("notional_usd") or 0.0),
            closing=False,
            entry_buy_price=_float_or_none(row.get("entry_buy_price")),
            entry_sell_price=_float_or_none(row.get("entry_sell_price")),
            entry_fee_usd=float(row.get("entry_fee_usd") or 0.0),
            children=list(row.get("children") or []),
            signal_basis=str(row.get("signal_basis") or "raw_v1"),
            entry_fee_pending=bool(row.get("entry_fee_pending")),
            entry_receipts=dict(row.get("entry_receipts") or {}),
        )


@dataclass
class TierEngine:
    config: TierConfig
    ledger: PositionLedger
    strategy_id: str = "nvda-tier"
    open_tiers: dict[int, OpenTier] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)
    pending_action: dict[str, Any] | None = None
    completed_actions: list[str] = field(default_factory=list)
    targets: tuple[float, ...] | None = field(init=False, default=None)

    def __post_init__(self):
        caps = self.config.slot_caps()
        if caps is not None:
            self.targets = tuple(value * self.config.child_notional_usd for value in caps)

    def _confirmed(self, window: list[float], threshold: float) -> bool:
        return sum(1 for value in window if value >= threshold) >= self.config.confirm_min

    def _open_notional(self) -> float:
        return sum(position.notional_usd for position in self.open_tiers.values())

    def _entry_notional(self, tier_index: int) -> float:
        """Increment needed to reach the cumulative tier capacity."""
        target = self.targets[tier_index - 1] if self.targets is not None else None
        if target is None:
            return min(self.config.notional_usd, self.config.child_notional_usd)
        return min(self.config.child_notional_usd, target - self._open_notional())

    def _lots(self, pos: OpenTier) -> list[dict[str, Any]]:
        if not pos.children:
            pos.children = [{"qty": pos.qty, "notional_usd": pos.notional_usd,
                             "entry_edge": pos.entry_edge, "entry_dev": pos.entry_dev,
                             "opened_at": pos.opened_at, "entry_buy_price": pos.entry_buy_price,
                             "entry_sell_price": pos.entry_sell_price,
                             "entry_fee_usd": pos.entry_fee_usd}]
            pos.children[0].update(entry_fee_pending=pos.entry_fee_pending,
                                   entry_receipts=pos.entry_receipts)
        return pos.children

    def _refresh(self, pos: OpenTier) -> None:
        lots = self._lots(pos)
        pos.qty = sum(row["qty"] for row in lots)
        pos.notional_usd = sum(row["notional_usd"] for row in lots)
        pos.entry_fee_usd = sum(row["entry_fee_usd"] for row in lots)
        pos.entry_fee_pending = any(row.get("entry_fee_pending") for row in lots)
        for key in ("entry_edge", "entry_dev", "entry_buy_price", "entry_sell_price"):
            if all(row.get(key) is not None for row in lots):
                setattr(pos, key, sum(row[key] * row["qty"] for row in lots) / pos.qty)

    def cancel_open(self, action: dict[str, Any]) -> None:
        if self.pending_action and self.pending_action.get("token") == action.get("token"):
            self.pending_action = None

    def on_sample(self, *, stamp: float, edge: float, baseline: float, price_hint: float,
                  window: list[float], exchange: dict[str, Decimal | float | None],
                  allow_entry: bool = True, raw_edge: float | None = None) -> list[dict[str, Any]]:
        """Decide closes (always) and at most one entry for this direction.

        The ETH gradient runs bidirectionally but locks the active direction
        until the book is fully flat, so the caller passes allow_entry=False
        when the opposite direction still holds a tier.

        Nothing is written to the ledger here: the caller books the tier with
        ``confirm_open`` only after both legs and the venue positions agree.
        """
        actions: list[dict[str, Any]] = []
        dev = edge - baseline
        if self.pending_action is not None:
            return actions

        for index in sorted(self.open_tiers):
            pos = self.open_tiers[index]
            if pos.closing:
                # Already waiting on a venue fill; do not queue a second close.
                continue
            exit_edge = raw_edge if pos.signal_basis == "raw_v1" and raw_edge is not None else edge
            converged = pos.entry_edge - exit_edge
            if self.config.exit_cost_bps is None:
                # Legacy rule: wait for the spread to come back to its anchor.
                target_hit = exit_edge <= pos.frozen_baseline
                net_bps = converged - self.config.slippage_bps
            else:
                # Pay the locked exit slippage out of the convergence and keep
                # the profit target; a merely "reverted" spread is not enough.
                target_hit = converged >= (self.config.exit_cost_bps
                                           + self.config.profit_target_bps)
                net_bps = converged - self.config.exit_cost_bps
            # timeout_seconds=None keeps the tier open until it pays.
            expired = (self.config.timeout_seconds is not None
                       and stamp - pos.opened_at >= self.config.timeout_seconds)
            if not (target_hit or expired):
                continue
            lot = self._lots(pos)[0]
            fraction = min(1.0, self.config.child_notional_usd / lot["notional_usd"])
            pos.closing = True
            actions.append({"action": "close", "tier": index,
                            "token": uuid.uuid4().hex,
                            "reason": ("reverted" if self.config.exit_cost_bps is None
                                       else "target") if target_hit else "timeout",
                            "net_bps": net_bps, "qty": lot["qty"] * fraction,
                            "notional_usd": lot["notional_usd"] * fraction,
                            "entry_buy_price": lot.get("entry_buy_price"),
                            "entry_sell_price": lot.get("entry_sell_price"),
                            "entry_fee_usd": None if lot.get("entry_fee_pending") else lot["entry_fee_usd"] * fraction,
                            "entry_receipts": lot.get("entry_receipts") or {},
                            "entry_fee_fraction": fraction,
                            "entry_edge": lot["entry_edge"], "exit_edge": exit_edge,
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

        if not allow_entry or actions:
            return actions

        # Exactly one new tier per sample.  The runner sends it, proves both
        # legs landed, books it, and only then may the next tier be proposed.
        for tier in self.config.tiers:
            if tier.index in self.open_tiers and self.open_tiers[tier.index].closing:
                continue
            if self.targets is None and tier.index in self.open_tiers:
                continue
            if dev < tier.threshold_bps:
                continue
            if raw_edge is not None and raw_edge - baseline < tier.threshold_bps:
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
            actions.append({"action": "open", "tier": tier.index, "dev_bps": dev,
                            "token": uuid.uuid4().hex,
                            "signal_basis": "source_time_aligned_v1" if raw_edge is not None else "raw_v1",
                            "qty": qty, "notional_usd": increment,
                            "threshold_bps": tier.threshold_bps,
                            "edge": edge, "baseline": baseline, "stamp": stamp})
            self.pending_action = dict(actions[-1])
            break
        return actions

    def confirm_open(self, action: dict[str, Any], *, qty: Decimal | float | None = None,
                     entry_prices: dict[str, Any] | None = None,
                     fee_usd: float = 0.0, entry_receipts=None) -> OpenTier | None:
        """Book a tier after the venue proved both legs are really on the book."""
        index = int(action.get("tier"))
        token = action.get("token")
        if token and token in self.completed_actions:
            return None
        amount = Decimal(str(qty if qty is not None else action.get("qty") or 0))
        if amount <= 0:
            return None
        prices = entry_prices or {}
        self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, amount)
        self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, -amount)
        child = OpenTier(
            index=index,
            entry_edge=float(action.get("edge") or 0.0),
            entry_dev=float(action.get("dev_bps") or 0.0),
            frozen_baseline=float(action.get("baseline") or 0.0),
            opened_at=float(action["stamp"] if action.get("stamp") is not None else time.time()),
            qty=float(amount),
            notional_usd=float(action.get("notional_usd") or self.config.notional_usd),
            entry_buy_price=_float_or_none(prices.get(self.config.buy_venue)),
            entry_sell_price=_float_or_none(prices.get(self.config.sell_venue)),
            entry_fee_usd=float(fee_usd or 0.0),
            signal_basis=action.get("signal_basis") or "raw_v1",
            entry_receipts=dict(entry_receipts or {}),
            entry_fee_pending=any(v == "entropy" and r.get("fee_usd") is None
                                  for v, r in (entry_receipts or {}).items()),
        )
        pos = self.open_tiers.get(index)
        if pos is None:
            pos = child
            self.open_tiers[index] = pos
            self._lots(pos)
        else:
            self._lots(pos).extend(self._lots(child))
            self._refresh(pos)
        if token:
            self.completed_actions = (self.completed_actions + [token])[-2000:]
        self.cancel_open(action)
        return pos

    def confirm_close(self, action: dict[str, Any]) -> dict[str, Any] | None:
        """Book a close only after the venue confirmed both legs filled."""
        index = int(action.get("tier"))
        token = action.get("token")
        if token and token in self.completed_actions:
            return None
        pos = self.open_tiers.get(index)
        if pos is None:
            return None
        lot = self._lots(pos)[0]
        qty = min(float(action.get("qty") or pos.qty), lot["qty"])
        fraction = qty / lot["qty"]
        notional = lot["notional_usd"] * fraction
        trade = {"tier": index, "net_bps": float(action.get("net_bps") or 0.0),
                 "execution_token": token,
                 "hold_s": float(action.get("hold_s") or 0.0),
                 "reason": action.get("reason") or "reverted",
                 "entry_dev": lot["entry_dev"], "notional_usd": notional,
                 "entry_edge": lot["entry_edge"], "exit_edge": float(action.get("exit_edge") or 0.0),
                 "entry_buy_price": lot.get("entry_buy_price"), "entry_sell_price": lot.get("entry_sell_price")}
        self.trades.append(trade)
        self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, -Decimal(str(qty)))
        self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, Decimal(str(qty)))
        if fraction >= 1.0 - 1e-10:
            pos.children.pop(0)
        else:
            lot["qty"] -= qty
            lot["notional_usd"] -= notional
            lot["entry_fee_usd"] *= 1.0 - fraction
        if not pos.children:
            self.open_tiers.pop(index)
        else:
            self._refresh(pos)
            pos.closing = False
        if token:
            self.completed_actions = (self.completed_actions + [token])[-2000:]
        if self.pending_action and self.pending_action.get("token") == token:
            self.pending_action = None
        return trade

    def cancel_close(self, index: int) -> None:
        """Release the close lock so the next sample can try again."""
        pos = self.open_tiers.get(int(index))
        if pos is not None:
            pos.closing = False
        if self.pending_action and self.pending_action.get("action") == "close":
            self.pending_action = None

    def drop_tier(self, index: int) -> None:
        """Undo a booked tier (used when a close had to be abandoned)."""
        pos = self.open_tiers.pop(index, None)
        if pos is None:
            return
        self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, -Decimal(str(pos.qty)))
        self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, Decimal(str(pos.qty)))

    def dump_state(self) -> dict[str, Any]:
        return {"strategy_id": self.strategy_id,
                "open_tiers": [pos.to_state() for pos in self.open_tiers.values()],
                "pending_action": self.pending_action,
                "completed_actions": self.completed_actions[-2000:],
                "trades": self.trades[-200:]}

    def restore_state(self, data: dict[str, Any] | None) -> None:
        """Rebuild booked tiers (and their ledger legs) from a saved run."""
        if not data:
            return
        for row in data.get("open_tiers") or []:
            try:
                pos = OpenTier.from_state(row)
            except (KeyError, TypeError, ValueError):
                continue
            if pos.index in self.open_tiers or pos.qty <= 0:
                continue
            self.open_tiers[pos.index] = pos
            self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, Decimal(str(pos.qty)))
            self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, -Decimal(str(pos.qty)))
        self.trades = list(data.get("trades") or [])[-200:]
        self.completed_actions = list(data.get("completed_actions") or [])[-2000:]
        self.pending_action = data.get("pending_action")

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
