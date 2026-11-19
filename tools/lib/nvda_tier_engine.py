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
class ExitMark:
    """One executable way to close a child: buy back the short, sell the long."""
    buy_price: float
    sell_price: float
    source: str = "latest"


def exit_gross_bps(*, entry_buy: float, entry_sell: float,
                   exit_buy: float, exit_sell: float) -> float | None:
    """Realized child edge from four fills, not from the pre-trade signal.

    The entry side uses the actual average fill prices; the exit side uses the
    executable book for the size being closed.  Spread is therefore embedded
    once in the prices and must not be charged again as a fee.
    """
    if not all(p is not None and 0 < p < float("inf")
               for p in (entry_buy, entry_sell, exit_buy, exit_sell)):
        return None
    mid = (entry_buy + entry_sell) / 2.0
    if mid <= 0:
        return None
    # Long leg: exit_sell - entry_buy.  Short leg: entry_sell - exit_buy.
    return ((entry_sell - entry_buy) + (exit_sell - exit_buy)) / mid * 10000.0


def exit_cost_breakdown(*, entry_buy: float, entry_sell: float,
                        exit_buy: float, exit_sell: float,
                        exit_slippage_bps: float, entry_fee_bps: float,
                        exit_fee_bps: float, profit_target_bps: float) -> dict[str, float] | None:
    """One exit decision with every cost counted exactly once."""
    gross = exit_gross_bps(entry_buy=entry_buy, entry_sell=entry_sell,
                           exit_buy=exit_buy, exit_sell=exit_sell)
    if gross is None:
        return None
    cost = float(exit_slippage_bps) + float(entry_fee_bps) + float(exit_fee_bps)
    return {"gross_bps": gross, "exit_slippage_bps": float(exit_slippage_bps),
            "entry_fee_bps": float(entry_fee_bps), "exit_fee_bps": float(exit_fee_bps),
            "cost_bps": cost, "net_bps": gross - cost,
            "profit_target_bps": float(profit_target_bps)}


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
    # Measured exit slippage (P80), deliberately excluding fees: the exit
    # prices already embed the spread, and the two fees are charged below.
    exit_cost_bps: float | None = None
    profit_target_bps: float = 1.0
    # Fallback Entropy taker fee when the entry receipt has not arrived yet.
    entropy_fee_bps: float = 0.9
    select_eligible_child: bool = False

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
    # Cross-venue edge actually obtained by the two entry fills.  The exit
    # gate measures convergence from this, not from the pre-trade signal.
    entry_realized_edge: float | None = None

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
            "entry_realized_edge": self.entry_realized_edge,
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
            entry_realized_edge=_float_or_none(row.get("entry_realized_edge")),
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
    last_exit_eval: dict[int, dict[str, Any]] = field(default_factory=dict)
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
                             "entry_realized_edge": pos.entry_realized_edge,
                             "entry_fee_usd": pos.entry_fee_usd}]
            pos.children[0].update(entry_fee_pending=pos.entry_fee_pending,
                                   entry_receipts=pos.entry_receipts)
        for offset, lot in enumerate(pos.children):
            if not lot.get("child_id"):
                lot["child_id"] = f"legacy-{pos.index}-{offset}-{lot.get('opened_at', pos.opened_at)}"
        return pos.children

    def close_child(self, index: int, child_id: str | None = None) -> tuple[dict[str, Any], float, float]:
        """The named child, or the FIFO head for pre-migration actions."""
        pos = self.open_tiers[index]
        lots = self._lots(pos)
        matches = lots[:1] if child_id is None else [x for x in lots if x["child_id"] == child_id]
        if len(matches) != 1:
            raise KeyError(child_id)
        lot = matches[0]
        fraction, qty = self.child_close_size(lot)
        return lot, fraction, qty

    def child_close_size(self, lot: dict[str, Any]) -> tuple[float, float]:
        fraction = min(1.0, self.config.child_notional_usd / lot["notional_usd"])
        return fraction, lot["qty"] * fraction

    def exit_children(self, index: int) -> list[dict[str, Any]]:
        lots = self._lots(self.open_tiers[index])
        return lots if self.config.select_eligible_child else lots[:1]

    def _child_exit_eval(self, lot, marks) -> dict[str, Any] | None:
        best = None
        entry_buy, entry_sell = lot.get("entry_buy_price"), lot.get("entry_sell_price")
        for mark in marks:
            gross = exit_gross_bps(entry_buy=entry_buy, entry_sell=entry_sell,
                                   exit_buy=mark.buy_price, exit_sell=mark.sell_price)
            if gross is not None and (best is None or gross < best[0]):
                best = (gross, mark)
        if best is None:
            return None
        entropy = "entropy" in (self.config.buy_venue, self.config.sell_venue)
        lot_fee = float(lot.get("entry_fee_usd") or 0.0)
        if entropy and lot.get("entry_fee_pending"):
            ref = entry_buy if self.config.buy_venue == "entropy" else entry_sell
            lot_fee = lot["qty"] * ref * self.config.entropy_fee_bps / 10000.0
        entry_fee = lot_fee / lot["notional_usd"] * 10000.0
        exit_fee = entry_fee if lot_fee > 0 else (self.config.entropy_fee_bps if entropy else 0.0)
        result = exit_cost_breakdown(entry_buy=entry_buy, entry_sell=entry_sell,
                                     exit_buy=best[1].buy_price, exit_sell=best[1].sell_price,
                                     exit_slippage_bps=self.config.exit_cost_bps,
                                     entry_fee_bps=entry_fee, exit_fee_bps=exit_fee,
                                     profit_target_bps=self.config.profit_target_bps)
        return {**result, "mark_source": best[1].source, "child_id": lot["child_id"]}

    def _refresh(self, pos: OpenTier) -> None:
        lots = self._lots(pos)
        pos.qty = sum(row["qty"] for row in lots)
        pos.notional_usd = sum(row["notional_usd"] for row in lots)
        pos.entry_fee_usd = sum(row["entry_fee_usd"] for row in lots)
        pos.entry_fee_pending = any(row.get("entry_fee_pending") for row in lots)
        for key in ("entry_edge", "entry_dev", "entry_buy_price", "entry_sell_price",
                    "entry_realized_edge"):
            if all(row.get(key) is not None for row in lots):
                setattr(pos, key, sum(row[key] * row["qty"] for row in lots) / pos.qty)

    def cancel_open(self, action: dict[str, Any]) -> None:
        if self.pending_action and self.pending_action.get("token") == action.get("token"):
            self.pending_action = None

    def on_sample(self, *, stamp: float, edge: float, baseline: float, price_hint: float,
                  window: list[float], exchange: dict[str, Decimal | float | None],
                  allow_entry: bool = True, raw_edge: float | None = None,
                  exit_marks: dict[int | str, list[ExitMark]] | None = None) -> list[dict[str, Any]]:
        """Decide closes (always) and at most one entry for this direction.

        The ETH gradient runs bidirectionally but locks the active direction
        until the book is fully flat, so the caller passes allow_entry=False
        when the opposite direction still holds a tier.

        Nothing is written to the ledger here: the caller books the tier with
        ``confirm_open`` only after both legs and the venue positions agree.
        """
        actions: list[dict[str, Any]] = []
        self.last_exit_eval.clear()
        dev = edge - baseline
        if self.pending_action is not None:
            return actions

        for index in sorted(self.open_tiers):
            pos = self.open_tiers[index]
            if pos.closing:
                # Already waiting on a venue fill; do not queue a second close.
                continue
            exit_edge = raw_edge if pos.signal_basis == "raw_v1" and raw_edge is not None else edge
            entry_reference = (pos.entry_realized_edge if pos.entry_realized_edge is not None
                               else pos.entry_edge)
            converged = entry_reference - exit_edge
            lot, fraction, qty = self.close_child(index)
            exit_eval: dict[str, Any] | None = None
            if self.config.exit_cost_bps is None:
                # Legacy rule: wait for the spread to come back to its anchor.
                target_hit = exit_edge <= pos.frozen_baseline
                net_bps = converged - self.config.slippage_bps
            else:
                marks = exit_marks or {}
                # Scan FIFO order; the first eligible child wins. Tier marks
                # from old callers are valid for the head only.
                for offset, child in enumerate(self.exit_children(index)):
                    child_marks = marks.get(child["child_id"], marks.get(index, []) if offset == 0 else [])
                    if self.config.select_eligible_child and {m.source for m in child_marks} != {"aligned", "latest"}:
                        continue
                    evaluated = self._child_exit_eval(child, child_marks)
                    if offset == 0:
                        exit_eval = evaluated
                    if evaluated and evaluated["net_bps"] >= self.config.profit_target_bps:
                        lot = child
                        fraction, qty = self.child_close_size(child)
                        exit_eval = {**evaluated, "queue_offset": offset}
                        break
                if exit_eval is not None:
                    self.last_exit_eval[index] = dict(exit_eval)
                    target_hit = exit_eval["net_bps"] >= self.config.profit_target_bps
                    net_bps = exit_eval["net_bps"]
                elif exit_marks is not None:
                    # Live call without a fully executable close book: hold the
                    # tier rather than authorize an exit from the signal alone.
                    target_hit = False
                    net_bps = float("-inf")
                else:
                    # Paper/legacy call: keep the old convergence gate rather
                    # than invent prices.
                    target_hit = converged >= (self.config.exit_cost_bps
                                               + self.config.profit_target_bps)
                    net_bps = converged - self.config.exit_cost_bps
            # timeout_seconds=None keeps the tier open until it pays.
            expired = (self.config.timeout_seconds is not None
                       and stamp - pos.opened_at >= self.config.timeout_seconds)
            if not (target_hit or expired):
                continue
            pos.closing = True
            action = {"action": "close", "tier": index,
                      "token": uuid.uuid4().hex,
                      "child_id": lot["child_id"],
                      "reason": ("reverted" if self.config.exit_cost_bps is None
                                 else "target") if target_hit else "timeout",
                      "net_bps": net_bps, "qty": qty,
                      "notional_usd": lot["notional_usd"] * fraction,
                      "entry_buy_price": lot.get("entry_buy_price"),
                      "entry_sell_price": lot.get("entry_sell_price"),
                      "entry_fee_usd": None if lot.get("entry_fee_pending") else lot["entry_fee_usd"] * fraction,
                      "entry_receipts": lot.get("entry_receipts") or {},
                      "entry_fee_fraction": fraction,
                      "entry_edge": lot["entry_edge"], "exit_edge": exit_edge,
                      "hold_s": stamp - lot.get("opened_at", pos.opened_at)}
            if exit_eval is not None:
                action["exit_eval"] = exit_eval
            actions.append(action)
            if self.config.select_eligible_child:
                self.pending_action = dict(action)
                break

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
        buy_fill = _float_or_none(prices.get(self.config.buy_venue))
        sell_fill = _float_or_none(prices.get(self.config.sell_venue))
        realized_edge = None
        if buy_fill and sell_fill and buy_fill > 0 and sell_fill > 0:
            fill_mid = (buy_fill + sell_fill) / 2
            realized_edge = (sell_fill - buy_fill) / fill_mid * 10000.0
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
            entry_realized_edge=realized_edge,
        )
        self._lots(child)[0]["child_id"] = token or uuid.uuid4().hex
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

    def match_close(self, action: dict[str, Any]) -> tuple[dict[str, Any], float, float] | None:
        locked = self.pending_action
        if ((self.config.select_eligible_child and locked is None)
                or (locked is not None and (
                    locked.get("action") != "close"
                    or any(locked.get(k) != action.get(k) for k in ("token", "tier", "child_id", "qty"))
                ))):
            return None
        try:
            lot, fraction, planned_qty = self.close_child(int(action["tier"]), action.get("child_id"))
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            return None
        if action.get("qty") != planned_qty or planned_qty <= 0:
            return None
        return lot, fraction, planned_qty

    def confirm_close(self, action: dict[str, Any]) -> dict[str, Any] | None:
        """Book a close only after the venue confirmed both legs filled."""
        index = int(action.get("tier"))
        token = action.get("token")
        if token and token in self.completed_actions:
            return None
        matched = self.match_close(action)
        if matched is None:
            return None
        pos = self.open_tiers[index]
        lot, fraction, qty = matched
        notional = lot["notional_usd"] * fraction
        trade = {"tier": index, "net_bps": float(action.get("net_bps") or 0.0),
                 "execution_token": token, "child_id": lot["child_id"],
                 "hold_s": float(action.get("hold_s") or 0.0),
                 "reason": action.get("reason") or "reverted",
                 "entry_dev": lot["entry_dev"], "notional_usd": notional,
                 "entry_edge": lot["entry_edge"], "exit_edge": float(action.get("exit_edge") or 0.0),
                 "entry_realized_edge": lot.get("entry_realized_edge"),
                 "entry_buy_price": lot.get("entry_buy_price"), "entry_sell_price": lot.get("entry_sell_price")}
        self.trades.append(trade)
        self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, -Decimal(str(qty)))
        self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, Decimal(str(qty)))
        if fraction >= 1.0 - 1e-10:
            pos.children.remove(lot)
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

    def cancel_close(self, index: int, *, token: str | None = None) -> None:
        """Release the close lock so the next sample can try again."""
        locked = self.pending_action
        if locked and locked.get("action") == "close":
            if locked.get("tier") != int(index) or (token is not None and locked.get("token") != token):
                return
            self.pending_action = None
        pos = self.open_tiers.get(int(index))
        if pos is not None:
            pos.closing = False

    def drop_tier(self, index: int) -> None:
        """Undo a booked tier (used when a close had to be abandoned)."""
        pos = self.open_tiers.pop(index, None)
        if pos is None:
            return
        qty = sum((Decimal(str(lot["qty"])) for lot in self._lots(pos)), Decimal(0))
        self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, -qty)
        self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, qty)

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
        child_ids = set()
        for row in data.get("open_tiers") or []:
            try:
                pos = OpenTier.from_state(row)
            except (KeyError, TypeError, ValueError):
                continue
            if pos.index in self.open_tiers or pos.qty <= 0:
                continue
            lots = self._lots(pos)
            for lot in lots:
                if lot["child_id"] in child_ids:
                    raise ValueError("duplicate_child_id")
                child_ids.add(lot["child_id"])
            qty = sum((Decimal(str(lot["qty"])) for lot in lots), Decimal(0))
            if abs(qty - Decimal(str(pos.qty))) > Decimal("0.0000001"):
                raise ValueError("child_quantity_mismatch")
            self.open_tiers[pos.index] = pos
            self.ledger.add(self.strategy_id, self.config.asset, self.config.buy_venue, qty)
            self.ledger.add(self.strategy_id, self.config.asset, self.config.sell_venue, -qty)
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
