"""Stateful median signal model; order submission stays in the live executor.

All prices use one settlement currency. Bps use the average ONE-leg notional.
This module never submits orders and never silently expires an unknown order.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from statistics import median
from math import isfinite
from typing import Any

D = Decimal
LONG = "long_var_short_lighter"
SHORT = "short_var_long_lighter"
MULTIPLIERS = (D("1"), D("1.5"), D("2"), D("3"), D("4"))


@dataclass(frozen=True)
class Observation:
    at: float
    source_at: float
    event_id: str
    instrument: str
    size_key: str
    var_bid: D
    var_ask: D
    lighter_bid: D
    lighter_ask: D
    lighter_at: float

    @property
    def basis(self) -> D:
        v = (self.var_bid + self.var_ask) / 2
        l = (self.lighter_bid + self.lighter_ask) / 2
        return (l - v) / ((l + v) / 2) * 10000

    def valid(self, *, var_max_age: float, lighter_max_age: float) -> bool:
        return bool(
            self.event_id and self.instrument and self.size_key
            and all(p.is_finite() and p > 0 for p in (
                self.var_bid, self.var_ask, self.lighter_bid, self.lighter_ask
            ))
            and self.var_ask > self.var_bid
            and self.lighter_ask >= self.lighter_bid
            and 0 <= self.at - self.source_at <= var_max_age
            and 0 <= self.at - self.lighter_at <= lighter_max_age
        )


def executable_profit_bps(
    *, direction: str, var_entry: D, lighter_entry: D,
    var_exit: D, lighter_exit: D, remaining_reserve_bps: D,
) -> D:
    """Bid/ask prices already include spreads; no spread/entry loss is recharged."""
    prices = (var_entry, lighter_entry, var_exit, lighter_exit)
    if not all(p.is_finite() and p > 0 for p in prices):
        raise ValueError("invalid_execution_price")
    if not remaining_reserve_bps.is_finite() or remaining_reserve_bps < 0:
        raise ValueError("invalid_remaining_reserve")
    if direction not in (LONG, SHORT):
        raise ValueError("invalid_direction")
    sign = D(1) if direction == LONG else D(-1)
    gross = sign * ((var_exit - var_entry) + (lighter_entry - lighter_exit))
    return gross / ((var_entry + lighter_entry) / 2) * 10000 - remaining_reserve_bps


def slot_caps(var_equity: D, lighter_equity: D, child: D = D(20)) -> list[int]:
    if not all(x.is_finite() and x > 0 for x in (var_equity, lighter_equity, child)):
        raise ValueError("invalid_capacity_input")
    equity = min(var_equity, lighter_equity)
    return [int((equity * i / child).to_integral_value(rounding=ROUND_DOWN))
            for i in range(1, 6)]


class FrozenMedianGradient:
    SCHEMA = 1

    def __init__(self, *, deviation_bps: D = D(6), window_seconds: int = 3600,
                 coverage: D = D("0.8"), max_gap: float = 60,
                 reset_buffer_bps: D = D("0.5"), distances=None) -> None:
        if not deviation_bps.is_finite() or deviation_bps <= 0:
            raise ValueError("invalid_deviation")
        if window_seconds < 3 or not 0 < coverage <= 1 or max_gap <= 0:
            raise ValueError("invalid_history_policy")
        if not reset_buffer_bps.is_finite() or not 0 <= reset_buffer_bps < deviation_bps:
            raise ValueError("invalid_reset_buffer")
        self.deviation = deviation_bps
        self.distances = tuple(D(x) for x in distances) if distances is not None else tuple(deviation_bps*k for k in MULTIPLIERS)
        if len(self.distances) != 5 or self.distances[0] != deviation_bps or any(not x.is_finite() or x <= 0 for x in self.distances) or any(b <= a for a,b in zip(self.distances,self.distances[1:])):
            raise ValueError("invalid_gradient_distances")
        self.window = window_seconds
        self.coverage = coverage
        self.max_gap = max_gap
        self.reset_buffer = reset_buffer_bps
        self.history: deque[tuple[int, D]] = deque()
        self.confirmations: deque[tuple[str, int, D]] = deque(maxlen=3)
        self.signature: tuple[str, str] | None = None
        self.anchor: D | None = None
        self.direction: str | None = None
        self.episode_id: str | None = None
        self.review_reason: str | None = None
        self.orders: dict[str, dict[str, Any]] = {}
        self.tiers = {str(i): {"armed": True, "reset_seen": False,
                              "generation": 0, "last_event": None}
                      for i in range(1, 6)}
        self.last_at: float | None = None
        self.last_basis: D | None = None
        self.suspect_since: float | None = None

    def observe(self, row: Observation, *, var_max_age: float,
                lighter_max_age: float) -> bool:
        if not row.valid(var_max_age=var_max_age, lighter_max_age=lighter_max_age):
            self.confirmations.clear()
            return False
        if self.last_at is not None and row.at < self.last_at:
            return False
        signature = (row.instrument, row.size_key)
        if self.signature is not None and signature != self.signature:
            if self.episode_id is not None:
                self.review_reason = "median_quote_spec_changed"
                self.confirmations.clear()
                return False
            self.history.clear()
            self.confirmations.clear()
        self.signature = signature
        second = int(row.at)
        if not self.history or second > self.history[-1][0]:
            self.history.append((second, row.basis))
        while self.history and self.history[0][0] < second - self.window + 1:
            self.history.popleft()
        self.last_at, self.last_basis = row.at, row.basis
        if self.anchor is not None and self.direction is not None:
            distance = self.signed_distance(row.basis, self.anchor, self.direction)
            for i, multiplier in enumerate(MULTIPLIERS, 1):
                t = self.tiers[str(i)]
                if t["armed"] or t["last_event"] == row.event_id:
                    continue
                if distance <= self.distances[i-1] - self.reset_buffer:
                    t["reset_seen"] = True
                if (t["reset_seen"] and distance >= self.distances[i-1]
                        and not self.tier_unresolved(i)):
                    t["armed"] = True
                    t["reset_seen"] = False
                    t["generation"] += 1
                t["last_event"] = row.event_id
        if (not any(c[0] == row.event_id for c in self.confirmations)
                and (not self.confirmations or second > self.confirmations[-1][1])):
            self.confirmations.append((row.event_id, second, row.basis))
        rolling = self.rolling_median(now=row.at)
        if self.anchor is not None and rolling is not None:
            if abs(rolling - self.anchor) > D(12):
                if self.suspect_since is None:
                    self.suspect_since = row.at
                elif row.at - self.suspect_since >= 1800:
                    self.review_reason = "persistent_median_displacement"
            else:
                self.suspect_since = None
        return True

    @staticmethod
    def signed_distance(basis: D, anchor: D, direction: str) -> D:
        if direction not in (LONG, SHORT):
            raise ValueError("invalid_direction")
        return (basis - anchor) * (1 if direction == LONG else -1)

    def rolling_median(self, *, now: float) -> D | None:
        rows = [(t, b) for t, b in self.history if now - self.window < t <= now]
        if len(rows) < self.window * self.coverage:
            return None
        gaps = [b[0] - a[0] for a, b in zip(rows, rows[1:])]
        gaps.extend((now - rows[-1][0], rows[0][0] - (now - self.window)))
        if max(gaps) > self.max_gap:
            return None
        return median(b for _, b in rows)

    def levels(self, *, now: float) -> tuple[D, ...] | None:
        if self.anchor is None and self.rolling_median(now=now) is None:
            return None
        return self.distances

    def freeze(self, *, episode_id: str, direction: str, now: float) -> None:
        if self.episode_id is not None:
            if self.episode_id != episode_id or self.direction != direction:
                raise ValueError("existing_episode_must_be_reconciled")
            return
        anchor = self.rolling_median(now=now)
        if anchor is None or not episode_id or direction not in (LONG, SHORT):
            raise ValueError("median_episode_not_ready")
        self.anchor, self.episode_id, self.direction = anchor, episode_id, direction

    def tier_unresolved(self, tier: int) -> bool:
        return any(o["tier"] == tier and o["state"] != "closed"
                   for o in self.orders.values())

    def candidate_tier(self, *, direction: str, now: float) -> int:
        if self.review_reason or (self.direction is not None and direction != self.direction):
            return 0
        anchor = self.anchor if self.anchor is not None else self.rolling_median(now=now)
        if anchor is None or self.last_at is None or not 0 <= now - self.last_at <= 5:
            return 0
        observations = [c for c in self.confirmations if 0 <= now - c[1] <= 3]
        if len(observations) < 2 or observations[-1][2] != self.last_basis:
            return 0
        tier = 0
        for i, multiplier in enumerate(MULTIPLIERS, 1):
            threshold = self.distances[i-1]
            if (self.tiers[str(i)]["armed"]
                    and self.signed_distance(self.last_basis, anchor, direction) >= threshold
                    and sum(self.signed_distance(c[2], anchor, direction) >= threshold
                            for c in observations) >= 2):
                tier = i
        return tier

    def reserve(self, *, order_id: str, tier: int, capacity: int,
                existing_slots: int, now: float) -> bool:
        if order_id in self.orders:
            return False
        if self.review_reason or self.anchor is None or tier not in range(1, 6):
            raise ValueError("entry_not_authorized")
        if not self.tiers[str(tier)]["armed"] or existing_slots >= capacity:
            raise ValueError("tier_or_capacity_unavailable")
        if capacity <= 0 or existing_slots < 0 or tier > self.candidate_tier(
            direction=self.direction, now=now
        ):
            raise ValueError("tier_not_confirmed")
        if self.last_at is None or not 0 <= now - self.last_at <= 5:
            raise ValueError("current_signal_unavailable")
        pending = sum(o["state"] in ("pending", "closing") for o in self.orders.values())
        if pending:
            raise ValueError("prior_order_unresolved")
        self.orders[order_id] = {"tier": tier, "state": "pending",
                                 "generation": self.tiers[str(tier)]["generation"]}
        return True

    def confirm_open(self, order_id: str, *, matched_qty: bool) -> None:
        order = self.orders[order_id]
        if order["state"] == "open":
            return
        if order["state"] != "pending" or not matched_qty:
            raise ValueError("entry_final_fills_not_confirmed")
        order["state"] = "open"

    def start_close(self, order_id: str) -> bool:
        order = self.orders[order_id]
        if order["state"] == "closing":
            return False
        if order["state"] != "open":
            raise ValueError("close_requires_confirmed_open")
        order["state"] = "closing"
        self.tiers[str(order["tier"])]["armed"] = False
        self.tiers[str(order["tier"])]["reset_seen"] = False
        return True

    def confirm_closed(self, order_id: str, *, both_legs_final: bool,
                       remaining_qty: D, active_orders: int) -> None:
        order = self.orders[order_id]
        if not both_legs_final or remaining_qty != 0 or active_orders != 0:
            raise ValueError("close_not_reconciled")
        if order["state"] == "closed":
            return
        if order["state"] != "closing":
            raise ValueError("unexpected_close_confirmation")
        order["state"] = "closed"

    def finish_episode(self, *, both_venues_flat: bool, active_orders: int,
                       now: float) -> None:
        if (not both_venues_flat or active_orders != 0
                or any(o["state"] != "closed" for o in self.orders.values())):
            raise ValueError("episode_not_reconciled")
        if (self.review_reason or self.anchor is None or self.last_basis is None
                or self.last_at is None or not 0 <= now - self.last_at <= 5
                or self.signed_distance(self.last_basis, self.anchor, self.direction)
                > self.deviation - self.reset_buffer):
            raise ValueError("episode_waiting_for_real_price_reset")
        self.anchor = self.direction = self.episode_id = None
        self.confirmations.clear()
        self.suspect_since = None
        for t in self.tiers.values():
            t.update(armed=True, reset_seen=False, last_event=None)
            t["generation"] += 1

    def dump(self) -> dict[str, Any]:
        return deepcopy({"schema": self.SCHEMA, "deviation": str(self.deviation),
                "window": self.window, "coverage": str(self.coverage),
                "distances": [str(x) for x in self.distances],
                "max_gap": self.max_gap, "reset_buffer": str(self.reset_buffer),
                "anchor": str(self.anchor) if self.anchor is not None else None,
                "direction": self.direction, "episode_id": self.episode_id,
                "signature": self.signature, "review_reason": self.review_reason,
                "suspect_since": self.suspect_since,
                "orders": self.orders, "tiers": self.tiers})

    @classmethod
    def restore(cls, state: dict[str, Any]) -> FrozenMedianGradient:
        if state.get("schema") != cls.SCHEMA:
            raise ValueError("unsupported_median_state_schema")
        model = cls(deviation_bps=D(state["deviation"]),
                    window_seconds=state["window"], coverage=D(state["coverage"]),
                    max_gap=state["max_gap"], reset_buffer_bps=D(state["reset_buffer"]), distances=state.get("distances"))
        model.anchor = D(state["anchor"]) if state["anchor"] is not None else None
        model.direction, model.episode_id = state["direction"], state["episode_id"]
        model.signature = tuple(state["signature"]) if state["signature"] else None
        model.review_reason = state["review_reason"]
        model.suspect_since = state.get("suspect_since")
        if model.suspect_since is not None and (
            not isinstance(model.suspect_since, (int, float))
            or not isfinite(model.suspect_since)
        ):
            raise ValueError("invalid_anchor_suspicion_time")
        model.orders = {k: dict(v) for k, v in state["orders"].items()}
        model.tiers = {k: dict(v) for k, v in state["tiers"].items()}
        if model.anchor is not None and (
            not model.anchor.is_finite() or not model.episode_id
            or model.direction not in (LONG, SHORT)
        ):
            raise ValueError("invalid_frozen_anchor_state")
        if set(model.tiers) != {str(i) for i in range(1, 6)}:
            raise ValueError("invalid_tier_state")
        for tier in model.tiers.values():
            if (not isinstance(tier.get("armed"), bool)
                    or not isinstance(tier.get("reset_seen"), bool)
                    or type(tier.get("generation")) is not int
                    or tier["generation"] < 0):
                raise ValueError("invalid_tier_state")
        if any(o.get("state") not in ("pending", "open", "closing", "closed")
               or o.get("tier") not in range(1, 6) for o in model.orders.values()):
            raise ValueError("invalid_order_state")
        if model.anchor is None and (
            model.direction is not None or model.episode_id is not None
            or any(o["state"] != "closed" for o in model.orders.values())
        ):
            raise ValueError("active_orders_require_frozen_anchor")
        # Live confirmation evidence is deliberately not restored after a restart.
        return model
