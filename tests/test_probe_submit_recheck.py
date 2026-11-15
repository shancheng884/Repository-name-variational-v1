from __future__ import annotations
import asyncio
from decimal import Decimal as D

from tools.market_capture.config import DEFAULT_ASSETS
from tools.lib.venue_probe import ProbeRound, ProbeRunner, ProbeState

NVDA = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")


def q(bid, ask):
    return {"bid": str(bid), "ask": str(ask), "mid": str((D(str(bid)) + D(str(ask))) / 2),
            "executable_buy": str(ask), "executable_sell": str(bid), "age_s": 0.1,
            "provably_stale": False, "quote_kind": "book"}


class ScriptedHub:
    def __init__(self, first, later):
        self.first = first
        self.later = later
        self.calls = 0

    def latest(self, venue, symbol, *, max_age=10.0):
        self.calls += 1
        return dict(self.first[venue] if self.calls <= 2 else self.later[venue])


class Exec:
    def __init__(self, hub, venue):
        self.hub = hub; self.venue = venue; self.orders = []

    def normalize_qty(self, asset, qty):
        return qty

    async def positions(self, asset):
        return {"ok": True, "verified": True, "qty": "0", "active_orders": 0}

    async def market_order(self, **kwargs):
        side = kwargs["side"]
        quote = self.hub.latest(self.venue, "NVDA")
        price = quote["ask"] if side == "buy" else quote["bid"]
        self.orders.append((self.venue, side, price))
        return {"ok": True, "status": "filled", "filled_qty": str(kwargs["qty"]), "avg_price": price}


def build(tmp_path, first, later, **kwargs):
    hub = ScriptedHub(first, later)
    execs = {"entropy": Exec(hub, "entropy"), "lighter_mainnet": Exec(hub, "lighter_mainnet")}
    runner = ProbeRunner(hub=hub, executors=execs, state=ProbeState(tmp_path / "state.json"),
                         writer_path=tmp_path / "rounds.jsonl", assets=(NVDA,),
                         venues=("entropy", "lighter_mainnet"), horizons=(), **kwargs)
    item = ProbeRound("nvda-entropy-lighter_mainnet-001", "NVDA", "entropy", "lighter_mainnet", 1)
    return runner, execs, item


def test_entry_is_skipped_when_the_edge_closes_before_submit(tmp_path):
    """The decision quote was wide; the submit quote has already converged."""
    decision = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.30", "100.40")}
    submit = {"entropy": q("100.20", "100.30"), "lighter_mainnet": q("100.00", "100.10")}
    runner, execs, item = build(tmp_path, decision, submit, min_edge_bps=3.0)
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "skipped"
    assert row["reason"] == "edge_below_threshold_at_submit"
    assert row["orders_attempted"] is False
    assert execs["entropy"].orders == [] and execs["lighter_mainnet"].orders == []
    assert float(row["signal_edge_bps"]) > 3.0
    assert float(row["submit_edge_bps"]) < 0


def test_entry_still_fires_when_the_submit_quote_holds_the_edge(tmp_path):
    decision = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.30", "100.40")}
    submit = {"entropy": q("99.95", "100.05"), "lighter_mainnet": q("100.35", "100.45")}
    runner, execs, item = build(tmp_path, decision, submit, min_edge_bps=3.0)
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "filled"
    assert execs["entropy"].orders and execs["lighter_mainnet"].orders
