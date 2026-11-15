from __future__ import annotations
import asyncio
from decimal import Decimal as D

import pytest

from tools.market_capture.config import AVAILABLE_ASSETS, DEFAULT_ASSETS
from tools.lib.venue_probe import ProbeRound, ProbeRunner, ProbeState

NVDA = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")


def q(bid, ask):
    return {"bid": str(bid), "ask": str(ask), "mid": str((D(str(bid)) + D(str(ask))) / 2),
            "executable_buy": str(ask), "executable_sell": str(bid), "age_s": 0.1,
            "provably_stale": False, "quote_kind": "book"}


class ScriptedHub:
    """Serves `first` for the first N reads, then `later` forever."""

    def __init__(self, first, later=None, switch_after=2):
        self.first = first
        self.later = later or first
        self.switch_after = switch_after
        self.calls = 0

    def latest(self, venue, symbol, *, max_age=10.0):
        self.calls += 1
        src = self.first if self.calls <= self.switch_after else self.later
        return dict(src[venue])


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


def build(tmp_path, first, later=None, switch_after=2, **kwargs):
    hub = ScriptedHub(first, later, switch_after)
    execs = {"entropy": Exec(hub, "entropy"), "lighter_mainnet": Exec(hub, "lighter_mainnet")}
    runner = ProbeRunner(hub=hub, executors=execs, state=ProbeState(tmp_path / "state.json"),
                         writer_path=tmp_path / "rounds.jsonl", assets=(NVDA,),
                         venues=("entropy", "lighter_mainnet"), horizons=(), **kwargs)
    item = ProbeRound("nvda-entropy-lighter_mainnet-001", "NVDA", "entropy", "lighter_mainnet", 1)
    return runner, execs, item


def test_entry_is_skipped_when_the_edge_is_below_the_bar(tmp_path):
    quotes = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.00", "100.10")}
    runner, execs, item = build(tmp_path, quotes, min_edge_bps=2.0)
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "skipped"
    assert row["reason"] == "edge_below_threshold"
    assert row["orders_attempted"] is False
    assert execs["entropy"].orders == [] and execs["lighter_mainnet"].orders == []


def test_entry_fires_above_the_bar(tmp_path):
    quotes = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.20", "100.30")}
    runner, execs, item = build(tmp_path, quotes, min_edge_bps=2.0)
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "filled"
    assert len(execs["entropy"].orders) >= 1 and len(execs["lighter_mainnet"].orders) >= 1


def test_exit_waits_for_the_edge_to_revert(tmp_path):
    entry = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.20", "100.30")}
    after = {"entropy": q("100.20", "100.30"), "lighter_mainnet": q("99.90", "100.00")}
    runner, execs, item = build(tmp_path, entry, after, switch_after=4, min_edge_bps=2.0,
                                exit_on_revert=True, exit_poll_seconds=0.5, max_hold_seconds=10.0)
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "filled"
    assert row["exit"]["reason"] == "reverted"
    assert D(str(row["exit"]["edge_at_exit_bps"])) <= 0
    assert row["exit"]["hold_seconds"] < 10.0


def test_exit_gives_up_at_the_deadline_and_still_flattens(tmp_path):
    entry = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.20", "100.30")}
    runner, execs, item = build(tmp_path, entry, switch_after=4, min_edge_bps=2.0,
                                exit_on_revert=True, exit_poll_seconds=0.5, max_hold_seconds=5.0)
    row = asyncio.run(runner._execute_round(item))
    assert row["exit"]["reason"] == "deadline"
    assert row["status"] == "filled"
    assert row["exit"]["hold_seconds"] >= 5.0


def test_without_the_flags_behaviour_is_unchanged(tmp_path):
    quotes = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.20", "100.30")}
    runner, execs, item = build(tmp_path, quotes)
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "filled"
    assert row["exit"]["reason"] == "immediate"
    assert row["exit"]["hold_seconds"] < 2.0
