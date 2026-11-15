from __future__ import annotations
import asyncio
from decimal import Decimal as D

from tools.market_capture.config import DEFAULT_ASSETS
from tools.venue_probe import _pairs
from tools.lib.venue_probe import ProbeRound, ProbeRunner, ProbeState

NVDA = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")


def assets(*names):
    return tuple(a for a in DEFAULT_ASSETS if a.name in names)


def test_pair_parser_reads_a_per_pair_threshold():
    pairs, thresholds = _pairs("SNDK:entropy>lighter_rh@4.07,NVDA:entropy>lighter_mainnet",
                               assets("SNDK", "NVDA"), ("entropy", "lighter_rh", "lighter_mainnet"))
    assert pairs[0] == ("SNDK", "entropy", "lighter_rh")
    assert thresholds == {("SNDK", "entropy", "lighter_rh"): 4.07}
    assert len(pairs) == 2


def test_pair_parser_rejects_a_bad_threshold():
    import pytest
    with pytest.raises(SystemExit):
        _pairs("SNDK:entropy>lighter_rh@abc", assets("SNDK"), ("entropy", "lighter_rh"))


def q(bid, ask):
    return {"bid": str(bid), "ask": str(ask), "mid": str((D(str(bid)) + D(str(ask))) / 2),
            "executable_buy": str(ask), "executable_sell": str(bid), "age_s": 0.1,
            "provably_stale": False, "quote_kind": "book"}


class Hub:
    def __init__(self, quotes):
        self.quotes = quotes

    def latest(self, venue, symbol, *, max_age=10.0):
        return dict(self.quotes[venue])


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


def build(tmp_path, quotes, **kwargs):
    hub = Hub(quotes)
    execs = {"entropy": Exec(hub, "entropy"), "lighter_mainnet": Exec(hub, "lighter_mainnet")}
    runner = ProbeRunner(hub=hub, executors=execs, state=ProbeState(tmp_path / "state.json"),
                         writer_path=tmp_path / "rounds.jsonl", assets=(NVDA,),
                         venues=("entropy", "lighter_mainnet"), horizons=(), **kwargs)
    item = ProbeRound("nvda-entropy-lighter_mainnet-001", "NVDA", "entropy", "lighter_mainnet", 1)
    return runner, execs, item


def test_per_pair_threshold_overrides_the_global_one(tmp_path):
    quotes = {"entropy": q("99.90", "100.00"), "lighter_mainnet": q("100.20", "100.30")}
    runner, execs, item = build(tmp_path, quotes, min_edge_bps=0.5,
                                min_edge_by_pair={("NVDA", "entropy", "lighter_mainnet"): D("25")})
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "skipped"
    assert row["threshold_bps"] == "25"
    assert execs["entropy"].orders == []
