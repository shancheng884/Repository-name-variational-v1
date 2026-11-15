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


class QueueHub:
    """Holds the current quote per venue; tests mutate it between rounds."""

    def __init__(self, entropy, mainnet):
        self.entropy = entropy
        self.mainnet = mainnet

    def latest(self, venue, symbol, *, max_age=10.0):
        return dict(self.entropy if venue == "entropy" else self.mainnet)


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


def build(tmp_path, hub, **kwargs):
    execs = {"entropy": Exec(hub, "entropy"), "lighter_mainnet": Exec(hub, "lighter_mainnet")}
    runner = ProbeRunner(hub=hub, executors=execs, state=ProbeState(tmp_path / "state.json"),
                         writer_path=tmp_path / "rounds.jsonl", assets=(NVDA,),
                         venues=("entropy", "lighter_mainnet"), horizons=(), **kwargs)
    item = ProbeRound("nvda-entropy-lighter_mainnet-001", "NVDA", "entropy", "lighter_mainnet", 1)
    return runner, execs, item


def test_baseline_entry_waits_for_enough_history(tmp_path):
    hub = QueueHub(q("99.9", "100.0"), q("100.0", "100.1"))
    runner, execs, item = build(tmp_path, hub, baseline_entry=True, min_baseline_samples=3,
                                min_edge_bps=1.0)
    first = asyncio.run(runner._execute_round(item))
    second = asyncio.run(runner._execute_round(item))
    assert first["reason"] == "baseline_warming_up"
    assert second["reason"] == "baseline_warming_up"
    third = asyncio.run(runner._execute_round(item))
    assert third["reason"] != "baseline_warming_up"
    assert execs["entropy"].orders == []


def test_baseline_entry_fires_only_when_the_deviation_clears_the_bar(tmp_path):
    # warm-up at a steady +10bps edge, then a jump to +25bps
    warm = [q("99.90", "100.00")]
    main_warm = [q("100.10", "100.20")]
    hub = QueueHub(q("99.90","100.00"), q("100.10","100.20"))
    runner, execs, item = build(tmp_path, hub, baseline_entry=True, min_baseline_samples=2,
                                min_edge_bps=4.0)
    asyncio.run(runner._execute_round(item))
    asyncio.run(runner._execute_round(item))
    hub.entropy = q("99.90", "100.00")
    hub.mainnet = q("100.35", "100.45")
    row = asyncio.run(runner._execute_round(item))
    assert row["status"] == "filled"
    assert (row.get("decision") or {}).get("baseline_bps") is not None
    assert execs["entropy"].orders


def test_round_pnl_reads_the_fills():
    row = {"buy_venue": "entropy", "sell_venue": "lighter_mainnet",
           "entry": {"legs": {"entropy": {"filled_qty": "1", "avg_price": "100.0"},
                              "lighter_mainnet": {"filled_qty": "1", "avg_price": "101.0"}}},
           "exit": {"legs": {"entropy": {"avg_price": "101.0"},
                             "lighter_mainnet": {"avg_price": "100.0"}}}}
    assert ProbeRunner._round_pnl_usd(row) == D("2")


