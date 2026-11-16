from __future__ import annotations
import asyncio

from tools.market_capture.config import DEFAULT_ASSETS
from tools.lib.venue_probe import ProbeRound, ProbeRunner, ProbeState

NVDA = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")


def q():
    return {"bid": "100.0", "ask": "100.1", "mid": "100.05",
            "executable_buy": "100.1", "executable_sell": "100.0", "age_s": 0.1,
            "provably_stale": False}


class Hub:
    def latest(self, venue, symbol, *, max_age=10.0):
        return q()


class Exec:
    def __init__(self, verified=True, qty="0"):
        self.verified = verified
        self.qty = qty
        self.orders = []

    def normalize_qty(self, asset, value):
        return value

    async def positions(self, asset):
        return {"ok": self.verified, "verified": self.verified,
                "qty": None if not self.verified else self.qty, "active_orders": 0}

    async def market_order(self, **kwargs):
        self.orders.append(kwargs)
        return {"ok": True, "status": "filled", "filled_qty": str(kwargs["qty"]), "avg_price": "100.05"}


def build(tmp_path, entropy, mainnet):
    execs = {"entropy": entropy, "lighter_mainnet": mainnet}
    runner = ProbeRunner(hub=Hub(), executors=execs, state=ProbeState(tmp_path / "state.json"),
                         writer_path=tmp_path / "rounds.jsonl", assets=(NVDA,),
                         venues=("entropy", "lighter_mainnet"), horizons=())
    item = ProbeRound("nvda-entropy-lighter_mainnet-001", "NVDA", "entropy", "lighter_mainnet", 1)
    return runner, execs, item


def test_unverified_account_skips_instead_of_stopping(tmp_path):
    runner, execs, item = build(tmp_path, Exec(verified=False), Exec())
    row = asyncio.run(runner._run_one(item))
    assert row["status"] == "skipped"
    assert row["reason"] == "pre_round_account_unverified"
    assert execs["entropy"].orders == [] and execs["lighter_mainnet"].orders == []


def test_verified_open_position_still_requires_review(tmp_path):
    runner, execs, item = build(tmp_path, Exec(qty="0.5"), Exec())
    row = asyncio.run(runner._run_one(item))
    assert row["status"] == "manual_review_required"
    assert row["reason"] == "pre_round_position_not_flat"
    assert execs["entropy"].orders == []
