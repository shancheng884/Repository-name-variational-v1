from __future__ import annotations
import asyncio
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from tools.lib.venue_probe import EntropyProbeExecutor, ProbeRunner, ProbeState


class RateLimited(Exception):
    def __str__(self):
        return "(429, None, 'null', None, {})"


class FakeHub:
    def latest(self, venue, symbol, *, max_age=10.0):
        return None


class CountingExec:
    def __init__(self, qty, verified=True):
        self.qty = qty
        self.verified = verified
        self.closed = []

    async def positions(self, asset):
        return {"ok": self.verified, "verified": self.verified,
                "qty": None if not self.verified else str(self.qty), "active_orders": 0}

    def normalize_qty(self, asset, qty):
        return qty

    async def market_order(self, **kwargs):
        self.closed.append(kwargs)
        return {"ok": True, "status": "filled", "filled_qty": str(kwargs.get("qty"))}


def make_runner(tmp_path, executors):
    return ProbeRunner(hub=FakeHub(), executors=executors, state=ProbeState(tmp_path / "state.json"),
                       writer_path=tmp_path / "rounds.jsonl", assets=(), venues=())


def test_unreadable_account_blocks_the_whole_recovery(tmp_path):
    """The venue we cannot read may be the leg that filled.

    Flattening the readable leg then leaves a naked position on the other side,
    which is exactly how a 429 on entropy's user_state left 0.125 NVDA open
    while the lighter leg was closed and the round reported "flattened".
    """
    buy = CountingExec(D("0"), verified=False)
    sell = CountingExec(D("-0.125"))
    runner = make_runner(tmp_path, {"entropy": buy, "lighter_mainnet": sell})
    item = SimpleNamespace(round_id="r1", buy_venue="entropy", sell_venue="lighter_mainnet")
    result = asyncio.run(runner._resolve_unknown_entry(SimpleNamespace(name="NVDA"), item, D("0.125")))
    assert result["decision"] == "unresolved"
    assert buy.closed == [] and sell.closed == []
    assert result["position_qty"]["entropy"] is None


def test_both_legs_verified_keeps_the_existing_hedged_path(tmp_path):
    buy = CountingExec(D("0.125"))
    sell = CountingExec(D("-0.125"))
    runner = make_runner(tmp_path, {"entropy": buy, "lighter_mainnet": sell})
    item = SimpleNamespace(round_id="r1", buy_venue="entropy", sell_venue="lighter_mainnet")
    result = asyncio.run(runner._resolve_unknown_entry(SimpleNamespace(name="NVDA"), item, D("0.125")))
    assert result["decision"] == "both_legs_confirmed"
    assert buy.closed == [] and sell.closed == []


class FakeInfo:
    """Hyperliquid info stub that rate-limits the first calls, then answers."""

    def __init__(self, positions_qty="0", fail_positions=2, fail_fills=1):
        self.positions_qty = positions_qty
        self.fail_positions = fail_positions
        self.fail_fills = fail_fills
        self.cloid_seen = None
        self.fill_poll_calls = 0

    def meta(self, dex):
        return {"universe": [{"name": "io:NVDA", "szDecimals": 3}]}

    def user_state(self, address, dex):
        if self.fail_positions:
            self.fail_positions -= 1
            raise RateLimited()
        return {"assetPositions": [{"position": {"coin": "io:NVDA", "szi": self.positions_qty}}]}

    def open_orders(self, address, dex):
        return []

    def all_mids(self, dex):
        return {"io:NVDA": "159.5"}

    def user_fills_by_time(self, address, start_ms):
        self.fill_poll_calls += 1
        if self.fail_fills:
            self.fail_fills -= 1
            raise RateLimited()
        return [{"cloid": str(self.cloid_seen), "coin": "io:NVDA", "sz": "0.125",
                 "px": "159.5", "fee": "0.001", "time": 1794679373000, "oid": 1}]


class FakeExchange:
    def __init__(self, info):
        self.info = info

    def order(self, coin, is_buy, qty, price, order_type, reduce_only=False, cloid=None):
        self.info.cloid_seen = cloid
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 1}}]}}}


def build_executor(info, exchange):
    return EntropyProbeExecutor(
        info_url="https://api.hyperliquid.xyz/info",
        account_address="0x" + "1" * 40,
        private_key="0x" + "2" * 64,
        dex="io", info=info, exchange=exchange,
    )


def test_positions_survive_a_rate_limited_read():
    info = FakeInfo(positions_qty="0.125", fail_positions=2)
    ex = build_executor(info, FakeExchange(info))
    asyncio.run(ex.start())
    result = asyncio.run(ex.positions(SimpleNamespace(name="NVDA", symbol=lambda v: "io:NVDA")))
    assert result["verified"] is True
    assert result["qty"] == "0.125"
    assert info.fail_positions == 0


def test_fill_poll_survives_a_rate_limited_read():
    info = FakeInfo(fail_positions=0, fail_fills=1)
    ex = build_executor(info, FakeExchange(info))
    asyncio.run(ex.start())
    asset = SimpleNamespace(name="NVDA", symbol=lambda v: "io:NVDA")
    receipt = asyncio.run(ex.market_order(
        asset=asset, side="sell", qty=D("0.125"), quote={},
        client_order_id="probe-test-1", reduce_only=True, max_slippage_bps=D("50"),
    ))
    assert receipt["status"] == "filled"
    assert receipt["filled_qty"] == "0.125"
    assert info.fill_poll_calls >= 2
