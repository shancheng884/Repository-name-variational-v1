"""The paired order must hit each venue in the direction the ladder decided."""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec
from tools.lib.venue_probe import leverage_from_imf_percent, required_margin_usd
from tools.nvda_tier_live import margin_for_pair, place_pair


class FakeExecutor:
    def __init__(self, name):
        self.name = name
        self.calls = []

    async def market_order(self, **kwargs):
        self.calls.append(kwargs)
        return {"ok": True, "status": "filled", "client_order_id": kwargs["client_order_id"]}


def _pair():
    buy = FakeExecutor("buy")
    sell = FakeExecutor("sell")
    return buy, sell, {"buy": buy, "sell": sell}


def test_opening_buys_the_buy_leg_and_sells_the_sell_leg():
    buy, sell, ex = _pair()
    asyncio.run(place_pair(ex, None, "buy", "sell", Decimal("1"), opening=True, quotes={}))
    assert buy.calls[0]["side"] == "buy"
    assert sell.calls[0]["side"] == "sell"
    assert buy.calls[0]["reduce_only"] is False
    assert sell.calls[0]["reduce_only"] is False


def test_closing_sells_the_buy_leg_and_buys_the_sell_leg():
    buy, sell, ex = _pair()
    asyncio.run(place_pair(ex, None, "buy", "sell", Decimal("1"), opening=False, quotes={}))
    assert buy.calls[0]["side"] == "sell"
    assert sell.calls[0]["side"] == "buy"
    assert buy.calls[0]["reduce_only"] is True
    assert sell.calls[0]["reduce_only"] is True

def test_a_percent_margin_fraction_reads_back_as_leverage():
    assert leverage_from_imf_percent("5.00") == 20.0
    assert leverage_from_imf_percent("50.00") == 2.0
    assert leverage_from_imf_percent(None) is None


def test_required_margin_is_notional_over_leverage_plus_the_buffer():
    assert required_margin_usd(Decimal("20"), Decimal("20"), 25) == Decimal("1.25")
    assert required_margin_usd(Decimal("20"), Decimal("2"), 0) == Decimal("10")
    assert required_margin_usd(Decimal("20"), None, 25) is None


class StateExecutor:
    def __init__(self, state):
        self.state = state

    async def account_state(self, asset):
        return self.state


def test_margin_gate_blocks_the_leg_that_cannot_fund_itself():
    ex = {"rh": StateExecutor({"ok": True, "available_margin": "1.00", "leverage": 20.0}),
          "ent": StateExecutor({"ok": True, "available_margin": "153.43", "leverage": 10.0})}
    gate = asyncio.run(margin_for_pair(ex, None, ("rh", "ent"), Decimal("20"), 25))
    assert gate["ok"] is False
    assert gate["legs"]["rh"]["error"] == "insufficient_margin"
    assert gate["legs"]["rh"]["required_margin"] == "1.25"
    assert gate["legs"]["ent"]["ok"] is True


def test_margin_gate_passes_when_every_leg_has_headroom():
    ex = {"rh": StateExecutor({"ok": True, "available_margin": "153.43", "leverage": 20.0}),
          "ent": StateExecutor({"ok": True, "available_margin": "153.43", "leverage": 10.0})}
    gate = asyncio.run(margin_for_pair(ex, None, ("rh", "ent"), Decimal("20"), 25))
    assert gate["ok"] is True


def test_margin_gate_refuses_an_unreadable_venue():
    ex = {"rh": StateExecutor({"ok": False, "error": "rh_account_unverifiable"})}
    gate = asyncio.run(margin_for_pair(ex, None, ("rh",), Decimal("20"), 25))
    assert gate["ok"] is False
    assert gate["legs"]["rh"]["error"] == "rh_account_unverifiable"


def test_dropping_a_refused_tier_reverses_its_ledger_legs():
    ledger = PositionLedger()
    cfg = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_rh",
                     tiers=(TierSpec(1, 7.0),), notional_usd=20.0, timeout_seconds=None)
    engine = TierEngine(config=cfg, ledger=ledger)
    engine.ledger.add(engine.strategy_id, "NVDA", "entropy", Decimal("0.125"))
    engine.ledger.add(engine.strategy_id, "NVDA", "lighter_rh", Decimal("-0.125"))
    engine.open_tiers[1] = type("T", (), {"qty": 0.125})()
    engine.drop_tier(1)
    assert engine.open_tiers == {}
    assert ledger.net("NVDA", "entropy") == 0
    assert ledger.net("NVDA", "lighter_rh") == 0
