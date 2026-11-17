"""The paired order must hit each venue in the direction the ladder decided."""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec
from tools.lib.venue_probe import best_bid_ask, leverage_from_imf_percent
from tools.nvda_tier_live import place_pair, position_qty, settle_positions


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


class BookExecutor:
    """A venue whose position only moves when a reduce-only order is filled."""

    def __init__(self, qty, fill=True):
        self.qty = Decimal(str(qty))
        self.fill = fill
        self.orders = []

    async def positions(self, asset):
        return {"ok": True, "verified": True, "qty": str(self.qty), "active_orders": 0}

    async def market_order(self, **kwargs):
        self.orders.append(kwargs)
        if not self.fill:
            return {"ok": False, "status": "rejected"}
        delta = Decimal(str(kwargs["qty"]))
        self.qty += -delta if kwargs["side"] == "sell" else delta
        return {"ok": True, "status": "filled"}


def test_settle_closes_a_residual_with_a_reduce_only_order():
    ex = {"rh": BookExecutor("0"), "ent": BookExecutor("0.897")}
    result = asyncio.run(settle_positions(ex, None, ("rh", "ent"),
                                          {"rh": Decimal("0"), "ent": Decimal("0")}))
    assert result["ok"] is True
    assert ex["ent"].orders[0]["side"] == "sell"
    assert ex["ent"].orders[0]["reduce_only"] is True
    assert ex["ent"].qty == 0
    assert ex["rh"].orders == []


def test_settle_reports_a_leg_it_cannot_correct():
    ex = {"rh": BookExecutor("0"), "ent": BookExecutor("0.897", fill=False)}
    result = asyncio.run(settle_positions(ex, None, ("rh", "ent"),
                                          {"rh": Decimal("0"), "ent": Decimal("0")}, attempts=1))
    assert result["ok"] is False
    assert result["error"] == "position_mismatch"
    assert result["legs"]["ent"]["residual"] != "0"


def test_settle_refuses_to_treat_an_unreadable_venue_as_flat():
    class Blind:
        async def positions(self, asset):
            return {"ok": False, "verified": False, "error": "unreachable"}

    result = asyncio.run(settle_positions({"rh": Blind()}, None, ("rh",),
                                          {"rh": Decimal("0")}, attempts=1))
    assert result["ok"] is False
    assert result["error"] == "position_unreadable"


def test_position_qty_needs_a_verified_read():
    class Blind:
        async def positions(self, asset):
            return {"ok": False, "verified": False}

    assert asyncio.run(position_qty({"rh": Blind()}, None, "rh")) is None
    assert asyncio.run(position_qty({"rh": BookExecutor("1.5")}, None, "rh")) == Decimal("1.5")


def test_best_bid_ask_uses_the_outermost_prices():
    book = {"bids": [{"price": "154.90"}, {"price": "154.95"}],
            "asks": [{"price": "155.00"}, {"price": "154.98"}]}
    assert best_bid_ask(book) == (Decimal("154.95"), Decimal("154.98"))
    assert best_bid_ask({}) == (None, None)
    assert best_bid_ask({"bids": [{"price": "x"}], "asks": []}) == (None, None)
