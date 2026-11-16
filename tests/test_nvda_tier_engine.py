from __future__ import annotations
from decimal import Decimal as D

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec

CFG = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_mainnet",
                 tiers=tuple(TierSpec(i, t) for i, t in enumerate([7.0, 8.0, 9.0, 10.0, 11.0], start=1)),
                 notional_usd=20.0, timeout_seconds=60.0)


def exchange_from(ledger, asset="NVDA", venues=("entropy", "lighter_mainnet")):
    return {v: ledger.net(asset, v) for v in venues}


def test_two_strategies_net_to_zero_on_a_shared_account():
    ledger = PositionLedger()
    ledger.add("a", "NVDA", "entropy", D("0.125"))
    ledger.add("b", "NVDA", "entropy", D("-0.125"))
    assert ledger.net("NVDA", "entropy") == 0
    assert ledger.reconcile("NVDA", ("entropy",), {"entropy": 0})["ok"] is True


def test_reconcile_flags_a_real_difference():
    ledger = PositionLedger()
    ledger.add("a", "NVDA", "entropy", D("0.125"))
    check = ledger.reconcile("NVDA", ("entropy",), {"entropy": 0})
    assert check["ok"] is False
    assert check["diffs"]["entropy"]["delta"] == "-0.125"


def test_reconcile_treats_an_unreadable_venue_as_not_ok():
    ledger = PositionLedger()
    check = ledger.reconcile("NVDA", ("entropy",), {"entropy": None})
    assert check["ok"] is False
    assert check["diffs"]["entropy"]["error"] == "unreadable"


def test_engine_opens_each_tier_once_and_closes_on_revert():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    exchange = exchange_from(ledger)
    opened = engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                              window=[8.0, 8.0, 8.0], exchange=exchange)
    assert [a["tier"] for a in opened if a["action"] == "open"] == [1, 2]
    again = engine.on_sample(stamp=1, edge=8.0, baseline=0.0, price_hint=160.0,
                             window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    assert again == []
    closed = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                              window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    assert len([a for a in closed if a["action"] == "close"]) == 2
    assert raw_net(ledger) == 0


def raw_net(ledger):
    return ledger.net("NVDA", "entropy") + ledger.net("NVDA", "lighter_mainnet")


def test_engine_times_out_an_unreverted_tier():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                     window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    actions = engine.on_sample(stamp=61, edge=8.0, baseline=0.0, price_hint=160.0,
                               window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    assert any(a["action"] == "close" and a["reason"] == "timeout" for a in actions)


def test_engine_blocks_when_the_account_does_not_match_the_books():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    actions = engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                               window=[8.0, 8.0, 8.0], exchange={"entropy": 5, "lighter_mainnet": 0})
    assert actions and actions[0]["action"] == "blocked"
    assert not engine.open_tiers
