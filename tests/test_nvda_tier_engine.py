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
    closes = [a for a in closed if a["action"] == "close"]
    assert len(closes) == 2
    # The books only move once the venue confirms the fills.
    assert ledger.net("NVDA", "entropy") != 0
    for action in closes:
        engine.confirm_close(action)
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


def _cfg_with_equity(entropy, lighter, child=20.0):
    return TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_mainnet",
                      tiers=tuple(TierSpec(i, t) for i, t in enumerate([7.0, 8.0, 9.0, 10.0, 11.0], start=1)),
                      child_notional_usd=child, timeout_seconds=None,
                      equity_usd={"entropy": entropy, "lighter_mainnet": lighter})


def test_slot_caps_are_k_times_the_smaller_equity():
    cfg = _cfg_with_equity(190.9, 127.76)
    assert cfg.slot_caps() == (6, 12, 19, 25, 31)
    assert cfg.tier_target_notional_usd(1) == 120
    assert cfg.tier_target_notional_usd(3) == 380
    assert cfg.tier_target_notional_usd(5) == 620


def test_slot_caps_never_exceed_five_times_equity():
    cfg = _cfg_with_equity(1000.0, 1000.0)
    caps = cfg.slot_caps()
    assert caps[-1] * 20 == 5000
    cfg_small = _cfg_with_equity(30.0, 25.0)
    assert cfg_small.slot_caps() == (1, 2, 3, 5, 6)


def test_slot_caps_need_both_venues_verified():
    cfg = _cfg_with_equity(190.0, None)
    assert cfg.slot_caps() is None
    assert cfg.tier_target_notional_usd(1) is None


def test_engine_sizes_each_tier_to_the_cumulative_target():
    ledger = PositionLedger()
    cfg = _cfg_with_equity(200.0, 100.0)
    engine = TierEngine(config=cfg, ledger=ledger)
    actions = engine.on_sample(stamp=0, edge=11.0, baseline=0.0, price_hint=100.0,
                               window=[11.0, 11.0, 11.0], exchange=exchange_from(ledger))
    sizes = {a["tier"]: a["notional_usd"] for a in actions if a["action"] == "open"}
    assert sizes == {1: 100.0, 2: 100.0, 3: 100.0, 4: 100.0, 5: 100.0}
    # 1x equity per tier, 5x total at full ladder
    assert engine._open_notional() == 500.0


def test_engine_tops_out_at_the_hard_cap_when_the_last_tier_overshoots():
    ledger = PositionLedger()
    cfg = _cfg_with_equity(200.0, 105.0)
    engine = TierEngine(config=cfg, ledger=ledger)
    actions = engine.on_sample(stamp=0, edge=11.0, baseline=0.0, price_hint=100.0,
                               window=[11.0, 11.0, 11.0], exchange=exchange_from(ledger))
    sizes = {a["tier"]: a["notional_usd"] for a in actions if a["action"] == "open"}
    assert sum(sizes.values()) == 520.0
    assert sizes[4] == 120.0


def test_unknown_equity_falls_back_to_the_fixed_child_notional():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    actions = engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                               window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    assert all(a["notional_usd"] == 20.0 for a in actions if a["action"] == "open")


def test_entry_is_blocked_while_the_other_direction_owns_the_book():
    ledger = PositionLedger()
    engine = TierEngine(config=_cfg_with_equity(200.0, 100.0), ledger=ledger)
    actions = engine.on_sample(stamp=0, edge=11.0, baseline=0.0, price_hint=100.0,
                               window=[11.0, 11.0, 11.0], exchange=exchange_from(ledger),
                               allow_entry=False)
    assert actions == []
    assert not engine.open_tiers
    assert engine._open_notional() == 0.0


def test_exits_still_run_when_entry_is_locked():
    ledger = PositionLedger()
    engine = TierEngine(config=_cfg_with_equity(200.0, 100.0), ledger=ledger)
    engine.on_sample(stamp=0, edge=11.0, baseline=0.0, price_hint=100.0,
                     window=[11.0, 11.0, 11.0], exchange=exchange_from(ledger))
    assert engine.open_tiers
    actions = engine.on_sample(stamp=5, edge=0.0, baseline=0.0, price_hint=100.0,
                               window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger),
                               allow_entry=False)
    assert len(actions) == 5
    assert all(a["action"] == "close" for a in actions)
    for action in actions:
        engine.confirm_close(action)
    assert not engine.open_tiers
    assert engine._open_notional() == 0.0


def test_two_directions_share_one_ledger_and_net_out():
    ledger = PositionLedger()
    c1 = _cfg_with_equity(200.0, 100.0)
    c2 = TierConfig(asset="NVDA", buy_venue="lighter_mainnet", sell_venue="entropy",
                    tiers=tuple(TierSpec(i, t) for i, t in enumerate([7.0, 8.0, 9.0, 10.0, 11.0], start=1)),
                    child_notional_usd=20.0, timeout_seconds=None,
                    equity_usd={"entropy": 200.0, "lighter_mainnet": 100.0})
    forward = TierEngine(config=c1, ledger=ledger, strategy_id="nvda-forward")
    reverse = TierEngine(config=c2, ledger=ledger, strategy_id="nvda-reverse")
    forward.on_sample(stamp=0, edge=11.0, baseline=0.0, price_hint=100.0,
                      window=[11.0, 11.0, 11.0], exchange=exchange_from(ledger))
    reverse.on_sample(stamp=0, edge=11.0, baseline=0.0, price_hint=100.0,
                      window=[11.0, 11.0, 11.0], exchange=exchange_from(ledger))
    assert ledger.net("NVDA", "entropy") == 0
    assert ledger.net("NVDA", "lighter_mainnet") == 0
    assert ledger.reconcile("NVDA", ("entropy", "lighter_mainnet"), exchange_from(ledger))["ok"] is True


def test_a_close_is_not_booked_and_not_repeated_while_it_waits():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                     window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    first = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                             window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    assert [a["tier"] for a in first if a["action"] == "close"] == [1, 2]
    again = engine.on_sample(stamp=11, edge=0.0, baseline=0.0, price_hint=160.0,
                             window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    assert [a for a in again if a["action"] == "close"] == []
    assert len(engine.open_tiers) == 2


def test_cancel_close_lets_the_same_tier_be_closed_again():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                     window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    actions = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                               window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    engine.cancel_close(actions[0]["tier"])
    retry = engine.on_sample(stamp=12, edge=0.0, baseline=0.0, price_hint=160.0,
                             window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    assert any(a["action"] == "close" and a["tier"] == actions[0]["tier"] for a in retry)


def test_confirm_close_books_the_trade_once():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                     window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    actions = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                               window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    action = actions[0]
    assert engine.confirm_close(action)["tier"] == action["tier"]
    assert len(engine.trades) == 1
    assert engine.confirm_close(action) is None
    assert len(engine.trades) == 1
