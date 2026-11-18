from __future__ import annotations
from decimal import Decimal as D

from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec

CFG = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_mainnet",
                 tiers=tuple(TierSpec(i, t) for i, t in enumerate([7.0, 8.0, 9.0, 10.0, 11.0], start=1)),
                 notional_usd=20.0, timeout_seconds=60.0)


def exchange_from(ledger, asset="NVDA", venues=("entropy", "lighter_mainnet")):
    return {v: ledger.net(asset, v) for v in venues}


def open_one(engine, ledger, *, stamp=0, edge=8.0, baseline=0.0, price_hint=160.0):
    """Propose, then book, exactly one tier the way the runner does."""
    actions = engine.on_sample(stamp=stamp, edge=edge, baseline=baseline, price_hint=price_hint,
                               window=[edge, edge, edge], exchange=exchange_from(ledger))
    opens = [a for a in actions if a["action"] == "open"]
    assert len(opens) == 1
    engine.confirm_open(opens[0], qty=D(str(opens[0]["qty"])))
    return opens[0]


def test_two_strategies_net_to_zero_on_a_shared_account():
    ledger = PositionLedger()
    ledger.add("a", "NVDA", "entropy", D("0.125"))
    ledger.add("b", "NVDA", "entropy", D("-0.125"))
    assert ledger.net("NVDA", "entropy") == 0
    assert ledger.reconcile("NVDA", ("entropy",), {"entropy": 0})["ok"] is True


def test_ledger_accepts_an_asset_object_instead_of_a_name():
    """Regression: passing the asset object used to read the expected size as zero."""
    class Asset:
        name = "NVDA"

    ledger = PositionLedger()
    ledger.add("a", Asset(), "entropy", D("0.9"))
    assert ledger.net("NVDA", "entropy") == D("0.9")
    assert ledger.net(Asset(), "entropy") == D("0.9")
    check = ledger.reconcile(Asset(), ("entropy",), {"entropy": D("0.9")})
    assert check["ok"] is True


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


def test_a_tier_is_not_booked_before_it_is_confirmed():
    """Regression: five tiers used to reach the ledger before a single order was sent."""
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    actions = engine.on_sample(stamp=0, edge=8.0, baseline=0.0, price_hint=160.0,
                               window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    assert [a["tier"] for a in actions if a["action"] == "open"] == [1]
    assert engine.open_tiers == {}
    assert ledger.net("NVDA", "entropy") == 0
    assert ledger.net("NVDA", "lighter_mainnet") == 0


def test_the_engine_offers_one_tier_per_sample():
    """A burst through every threshold must not send five orders at once."""
    ledger = PositionLedger()
    cfg = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_mainnet",
                     tiers=tuple(TierSpec(i, t) for i, t in enumerate([1, 2, 3, 4, 5], start=1)),
                     notional_usd=20.0, timeout_seconds=None)
    engine = TierEngine(config=cfg, ledger=ledger)
    first = engine.on_sample(stamp=0, edge=9.0, baseline=0.0, price_hint=100.0,
                             window=[9.0, 9.0, 9.0], exchange=exchange_from(ledger))
    assert len([a for a in first if a["action"] == "open"]) == 1
    assert first[0]["notional_usd"] == 20.0


def test_confirm_open_books_the_tier_once():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    action = open_one(engine, ledger)
    assert engine.open_tiers[1].qty == action["qty"]
    assert ledger.net("NVDA", "entropy") > 0
    assert engine.confirm_open(action, qty=action["qty"]) is None
    assert len(engine.open_tiers) == 1


def test_engine_opens_each_tier_once_then_closes_on_revert():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    open_one(engine, ledger, edge=8.0)
    open_one(engine, ledger, stamp=1, edge=8.0)
    assert sorted(engine.open_tiers) == [1, 2]
    again = engine.on_sample(stamp=2, edge=8.0, baseline=0.0, price_hint=160.0,
                             window=[8.0, 8.0, 8.0], exchange=exchange_from(ledger))
    assert [a for a in again if a["action"] == "open"] == []
    closed = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                              window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    closes = [a for a in closed if a["action"] == "close"]
    assert len(closes) == 2
    # The books only move once the venue confirms the closing fills.
    assert ledger.net("NVDA", "entropy") != 0
    for action in closes:
        engine.confirm_close(action)
    assert raw_net(ledger) == 0


def raw_net(ledger):
    return ledger.net("NVDA", "entropy") + ledger.net("NVDA", "lighter_mainnet")


def test_engine_times_out_an_unreverted_tier():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    open_one(engine, ledger, edge=8.0)
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
    sizes = []
    for index in range(25):
        action = open_one(engine, ledger, stamp=index, edge=11.0, price_hint=100.0)
        sizes.append(action["notional_usd"])
    # 1x equity per tier, 5x total at full ladder
    assert sizes == [20.0] * 25
    assert len(engine.open_tiers) == 5
    assert engine._open_notional() == 500.0


def test_engine_tops_out_at_the_hard_cap_when_the_last_tier_overshoots():
    ledger = PositionLedger()
    cfg = _cfg_with_equity(200.0, 105.0)
    engine = TierEngine(config=cfg, ledger=ledger)
    sizes = []
    for index in range(26):
        sizes.append(open_one(engine, ledger, stamp=index, edge=11.0, price_hint=100.0)["notional_usd"])
    assert sum(sizes) == 520.0
    assert engine._open_notional() == 520.0
    assert sizes[-1] == 20.0
    assert all(size <= 20 for size in sizes)


def test_unknown_equity_falls_back_to_the_fixed_child_notional():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    action = open_one(engine, ledger, edge=8.0)
    assert action["notional_usd"] == 20.0


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
    for index in range(25):
        open_one(engine, ledger, stamp=index, edge=11.0, price_hint=100.0)
    actions = engine.on_sample(stamp=6, edge=0.0, baseline=0.0, price_hint=100.0,
                               window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger),
                               allow_entry=False)
    assert len(actions) == 5
    assert all(a["action"] == "close" for a in actions)
    for step in range(5):
        if step:
            actions = engine.on_sample(stamp=30 + step, edge=0, baseline=0,
                                       price_hint=100, window=[0, 0, 0],
                                       exchange=exchange_from(ledger), allow_entry=False)
        for action in actions:
            assert action["notional_usd"] <= 20
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
    open_one(forward, ledger, edge=11.0, price_hint=100.0)
    open_one(reverse, ledger, edge=11.0, price_hint=100.0)
    assert ledger.net("NVDA", "entropy") == 0
    assert ledger.net("NVDA", "lighter_mainnet") == 0
    assert ledger.reconcile("NVDA", ("entropy", "lighter_mainnet"), exchange_from(ledger))["ok"] is True


def test_a_close_is_not_booked_and_not_repeated_while_it_waits():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    open_one(engine, ledger, edge=8.0)
    first = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                             window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    assert [a["tier"] for a in first if a["action"] == "close"] == [1]
    again = engine.on_sample(stamp=11, edge=0.0, baseline=0.0, price_hint=160.0,
                             window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    assert [a for a in again if a["action"] == "close"] == []
    assert len(engine.open_tiers) == 1


def test_cancel_close_lets_the_same_tier_be_closed_again():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    open_one(engine, ledger, edge=8.0)
    actions = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                               window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    engine.cancel_close(actions[0]["tier"])
    retry = engine.on_sample(stamp=12, edge=0.0, baseline=0.0, price_hint=160.0,
                             window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    assert any(a["action"] == "close" and a["tier"] == actions[0]["tier"] for a in retry)


def test_confirm_close_books_the_trade_once():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    open_one(engine, ledger, edge=8.0)
    actions = engine.on_sample(stamp=10, edge=0.0, baseline=0.0, price_hint=160.0,
                               window=[0.0, 0.0, 0.0], exchange=exchange_from(ledger))
    action = actions[0]
    assert engine.confirm_close(action)["tier"] == action["tier"]
    assert len(engine.trades) == 1
    assert engine.confirm_close(action) is None
    assert len(engine.trades) == 1


def test_engine_state_restores_tiers_and_ledger():
    """Regression: a restart used to come back with an empty book."""
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    open_one(engine, ledger, edge=9.5)
    saved = engine.dump_state()

    fresh_ledger = PositionLedger()
    revived = TierEngine(config=CFG, ledger=fresh_ledger)
    revived.restore_state(saved)
    assert sorted(revived.open_tiers) == [1]
    assert revived.open_tiers[1].frozen_baseline == 0.0
    assert fresh_ledger.net("NVDA", "entropy") == D(str(engine.open_tiers[1].qty))
    assert fresh_ledger.net("NVDA", "lighter_mainnet") == -D(str(engine.open_tiers[1].qty))


def test_pending_child_reserves_the_capacity_without_booking_a_fill():
    ledger = PositionLedger()
    engine = TierEngine(config=_cfg_with_equity(200, 100), ledger=ledger)
    kwargs = dict(stamp=0, edge=10, baseline=0, price_hint=100,
                  window=[10, 10], exchange=exchange_from(ledger))
    action = engine.on_sample(**kwargs)[0]
    assert action["notional_usd"] == 20
    assert engine.on_sample(**kwargs) == []
    assert ledger.net("NVDA", "entropy") == 0
    engine.cancel_open(action)
    assert engine.on_sample(**kwargs)


def test_old_confirmations_do_not_open_after_the_current_edge_falls_back():
    engine = TierEngine(config=CFG, ledger=PositionLedger())
    actions = engine.on_sample(stamp=0, edge=1, baseline=0, price_hint=100,
                               window=[10, 10, 1], exchange={"entropy": 0, "lighter_mainnet": 0})
    assert actions == []


def test_children_preserve_the_first_tier_anchor_and_charge_each_entry_fee_once():
    ledger = PositionLedger()
    engine = TierEngine(config=_cfg_with_equity(200, 100), ledger=ledger)
    a = engine.on_sample(stamp=0, edge=10, baseline=1, price_hint=100,
                         window=[9, 9], exchange=exchange_from(ledger))[0]
    engine.confirm_open(a, entry_prices={"entropy": 100, "lighter_mainnet": 101}, fee_usd=.01)
    b = engine.on_sample(stamp=1, edge=12, baseline=2, price_hint=100,
                         window=[10, 10], exchange=exchange_from(ledger))[0]
    engine.confirm_open(b, entry_prices={"entropy": 102, "lighter_mainnet": 103}, fee_usd=.02)
    assert engine.open_tiers[1].frozen_baseline == 1
    assert engine.open_tiers[1].notional_usd == 40
    close = engine.on_sample(stamp=2, edge=1, baseline=2, price_hint=100,
                             window=[0, 0], exchange=exchange_from(ledger))[0]
    assert close["entry_fee_usd"] == .01
    assert close["entry_buy_price"] == 100
    engine.confirm_close(close)
    assert engine.confirm_close(close) is None
    assert engine.open_tiers[1].notional_usd == 20
    assert engine.open_tiers[1].entry_fee_usd == .02
    assert ledger.net("NVDA", "entropy") == D("0.2")


def test_legacy_large_tier_is_unwound_in_children_after_restore():
    ledger = PositionLedger()
    engine = TierEngine(config=_cfg_with_equity(200, 100), ledger=ledger)
    engine.restore_state({"open_tiers": [{"index": 1, "qty": 1, "notional_usd": 100,
                                         "frozen_baseline": 0, "entry_fee_usd": .05}]})
    for i in range(5):
        action = engine.on_sample(stamp=i, edge=0, baseline=0, price_hint=100,
                                  window=[0, 0], exchange=exchange_from(ledger))[0]
        assert action["notional_usd"] <= 20.00000001
        engine.confirm_close(action)
    assert not engine.open_tiers
    assert abs(ledger.net("NVDA", "entropy")) < D("0.0000001")


def test_pending_child_survives_restart_and_prevents_a_duplicate():
    ledger = PositionLedger()
    engine = TierEngine(config=CFG, ledger=ledger)
    engine.on_sample(stamp=0, edge=10, baseline=0, price_hint=100,
                     window=[10, 10], exchange=exchange_from(ledger))
    other = TierEngine(config=CFG, ledger=PositionLedger())
    other.restore_state(engine.dump_state())
    assert other.pending_action == engine.pending_action
    assert other.on_sample(stamp=1, edge=10, baseline=0, price_hint=100,
                           window=[10, 10], exchange=exchange_from(ledger)) == []

def _cfg_exit_gate(exit_cost=5.14, target=1.0):
    return TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_mainnet",
                      tiers=(TierSpec(1, 6.14),), notional_usd=20.0, child_notional_usd=20.0,
                      timeout_seconds=None, exit_cost_bps=exit_cost,
                      profit_target_bps=target)


def test_a_reverted_spread_is_not_enough_when_exit_costs_are_locked():
    """The new exit needs convergence that pays P80 slippage plus the target."""
    ledger = PositionLedger()
    engine = TierEngine(config=_cfg_exit_gate(), ledger=ledger)
    first = engine.on_sample(stamp=0, edge=12.0, baseline=5.0, price_hint=160.0,
                             window=[12.0, 12.0, 12.0], exchange=exchange_from(ledger))
    action = [a for a in first if a["action"] == "open"][0]
    engine.confirm_open(action, qty=D(str(action["qty"])))
    # Only 4.8 bps of convergence: the exit cost is not covered yet.
    early = engine.on_sample(stamp=1, edge=7.2, baseline=5.0, price_hint=160.0,
                             window=[7.2, 7.2, 7.2], exchange=exchange_from(ledger))
    assert [a for a in early if a["action"] == "close"] == []
    # 6.5 bps of convergence pays 5.14 exit slippage plus 1 bps profit, even
    # though the spread has not fully reverted to the anchor.
    paid = engine.on_sample(stamp=2, edge=5.5, baseline=5.0, price_hint=160.0,
                            window=[5.5, 5.5, 5.5], exchange=exchange_from(ledger))
    closes = [a for a in paid if a["action"] == "close"]
    assert len(closes) == 1
    assert closes[0]["reason"] == "target"
    assert round(closes[0]["net_bps"], 2) == 1.36


def test_fixed_margin_150_gives_the_same_ladder_on_both_venues():
    cfg = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_rh",
                     tiers=tuple(TierSpec(i, 6.0 + i) for i in range(1, 6)),
                     child_notional_usd=20.0, max_equity_multiple=5.0,
                     equity_usd={"entropy": 150.0, "lighter_rh": 150.0})
    assert cfg.slot_caps() == (7, 15, 22, 30, 37)
    assert [cfg.tier_target_notional_usd(i) for i in range(1, 6)] == [140.0, 300.0, 440.0, 600.0, 740.0]


def test_exit_is_gated_on_the_realized_entry_edge_not_the_signal():
    """A bad entry fill cannot be closed on convergence the signal only imagined."""
    ledger = PositionLedger()
    engine = TierEngine(config=_cfg_exit_gate(), ledger=ledger)
    first = engine.on_sample(stamp=0, edge=12.0, baseline=5.0, price_hint=100.0,
                             window=[12.0, 12.0, 12.0], exchange=exchange_from(ledger))
    action = [a for a in first if a["action"] == "open"][0]
    # Signal said 12 bps, the two fills only delivered 2 bps.
    engine.confirm_open(action, qty=D(str(action["qty"])),
                        entry_prices={"entropy": 100.00, "lighter_mainnet": 100.02})
    assert abs(engine.open_tiers[1].entry_realized_edge - 2.0) < 0.01
    # Signal convergence is 6.5 bps, but from the real fills it is -3.5 bps.
    actions = engine.on_sample(stamp=1, edge=5.5, baseline=5.0, price_hint=100.0,
                               window=[5.5, 5.5, 5.5], exchange=exchange_from(ledger))
    assert [a for a in actions if a["action"] == "close"] == []
