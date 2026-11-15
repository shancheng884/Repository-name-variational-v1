from __future__ import annotations
import pytest

from tools.market_capture.config import DEFAULT_ASSETS
from tools.lib.venue_probe import enumerate_probe_plan


def assets(*names):
    return tuple(a for a in DEFAULT_ASSETS if a.name in names)


def test_targeted_plan_contains_only_the_requested_pairs():
    pairs = [("SNDK", "entropy", "lighter_rh"), ("NVDA", "entropy", "lighter_mainnet")]
    plan = enumerate_probe_plan(assets("SNDK", "NVDA"), ("entropy", "lighter_rh", "lighter_mainnet"),
                                pairs=pairs, rounds_per_pair=3)
    assert len(plan) == 6
    assert {(r.asset, r.buy_venue, r.sell_venue) for r in plan} == set(pairs)
    assert [(r.asset, r.buy_venue, r.sell_venue) for r in plan[:2]] == pairs


def test_targeted_plan_round_robins_instead_of_stacking():
    pairs = [("SNDK", "entropy", "lighter_rh"), ("SNDK", "lighter_rh", "entropy")]
    plan = enumerate_probe_plan(assets("SNDK"), ("entropy", "lighter_rh"),
                                pairs=pairs, rounds_per_pair=4)
    got = [(r.buy_venue, r.sell_venue) for r in plan]
    assert got == [("entropy", "lighter_rh"), ("lighter_rh", "entropy")] * 4


def test_targeted_plan_ids_stay_unique():
    pairs = [("SNDK", "entropy", "lighter_rh")]
    plan = enumerate_probe_plan(assets("SNDK"), ("entropy", "lighter_rh"),
                                pairs=pairs, rounds_per_pair=5)
    assert len({r.round_id for r in plan}) == 5
    assert [r.sequence for r in plan] == [1, 2, 3, 4, 5]


def test_default_plan_is_unchanged_without_pairs():
    plan = enumerate_probe_plan(assets("SNDK"), ("entropy", "lighter_rh"))
    assert {(r.buy_venue, r.sell_venue) for r in plan} == {("entropy", "lighter_rh"), ("lighter_rh", "entropy")}


def test_pair_parser_rejects_an_asset_that_was_not_selected():
    from tools.venue_probe import _pairs
    with pytest.raises(SystemExit):
        _pairs("QQQ:entropy>lighter_rh", assets("SNDK"), ("entropy", "lighter_rh"))


def test_pair_parser_rejects_an_unsupported_venue():
    from tools.venue_probe import _pairs
    with pytest.raises(SystemExit):
        _pairs("QQQ:entropy>lighter_rh", assets("QQQ"), ("entropy", "lighter_rh"))


def test_pair_parser_rejects_the_same_venue_twice():
    from tools.venue_probe import _pairs
    with pytest.raises(SystemExit):
        _pairs("SNDK:entropy>entropy", assets("SNDK"), ("entropy",))


def test_pair_parser_accepts_a_valid_list():
    from tools.venue_probe import _pairs
    got, thresholds = _pairs("SNDK:entropy>lighter_rh, NVDA:lighter_mainnet>entropy",
                             assets("SNDK", "NVDA"), ("entropy", "lighter_rh", "lighter_mainnet"))
    assert got == [("SNDK", "entropy", "lighter_rh"), ("NVDA", "lighter_mainnet", "entropy")]
    assert thresholds == {}


