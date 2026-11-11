import json
from dataclasses import replace
from decimal import Decimal as D

import pytest

from tools.lib.frozen_median import (
    LONG, SHORT, FrozenMedianGradient, Observation, executable_profit_bps, slot_caps,
)


def observation(t, *, basis=D(10), event=None):
    # Symmetric midpoints make the requested basis exact.
    v = D(2000) * (1 - basis / 20000)
    l = D(2000) * (1 + basis / 20000)
    return Observation(t, t, event or str(t), "ETH:USDC", "20_USDC",
                       v - D("0.1"), v + D("0.1"),
                       l - D("0.01"), l + D("0.01"), t)


def observe(m, t, **kw):
    return m.observe(observation(t, **kw), var_max_age=5, lighter_max_age=2)


def warmed():
    m = FrozenMedianGradient(window_seconds=10, max_gap=2)
    for t in range(10):
        observe(m, t)
    return m


def ready_order():
    m = warmed()
    observe(m, 10, basis=D(17))
    observe(m, 11, basis=D(17))
    m.freeze(episode_id="episode-1", direction=LONG, now=11)
    assert m.reserve(order_id="entry-1", tier=1, capacity=6, existing_slots=0, now=11)
    return m


def test_default_levels_are_six_nine_twelve_eighteen_twentyfour():
    m = warmed()
    assert m.levels(now=9) == tuple(map(D, (6, 9, 12, 18, 24)))


@pytest.mark.parametrize("field,value", [
    ("var_bid", D("NaN")), ("var_ask", D("Infinity")),
    ("lighter_bid", D(0)), ("var_ask", D(1)),
    ("event_id", ""), ("size_key", ""), ("instrument", ""),
    ("source_at", -10), ("source_at", 2), ("lighter_at", -3),
])
def test_invalid_observation_is_not_history(field, value):
    m = FrozenMedianGradient()
    r = replace(observation(1), **{field: value})
    assert not m.observe(r, var_max_age=5, lighter_max_age=2)
    assert not m.history


def test_old_thirty_second_history_cannot_qualify_one_second_window():
    m = FrozenMedianGradient()
    for t in range(0, 3600, 30):
        observe(m, t)
    assert m.rolling_median(now=3599) is None


def test_repeated_same_second_cannot_increase_coverage():
    m = FrozenMedianGradient(window_seconds=10)
    for _ in range(100):
        observe(m, 1)
    assert len(m.history) == 1
    assert m.rolling_median(now=1) is None


def test_stale_or_gap_history_not_ready():
    m = warmed()
    assert m.rolling_median(now=15) is None


def test_anchor_and_distance_remain_frozen_on_median_drift():
    m = ready_order()
    anchor = m.anchor
    for t in range(12, 24):
        observe(m, t, basis=D(30))
    assert m.rolling_median(now=23) == 30
    assert m.anchor == anchor == 10
    assert m.levels(now=23)[0] == 6


def test_quote_spec_change_locks_entries_without_moving_anchor():
    m = ready_order()
    r = replace(observation(12), size_key="50_USDC")
    assert not m.observe(r, var_max_age=5, lighter_max_age=2)
    assert m.review_reason == "median_quote_spec_changed"
    assert m.anchor == 10
    assert m.candidate_tier(direction=LONG, now=12) == 0


def test_quote_spec_change_while_flat_restarts_history():
    m = warmed()
    r = replace(observation(10), size_key="50_USDC")
    assert m.observe(r, var_max_age=5, lighter_max_age=2)
    assert len(m.history) == 1


def test_same_quote_does_not_confirm_twice():
    m = warmed()
    observe(m, 10, basis=D(17), event="same")
    observe(m, 11, basis=D(17), event="same")
    assert m.candidate_tier(direction=LONG, now=11) == 0
    observe(m, 12, basis=D(17), event="new")
    assert m.candidate_tier(direction=LONG, now=12) == 1


def test_latest_observation_must_still_qualify():
    m = warmed()
    observe(m, 10, basis=D(17))
    observe(m, 11, basis=D(17))
    observe(m, 12, basis=D(10))
    assert m.candidate_tier(direction=LONG, now=12) == 0


def test_short_direction_uses_mirrored_distance():
    m = warmed()
    observe(m, 10, basis=D(3))
    observe(m, 11, basis=D(3))
    assert m.candidate_tier(direction=SHORT, now=11) == 1
    assert m.candidate_tier(direction=LONG, now=11) == 0


def test_unknown_submission_does_not_allow_new_order_after_restart():
    m = FrozenMedianGradient.restore(json.loads(json.dumps(ready_order().dump())))
    observe(m, 12, basis=D(17))
    observe(m, 13, basis=D(17))
    assert not m.reserve(order_id="entry-1", tier=1, capacity=6, existing_slots=0, now=13)
    with pytest.raises(ValueError, match="prior_order_unresolved"):
        m.reserve(order_id="entry-2", tier=1, capacity=6, existing_slots=0, now=13)


def test_pending_order_cannot_be_marked_open_without_matched_fills():
    m = ready_order()
    with pytest.raises(ValueError, match="final_fills"):
        m.confirm_open("entry-1", matched_qty=False)
    assert m.orders["entry-1"]["state"] == "pending"


@pytest.mark.parametrize("final,remaining,active", [
    (False, D(0), 0), (True, D("0.001"), 0), (True, D(0), 1),
])
def test_partial_or_unknown_close_cannot_release_slot(final, remaining, active):
    m = ready_order()
    m.confirm_open("entry-1", matched_qty=True)
    assert m.start_close("entry-1")
    assert not m.start_close("entry-1")
    with pytest.raises(ValueError, match="not_reconciled"):
        m.confirm_closed("entry-1", both_legs_final=final,
                         remaining_qty=remaining, active_orders=active)
    assert m.tier_unresolved(1)
    assert not m.tiers["1"]["armed"]


def test_rearm_requires_closed_leg_then_actual_reset_and_new_crossing():
    m = ready_order()
    m.confirm_open("entry-1", matched_qty=True)
    m.start_close("entry-1")
    observe(m, 12, basis=D(10))
    observe(m, 13, basis=D(17))
    assert not m.tiers["1"]["armed"]
    m.confirm_closed("entry-1", both_legs_final=True, remaining_qty=D(0), active_orders=0)
    observe(m, 14, basis=D(17))
    # Reset seen while closing is allowed, but cannot release an unresolved leg.
    assert m.tiers["1"]["armed"]
    assert m.tiers["1"]["generation"] == 1
    observe(m, 15, basis=D(17))
    assert m.tiers["1"]["generation"] == 1


def test_capacity_counts_existing_reservations_and_five_times_ceiling():
    assert slot_caps(D("139.984725"), D("131.614007")) == [6, 13, 19, 26, 32]
    m = warmed()
    observe(m, 10, basis=D(17))
    observe(m, 11, basis=D(17))
    m.freeze(episode_id="ep", direction=LONG, now=11)
    with pytest.raises(ValueError, match="capacity_unavailable"):
        m.reserve(order_id="x", tier=1, capacity=6, existing_slots=6, now=11)


def test_unconfirmed_upper_tier_cannot_reserve():
    m = warmed()
    observe(m, 10, basis=D(17))
    observe(m, 11, basis=D(17))
    m.freeze(episode_id="ep", direction=LONG, now=11)
    with pytest.raises(ValueError, match="tier_not_confirmed"):
        m.reserve(order_id="x", tier=5, capacity=32, existing_slots=0, now=11)


def test_restore_preserves_anchor_but_not_live_confirmation():
    m = ready_order()
    r = FrozenMedianGradient.restore(json.loads(json.dumps(m.dump())))
    assert r.anchor == m.anchor
    assert r.orders == m.orders
    assert r.candidate_tier(direction=LONG, now=11) == 0
    r.orders["entry-1"]["state"] = "open"
    assert m.orders["entry-1"]["state"] == "pending"


def test_dump_does_not_alias_live_orders():
    m = ready_order()
    state = m.dump()
    state["orders"]["entry-1"]["state"] = "closed"
    assert m.orders["entry-1"]["state"] == "pending"


@pytest.mark.parametrize("direction,v_entry,l_entry,v_exit,l_exit", [
    (LONG, "2000", "2002", "2001", "2002"),
    (SHORT, "2002", "2000", "2002", "2001"),
])
def test_executable_prices_charge_remaining_reserve_only(direction,v_entry,l_entry,v_exit,l_exit):
    result = executable_profit_bps(direction=direction, var_entry=D(v_entry),
        lighter_entry=D(l_entry), var_exit=D(v_exit), lighter_exit=D(l_exit),
        remaining_reserve_bps=D("0.5"))
    assert result == D(1) / D(2001) * 10000 - D("0.5")


def test_invalid_state_schema_fails_closed():
    with pytest.raises(ValueError, match="schema"):
        FrozenMedianGradient.restore({"schema": 999})


def test_invalid_cost_values_fail_closed():
    with pytest.raises(ValueError, match="reserve"):
        executable_profit_bps(direction=LONG, var_entry=D(1), lighter_entry=D(1),
            var_exit=D(1), lighter_exit=D(1), remaining_reserve_bps=D(-1))


def test_episode_cannot_finish_while_order_pending():
    m = ready_order()
    with pytest.raises(ValueError, match="not_reconciled"):
        m.finish_episode(both_venues_flat=True, active_orders=0, now=11)
