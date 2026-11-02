from decimal import Decimal

from tools.lib.quote_allocation import (
    ExitCandidate,
    allocated_lots,
    exit_candidate_priority,
    rank_candidates,
    recently_refreshed,
)


def test_higher_pnl_wins_when_nobody_was_recently_refreshed():
    ranked = rank_candidates([
        ExitCandidate("a", Decimal("2"), waited_seconds=10),
        ExitCandidate("b", Decimal("5"), waited_seconds=10),
    ])
    assert [item.lot_id for item in ranked] == ["b", "a"]


def test_a_lot_quoted_inside_the_cooldown_yields_the_slot():
    ranked = rank_candidates([
        ExitCandidate("fresh", Decimal("9"), seconds_since_refresh=5),
        ExitCandidate("waiting", Decimal("4"), seconds_since_refresh=600),
    ])
    assert [item.lot_id for item in ranked] == ["waiting", "fresh"]


def test_starved_lot_is_promoted_ahead_of_another_recently_quoted_lot():
    ranked = rank_candidates([
        ExitCandidate("starved", Decimal("1"), waited_seconds=300,
                      seconds_since_refresh=5),
        ExitCandidate("quoted", Decimal("8"), waited_seconds=10,
                      seconds_since_refresh=5),
    ])
    assert [item.lot_id for item in ranked] == ["starved", "quoted"]


def test_between_eligible_lots_the_higher_pnl_still_wins():
    ranked = rank_candidates([
        ExitCandidate("starved", Decimal("1"), waited_seconds=300,
                      seconds_since_refresh=600),
        ExitCandidate("other", Decimal("8"), waited_seconds=10,
                      seconds_since_refresh=600),
    ])
    assert [item.lot_id for item in ranked] == ["other", "starved"]


def test_forced_exit_always_goes_first():
    ranked = rank_candidates([
        ExitCandidate("normal", Decimal("50"), seconds_since_refresh=None),
        ExitCandidate("risk", Decimal("-40"), seconds_since_refresh=1, forced=True),
    ])
    assert ranked[0].lot_id == "risk"


def test_allocation_hands_out_only_the_available_slots():
    candidates = [
        ExitCandidate("a", Decimal("3")),
        ExitCandidate("b", Decimal("2")),
        ExitCandidate("c", Decimal("1")),
    ]
    assert allocated_lots(candidates, slots=1) == ["a"]
    assert allocated_lots(candidates, slots=2) == ["a", "b"]
    assert allocated_lots(candidates, slots=0) == []


def test_priority_is_a_total_order_for_main_py_comparison():
    high = exit_candidate_priority(
        pnl_bps=Decimal("10"), waited_seconds=5, recently_refreshed=False
    )
    low = exit_candidate_priority(
        pnl_bps=Decimal("9"), waited_seconds=5, recently_refreshed=False
    )
    fresh = exit_candidate_priority(
        pnl_bps=Decimal("99"), waited_seconds=5, recently_refreshed=True
    )
    assert high > low
    assert low > fresh


def test_missing_pnl_sorts_last_and_payloads_are_accepted():
    ranked = rank_candidates([
        {"lot_id": "unknown", "pnl_bps": None},
        {"lot_id": "known", "pnl_bps": "1.5"},
    ])
    assert [item.lot_id for item in ranked] == ["known", "unknown"]


def test_recently_refreshed_boundary():
    assert recently_refreshed(0) is True
    assert recently_refreshed(44.9) is True
    assert recently_refreshed(45) is False
    assert recently_refreshed(None) is False

def test_runtime_marks_the_lot_that_just_spent_the_rfq_slot():
    from main import VariationalToLighterRuntime

    runtime = object.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_exit_refresh_at_monotonic = {}

    assert runtime.live_inventory_exit_lot_recently_refreshed(11) is False
    runtime.note_live_inventory_exit_refresh(11)
    assert runtime.live_inventory_exit_lot_recently_refreshed(11) is True
    assert runtime.live_inventory_exit_lot_recently_refreshed(12) is False


def test_runtime_tolerates_a_missing_allocation_state():
    from main import VariationalToLighterRuntime

    runtime = object.__new__(VariationalToLighterRuntime)

    assert runtime.live_inventory_exit_lot_recently_refreshed(None) is False
    runtime.note_live_inventory_exit_refresh(None)
    assert getattr(runtime, "live_inventory_exit_refresh_at_monotonic", None) is None