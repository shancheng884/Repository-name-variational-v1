from decimal import Decimal

import main as app

SHORT = "short_var_long_lighter"
LONG = "long_var_short_lighter"


def _runtime() -> app.VariationalToLighterRuntime:
    runtime = app.VariationalToLighterRuntime.__new__(app.VariationalToLighterRuntime)
    runtime.live_inventory_v4_near_bar_state = {}
    return runtime


def test_near_bar_enters_and_leaves_once(monkeypatch) -> None:
    runtime = _runtime()
    clock = {"now": 1000.0}
    monkeypatch.setattr(app.time, "monotonic", lambda: clock["now"])

    # 3 bps away: not "near" yet.
    assert runtime.audit_live_inventory_v4_near_bar(
        direction=SHORT,
        edge_bps=Decimal("-13.4"),
        threshold_bps=Decimal("-16.4"),
        sample_index=1,
        asset="ETH",
    ) == []

    entered = runtime.audit_live_inventory_v4_near_bar(
        direction=SHORT,
        edge_bps=Decimal("-15.7"),
        threshold_bps=Decimal("-16.4"),
        sample_index=2,
        asset="ETH",
    )
    assert [name for name, _ in entered] == ["live_inventory_v4_near_bar_entered"]
    assert entered[0][1]["distance_to_bar_bps"] == "0.7"

    # Closer still: same episode, no extra line, closest distance recorded.
    clock["now"] = 1004.0
    assert runtime.audit_live_inventory_v4_near_bar(
        direction=SHORT,
        edge_bps=Decimal("-16.1"),
        threshold_bps=Decimal("-16.4"),
        sample_index=3,
        asset="ETH",
    ) == []

    clock["now"] = 1009.0
    left = runtime.audit_live_inventory_v4_near_bar(
        direction=SHORT,
        edge_bps=Decimal("-12.0"),
        threshold_bps=Decimal("-16.4"),
        sample_index=4,
        asset="ETH",
    )
    assert [name for name, _ in left] == ["live_inventory_v4_near_bar_left"]
    assert left[0][1]["closest_distance_bps"] == "0.3"
    assert left[0][1]["seconds_in_band"] == 9.0
    assert runtime.live_inventory_v4_near_bar_state == {}


def test_near_bar_is_quiet_when_the_edge_is_past_the_bar(monkeypatch) -> None:
    runtime = _runtime()
    monkeypatch.setattr(app.time, "monotonic", lambda: 1000.0)
    # Past the bar is a crossing, not a near miss: the near-bar audit stays quiet.
    assert runtime.audit_live_inventory_v4_near_bar(
        direction=SHORT,
        edge_bps=Decimal("-17.0"),
        threshold_bps=Decimal("-16.4"),
        sample_index=1,
        asset="ETH",
    ) == []
    assert runtime.live_inventory_v4_near_bar_state == {}


def test_near_bar_is_direction_aware_on_the_long_side(monkeypatch) -> None:
    runtime = _runtime()
    monkeypatch.setattr(app.time, "monotonic", lambda: 1000.0)
    entered = runtime.audit_live_inventory_v4_near_bar(
        direction=LONG,
        edge_bps=Decimal("15.5"),
        threshold_bps=Decimal("16.0"),
        sample_index=1,
        asset="ETH",
    )
    assert entered[0][1]["distance_to_bar_bps"] == "0.5"


def test_near_bar_ignores_missing_inputs(monkeypatch) -> None:
    runtime = _runtime()
    monkeypatch.setattr(app.time, "monotonic", lambda: 1000.0)
    assert runtime.audit_live_inventory_v4_near_bar(
        direction=SHORT,
        edge_bps=None,
        threshold_bps=Decimal("-16.4"),
        sample_index=1,
        asset="ETH",
    ) == []
