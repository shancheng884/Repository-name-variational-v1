from decimal import Decimal

import main as app

SHORT = "short_var_long_lighter"
LONG = "long_var_short_lighter"


def _runtime() -> app.VariationalToLighterRuntime:
    runtime = app.VariationalToLighterRuntime.__new__(app.VariationalToLighterRuntime)
    runtime.live_inventory_v4_crossing_state = {}
    return runtime


def test_crossing_opens_once_and_closes_with_reversion(monkeypatch) -> None:
    runtime = _runtime()
    clock = {"now": 1000.0}
    monkeypatch.setattr(app.time, "monotonic", lambda: clock["now"])

    assert runtime.audit_live_inventory_v4_threshold_crossing(
        direction=SHORT,
        edge_bps=Decimal("-10.0"),
        threshold_bps=Decimal("-16.4"),
        sample_index=1,
        asset="ETH",
    ) == []

    opened = runtime.audit_live_inventory_v4_threshold_crossing(
        direction=SHORT,
        edge_bps=Decimal("-17.0"),
        threshold_bps=Decimal("-16.4"),
        sample_index=2,
        asset="ETH",
    )
    assert [name for name, _ in opened] == ["live_inventory_v4_crossing_opened"]
    assert opened[0][1]["margin_bps"] == "0.6"

    # Still qualified: no extra log line, but the peak keeps updating.
    clock["now"] = 1002.0
    assert runtime.audit_live_inventory_v4_threshold_crossing(
        direction=SHORT,
        edge_bps=Decimal("-18.5"),
        threshold_bps=Decimal("-16.4"),
        sample_index=3,
        asset="ETH",
    ) == []

    clock["now"] = 1010.0
    closed = runtime.audit_live_inventory_v4_threshold_crossing(
        direction=SHORT,
        edge_bps=Decimal("-12.0"),
        threshold_bps=Decimal("-16.4"),
        sample_index=4,
        asset="ETH",
    )
    assert [name for name, _ in closed] == ["live_inventory_v4_crossing_closed"]
    payload = closed[0][1]
    assert payload["peak_edge_bps"] == "-18.5"
    assert payload["seconds_past_bar"] == 10.0
    assert payload["ticks_past_bar"] == 3
    assert payload["reverted_bps"] == "6.5"
    assert runtime.live_inventory_v4_crossing_state == {}


def test_crossing_ignores_ticks_that_never_cross(monkeypatch) -> None:
    runtime = _runtime()
    monkeypatch.setattr(app.time, "monotonic", lambda: 1000.0)
    for _ in range(3):
        assert runtime.audit_live_inventory_v4_threshold_crossing(
            direction=SHORT,
            edge_bps=Decimal("-11.0"),
            threshold_bps=Decimal("-16.4"),
            sample_index=1,
            asset="ETH",
        ) == []
    assert runtime.live_inventory_v4_crossing_state == {}


def test_crossing_is_direction_aware_on_the_long_side(monkeypatch) -> None:
    runtime = _runtime()
    monkeypatch.setattr(app.time, "monotonic", lambda: 1000.0)
    events = runtime.audit_live_inventory_v4_threshold_crossing(
        direction=LONG,
        edge_bps=Decimal("17.0"),
        threshold_bps=Decimal("16.0"),
        sample_index=1,
        asset="ETH",
    )
    assert events[0][0] == "live_inventory_v4_crossing_opened"

    # A long edge below its bar closes the episode even though it is positive.
    closed = runtime.audit_live_inventory_v4_threshold_crossing(
        direction=LONG,
        edge_bps=Decimal("15.0"),
        threshold_bps=Decimal("16.0"),
        sample_index=2,
        asset="ETH",
    )
    assert closed[0][0] == "live_inventory_v4_crossing_closed"
    assert closed[0][1]["reverted_bps"] == "2.0"


def test_crossing_survives_a_missing_threshold(monkeypatch) -> None:
    runtime = _runtime()
    monkeypatch.setattr(app.time, "monotonic", lambda: 1000.0)
    assert runtime.audit_live_inventory_v4_threshold_crossing(
        direction=SHORT,
        edge_bps=None,
        threshold_bps=None,
        sample_index=1,
        asset="ETH",
    ) == []
