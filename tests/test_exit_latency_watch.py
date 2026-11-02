from datetime import datetime, timezone

from tools.lib.exit_latency_watch import (
    WatchState,
    format_event,
    load_state,
    lot_key,
    save_state,
)


def _row(event, *, lot=1, at="2026-11-02T05:00:00+00:00", pnl=None, threshold=None,
         reason=None, run="run-1", asset="ETH"):
    row = {"event": event, "run_id": run, "asset": asset, "lot_id": lot, "logged_at": at}
    if pnl is not None:
        row["executable_pnl_bps"] = str(pnl)
    if threshold is not None:
        row["effective_min_exit_pnl_bps"] = str(threshold)
    if reason:
        row["reason"] = reason
    return row


def test_lot_key_prefers_lot_id_and_falls_back_to_cycle_id():
    assert lot_key({"run_id": "r", "lot_id": 4}) == ("r", "4")
    assert lot_key({"run_id": "r", "cycle_id": 7}) == ("r", "7")
    assert lot_key({"run_id": "r"}) is None


def test_a_qualified_row_is_reported_once_per_lot():
    state = WatchState()
    row = _row("live_inventory_exit_refresh", pnl=4.5, threshold=3.5)

    first = state.observe(row)
    second = state.observe(row)

    assert first["kind"] == "QUALIFIED"
    assert first["pnl_bps"] == "4.5"
    assert first["threshold_bps"] == "3.5"
    assert second is None
    assert state.qualified_total == 1


def test_a_blocked_below_threshold_row_does_not_qualify():
    state = WatchState()
    row = _row("live_inventory_exit_blocked", pnl=1.0, threshold=3.5,
               reason="basis_exit_refresh_pnl_below_threshold")
    assert state.observe(row) is None
    assert state.qualified == {}


def test_close_reports_latency_from_the_qualification_time():
    state = WatchState()
    at = datetime(2026, 11, 2, 5, 0, tzinfo=timezone.utc)
    state.observe(_row("live_inventory_exit_refresh", pnl=4, threshold=3.5), now=at)

    closed = state.observe(
        _row("live_inventory_exited", at="2026-11-02T05:02:00+00:00"),
        now=datetime(2026, 11, 2, 5, 2, tzinfo=timezone.utc),
    )

    assert closed["kind"] == "CLOSED"
    assert closed["latency_s"] == 120.0
    assert state.closed_total == 1
    assert state.qualified == {}


def test_a_close_without_a_qualification_reports_unknown_latency():
    state = WatchState()
    closed = state.observe(_row("live_inventory_exited"))
    assert closed["kind"] == "CLOSED"
    assert closed["latency_s"] is None
    assert "latency_s=unknown" in format_event(closed)


def test_a_duplicate_close_is_ignored():
    state = WatchState()
    row = _row("live_inventory_exited")
    assert state.observe(row) is not None
    assert state.observe(row) is None
    assert state.closed_total == 1


def test_state_survives_a_restart(tmp_path):
    state = WatchState()
    state.observe(
        _row("live_inventory_exit_refresh", pnl=4, threshold=3.5),
        now=datetime(2026, 11, 2, 5, 0, tzinfo=timezone.utc),
    )
    state.observe(_row("live_inventory_exited", lot=2))
    path = tmp_path / "state.json"

    save_state(path, state)
    restored = load_state(path)

    assert len(restored.qualified) == 1
    assert restored.closed_total == 1
    assert ("run-1", "2") in restored.closed
    closed = restored.observe(_row("live_inventory_exited", lot=1),
                              now=datetime(2026, 11, 2, 5, 3, tzinfo=timezone.utc))
    assert closed["latency_s"] == 180.0


def test_a_missing_state_file_is_empty_not_an_error(tmp_path):
    state = load_state(tmp_path / "missing.json")
    assert state.qualified == {}
    assert state.closed == set()


def test_qualified_format_matches_the_heartbeat_grep():
    state = WatchState()
    event = state.observe(_row("live_inventory_exit_refresh", pnl=4, threshold=3.5))
    line = format_event(event)
    assert line.startswith("QUALIFIED lot=1 pnl=4")
    assert "thr=3.5" in line