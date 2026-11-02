from datetime import datetime, timezone

from tools.lib.exit_latency_acceptance import measure, summary_lines


def _row(event, *, lot, at, pnl=None, threshold=None, reason=None):
    row = {"event": event, "run_id": "run-1", "asset": "ETH", "lot_id": lot,
           "logged_at": at}
    if pnl is not None:
        row["executable_pnl_bps"] = str(pnl)
    if threshold is not None:
        row["effective_min_exit_pnl_bps"] = str(threshold)
    if reason:
        row["reason"] = reason
    return row


def test_lot_closed_within_the_budget_passes():
    rows = [
        _row("live_inventory_v4_exit_current_quote_confirmed", lot=1,
             at="2026-11-02T05:00:00+00:00", pnl=4, threshold=3.5),
        _row("live_inventory_exited", lot=1, at="2026-11-02T05:01:30+00:00"),
    ]
    summary = measure(rows)
    assert summary["pass"] == 1
    assert summary["slow"] == 0
    assert summary["detail"][0]["latency_seconds"] == 90.0


def test_lot_that_takes_longer_than_the_budget_is_slow():
    rows = [
        _row("live_inventory_exit_refresh", lot=2,
             at="2026-11-02T05:00:00+00:00", pnl=5, threshold=3.5),
        _row("live_inventory_exited", lot=2, at="2026-11-02T05:05:00+00:00"),
    ]
    summary = measure(rows, max_latency_seconds=180)
    assert summary["slow"] == 1
    assert summary["detail"][0]["status"] == "SLOW"


def test_a_block_below_threshold_does_not_start_the_clock():
    rows = [
        _row("live_inventory_exit_blocked", lot=3,
             at="2026-11-02T04:00:00+00:00", pnl=1, threshold=3.5,
             reason="basis_exit_refresh_pnl_below_threshold"),
        _row("live_inventory_exit_blocked", lot=3,
             at="2026-11-02T05:00:00+00:00", pnl=4, threshold=3.5,
             reason="v4_exit_confirmation_pending"),
        _row("live_inventory_exited", lot=3, at="2026-11-02T05:02:00+00:00"),
    ]
    summary = measure(rows)
    assert summary["detail"][0]["latency_seconds"] == 120.0


def test_lot_that_has_not_closed_yet_is_reported_open():
    rows = [
        _row("live_inventory_exit_refresh", lot=4,
             at="2026-11-02T05:00:00+00:00", pnl=6, threshold=3.5),
    ]
    summary = measure(
        rows, now=datetime(2026, 11, 2, 5, 10, tzinfo=timezone.utc)
    )
    assert summary["open"] == 1
    assert summary["detail"][0]["latency_seconds"] == 600.0


def test_lot_that_never_qualified_is_separated():
    rows = [_row("live_inventory_exit_blocked", lot=5,
                 at="2026-11-02T05:00:00+00:00", pnl=1, threshold=3.5,
                 reason="basis_exit_refresh_pnl_below_threshold")]
    summary = measure(rows)
    assert summary["not_qualified"] == 1


def test_repeated_qualifications_use_the_earliest_one():
    rows = [
        _row("live_inventory_exit_refresh", lot=6,
             at="2026-11-02T05:00:00+00:00", pnl=4, threshold=3.5),
        _row("live_inventory_exit_refresh", lot=6,
             at="2026-11-02T05:02:00+00:00", pnl=4.5, threshold=3.5),
        _row("live_inventory_exited", lot=6, at="2026-11-02T05:03:00+00:00"),
    ]
    summary = measure(rows)
    assert summary["detail"][0]["latency_seconds"] == 180.0


def test_asset_filter_and_summary_lines():
    rows = [
        _row("live_inventory_exit_refresh", lot=7,
             at="2026-11-02T05:00:00+00:00", pnl=4, threshold=3.5),
        {**_row("live_inventory_exit_refresh", lot=8,
                at="2026-11-02T05:00:00+00:00", pnl=4, threshold=3.5),
         "asset": "WTI"},
    ]
    summary = measure(rows, asset="ETH",
                      now=datetime(2026, 11, 2, 5, 1, tzinfo=timezone.utc))
    assert summary["lots"] == 1
    assert summary_lines(summary)[0].startswith("批次=1")