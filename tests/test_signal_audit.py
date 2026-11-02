from decimal import Decimal

from tools.lib.signal_audit import (
    SIGNAL_EVENTS,
    audit_rows,
    row_signature,
    signal_key,
    summary_lines,
)


def _row(event, *, lot=1, sample=7, reason=None, pnl=None, threshold=None, at=None):
    row = {"event": event, "run_id": "run-1", "asset": "ETH", "lot_id": lot,
           "sample_index": sample, "logged_at": at or "2026-11-02T05:00:00+00:00"}
    if reason:
        row["reason"] = reason
    if pnl is not None:
        row["refreshed_pnl_bps"] = str(pnl)
    if threshold is not None:
        row["effective_min_exit_pnl_bps"] = str(threshold)
    return row


def test_signal_key_prefers_sample_id_then_index_then_cycle():
    assert signal_key({"run_id": "r", "sample_id": "s1"}) == ("r", "sample", "s1")
    assert signal_key({"run_id": "r", "sample_index": 9}) == ("r", "index", "9")
    assert signal_key({"run_id": "r", "cycle_id": 4, "lot_id": 2}) == (
        "r", "cycle", "4", "2")
    assert signal_key({"run_id": "r", "lot_id": 2})[:2] == ("r", "lot")


def test_repeated_decisions_inside_one_signal_count_once():
    rows = [
        _row("live_inventory_exit_blocked", reason="basis_exit_refresh_quote_throttled"),
        _row("live_inventory_exit_blocked", reason="basis_exit_refresh_quote_throttled"),
        _row("live_inventory_exit_blocked", reason="basis_exit_refresh_quote_throttled"),
        _row("live_inventory_exit_blocked", sample=8,
             reason="basis_exit_refresh_quote_throttled"),
    ]
    summary = audit_rows(rows)
    assert summary["rows"] == 4
    assert summary["signals"] == 2
    assert summary["duplicate_rows"] == 2
    assert summary["reasons"] == {"basis_exit_refresh_quote_throttled": 2}


def test_signals_that_cross_their_threshold_are_marked_once():
    rows = [
        _row("live_inventory_exit_blocked", reason="basis_exit_refresh_pnl_below_threshold",
             pnl=Decimal("2.0"), threshold=Decimal("3.5")),
        _row("live_inventory_exit_refresh", pnl=Decimal("4.1"), threshold=Decimal("3.5")),
    ]
    summary = audit_rows(rows)
    assert summary["signals_crossed"] == 1
    assert summary["signals_refreshed"] == 1
    assert summary["signal_detail"][0]["crossed"] is True
    assert summary["signal_detail"][0]["best_pnl_bps"] == "4.1"


def test_asset_filter_and_foreign_events_are_ignored():
    rows = [
        _row("live_inventory_exit_blocked", reason="x"),
        {**_row("live_inventory_exit_blocked", reason="x"), "asset": "WTI"},
        {"event": "something_else", "asset": "ETH", "sample_index": 1},
    ]
    summary = audit_rows(rows, asset="ETH")
    assert summary["rows"] == 1
    assert "something_else" not in SIGNAL_EVENTS


def test_signature_separates_different_reasons_in_the_same_signal():
    first = _row("live_inventory_exit_blocked", reason="a")
    second = _row("live_inventory_exit_blocked", reason="b")
    assert row_signature(first) != row_signature(second)


def test_summary_lines_report_distinct_signals():
    rows = [_row("live_inventory_exit_blocked", reason="r") for _ in range(3)]
    lines = summary_lines(audit_rows(rows))
    assert lines[0].startswith("信号=1 行数=3 重复行=2")