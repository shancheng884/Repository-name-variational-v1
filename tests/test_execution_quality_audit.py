from decimal import Decimal

from tools.execution_quality_audit import summarize


def row(lot, *, estimated="5", actual="1", at="2026-10-22T00:00:00+00:00"):
    return {
        "run_id": "run-1", "lot_id": lot, "direction": "short_var_long_lighter",
        "logged_at": at, "actual_pnl_status": "lighter_final_fill_confirmed",
        "estimated_pnl_bps": estimated, "actual_pnl_bps": actual,
        "estimated_pnl_usd": estimated, "actual_pnl_usd": actual,
    }


def test_replay_uses_only_prior_execution_losses_and_counts_groups():
    first = row(1, estimated="5", actual="-5")
    first["portfolio_component_lot_count"] = 10
    second = row(11, estimated="5", actual="1", at="2026-10-22T00:01:00+00:00")
    report = summarize([second, first, first])
    assert report["groups"] == 2
    assert report["prior_gate_pass"] == 1
    assert report["prior_gate_reject"] == 1
    assert report["actual_price_pnl_usd"] == Decimal("-4")


def test_window_keeps_prior_history_without_counting_it_as_new_profit():
    first = row(1, estimated="5", actual="-5")
    second = row(2, at="2026-10-22T00:01:00+00:00")
    report = summarize([first, second], since="2026-10-22T00:00:30+00:00")
    assert report["groups"] == 1 and report["prior_gate_reject"] == 1
    assert report["actual_price_pnl_usd"] == 1


def test_unknown_fills_and_legacy_latency_do_not_create_execution_samples():
    incomplete = row(1)
    incomplete["actual_pnl_status"] = "leg_quantity_mismatch_requires_reconciliation"
    complete = row(2)
    complete["exit_var_fill_to_lighter_fill_ms"] = "800"
    report = summarize([incomplete, complete])
    assert report["groups"] == 1
    assert report["wire_to_report_samples"] == []
