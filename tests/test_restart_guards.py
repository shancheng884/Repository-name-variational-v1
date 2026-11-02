from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from tools.lib.restart_guards import (
    assess_health,
    assess_state,
    build_launch_command,
    classify_reconcile,
    expected_qty,
    find_startup_reconcile,
    lot_signature,
    summarize_positions,
    tail_events,
)

NOW = datetime(2026, 11, 2, 5, 0, tzinfo=timezone.utc)


def _open_state(lots=3, qty="0.0088", asset="ETH"):
    return {
        "asset": asset,
        "status": "open",
        "open_lots": [{"lot_id": 70 + i, "qty": qty} for i in range(lots)],
        "pending_actions": [],
    }


def _health(age_seconds=5.0, **overrides):
    payload = {
        "updated_at": (NOW - timedelta(seconds=age_seconds)).isoformat(),
        "risk_action": "normal",
        "risk_reason": "account_risk_normal",
        "run_id": "run-1",
    }
    payload.update(overrides)
    return payload


def test_open_state_passes():
    result = assess_state(_open_state(), asset="ETH")

    assert result["ok"] is True
    assert result["status"] == "open"
    assert result["lots"] == 3
    assert result["expected_qty"] == "0.0264"


def test_flat_state_passes():
    state = {"asset": "ETH", "status": "flat", "open_lots": [], "pending_actions": []}

    assert assess_state(state, asset="ETH")["ok"] is True


def test_asset_mismatch_blocks():
    result = assess_state(_open_state(asset="WTI"), asset="ETH")

    assert result["ok"] is False
    assert any(item.startswith("asset_mismatch") for item in result["reasons"])


def test_pending_actions_block():
    state = _open_state()
    state["pending_actions"] = [{"kind": "open"}]

    result = assess_state(state, asset="ETH")

    assert result["ok"] is False
    assert "pending_actions:1" in result["reasons"]


def test_manual_review_blocks():
    state = _open_state()
    state["manual_review_reason"] = "pending_order_terminal_status_unconfirmed"

    result = assess_state(state, asset="ETH")

    assert result["ok"] is False
    assert any(item.startswith("manual_review") for item in result["reasons"])


def test_open_without_lots_blocks():
    state = {"asset": "ETH", "status": "open", "open_lots": [], "pending_actions": []}

    assert "state_open_without_lots" in assess_state(state, asset="ETH")["reasons"]


def test_flat_with_lots_blocks():
    state = {"asset": "ETH", "status": "flat", "open_lots": [{"lot_id": 1, "qty": "1"}], "pending_actions": []}

    assert "state_flat_with_lots" in assess_state(state, asset="ETH")["reasons"]


def test_missing_state_reports_reason():
    assert assess_state({}, asset="ETH")["reasons"] == ["state_unavailable"]


def test_fresh_health_passes():
    result = assess_health(_health(), now=NOW, max_age_seconds=90)

    assert result["ok"] is True
    assert result["age_seconds"] == 5.0


def test_stale_health_blocks():
    result = assess_health(_health(age_seconds=600), now=NOW, max_age_seconds=90)

    assert result["ok"] is False
    assert any(item.startswith("health_stale") for item in result["reasons"])


def test_future_health_blocks():
    result = assess_health(_health(age_seconds=-600), now=NOW, max_age_seconds=90)

    assert result["ok"] is False
    assert any(item.startswith("health_clock_ahead") for item in result["reasons"])


def test_health_manual_review_blocks():
    result = assess_health(_health(manual_review_reason="stuck"), now=NOW, max_age_seconds=90)

    assert result["ok"] is False
    assert any(item.startswith("health_manual_review") for item in result["reasons"])


def test_health_missing_risk_action_blocks():
    health = _health()
    health.pop("risk_action")

    result = assess_health(health, now=NOW, max_age_seconds=90)

    assert "health_risk_action_missing" in result["reasons"]


def test_positions_match_ignoring_sign():
    result = summarize_positions(variational_qty="0.1144", lighter_qty="-0.1144", expected="0.1144")

    assert result["ok"] is True


def test_positions_mismatch_blocks():
    result = summarize_positions(variational_qty="0.396", lighter_qty="-0.198", expected="0.1144")

    assert result["ok"] is False
    assert any(item.startswith("variational_qty_mismatch") for item in result["reasons"])


def test_positions_imbalance_blocks():
    result = summarize_positions(variational_qty="0.1144", lighter_qty="-0.1000", expected="0.1144")

    assert result["ok"] is False
    assert any(item.startswith("venues_imbalanced") or item.startswith("lighter_qty_mismatch") for item in result["reasons"])


def test_positions_unreadable_blocks():
    result = summarize_positions(variational_qty=None, lighter_qty="0", expected="0.1")

    assert result["ok"] is False
    assert result["reasons"] == ["position_values_unreadable"]


def test_tail_events_skips_broken_lines(tmp_path: Path):
    log = tmp_path / "order_metrics.jsonl"
    log.write_text('{"event": "a"}\nnot-json\n{"event": "b"}\n', encoding="utf-8")

    rows = tail_events(log)

    assert [row["event"] for row in rows] == ["a", "b"]


def test_tail_events_missing_file_is_empty(tmp_path: Path):
    assert tail_events(tmp_path / "nope.jsonl") == []


def test_find_startup_reconcile_filters_by_run_id():
    rows = [
        {"event": "live_inventory_startup_reconcile_ok", "run_id": "old", "status": "both_exchanges_flat"},
        {"event": "live_inventory_startup_reconcile_ok", "run_id": "new", "status": "both_exchanges_matched"},
        {"event": "other", "run_id": "new"},
    ]

    event = find_startup_reconcile(rows, run_id="new")

    assert event is not None
    assert event["status"] == "both_exchanges_matched"


def test_classify_reconcile_passes_with_matching_positions():
    event = {
        "status": "both_exchanges_matched",
        "variational_position_qty": "-0.1144",
        "lighter_position_qty": "0.1144",
    }

    result = classify_reconcile(event, expected="0.1144")

    assert result["ok"] is True
    assert result["positions"]["ok"] is True


def test_classify_reconcile_rejects_manual_review_status():
    result = classify_reconcile({"status": "manual_review_required"}, expected="0.1144")

    assert result["ok"] is False
    assert any(item.startswith("startup_reconcile_status") for item in result["reasons"])


def test_classify_reconcile_missing_event():
    assert classify_reconcile(None, expected="0.1")["reasons"] == ["startup_reconcile_event_missing"]


def test_expected_qty_sums_absolute_values():
    lots = [{"qty": "0.0088"}, {"qty": "-0.0088"}]

    assert expected_qty(lots) == Decimal("0.0176")


def test_lot_signature_is_sorted_and_ignores_junk():
    lots = [{"lot_id": 2, "qty": "0.2"}, {"lot_id": 1, "qty": "0.1"}, "junk"]

    assert lot_signature(lots) == [("1", "0.1"), ("2", "0.2")]


def test_launch_command_includes_resume_only_when_open(tmp_path: Path):
    resume_cmd = build_launch_command(
        repo=tmp_path, asset="eth", resume=True, session="s1", log_path=tmp_path / "log" / "s1.log"
    )
    flat_cmd = build_launch_command(
        repo=tmp_path, asset="ETH", resume=False, session="s2", log_path=tmp_path / "log" / "s2.log"
    )

    assert "--resume-open-position" in resume_cmd
    assert "--resume-open-position" not in flat_cmd
    assert "--asset ETH" in resume_cmd
    assert "tools/live.py" in flat_cmd
    assert str(tmp_path / "log" / "s1.log") in resume_cmd
