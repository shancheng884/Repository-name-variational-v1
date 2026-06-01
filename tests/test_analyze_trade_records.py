import json

from tools.analyze_trade_records import collect_manual_review_details, load_from_jsonl, summarize


def test_jsonl_analysis_keeps_auto_live_manual_review_events(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    event = {
        "event": "auto_live_manual_review_required",
        "record_kind": "auto_live_manual_review",
        "logged_at": "2026-06-01T16:00:00Z",
        "mode": "live",
        "asset": "BTC",
        "auto_live_cycle_id": 1,
        "direction": "long_var_short_lighter",
        "qty": "0.00022",
        "reason": "exit_precheck_failed:hedge_price_deviation_exceeds_risk_limit",
        "rollback_action": "manual_review_required",
        "action": "stop_auto_live_until_restart",
    }
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    rows = load_from_jsonl(path, {"BTC"}, "")
    stats = summarize(rows)
    details = collect_manual_review_details(rows)

    assert stats["BTC"]["record_kinds"]["auto_live_manual_review"] == 1
    assert stats["BTC"]["manual_review_reasons"]["exit_precheck_failed:hedge_price_deviation_exceeds_risk_limit"] == 1
    assert details == [
        {
            "asset": "BTC",
            "auto_live_cycle_id": "1",
            "direction": "long_var_short_lighter",
            "qty": "0.00022",
            "reason": "exit_precheck_failed:hedge_price_deviation_exceeds_risk_limit",
            "action": "stop_auto_live_until_restart",
            "logged_at": "2026-06-01T16:00:00Z",
        }
    ]
