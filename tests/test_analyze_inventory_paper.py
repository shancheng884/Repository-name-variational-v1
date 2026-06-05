import json
from decimal import Decimal

from tools.analyze_inventory_paper import load_events, summarize_events


def test_inventory_paper_summary_groups_exits_by_direction(tmp_path) -> None:
    path = tmp_path / "inventory_paper.jsonl"
    rows = [
        {
            "event": "inventory_paper_entered",
            "direction": "long_var_short_lighter",
            "lot_id": 1,
            "open_lots_total": 1,
            "realized_pnl_usd": "0",
        },
        {
            "event": "inventory_paper_exited",
            "direction": "long_var_short_lighter",
            "lot_id": 1,
            "pnl_usd": "0.10",
            "pnl_bps": "20",
            "holding_samples": 4,
            "open_lots_total": 0,
            "realized_pnl_usd": "0.10",
        },
        {
            "event": "inventory_paper_entered",
            "direction": "short_var_long_lighter",
            "lot_id": 2,
            "open_lots_total": 1,
            "realized_pnl_usd": "0.10",
        },
        {
            "event": "inventory_paper_exited",
            "direction": "short_var_long_lighter",
            "lot_id": 2,
            "pnl_usd": "-0.05",
            "pnl_bps": "-10",
            "holding_samples": 7,
            "open_lots_total": 0,
            "realized_pnl_usd": "0.05",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    summary = summarize_events(load_events(path), latest_limit=2)

    assert summary.events == 4
    assert summary.entered == 2
    assert summary.exited == 2
    assert summary.latest_open_lots_total == 0
    assert summary.latest_realized_pnl_usd == Decimal("0.05")
    assert summary.max_open_lots_total == 1
    assert len(summary.losing_exits) == 1
    assert summary.by_direction["long_var_short_lighter"].total_pnl_usd == Decimal("0.10")
    assert summary.by_direction["long_var_short_lighter"].avg_pnl_bps == Decimal("20")
    assert summary.by_direction["short_var_long_lighter"].total_pnl_usd == Decimal("-0.05")
    assert summary.by_direction["short_var_long_lighter"].min_holding_samples == 7
    assert len(summary.latest_events) == 2
