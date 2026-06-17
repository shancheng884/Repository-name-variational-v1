import json
from decimal import Decimal

from tools.analyze_fresh_basis_inventory import load_rows, summarize_rows


def test_fresh_basis_summary_counts_actions_and_exit_reasons(tmp_path) -> None:
    path = tmp_path / "fresh_basis.jsonl"
    rows = [
        {
            "sample_index": 0,
            "z": "-5",
            "long_roundtrip_pnl_bps": "-4.8",
            "short_roundtrip_pnl_bps": "-5.0",
            "realized_pnl_usd": "0",
            "open_lots": 1,
            "actions": [
                {
                    "event": "inventory_paper_entered",
                    "direction": "long_var_short_lighter",
                    "edge_bps": "5",
                }
            ],
        },
        {
            "sample_index": 5,
            "z": "1",
            "realized_pnl_usd": "0.01",
            "open_lots": 0,
            "actions": [
                {
                    "event": "inventory_paper_exited",
                    "direction": "long_var_short_lighter",
                    "exit_reason": "signal_reverted",
                    "pnl_bps": "2",
                    "pnl_usd": "0.01",
                }
            ],
        },
        {
            "sample_index": 9,
            "z": "-6",
            "long_roundtrip_pnl_bps": "-4.9",
            "realized_pnl_usd": "-0.02",
            "open_lots": 0,
            "actions": [
                {
                    "event": "inventory_paper_entered",
                    "direction": "long_var_short_lighter",
                    "edge_bps": "6",
                },
                {
                    "event": "inventory_paper_exited",
                    "direction": "long_var_short_lighter",
                    "exit_reason": "max_unrealized_loss_bps",
                    "pnl_bps": "-6",
                    "pnl_usd": "-0.03",
                },
            ],
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    summary = summarize_rows(load_rows(path))

    assert summary.rows == 3
    assert summary.entered == 2
    assert summary.exited == 2
    assert summary.open_lots == 0
    assert summary.realized_pnl_usd == Decimal("-0.02")
    assert summary.winning_exits == 1
    assert summary.losing_exits == 1
    assert summary.avg_pnl_bps == Decimal("-2")
    assert summary.exit_reasons["signal_reverted"] == 1
    assert summary.exit_reasons["max_unrealized_loss_bps"] == 1
    assert summary.avg_entry_roundtrip_pnl_bps == Decimal("-4.85")
