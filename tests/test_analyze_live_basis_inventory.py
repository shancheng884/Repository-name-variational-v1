from __future__ import annotations

import json
from decimal import Decimal

from tools.analyze_live_basis_inventory import summarize


def write_jsonl(path, rows) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_summarize_live_basis_inventory(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    write_jsonl(
        path,
        [
            {
                "event": "live_inventory_dry_entered",
                "execution_mode": "dry_decision",
                "asset": "ETH",
                "lot_id": 1,
                "z": "-5",
                "edge_bps": "9",
                "roundtrip_pnl_bps": "-2",
            },
            {
                "event": "live_inventory_dry_exited",
                "execution_mode": "dry_decision",
                "asset": "ETH",
                "lot_id": 1,
                "pnl_usd": "0.002",
                "pnl_bps": "1.0",
                "exit_reason": "signal_reverted",
            },
            {
                "event": "live_inventory_dry_entered",
                "execution_mode": "dry_decision",
                "asset": "ETH",
                "lot_id": 2,
                "z": "-4",
                "edge_bps": "8",
                "roundtrip_pnl_bps": "-3",
            },
            {
                "event": "live_inventory_dry_exited",
                "execution_mode": "dry_decision",
                "asset": "ETH",
                "lot_id": 2,
                "pnl_usd": "-0.001",
                "pnl_bps": "-0.5",
                "exit_reason": "max_hold_samples",
            },
            {
                "event": "live_inventory_basis_state",
                "execution_mode": "dry_decision",
                "asset": "ETH",
                "sample_index": 10,
                "completed_cycles": 2,
                "realized_pnl_usd": "0.001",
            },
            {
                "event": "live_inventory_dry_entered",
                "execution_mode": "dry_decision",
                "asset": "BTC",
                "lot_id": 99,
                "z": "99",
                "edge_bps": "99",
                "roundtrip_pnl_bps": "99",
            },
        ],
    )

    summary = summarize(path, asset="ETH", execution_mode="dry_decision")

    assert summary.entered == 2
    assert summary.exited == 2
    assert summary.open_lots == 0
    assert summary.winning_exits == 1
    assert summary.losing_exits == 1
    assert summary.realized_pnl_usd == Decimal("0.001")
    assert summary.avg_pnl_bps == Decimal("0.25")
    assert summary.avg_entry_z == Decimal("-4.5")
    assert summary.avg_entry_edge_bps == Decimal("8.5")
    assert summary.avg_entry_roundtrip_pnl_bps == Decimal("-2.5")
    assert summary.exit_reasons == {"signal_reverted": 1, "max_hold_samples": 1}
    assert summary.latest_state is not None
    assert summary.latest_state["completed_cycles"] == 2


def test_summarize_live_basis_inventory_live_events(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    write_jsonl(
        path,
        [
            {
                "event": "live_inventory_entered",
                "execution_mode": "live",
                "asset": "ETH",
                "lot_id": 1,
                "z": "-4",
                "edge_bps": "8",
                "roundtrip_pnl_bps": "-2",
            },
            {
                "event": "live_inventory_exited",
                "execution_mode": "live",
                "asset": "ETH",
                "lot_id": 1,
                "pnl_usd": "0.003",
                "pnl_bps": "1.5",
                "exit_reason": "signal_reverted",
            },
        ],
    )

    summary = summarize(path, asset="ETH", execution_mode="live")

    assert summary.entered == 1
    assert summary.exited == 1
    assert summary.winning_exits == 1
    assert summary.realized_pnl_usd == Decimal("0.003")
