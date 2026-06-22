from __future__ import annotations

import argparse
import json
from decimal import Decimal

from tools.inspect_live_basis_state import inspect
from tools.replay_live_basis_params import replay


def write_jsonl(path, rows):
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_inspect_live_basis_state_estimates_open_pnl(tmp_path) -> None:
    state = tmp_path / "state.json"
    metrics = tmp_path / "metrics.jsonl"
    state.write_text(
        json.dumps(
            {
                "status": "open",
                "completed_cycles": 0,
                "realized_pnl_usd": "0",
                "open_lots": [
                    {
                        "lot_id": 1,
                        "entry_kind": "basis_initial",
                        "direction": "long_var_short_lighter",
                        "qty": "1",
                        "entry_basis_bps": "-10",
                        "entry_var_fill_price": "100",
                        "entry_lighter_fill_price": "110",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    write_jsonl(
        metrics,
        [
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "basis_bps": "-5",
                "z": "0",
                "var_bid": "105",
                "var_ask": "106",
                "lighter_buy_price": "107",
                "lighter_sell_price": "106",
            }
        ],
    )

    report = inspect(state, metrics, asset="ETH")

    assert report["open_lots"] == 1
    assert report["weighted_entry_basis_bps"] == "-10"
    assert report["estimated_open_pnl_usd"] == "8"


def test_replay_live_basis_params_respects_abs_entry_gate(tmp_path) -> None:
    metrics = tmp_path / "metrics.jsonl"
    write_jsonl(
        metrics,
        [
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "sample_index": 1,
                "basis_bps": "-8",
                "z": "-5",
                "var_bid": "99",
                "var_ask": "100",
                "lighter_buy_price": "101",
                "lighter_sell_price": "100",
                "long_edge_bps": "10",
                "long_roundtrip_pnl_bps": "-2",
            },
            {
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "sample_index": 2,
                "basis_bps": "-13",
                "z": "-5",
                "var_bid": "99",
                "var_ask": "100",
                "lighter_buy_price": "101",
                "lighter_sell_price": "100",
                "long_edge_bps": "10",
                "long_roundtrip_pnl_bps": "-2",
            },
        ],
    )
    args = argparse.Namespace(
        asset="ETH",
        lot_notional_usd=Decimal("20"),
        max_total_lots=1,
        max_cycles=1,
        z_entry=Decimal("3"),
        z_exit=Decimal("999"),
        min_entry_edge_bps=Decimal("7"),
        min_abs_entry_bps=Decimal("12"),
        max_entry_roundtrip_cost_bps=Decimal("3"),
        addon_min_basis_improvement_bps=Decimal("4"),
        min_exit_pnl_bps=Decimal("1"),
        min_hold_samples=0,
    )

    result = replay(metrics, args)

    assert result["rows_seen"] == 2
    assert result["entered"] == 1
