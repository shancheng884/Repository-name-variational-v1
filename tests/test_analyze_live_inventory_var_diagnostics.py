import json
from decimal import Decimal
from pathlib import Path

from tools.analyze_live_inventory_var_diagnostics import load_rows, median


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_load_rows_finds_closest_snapshot_price(tmp_path: Path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    _write_jsonl(
        path,
        [
            {"event": "lighter_fill", "entry_var_final_vs_snapshot_buy_bps": "1"},
            {
                "event": "live_inventory_final_pnl",
                "logged_at": "2026-06-15T00:00:00+00:00",
                "asset": "BTC",
                "lot_id": 7,
                "direction": "long_var_short_lighter",
                "final_pnl_bps": "-1.2",
                "entry_signal_edge_bps": "15.5",
                "entry_final_edge_bps": "1.9",
                "entry_edge_capture_loss_bps": "13.6",
                "entry_var_fill_drift_bps": "13.4",
                "entry_lighter_fill_drift_bps": "-0.1",
                "var_full_spread_bps": "8.0",
                "entry_var_final_vs_snapshot_bid_bps": "14.0",
                "entry_var_final_vs_snapshot_ask_bps": "0.2",
                "entry_var_final_vs_snapshot_mid_bps": "7.0",
                "entry_var_final_vs_snapshot_buy_bps": "13.4",
                "entry_var_final_vs_snapshot_sell_bps": "0.1",
            },
        ],
    )

    rows = load_rows(path)

    assert len(rows) == 1
    assert rows[0].closest_snapshot_price == "sell"
    assert rows[0].closest_snapshot_drift_bps == Decimal("0.1")
    assert rows[0].entry_edge_capture_loss_bps == Decimal("13.6")


def test_latest_limits_rows(tmp_path: Path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    _write_jsonl(
        path,
        [
            {"event": "live_inventory_final_pnl", "lot_id": 1},
            {"event": "live_inventory_final_pnl", "lot_id": 2},
        ],
    )

    rows = load_rows(path, latest=1)

    assert [row.lot_id for row in rows] == ["2"]


def test_median_handles_even_and_odd_counts() -> None:
    assert median([Decimal("3"), Decimal("1"), Decimal("2")]) == Decimal("2")
    assert median([Decimal("1"), Decimal("3")]) == Decimal("2")
    assert median([]) is None
