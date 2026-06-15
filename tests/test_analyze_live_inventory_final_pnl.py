import json
from decimal import Decimal
from pathlib import Path

from tools.analyze_live_inventory_final_pnl import analyze


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_final_pnl_uses_variational_final_fill_prices(tmp_path: Path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    qty = "0.0003"
    _write_jsonl(
        path,
        [
            {
                "event": "live_inventory_entered",
                "logged_at": "2026-06-15T00:00:00+00:00",
                "asset": "BTC",
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": qty,
                "var_price": "100",
                "lighter_price": "110",
            },
            {
                "event": "lighter_fill",
                "logged_at": "2026-06-15T00:00:00.100000+00:00",
                "asset": "BTC",
                "side": "buy",
                "qty": qty,
                "lighter_order_side": "SELL",
                "lighter_filled_price": "110",
                "auto_live_role": "live_inventory_entry",
            },
            {
                "event": "variational_fill",
                "logged_at": "2026-06-15T00:00:00.200000+00:00",
                "asset": "BTC",
                "side": "buy",
                "qty": qty,
                "variational_filled_price": "130",
                "synthetic_eager_fill": False,
            },
            {
                "event": "live_inventory_exited",
                "logged_at": "2026-06-15T00:00:10+00:00",
                "asset": "BTC",
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": qty,
                "var_price": "111",
                "lighter_price": "112",
                "pnl_usd": "0.003",
            },
            {
                "event": "lighter_fill",
                "logged_at": "2026-06-15T00:00:10.100000+00:00",
                "asset": "BTC",
                "side": "sell",
                "qty": qty,
                "lighter_order_side": "BUY",
                "lighter_filled_price": "112",
                "auto_live_role": "live_inventory_exit",
            },
            {
                "event": "live_inventory_actual_pnl",
                "logged_at": "2026-06-15T00:00:10.150000+00:00",
                "asset": "BTC",
                "lot_id": 1,
                "actual_pnl_usd": "0.003",
            },
            {
                "event": "variational_fill",
                "logged_at": "2026-06-15T00:00:10.200000+00:00",
                "asset": "BTC",
                "side": "sell",
                "qty": qty,
                "variational_filled_price": "111",
                "synthetic_eager_fill": False,
            },
        ],
    )

    lots = analyze(path)

    assert len(lots) == 1
    lot = lots[0]
    assert lot.reported_actual_pnl_usd == Decimal("0.003")
    assert lot.entry_var_final_price == Decimal("130")
    assert lot.exit_var_final_price == Decimal("111")
    assert lot.entry_lighter_final_price == Decimal("110")
    assert lot.exit_lighter_final_price == Decimal("112")
    assert lot.final_var_leg_pnl_usd == Decimal("-0.0057")
    assert lot.final_lighter_leg_pnl_usd == Decimal("-0.0006")
    assert lot.final_pnl_usd == Decimal("-0.0063")
