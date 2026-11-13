from tools.lib.probe_metrics import calibrate, decompose_round


def test_cost_decomposition_separates_spread_slippage_and_pnl():
    row = {
        "round_id": "r1", "asset": "NVDA", "buy_venue": "a", "sell_venue": "b", "status": "filled",
        "decision": {"signal_edge_bps": "20", "quotes": {
            "a": {"bid": "100", "ask": "100.2", "mid": "100.1"},
            "b": {"bid": "100.2", "ask": "100.4", "mid": "100.3"},
        }},
        "entry": {"legs": {
            "a": {"side": "buy", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01"},
            "b": {"side": "sell", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01"},
        }, "quotes": {}},
        "exit": {"quotes_at_close": {
            "a": {"bid": "100.2", "ask": "100.4", "mid": "100.3"},
            "b": {"bid": "100.0", "ask": "100.2", "mid": "100.1"},
        }, "legs": {
            "a": {"side": "sell", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01"},
            "b": {"side": "buy", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01"},
        }},
    }
    out = decompose_round(row)
    assert out["total_price_cost_bps"] is not None
    assert "entry:a" in out["components"] and "exit:b" in out["components"]
    assert out["fees_usd"] == "0.04"
    assert out["gross_pnl_bps"] is not None


def test_calibration_is_per_asset_venue_and_not_double_counted():
    row = {
        "round_id": "r1", "asset": "NVDA", "buy_venue": "a", "sell_venue": "b", "status": "filled",
        "decision": {"quotes": {"a": {"bid": "100", "ask": "100.2", "mid": "100.1"}, "b": {"bid": "100.2", "ask": "100.4", "mid": "100.3"}}},
        "entry": {"legs": {"a": {"side": "buy", "filled_qty": "0.2", "avg_price": "100.2"}, "b": {"side": "sell", "filled_qty": "0.2", "avg_price": "100.2"}}},
        "exit": {"quotes_at_close": {"a": {"bid": "100.2", "ask": "100.4", "mid": "100.3"}, "b": {"bid": "100.0", "ask": "100.2", "mid": "100.1"}}, "legs": {"a": {"side": "sell", "filled_qty": "0.2", "avg_price": "100.2"}, "b": {"side": "buy", "filled_qty": "0.2", "avg_price": "100.2"}}},
    }
    out = calibrate([row], target_bps=__import__("decimal").Decimal("1"))
    assert out["venues"]["NVDA:a"]["samples"] == 1
    assert "NVDA:a" in out["recommended_trigger_bps"]
    assert out["fee_policy"].startswith("fees_reported_separately")
