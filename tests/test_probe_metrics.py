from decimal import Decimal

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

def _round(**over):
    row = {
        "round_id": "r1", "run_id": "run-1", "asset": "NVDA", "buy_venue": "a", "sell_venue": "b",
        "status": "filled",
        "decision": {"signal_edge_bps": "20", "quotes": {
            "a": {"bid": "100", "ask": "100.2", "mid": "100.1"},
            "b": {"bid": "100.2", "ask": "100.4", "mid": "100.3"}}},
        "entry": {"legs": {
            "a": {"side": "buy", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01",
                  "source_at_epoch": 1000.0},
            "b": {"side": "sell", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01",
                  "source_at_epoch": 1000.2}}},
        "exit": {"quotes_at_close": {
            "a": {"bid": "100.2", "ask": "100.4", "mid": "100.3"},
            "b": {"bid": "100.0", "ask": "100.2", "mid": "100.1"}},
            "legs": {
                "a": {"side": "sell", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01"},
                "b": {"side": "buy", "filled_qty": "0.2", "avg_price": "100.2", "fee_usd": "0.01"}}},
    }
    row.update(over)
    return row


def test_hedge_delay_uses_executor_source_stamp():
    from decimal import Decimal
    out = decompose_round(_round())
    assert out["hedge_delay_ms"] == 200
    assert out["first_filled_venue"] == "a"
    assert out["complete"] is True


def test_hedge_delay_legacy_fill_stamp_still_supported():
    row = _round()
    row["entry"]["legs"]["a"].pop("source_at_epoch")
    row["entry"]["legs"]["a"]["fill_at_epoch"] = 1000.0
    assert decompose_round(row)["hedge_delay_ms"] == 200


def test_execution_quote_split_adds_up_without_double_counting():
    row = _round()
    row["entry"]["legs"]["a"]["execution_quote"] = {"bid": "100.25", "ask": "100.3"}
    out = decompose_round(row)
    leg = out["components"]["entry:a"]
    total = Decimal(leg["price_cost_bps"])
    parts = Decimal(leg["half_spread_bps"]) + Decimal(leg["signal_to_exec_bps"]) + Decimal(leg["exec_to_fill_bps"])
    assert leg["stage_split"] == "three_stage"
    assert abs(total - parts) < Decimal("0.0000001")
    assert Decimal(leg["signal_to_exec_bps"]) > 0


def test_missing_fee_is_reported_unverified_not_zero():
    row = _round()
    row["entry"]["legs"]["a"].pop("fee_usd")
    out = decompose_round(row)
    assert out["fees_status"] == "unverified"
    assert out["fees_usd"] == "0.03"
    assert out["fees_unverified_legs"] == ["entry:a"]


def test_venue_attribution_uses_own_legs_not_half_of_round_total():
    row = _round()
    row["entry"]["legs"]["a"]["avg_price"] = "100.21"   # cheap venue
    row["entry"]["legs"]["b"]["avg_price"] = "100.10"   # expensive venue: sells below bid
    out = decompose_round(row)
    a = Decimal(out["venue_roundtrip_cost_bps"]["a"])
    b = Decimal(out["venue_roundtrip_cost_bps"]["b"])
    assert b > a
    assert abs((a + b) - Decimal(out["total_price_cost_bps"])) < Decimal("0.000001")


def test_cost_distribution_excludes_incomplete_rounds():
    from decimal import Decimal
    rolled = _round(round_id="r2", status="rolled_back")
    rolled.pop("exit")
    out = calibrate([_round(), rolled], target_bps=Decimal("1"))
    assert out["complete_rounds"] == 1
    assert out["excluded_rounds"] == {"rolled_back": 1}
    assert out["venues"]["NVDA:a"]["samples"] == 1
    assert "NVDA:a->b" in out["recommended_pair_trigger_bps"]


def test_pair_trigger_covers_p80_total_cost_plus_target():
    from decimal import Decimal
    out = calibrate([_round()], target_bps=Decimal("1"))
    pair = out["pairs"]["NVDA:a->b"]
    assert pair["samples"] == 1
    assert Decimal(out["recommended_pair_trigger_bps"]["NVDA:a->b"]) == Decimal(pair["p80_total_cost_bps"]) + Decimal("1")

def test_stage_and_hedge_delay_aggregates_are_reported():
    out = calibrate([_round()])
    assert out["stages"]["half_spread"]["samples"] == 4
    assert out["stages"]["signal_to_exec"]["samples"] == 0
    assert {k: out["hedge_delay_ms"][k] for k in ("samples", "p50_ms", "p80_ms")} == {"samples": 1, "p50_ms": "200", "p80_ms": "200"}
    assert out["hedge_delay_ms"]["sources"] == {"exchange_raw": 1}

def test_adverse_selection_curve_is_aggregated():
    from tools.lib.probe_metrics import adverse_move
    row = _round()
    row["adverse"] = [
        {"delay_s": 0.25, "points": {"a": {"adverse_bps": "1.0"}, "b": {"adverse_bps": "-0.5"}}},
        {"delay_s": 3.0, "points": {"a": {"adverse_bps": "4.0"}, "b": {"adverse_bps": "2.0"}}},
    ]
    out = adverse_move(row)
    assert out["a"] == {"0.25": Decimal("1.0"), "3.0": Decimal("4.0")}
    result = calibrate([row])
    assert result["adverse_selection"]["by_horizon"]["NVDA:3.0s"]["p50_bps"] == "3.00"
    assert result["adverse_selection"]["by_venue"]["NVDA:a:0.25s"]["samples"] == 1

