from __future__ import annotations

from decimal import Decimal

from tools.wti_cost_audit import build_report


def _execution(action_id, kind, *, missing_lighter_entry_fee=False):
    def leg(venue, side, reference, fill, fee):
        fee_confirmed = fee is not None
        return {
            "request": {
                "side": side,
                "qty": "0.29",
                "reference_price": reference,
            },
            "result": {
                "classification": "filled",
                "terminal_confirmed": True,
                "filled_qty": "0.29",
                "average_price": fill,
                "fee_usd": fee,
                "fee_confirmed": fee_confirmed,
            },
        }

    if kind == "open":
        lighter_fee = None if missing_lighter_entry_fee else "0.004"
        legs = {
            "variational": leg("variational", "buy", "70", "70.01", "0.001"),
            "lighter": leg("lighter", "sell", "70.3", "70.29", lighter_fee),
        }
    else:
        legs = {
            "variational": leg("variational", "sell", "70.2", "70.19", "0.002"),
            "lighter": leg("lighter", "buy", "70.1", "70.11", "0.003"),
        }
    return {"action_id": action_id, "kind": kind, "legs": legs}


def _state(*, missing_fee=True):
    return {
        "last_closed_position": {
            "entry_execution_id": "entry-1",
            "exit_execution_id": "exit-1",
            "direction": "long_var_short_lighter",
            "var_entry_price": "70",
            "lighter_entry_price": "70.3",
            "var_qty": "0.29",
            "lighter_qty": "0.29",
            "multiplier": "1",
            "layers": [{"entry_execution_id": "entry-1"}],
            "actual_gross_pnl_usd": "0.01",
            "actual_net_pnl_usd": None,
            "actual_net_pnl_status": "fees_unverified",
            "funding_cashflow_usd": None,
            "funding_cashflow_confirmed": False,
        },
        "execution_history": [
            _execution("entry-1", "open", missing_lighter_entry_fee=missing_fee),
            _execution("exit-1", "close"),
        ],
    }


def test_cost_audit_reports_slippage_without_double_subtracting_it():
    report = build_report(_state())
    costs = report["execution_cost_evidence"]

    assert report["status"] == "fills_verified_fees_incomplete"
    assert report["orders_attempted"] is False
    assert report["actual_gross_pnl_usd"] == "0.01"
    assert report["actual_fees_usd"] is None
    assert report["confirmed_fee_usd_partial"] == "0.006"
    assert report["pnl_after_confirmed_trading_fees_before_funding_usd"] is None
    assert costs["all_fills_verified"] is True
    assert costs["verified_filled_leg_count"] == 4
    assert costs["all_fees_verified_usd"] is False
    assert costs["expected_filled_leg_count"] == 4
    assert Decimal(costs["signed_slippage_usd"]) == Decimal("0.0116")
    assert costs["do_not_subtract_slippage_again"] is True
    assert report["actual_net_pnl_status"] == "fees_unverified"


def test_cost_audit_computes_pre_funding_pnl_only_with_complete_fee_evidence():
    report = build_report(_state(missing_fee=False))
    assert report["status"] == "fills_and_fees_verified"
    assert report["execution_cost_evidence"]["all_fees_verified_usd"] is True
    assert report["actual_fees_usd"] == "0.01"
    assert Decimal(report["pnl_after_confirmed_trading_fees_before_funding_usd"]) == 0
    assert report["actual_net_pnl_usd"] is None
    assert report["funding_cashflow_confirmed"] is False


def test_cost_audit_does_not_claim_complete_costs_when_action_record_is_missing():
    state = _state(missing_fee=False)
    state["execution_history"].pop()
    report = build_report(state)
    costs = report["execution_cost_evidence"]
    assert report["status"] == "partial_evidence"
    assert costs["all_fills_verified"] is False
    assert costs["all_fees_verified_usd"] is False
    assert costs["actual_fees_usd"] is None
    assert costs["action_record_missing_count"] == 1
