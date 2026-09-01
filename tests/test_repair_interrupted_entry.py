from decimal import Decimal

from tools.repair_interrupted_entry import build_repaired_state


def test_build_repaired_state_promotes_verified_completed_entry() -> None:
    lot = {
        "lot_id": 1,
        "direction": "short_var_long_lighter",
        "qty": "0.0082",
        "entry_var_fill_price": "2418.88",
        "entry_lighter_fill_price": "2418.72",
    }
    context = {
        "variational_positions_result": {
            "result": {
                "positions": [
                    {
                        "position_info": {
                            "qty": "-0.0164",
                            "avg_entry_price": "2419.095",
                            "instrument": {"underlying": "ETH"},
                        }
                    }
                ]
            }
        },
        "lighter_account_result": {
            "accounts": [
                {
                    "positions": [
                        {
                            "symbol": "ETH",
                            "position": "0.0164",
                            "sign": 1,
                            "avg_entry_price": "2418.78",
                        }
                    ]
                }
            ]
        },
    }
    current = {
        "status": "manual_review_required",
        "manual_review_reason": "startup_reconcile_exchange_position_mismatch",
        "manual_review_context": context,
        "open_lots": [lot],
        "pending_actions": [],
        "next_lot_id": 3,
        "v4_episode_id": "episode-1",
        "v4_next_tranche_index": 2,
    }
    interrupted = {
        "run_id": "run-1",
        "pending_actions": [
            {
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "lighter_record_key": "lighter-2",
                "lot_id": 2,
                "qty": "0.0082",
                "role": "live_inventory_entry_pending_var_fill",
                "side": "sell",
                "submitted_at": "2026-09-01T18:00:22+00:00",
            }
        ]
    }
    rows = [
        {
            "event": "lighter_fill",
            "trade_key": "lighter-2",
            "lighter_filled_base_amount": "0.0082",
            "lighter_filled_price": "2418.85",
        },
        {
            "event": "live_inventory_var_entry_submitted",
            "run_id": "run-1",
            "lot_id": 2,
            "direction": "short_var_long_lighter",
            "qty": "0.0082",
            "basis_trace_id": "basis-2",
            "sample_index": 20,
            "edge_bps": "2",
            "v4_entry_threshold_bps": "1",
            "v4_real_gradient_active_tier": 2,
            "var_bid": "2419.20",
            "lighter_buy_price": "2418.85",
            "var_submit_ms": "1261.815",
            "lighter_submit_ms": "15.547",
        },
    ]

    repaired, summary = build_repaired_state(
        current=current,
        interrupted=interrupted,
        rows=rows,
        asset="ETH",
    )

    assert repaired["status"] == "open"
    assert repaired["pending_actions"] == []
    assert len(repaired["open_lots"]) == 2
    recovered = repaired["open_lots"][1]
    assert Decimal(recovered["entry_var_fill_price"]) == Decimal("2419.31")
    assert Decimal(recovered["entry_lighter_fill_price"]) == Decimal("2418.85")
    assert recovered["entry_gradient_tier"] == 2
    assert recovered["tranche_index"] == 2
    assert repaired["v4_next_tranche_index"] == 3
    assert "manual_review_reason" not in repaired
    assert summary["expected_total_qty"] == "0.0164"


def test_build_repaired_state_rejects_unmatched_exchange_quantity() -> None:
    current = {
        "status": "manual_review_required",
        "manual_review_reason": "startup_reconcile_exchange_position_mismatch",
        "manual_review_context": {
            "variational_positions_result": {
                "result": {
                    "positions": [
                        {
                            "position_info": {
                                "qty": "-0.0246",
                                "avg_entry_price": "2419",
                                "instrument": {"underlying": "ETH"},
                            }
                        }
                    ]
                }
            },
            "lighter_account_result": {
                "accounts": [
                    {
                        "positions": [
                            {
                                "symbol": "ETH",
                                "position": "0.0164",
                                "sign": 1,
                                "avg_entry_price": "2418.78",
                            }
                        ]
                    }
                ]
            },
        },
        "open_lots": [
            {
                "lot_id": 1,
                "direction": "short_var_long_lighter",
                "qty": "0.0082",
                "entry_var_fill_price": "2418.88",
                "entry_lighter_fill_price": "2418.72",
            }
        ],
        "pending_actions": [],
    }
    interrupted = {
        "run_id": "run-1",
        "pending_actions": [
            {
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "lighter_record_key": "lighter-2",
                "lot_id": 2,
                "qty": "0.0082",
                "role": "live_inventory_entry_pending_var_fill",
            }
        ]
    }

    try:
        build_repaired_state(
            current=current,
            interrupted=interrupted,
            rows=[],
            asset="ETH",
        )
    except ValueError as exc:
        assert "Variational quantity" in str(exc)
    else:
        raise AssertionError("repair should reject a third unmatched exchange lot")
