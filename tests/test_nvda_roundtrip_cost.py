from tools.nvda_roundtrip_cost import percentile, roundtrip_slippage


def _row(event, **extra):
    row = {"event": event, "direction": "reverse", "tier": 1}
    row.update(extra)
    return row


def test_two_sided_signal_to_fill_slippage_is_measured_once():
    opened = _row(
        "opened",
        receipts={
            "lighter_rh": {"execution_quote": {"bid": "100.00", "ask": "100.10"},
                           "avg_price": "100.10"},
            "entropy": {"execution_quote": {"bid": "100.20", "ask": "100.30"},
                        "avg_price": "100.20"},
        },
    )
    closed = _row(
        "closed",
        receipts={
            "lighter_rh": {"execution_quote": {"bid": "99.90", "ask": "100.00"},
                           "avg_price": "99.90"},
            "entropy": {"execution_quote": {"bid": "100.00", "ask": "100.10"},
                        "avg_price": "100.10"},
        },
    )
    result = roundtrip_slippage([opened, closed])
    assert result["roundtrips"] == 1
    assert result["roundtrip_slip_bps"]["n"] == 1
    # Both fills are exactly at the executable touch: no slippage either side.
    assert abs(result["entry_slip_bps"]["p50"]) < 0.01
    assert abs(result["exit_slip_bps"]["p50"]) < 0.01


def test_percentile_handles_empty_and_small_samples():
    assert percentile([], 0.5) is None
    assert percentile([3.0], 0.8) == 3.0
    assert percentile([1.0, 2.0, 3.0, 4.0, 5.0], 0.5) == 3.0
