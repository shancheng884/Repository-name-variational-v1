from tools.nvda_roundtrip_cost import executable_close_price, percentile, roundtrip_slippage


def test_non_fifo_close_pairs_by_child_or_legacy_entry_order_ids_only():
    def make_open(token, rh_px):
        return _row("opened", execution_token=token, receipts={
            "lighter_rh": {"order_id": token + "-rh", "avg_price": rh_px,
                           "execution_quote": {"bid": "100", "ask": "100"}},
            "entropy": {"order_id": token + "-ent", "avg_price": "100",
                        "execution_quote": {"bid": "100", "ask": "100"}},
        })
    old, new = make_open("old", "100.5"), make_open("new", "100.02")
    close = _row("closed", child_id="new", receipts={
        v: {"avg_price": "100", "execution_quote": {"bid": "100", "ask": "100"}}
        for v in ("lighter_rh", "entropy")
    })
    assert round(roundtrip_slippage([old, new, close])["entry_slip_bps"]["p50"], 2) == 2.0
    assert roundtrip_slippage([old, dict(close, child_id="unknown")])["roundtrips"] == 0
    legacy = dict(close, child_id="legacy-1-1-1", entry_order_ids={"lighter_rh": "new-rh", "entropy": "new-ent"})
    assert round(roundtrip_slippage([old, new, legacy])["entry_slip_bps"]["p50"], 2) == 2.0


def test_residual_exit_uses_matched_qty_vwap_and_excludes_corrections():
    def receipt(bid, fill, book_at):
        return {"status": "filled", "filled_qty": ".2", "avg_price": fill,
                "execution_quote": {"bid": bid, "ask": "101", "ask_size": ".2", "quote_recv_at": "t1",
                                    "depth_audit": {"requested_qty": ".2", "book_recv_at": book_at,
                                                    "sell_visible_qty_shortfall": "0", "sell_vwap": "100"}}}
    rh = receipt("100.1", "100", "t1")
    ent = receipt("100.9", "101", "t1")
    closed = _row("closed", qty=".2", receipts={"lighter_rh": rh, "entropy": ent})
    assert roundtrip_slippage([closed])["exit_after_executable_book_bps"]["p80"] == 0
    assert roundtrip_slippage([dict(closed, settle={"corrections": [{}]})])[
        "exit_after_executable_book_bps"]["n"] == 0
    assert executable_close_price(receipt("100.1", "100", "older"), .2, "sell") is None


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
