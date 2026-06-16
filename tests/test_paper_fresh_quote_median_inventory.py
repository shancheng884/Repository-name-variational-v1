from decimal import Decimal
from types import SimpleNamespace

import pytest

from inventory_engine import DIRECTION_LONG_VAR_SHORT_LIGHTER
from tools.paper_fresh_quote_inventory import make_sample
from tools.paper_fresh_quote_median_inventory import MedianState, RollingMedian, median_signal, parse_windows, state_row


def _sample(*, ask: str, lighter_bid: str):
    return make_sample(
        snapshot=SimpleNamespace(
            logged_at="now",
            asset="BTC",
            bid=Decimal(lighter_bid),
            ask=Decimal("102"),
            buy_fill_price=Decimal("102"),
            sell_fill_price=Decimal(lighter_bid),
        ),
        quote_message={"ok": True, "result": {"quoteId": "q1", "bid": "100", "ask": ask, "quoteTimestamp": "ts"}},
        quote_ms=Decimal("12.5"),
    )


def test_parse_windows_requires_name_size_pairs() -> None:
    assert parse_windows("5m:300,1h:3600") == {"5m": 300, "1h": 3600}
    with pytest.raises(ValueError):
        parse_windows("5m")


def test_rolling_median_uses_bounded_window() -> None:
    window = RollingMedian(3)
    for value in [Decimal("1"), Decimal("2"), Decimal("100"), Decimal("4")]:
        window.add(value)

    assert window.count() == 3
    assert window.median() == Decimal("4")


def test_median_signal_waits_for_baseline_then_returns_deviation() -> None:
    state = MedianState({"base": 10})
    sample1 = _sample(ask="100", lighter_bid="101")
    sample2 = _sample(ask="100", lighter_bid="102")
    state.add(sample1)

    deviation, baseline, _, counts = median_signal(
        sample=sample1,
        median_state=state,
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        baseline_window="base",
        min_baseline_samples=2,
    )

    assert deviation is None
    assert baseline is not None
    assert counts["base"] == 1

    state.add(sample2)
    deviation, baseline, _, counts = median_signal(
        sample=sample2,
        median_state=state,
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        baseline_window="base",
        min_baseline_samples=2,
    )

    assert counts["base"] == 2
    assert baseline is not None
    assert deviation == sample2.long_edge_bps - baseline


def test_state_row_serializes_median_fields() -> None:
    state = MedianState({"base": 10})
    sample = _sample(ask="100", lighter_bid="101")
    state.add(sample)

    row = state_row(
        sample=sample,
        engine=SimpleNamespace(open_lots=lambda direction=None: 0, realized_pnl_usd=Decimal("0")),
        median_state=state,
        sample_index=0,
        events=[],
        baseline_window="base",
        min_baseline_samples=1,
    )

    assert row["event"] == "fresh_quote_median_inventory_paper_state"
    assert row["long_deviation_bps"] == "0"
    assert row["long_counts"] == {"base": 1}
