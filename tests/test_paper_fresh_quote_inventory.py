from decimal import Decimal
from types import SimpleNamespace

from inventory_engine import DIRECTION_LONG_VAR_SHORT_LIGHTER, PaperInventoryEngine
from tools.paper_fresh_quote_inventory import make_sample, sample_prices, state_row


def _sample(*, bid: str, ask: str, lighter_bid: str = "101", lighter_ask: str = "102"):
    return make_sample(
        snapshot=SimpleNamespace(
            logged_at="now",
            asset="BTC",
            bid=Decimal(lighter_bid),
            ask=Decimal(lighter_ask),
            buy_fill_price=Decimal(lighter_ask),
            sell_fill_price=Decimal(lighter_bid),
        ),
        quote_message={"ok": True, "result": {"quoteId": "q1", "bid": bid, "ask": ask, "quoteTimestamp": "ts"}},
        quote_ms=Decimal("12.5"),
    )


def test_make_sample_computes_fresh_edges() -> None:
    sample = _sample(bid="100", ask="100.5")

    assert sample is not None
    assert sample.var_bid == Decimal("100")
    assert sample.var_ask == Decimal("100.5")
    assert sample.long_edge_bps > 0
    assert sample.short_edge_bps < 0


def test_inventory_engine_enters_and_exits_from_fresh_samples() -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("20"),
        max_lots=3,
        max_total_lots=3,
        entry_bps=Decimal("3"),
        exit_bps=Decimal("1"),
        min_hold_samples=1,
    )
    entry = _sample(bid="100", ask="100.5", lighter_bid="101", lighter_ask="102")
    edge, var_entry, lighter_entry, var_exit, lighter_exit = sample_prices(entry, DIRECTION_LONG_VAR_SHORT_LIGHTER)

    events = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=edge,
        var_entry_price=var_entry,
        lighter_entry_price=lighter_entry,
        var_exit_price=var_exit,
        lighter_exit_price=lighter_exit,
        logged_at=entry.logged_at,
        sample_index=0,
    )

    assert [event.event for event in events] == ["inventory_paper_entered"]
    assert engine.open_lots() == 1

    exit_sample = _sample(bid="101.2", ask="101.3", lighter_bid="101", lighter_ask="101.1")
    edge, var_entry, lighter_entry, var_exit, lighter_exit = sample_prices(exit_sample, DIRECTION_LONG_VAR_SHORT_LIGHTER)
    events = engine.on_sample(
        direction=DIRECTION_LONG_VAR_SHORT_LIGHTER,
        edge_bps=edge,
        var_entry_price=var_entry,
        lighter_entry_price=lighter_entry,
        var_exit_price=var_exit,
        lighter_exit_price=lighter_exit,
        logged_at=exit_sample.logged_at,
        sample_index=1,
    )

    assert [event.event for event in events] == ["inventory_paper_exited"]
    assert engine.open_lots() == 0
    assert engine.realized_pnl_usd > 0


def test_state_row_serializes_decimal_fields() -> None:
    engine = PaperInventoryEngine(
        lot_notional_usd=Decimal("20"),
        max_lots=3,
        max_total_lots=3,
        entry_bps=Decimal("3"),
        exit_bps=Decimal("1"),
        min_hold_samples=1,
    )
    sample = _sample(bid="100", ask="100.5")

    row = state_row(sample=sample, engine=engine, sample_index=0, events=[])

    assert row["event"] == "fresh_quote_inventory_paper_state"
    assert row["best_direction"] == "long_var_short_lighter"
    assert row["open_lots"] == 0
    assert isinstance(row["best_edge_bps"], str)
