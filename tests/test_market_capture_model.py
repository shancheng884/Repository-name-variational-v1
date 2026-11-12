from datetime import datetime, timezone
from decimal import Decimal as D

from tools.market_capture.model import (
    Book, ReferenceQuote, adverse_bps, book_from_levels, edge_bps,
    executable_price, parse_epoch, spread_bps,
)


def test_executable_price_walks_depth_and_refuses_short_book():
    levels = [(D("100"), D("1")), (D("101"), D("1"))]
    assert executable_price(levels, "buy", D("150")) == D("150") / (D("1") + D("50") / D("101"))
    assert executable_price(levels, "buy", D("250")) is None
    assert executable_price(levels, "sell", D("50")) == D("101")


def test_spread_and_edge_are_bps():
    assert spread_bps(D("100"), D("101")) == D("99.50248756218905472636815920")
    assert edge_bps(D("100"), D("101")) == D("99.50248756218905472636815920")


def test_adverse_direction():
    assert adverse_bps("buy", D("100"), D("99")) == D("100")
    assert adverse_bps("sell", D("100"), D("101")) == D("100")
    assert adverse_bps("buy", D("100"), D("101")) == D("-100")


def test_parse_epoch_accepts_seconds_ms_us_ns_and_iso():
    base = 1_700_000_000
    assert int(parse_epoch(base).timestamp()) == base
    assert int(parse_epoch(base * 1000).timestamp()) == base
    assert int(parse_epoch(base * 1_000_000).timestamp()) == base
    assert int(parse_epoch(base * 1_000_000_000).timestamp()) == base
    assert parse_epoch("2026-11-12T14:51:00.965428Z").tzinfo is not None


def test_book_quote_row_keeps_executable_and_reference_separate():
    book = book_from_levels(
        "lighter_mainnet", "NVDA",
        [{"price": "100", "size": "2"}], [{"price": "100.5", "size": "2"}],
        recv_at=datetime.now(timezone.utc),
    )
    row = book.quote_row(notional=D("20"))
    assert row["book_available"] is True
    assert row["bid"] == "100" and row["ask"] == "100.5"
    assert row["executable_buy"] == "100.5"
    assert row["executable_sell"] == "100"
    assert row["spread_bps"] == "49.87531172069825436408977556"

    ref = ReferenceQuote("variational", "NVDA", D("100.2"), datetime.now(timezone.utc))
    ref_row = ref.quote_row()
    assert ref_row["book_available"] is False
    assert ref_row["mid"] == "100.2"
    assert ref_row["executable_buy"] is None
