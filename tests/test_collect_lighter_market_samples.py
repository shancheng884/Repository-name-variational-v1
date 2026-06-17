from decimal import Decimal

from tools.collect_lighter_market_samples import (
    MarketBook,
    apply_order_book_message,
    estimate_fill_price,
    market_sample_row,
)


def test_estimate_fill_price_uses_depth_for_notional() -> None:
    price = estimate_fill_price(
        {
            Decimal("100"): Decimal("0.1"),
            Decimal("101"): Decimal("0.2"),
        },
        side="BUY",
        notional_usd=Decimal("20"),
    )

    assert price is not None
    assert price > Decimal("100")
    assert price < Decimal("101")


def test_apply_order_book_message_routes_by_market_id() -> None:
    btc = MarketBook(asset="BTC", market_id=1)
    eth = MarketBook(asset="ETH", market_id=2)
    books = {1: btc, 2: eth}

    apply_order_book_message(
        books,
        {
            "type": "subscribed/order_book",
            "channel": "order_book/2",
            "order_book": {
                "market_id": 2,
                "offset": 10,
                "bids": [["2000", "1"]],
                "asks": [["2001", "1"]],
            },
        },
    )

    assert eth.ready is True
    assert btc.ready is False
    assert max(eth.bids) == Decimal("2000")
    assert min(eth.asks) == Decimal("2001")


def test_market_sample_row_serializes_lighter_fields() -> None:
    book = MarketBook(asset="SOL", market_id=3)
    book.bids[Decimal("150")] = Decimal("10")
    book.asks[Decimal("151")] = Decimal("10")
    book.ready = True

    row = market_sample_row(book, notional_usd=Decimal("20"))

    assert row is not None
    assert row["event"] == "market_sample"
    assert row["asset"] == "SOL"
    assert row["lighter_bid"] == "150"
    assert row["lighter_ask"] == "151"
    assert row["source"] == "lighter_multi_asset_collector"
