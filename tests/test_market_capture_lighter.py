from datetime import datetime, timezone
from decimal import Decimal as D
import json

from tools.market_capture.config import CaptureConfig, DEFAULT_ASSETS
from tools.market_capture.hub import MarketHub
from tools.market_capture.storage import TapeWriter
from tools.market_capture.venues.lighter import LighterBookFeed, LighterFillFeed


class FakeResponse:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


def make_feed(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "tools.market_capture.venues.lighter.requests.get",
        lambda *a, **k: FakeResponse({"order_books": [
            {"symbol": "NVDA", "market_id": 15, "multiplier": "1"},
            {"symbol": "OPENAI", "market_id": 42, "multiplier": "1"},
        ]}),
    )
    writer = TapeWriter(tmp_path).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    feed = LighterBookFeed(
        name="lighter_mainnet", rest_url="https://x", ws_url="wss://x",
        asset_symbols={a.name: a.symbol("lighter_mainnet") for a in DEFAULT_ASSETS},
        writer=writer, hub=hub, logger=__import__("logging").getLogger("t"),
    )
    return feed, hub, writer


def test_load_markets_and_apply_snapshot(tmp_path, monkeypatch):
    feed, hub, writer = make_feed(tmp_path, monkeypatch)
    import asyncio
    asyncio.run(feed.load_markets())
    assert feed.markets["NVDA"]["market_id"] == 15
    assert feed.market_to_asset[42] == "OPENAI"
    message = {
        "type": "subscribed/order_book", "channel": "order_book:15",
        "order_book": {"bids": [{"price": "100", "size": "2"}],
                       "asks": [{"price": "100.5", "size": "3"}],
                       "nonce": 10, "begin_nonce": 10},
        "timestamp": 1700000000000,
    }
    assert feed._handle(message) is False
    book = hub.books[("lighter_mainnet", "NVDA")]
    assert book.best_bid == D("100") and book.best_ask == D("100.5")
    assert book.sequence == 10
    writer.close()


def test_sequence_gap_requests_resubscribe(tmp_path, monkeypatch):
    feed, hub, writer = make_feed(tmp_path, monkeypatch)
    import asyncio
    asyncio.run(feed.load_markets())
    feed._handle({"type": "subscribed/order_book", "channel": "order_book:15",
                  "order_book": {"bids": [{"price": "100", "size": "1"}],
                                 "asks": [{"price": "101", "size": "1"}],
                                 "nonce": 10, "begin_nonce": 10}})
    gap = feed._handle({"type": "update/order_book", "channel": "order_book:15",
                        "order_book": {"bids": [{"price": "100", "size": "1"}],
                                       "asks": [{"price": "101", "size": "1"}],
                                       "nonce": 12, "begin_nonce": 12}})
    assert gap is True
    assert feed.books[15]["ready"] is False
    assert "sequence_gap" in feed.health.last_error
    writer.close()


def test_own_fill_side_and_dedupe(tmp_path, monkeypatch):
    feed, hub, writer = make_feed(tmp_path, monkeypatch)
    import asyncio
    asyncio.run(feed.load_markets())
    from tools.market_capture.audit import FillAuditor
    auditor = FillAuditor(hub, writer)
    fills = LighterFillFeed(
        name="lighter_mainnet", rest_url="https://x",
        asset_symbols={"NVDA": "NVDA"}, markets={"NVDA": feed.markets["NVDA"]},
        writer=writer, auditor=auditor, account_index=7, api_key_index=3,
        private_key="secret", interval=2,
    )
    row = {
        "market_id": 15, "price": "100", "size": "1",
        "ask_account_id": 7, "bid_account_id": 9, "trade_id": 55,
        "timestamp": 1700000000000,
    }
    fill = fills._normalize(row)
    assert fill.side == "sell" and fill.symbol == "NVDA" and fill.trade_id == "55"
    row["ask_account_id"] = 9
    row["bid_account_id"] = 7
    assert fills._normalize(row).side == "buy"
    row["ask_account_id"] = 9
    row["bid_account_id"] = 9
    assert fills._normalize(row) is None
    writer.close()


def test_old_fills_are_ignored_after_restart(tmp_path, monkeypatch):
    feed, hub, writer = make_feed(tmp_path, monkeypatch)
    import asyncio
    asyncio.run(feed.load_markets())
    from tools.market_capture.audit import FillAuditor
    from datetime import datetime, timezone
    auditor = FillAuditor(hub, writer)
    cutoff = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
    fills = LighterFillFeed(
        name="lighter_mainnet", rest_url="https://x",
        asset_symbols={"NVDA": "NVDA"}, markets={"NVDA": feed.markets["NVDA"]},
        writer=writer, auditor=auditor, account_index=7, api_key_index=3,
        private_key="secret", interval=2, accept_after=cutoff,
    )
    old = {"market_id": 15, "price": "100", "size": "1", "ask_account_id": 7,
           "trade_id": 1, "timestamp": 1700000000000}
    assert fills._normalize(old) is None
    new = {**old, "trade_id": 2, "timestamp": int(datetime(2026, 11, 12, 15, 1, tzinfo=timezone.utc).timestamp() * 1000)}
    assert fills._normalize(new) is not None
    writer.close()
