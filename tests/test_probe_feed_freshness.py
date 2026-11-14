from __future__ import annotations
import asyncio, logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

import pytest

from tools.market_capture.config import AVAILABLE_ASSETS, CaptureConfig
from tools.market_capture.hub import MarketHub
from tools.market_capture.storage import TapeWriter
from tools.market_capture.venues.entropy import EntropyBookFeed
from tools.market_capture.venues.lighter import LighterBookFeed


def ago(seconds: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def ms(seconds: float) -> int:
    return int(ago(seconds).timestamp() * 1000)


def build(tmp_path):
    writer = TapeWriter(tmp_path).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    return writer, hub


def test_entropy_feed_publishes_bbo_and_book(tmp_path):
    writer, hub = build(tmp_path)
    feed = EntropyBookFeed(
        info_url="https://x", ws_url="wss://x", dex="io",
        asset_symbols={a.name: a.symbol("entropy") for a in AVAILABLE_ASSETS},
        writer=writer, hub=hub, logger=logging.getLogger("t"),
    )
    feed.coin_to_asset = {"io:NVDA": "NVDA"}
    feed.handle({"channel": "l2Book", "data": {
        "coin": "io:NVDA", "time": ms(6),
        "levels": [[{"px": "100.0", "sz": "5"}], [{"px": "100.5", "sz": "5"}]],
    }})
    feed.handle({"channel": "bbo", "data": {
        "coin": "io:NVDA", "time": ms(0.4),
        "bbo": [{"px": "101.0", "sz": "3"}, {"px": "101.2", "sz": "4"}],
    }})
    quote = hub.latest("entropy", "NVDA", max_age=6.0)
    assert quote["ask"] == D("101.2")
    assert quote["quote_kind"] == "book+bbo"
    assert feed.bbo_messages == 1 and feed.book_messages == 1
    writer.close()


def test_entropy_print_invalidates_its_own_ladder(tmp_path):
    writer, hub = build(tmp_path)
    feed = EntropyBookFeed(
        info_url="https://x", ws_url="wss://x", dex="io",
        asset_symbols={a.name: a.symbol("entropy") for a in AVAILABLE_ASSETS},
        writer=writer, hub=hub, logger=logging.getLogger("t"),
    )
    feed.coin_to_asset = {"io:NVDA": "NVDA"}
    feed.handle({"channel": "bbo", "data": {
        "coin": "io:NVDA", "time": ms(3),
        "bbo": [{"px": "100.0", "sz": "3"}, {"px": "100.5", "sz": "4"}],
    }})
    feed.handle({"channel": "trades", "data": [
        {"coin": "io:NVDA", "px": "100.9", "sz": "1", "side": "B", "time": ms(1), "tid": 1},
    ]})
    quote = hub.latest("entropy", "NVDA", max_age=6.0)
    assert quote["provably_stale"] is True
    assert feed.trade_messages == 1
    writer.close()


def test_lighter_print_invalidates_its_own_ladder(tmp_path):
    writer, hub = build(tmp_path)
    feed = LighterBookFeed(
        name="lighter_rh", rest_url="https://x", ws_url="wss://x",
        asset_symbols={"NVDA": "NVDA"}, writer=writer, hub=hub, logger=logging.getLogger("t"),
    )
    feed.market_to_asset = {7: "NVDA"}
    from tools.market_capture.model import book_from_levels
    hub.update_book(book_from_levels(
        "lighter_rh", "NVDA", [{"price": "100.0", "size": "5"}], [{"price": "100.5", "size": "5"}],
        recv_at=ago(2), source_at=ago(2),
    ))
    feed._handle_trades("trade:7", {"trades": [
        {"trade_id": "t1", "price": "101.0", "size": "1", "timestamp": ms(1)},
    ]})
    quote = hub.latest("lighter_rh", "NVDA", max_age=6.0)
    assert quote["provably_stale"] is True
    writer.close()
