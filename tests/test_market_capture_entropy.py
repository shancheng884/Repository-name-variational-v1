from datetime import datetime, timezone
from decimal import Decimal as D

from tools.market_capture.config import CaptureConfig, DEFAULT_ASSETS
from tools.market_capture.hub import MarketHub
from tools.market_capture.storage import TapeWriter
from tools.market_capture.venues.entropy import EntropyBookFeed, EntropyFillFeed


class FakeResponse:
    def __init__(self, payload): self.payload = payload
    def raise_for_status(self): return None
    def json(self): return self.payload


def test_entropy_universe_and_books(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "tools.market_capture.venues.entropy.requests.post",
        lambda *a, **k: FakeResponse({"universe": [
            {"name": "io:OAI"}, {"name": "io:ANTH"}, {"name": "io:NVDA"},
        ]}),
    )
    writer = TapeWriter(tmp_path).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    feed = EntropyBookFeed(
        info_url="https://x", ws_url="wss://x", dex="io",
        asset_symbols={a.name: a.symbol("entropy") for a in DEFAULT_ASSETS},
        writer=writer, hub=hub, logger=__import__("logging").getLogger("t"),
    )
    import asyncio
    asyncio.run(feed.load_universe())
    assert feed.coin_to_asset["io:OAI"] == "OPENAI"
    feed.handle({"channel": "l2Book", "data": {
        "coin": "io:NVDA", "time": 1700000000000,
        "levels": [[{"px": "100", "sz": "2"}], [{"px": "100.5", "sz": "3"}]],
    }})
    book = hub.books[("entropy", "NVDA")]
    assert book.best_bid == D("100") and book.best_ask == D("100.5")
    writer.close()


def test_entropy_fill_normalization(tmp_path):
    writer = TapeWriter(tmp_path).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    from tools.market_capture.audit import FillAuditor
    auditor = FillAuditor(hub, writer)
    feed = EntropyFillFeed(
        info_url="https://x", dex="io",
        asset_symbols={a.name: a.symbol("entropy") for a in DEFAULT_ASSETS},
        writer=writer, auditor=auditor, account_address="0x" + "1" * 40,
    )
    fill = feed._normalize({"coin": "io:NVDA", "side": "B", "px": "100", "sz": "1",
                            "time": 1700000000000, "fee": "0.01", "tid": 9})
    assert fill.side == "buy" and fill.symbol == "NVDA" and fill.fee_usd == D("0.01")
    assert feed._normalize({"coin": "io:OTHER", "side": "B", "px": "1", "sz": "1"}) is None
    writer.close()
