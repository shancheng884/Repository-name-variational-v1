from datetime import datetime, timezone
from decimal import Decimal as D

from tools.market_capture.config import CaptureConfig, DEFAULT_ASSETS
from tools.market_capture.hub import MarketHub
from tools.market_capture.storage import TapeWriter
from tools.market_capture.venues.variational import VariationalFillFeed, VariationalPriceFeed


def test_subscription_shape_and_reference_tick(tmp_path):
    writer = TapeWriter(tmp_path).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    feed = VariationalPriceFeed(
        ws_url="wss://x", assets=DEFAULT_ASSETS, writer=writer, hub=hub,
        logger=__import__("logging").getLogger("t"),
    )
    sub = feed._subscription()
    assert sub["action"] == "subscribe"
    assert len(sub["instruments"]) == 9
    assert sub["instruments"][0] == {
        "underlying": "OPENAI", "instrument_type": "perpetual_rwa_future",
        "settlement_asset": "USDC", "kind": "equity",
    }
    feed.handle({
        "channel": "instrument_price:P-RWA/EQY-NVDA-USDC",
        "pricing": {"price": "160.5", "timestamp": "2026-11-12T14:51:00.965428Z"},
    })
    quote = hub.references[("variational", "NVDA")]
    assert quote.mid == D("160.5")
    assert quote.source_at.year == 2026
    writer.close()


def test_variational_fill_normalization(tmp_path):
    writer = TapeWriter(tmp_path).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    from tools.market_capture.audit import FillAuditor
    auditor = FillAuditor(hub, writer)
    feed = VariationalFillFeed(
        command_url="ws://x", assets=DEFAULT_ASSETS, writer=writer,
        auditor=auditor, interval=5,
    )
    fill = feed._normalize({
        "order_id": "o1", "instrument": {"underlying": "NVDA"},
        "status": "cleared", "clearing_status": "success_trades_booked_into_pool",
        "side": "buy", "qty": "0.1", "price": "100",
        "execution_timestamp": "2026-11-12T14:51:00Z",
    })
    assert fill is not None and fill.symbol == "NVDA" and fill.side == "buy"
    assert feed._normalize({"status": "rejected", "instrument": {"underlying": "NVDA"},
                            "side": "buy", "qty": "1", "price": "1"}) is None
    writer.close()
