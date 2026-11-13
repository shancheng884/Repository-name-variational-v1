from datetime import datetime, timezone
from decimal import Decimal as D
import json

from tools.market_capture.config import DEFAULT_ASSETS, CaptureConfig
from tools.market_capture.hub import MarketHub
from tools.market_capture.model import ReferenceQuote
from tools.market_capture.storage import TapeWriter
from tools.market_capture.venues.var_indicative import VariationalIndicativeFeed


class FakeClient:
    def __init__(self, response):
        self.response = response
        self.payloads = []
    async def request(self, payload):
        self.payloads.append(payload)
        return self.response


def setup(tmp_path):
    writer = TapeWriter(tmp_path, flush_seconds=0.01).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    asset = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")
    hub.update_reference(ReferenceQuote("variational", "NVDA", D("169.38"), datetime.now(timezone.utc)))
    feed = VariationalIndicativeFeed(
        command_url="ws://x", assets=(asset,), notional_usd=D("20"),
        writer=writer, hub=hub, logger=__import__("logging").getLogger("t"),
    )
    return writer, hub, feed


def test_indicative_poll_records_exact_size_bbo_and_quota(tmp_path):
    writer, hub, feed = setup(tmp_path)
    client = FakeClient({
        "ok": True,
        "result": {
            "ok": True, "quoteId": "q-1", "bid": "169.39", "ask": "169.40",
            "quoteTimestamp": "2026-11-13T10:00:00.100Z",
            "rateLimitResetMs": 250,
        },
    })
    row = __import__("asyncio").run(feed.poll_one(client, feed.assets[0]))
    assert row["ok"] is True
    assert client.payloads[0]["type"] == "VAR_API_QUOTE"
    assert client.payloads[0]["amount"] == "0.11807769"
    latest = hub.latest("variational", "NVDA", max_age=5)
    assert latest["bid"] == D("169.39") and latest["ask"] == D("169.40")
    assert latest["quote_id"] == "q-1"
    assert latest["quote_kind"] == "variational_indicative"
    assert feed.public_state()["successes"] == 1
    assert feed.public_state()["last_rate_limit_reset_ms"] == 250
    writer.close()


def test_indicative_rejection_is_recorded_and_does_not_replace_quote(tmp_path):
    writer, hub, feed = setup(tmp_path)
    before = hub.references[("variational", "NVDA")]
    client = FakeClient({"ok": True, "result": {"ok": False, "error": "rate limit", "rateLimitResetMs": 1000}})
    row = __import__("asyncio").run(feed.poll_one(client, feed.assets[0]))
    assert row["ok"] is False
    assert feed.public_state()["rate_limited"] == 1
    assert hub.references[("variational", "NVDA")] == before
    writer.close()


def test_mid_stream_does_not_erase_fresh_indicative_bbo(tmp_path):
    writer, hub, feed = setup(tmp_path)
    now = datetime.now(timezone.utc)
    hub.update_reference(ReferenceQuote(
        "variational", "NVDA", D("169.395"), now,
        bid=D("169.39"), ask=D("169.40"), bbo_recv_at=now,
        quote_id="q-2", quote_kind="variational_indicative",
        notional_usd=D("20"), qty=D("0.118"),
    ))
    hub.update_reference(ReferenceQuote("variational", "NVDA", D("169.50"), now))
    latest = hub.latest("variational", "NVDA", max_age=5)
    assert latest["mid"] == D("169.50")
    assert latest["bid"] == D("169.39") and latest["ask"] == D("169.40")
    assert latest["quote_id"] == "q-2"
    writer.close()
