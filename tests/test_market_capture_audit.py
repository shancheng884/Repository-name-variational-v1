from datetime import datetime, timezone
from decimal import Decimal as D

from tools.market_capture.audit import FillAuditor
from tools.market_capture.config import CaptureConfig
from tools.market_capture.hub import MarketHub
from tools.market_capture.model import Fill, ReferenceQuote, book_from_levels
from tools.market_capture.storage import TapeWriter


def setup_hub(tmp_path):
    writer = TapeWriter(tmp_path).start()
    hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path))
    now = datetime.now(timezone.utc)
    hub.update_book(book_from_levels("lighter_mainnet", "NVDA",
        [{"price": "100", "size": "10"}], [{"price": "100.2", "size": "10"}], recv_at=now))
    hub.update_book(book_from_levels("entropy", "NVDA",
        [{"price": "100.3", "size": "10"}], [{"price": "100.4", "size": "10"}], recv_at=now))
    hub.update_reference(ReferenceQuote("variational", "NVDA", D("100.1"), now))
    return writer, hub


def test_fill_chain_has_slippage_hedge_and_adverse(tmp_path):
    writer, hub = setup_hub(tmp_path)
    auditor = FillAuditor(hub, writer, delays=(0.25, 1.0))
    fill = Fill("lighter_mainnet", "NVDA", "buy", D("1"), D("100.2"),
                datetime.now(timezone.utc), source_at=datetime.now(timezone.utc))
    chain = auditor.start_chain(fill)
    assert D(chain["fill_vs_mid_bps"]) > 0
    assert chain["hedge_at_fill"]["venue"] == "entropy"
    assert D(chain["hedge_at_fill"]["edge_bps"]) > 0
    auditor.capture_horizon(chain, fill, 0.25)
    auditor.capture_horizon(chain, fill, 1.0)
    summary = auditor.summarize(chain)
    assert summary["hedge_venue"] == "entropy"
    assert summary["worst_adverse_bps"] is not None
    writer.close()


def test_adverse_is_positive_when_mark_moves_against_buy(tmp_path):
    writer, hub = setup_hub(tmp_path)
    auditor = FillAuditor(hub, writer)
    fill = Fill("lighter_mainnet", "NVDA", "buy", D("1"), D("100.2"),
                datetime.now(timezone.utc))
    chain = auditor.start_chain(fill)
    now = datetime.now(timezone.utc)
    hub.update_book(book_from_levels("lighter_mainnet", "NVDA",
        [{"price": "99", "size": "10"}], [{"price": "99.2", "size": "10"}], recv_at=now))
    auditor.capture_horizon(chain, fill, 0.25)
    point = chain["horizons"][0]["points"][0]
    assert D(point["adverse_bps"]) > 0
    writer.close()
