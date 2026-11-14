from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

import pytest

from tools.market_capture.config import CaptureConfig
from tools.market_capture.hub import MarketHub
from tools.market_capture.model import book_from_levels
from tools.market_capture.storage import TapeWriter
from tools.lib.probe_metrics import adverse_move, calibrate, leg_timing
from tools.lib.venue_probe import ProbeRunner, ProbeState


def ago(seconds: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def make_hub(tmp_path):
    writer = TapeWriter(tmp_path).start()
    return writer, MarketHub(writer, CaptureConfig(output_dir=tmp_path))


def add_book(hub, *, bid="100.0", ask="100.5", at=None):
    at = at or ago(6)
    hub.update_book(book_from_levels(
        "entropy", "NVDA",
        [{"px": bid, "sz": "5"}], [{"px": ask, "sz": "5"}],
        recv_at=at, source_at=at,
    ))


def test_entropy_bbo_overlays_an_older_ladder(tmp_path):
    """l2Book lands every ~7-12s on Hyperliquid; bbo is the live top of book."""
    writer, hub = make_hub(tmp_path)
    add_book(hub, bid="100.0", ask="100.5", at=ago(6))
    hub.update_bbo(venue="entropy", symbol="NVDA", bid=D("101.0"), ask=D("101.2"),
                   recv_at=ago(0.5), source_at=ago(0.6))
    quote = hub.latest("entropy", "NVDA", max_age=6.0)
    assert quote["bid"] == D("101.0") and quote["ask"] == D("101.2")
    assert quote["executable_buy"] == D("101.2")
    assert quote["quote_kind"] == "book+bbo"
    assert quote["book_age_s"] > 5.0
    assert quote["age_s"] < 1.0
    writer.close()


def test_older_bbo_never_replaces_a_fresher_ladder(tmp_path):
    writer, hub = make_hub(tmp_path)
    hub.update_bbo(venue="entropy", symbol="NVDA", bid=D("100.0"), ask=D("100.5"),
                   recv_at=ago(5), source_at=ago(5))
    add_book(hub, bid="99.0", ask="99.5", at=ago(1))
    quote = hub.latest("entropy", "NVDA", max_age=6.0)
    assert quote["bid"] == D("99.0") and quote["ask"] == D("99.5")
    assert quote["quote_kind"] == "book"
    writer.close()


def test_trade_through_the_top_proves_the_quote_stale(tmp_path):
    """A quiet book is not a stale book; a print through the top is proof."""
    writer, hub = make_hub(tmp_path)
    add_book(hub, bid="100.0", ask="100.5", at=ago(3))
    hub.update_trade(venue="entropy", symbol="NVDA", price=D("101.4"), at=ago(1))
    quote = hub.latest("entropy", "NVDA", max_age=6.0)
    assert quote["provably_stale"] is True
    assert quote["staleness_evidence"]["reason"] == "trade_above_ask"
    assert quote["staleness_evidence"]["trade_price"] == "101.4"
    writer.close()


def test_trade_inside_the_spread_leaves_the_quote_usable(tmp_path):
    writer, hub = make_hub(tmp_path)
    add_book(hub, bid="100.0", ask="100.5", at=ago(3))
    hub.update_trade(venue="entropy", symbol="NVDA", price=D("100.2"), at=ago(1))
    assert hub.latest("entropy", "NVDA", max_age=6.0)["provably_stale"] is False
    writer.close()


def test_print_before_the_snapshot_cannot_invalidate_it(tmp_path):
    writer, hub = make_hub(tmp_path)
    hub.update_trade(venue="entropy", symbol="NVDA", price=D("105.0"), at=ago(5))
    add_book(hub, bid="100.0", ask="100.5", at=ago(2))
    assert hub.latest("entropy", "NVDA", max_age=6.0)["provably_stale"] is False
    writer.close()


class FakeHub:
    def __init__(self, quote):
        self.quote = quote

    def latest(self, venue, symbol, *, max_age=10.0):
        return dict(self.quote) if self.quote else None


class Asset:
    name = "NVDA"


def make_runner(tmp_path, quote):
    return ProbeRunner(
        hub=FakeHub(quote), executors={}, state=ProbeState(tmp_path / "state.json"),
        writer_path=tmp_path / "rounds.jsonl", assets=(), venues=(),
    )


def test_probe_refuses_to_trade_a_disproved_ladder(tmp_path):
    runner = make_runner(tmp_path, {
        "bid": "100.0", "ask": "100.5", "mid": "100.25", "age_s": 0.2,
        "provably_stale": True, "staleness_evidence": {"reason": "trade_above_ask"},
    })
    assert asyncio.run(runner._quote("entropy", Asset())) is None
    assert runner._last_quote_reject["reason"] == "quote_provably_stale"


def test_adverse_sampling_can_still_read_a_stale_quote(tmp_path):
    runner = make_runner(tmp_path, {
        "bid": "100.0", "ask": "100.5", "mid": "100.25", "age_s": 0.2, "provably_stale": True,
    })
    assert asyncio.run(runner._quote("entropy", Asset(), allow_stale=True)) is not None


def build_row(*, point_quote_at, valid=None):
    fill_at = ago(2)
    point = {
        "mid": "100.25",
        "adverse_bps": "1.5",
        "quote_at": point_quote_at.isoformat(),
        "quote_age_s": 0.1,
    }
    if valid is not None:
        point["valid"] = valid
    legs = {
        "entropy": {"completed_at": fill_at.isoformat(), "side": "buy", "avg_price": "100.5",
                    "filled_qty": "0.2", "submitted_at": fill_at.isoformat(), "acked_at": fill_at.isoformat()},
        "lighter_mainnet": {"completed_at": fill_at.isoformat(), "side": "sell", "avg_price": "100.4",
                            "filled_qty": "0.2", "submitted_at": fill_at.isoformat(), "acked_at": fill_at.isoformat()},
    }
    exit_legs = {
        "entropy": {"completed_at": ago(1).isoformat(), "side": "sell", "avg_price": "100.4",
                    "filled_qty": "0.2", "submitted_at": ago(1).isoformat(), "acked_at": ago(1).isoformat()},
        "lighter_mainnet": {"completed_at": ago(1).isoformat(), "side": "buy", "avg_price": "100.5",
                            "filled_qty": "0.2", "submitted_at": ago(1).isoformat(), "acked_at": ago(1).isoformat()},
    }
    quotes = {
        "entropy": {"bid": "100.0", "ask": "100.5", "mid": "100.25", "age_s": "0.2"},
        "lighter_mainnet": {"bid": "99.9", "ask": "100.4", "mid": "100.15", "age_s": "0.2"},
    }
    return {
        "schema": 2, "event": "probe_round", "status": "filled",
        "asset": "NVDA", "buy_venue": "entropy", "sell_venue": "lighter_mainnet",
        "decision": {"quotes": quotes, "signal_edge_bps": "1"},
        "entry": {"legs": legs, "quotes": quotes},
        "exit": {"legs": exit_legs, "quotes_at_close": quotes},
        "adverse": [{"delay_s": 0.25, "points": {"entropy": point}}],
    }


def test_markout_from_a_quote_older_than_the_fill_is_dropped():
    stale_row = build_row(point_quote_at=ago(5))
    assert adverse_move(stale_row) == {}
    fresh_row = build_row(point_quote_at=ago(1.75))
    assert adverse_move(fresh_row)["entropy"]["0.25"] == D("1.5")


def test_explicit_invalid_flag_wins_over_timestamps():
    row = build_row(point_quote_at=ago(1), valid=False)
    assert adverse_move(row) == {}


def test_calibration_counts_the_markouts_it_had_to_drop():
    result = calibrate([
        build_row(point_quote_at=ago(5)),
        build_row(point_quote_at=ago(1.75)),
    ])
    assert result["adverse_samples_rejected"] == 1
    assert result["adverse_selection"]["by_horizon"]["NVDA:0.25s"]["samples"] == 1


def test_second_resolution_fill_stamp_yields_a_window_not_a_point():
    base = ago(3)
    leg = {
        "submitted_at": base.isoformat(),
        "acked_at": (base + timedelta(milliseconds=70)).isoformat(),
        "completed_at": (base + timedelta(milliseconds=800)).isoformat(),
        "source_at": base.isoformat(),
        "timestamp_resolution_ms": 1000,
        "fill_window": {"resolution_ms": 1000, "lower_ms": 70.0, "upper_ms": 800.0, "capped_by_poll": False},
    }
    timing = leg_timing(leg)
    assert timing["timestamp_resolution_ms"] == 1000
    assert timing["exchange_fill_to_local_ms"] is None
    assert timing["fill_latency_bounds_ms"]["lower_ms"] == 70.0
    assert timing["fill_latency_bounds_ms"]["upper_ms"] == 800.0


def test_fine_resolution_keeps_the_point_estimate():
    base = ago(3)
    leg = {
        "submitted_at": base.isoformat(),
        "acked_at": (base + timedelta(milliseconds=70)).isoformat(),
        "completed_at": (base + timedelta(milliseconds=120)).isoformat(),
        "source_at": (base + timedelta(milliseconds=80)).isoformat(),
        "timestamp_resolution_ms": 1,
        "clock_offset_ms": 0,
        "fill_window": {"resolution_ms": 1, "lower_ms": 80.0, "upper_ms": 120.0},
    }
    timing = leg_timing(leg)
    assert timing["exchange_fill_to_local_ms"] == 40.0
    assert timing["submit_to_exchange_fill_ms"] == 80.0
