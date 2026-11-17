import asyncio
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal as D
import json
import time

from tools.lib.entropy_fill_stream import EntropyFillStream
from tools.lib.nvda_quote_alignment import BASIS, QuoteClock, edges, seed_aligned_history
from tools.lib.nvda_tier_engine import PositionLedger, TierConfig, TierEngine, TierSpec
from tools.lib.nvda_fee_updates import finalize_costs, refresh_open_fees
from tests.test_nvda_tier_live import _EntropyAsset, _EntropyInfo, _entropy_executor


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).isoformat()


def quote(venue, source, received, bid, ask):
    return {"venue": venue, "symbol": "NVDA", "source_at": iso(source), "recv_at": iso(received),
            "bid": str(bid), "ask": str(ask), "executable_buy": str(ask), "executable_sell": str(bid)}


def test_alignment_uses_a_common_source_time_not_the_faster_venues_future_price():
    clock = QuoteClock()
    ent = quote("entropy", 1, 1.3, 100, 100.01)
    old_rh = quote("lighter_rh", 1, 1.02, 100.01, 100.02)
    rh = quote("lighter_rh", 1.2, 1.22, 101, 101.01)
    for row in (ent, old_rh, rh):
        clock.record(row)
    aligned = clock.align(ent, rh, now=1.3)
    assert aligned["right"]["source_at"] == iso(1)
    assert aligned["edges"]["forward"] == 0
    assert edges(ent, rh)["forward"] > 90
    assert aligned["basis"] == BASIS


def test_source_lookup_never_uses_a_quote_that_had_not_arrived():
    clock = QuoteClock()
    clock.record(quote("entropy", .9, 1, 99, 100))
    clock.record(quote("entropy", 1, 2, 100, 101))
    assert clock.as_of("entropy", "NVDA", 1, received_by=1.5)["bid"] == "99"


def test_a_faster_venues_updates_do_not_create_two_independent_aligned_samples():
    clock = QuoteClock()
    ent, rh = quote("entropy", 1, 1.3, 100, 100.01), quote("lighter_rh", 1, 1.05, 100, 100.01)
    clock.record(ent); clock.record(rh)
    first = clock.align(ent, rh, now=1.3)
    newer = quote("lighter_rh", 1.2, 1.3, 101, 101.01)
    clock.record(newer)
    assert clock.align(ent, newer, now=1.3)["version"] == first["version"]


def test_alignment_does_not_rewind_when_an_old_source_quote_arrives_late():
    clock = QuoteClock()
    clock.record(quote("entropy", 2, 2.2, 102, 103))
    clock.record(quote("entropy", 1, 2.3, 100, 101))
    assert clock.as_of("entropy", "NVDA", 2, received_by=3)["bid"] == "102"


def test_receiving_the_same_source_version_twice_does_not_confirm_twice():
    clock = QuoteClock()
    ent, rh = quote("entropy", 1, 1.3, 100, 100.01), quote("lighter_rh", 1, 1.1, 100, 100.01)
    for row in (ent, rh):
        clock.record(row)
    first = clock.align(ent, rh, now=1.4)
    repeated = {**ent, "recv_at": iso(1.5)}
    clock.record(repeated)
    assert clock.align(repeated, rh, now=1.5)["version"] == first["version"]


def test_quote_replay_is_causal_and_samples_at_most_once_per_second(tmp_path):
    for venue in ("entropy", "lighter_rh"):
        path = tmp_path / "quotes" / venue / "NVDA"
        path.mkdir(parents=True)
        rows = [quote(venue, t - .1, t, 100, 100.01) for t in (1, 1.1, 2, 2.1, 3, 3.1, 100)]
        (path / "hour.jsonl").write_text("\n".join(json.dumps(r) for r in rows))
    history = {k: deque() for k in ("forward", "reverse")}
    seed_aligned_history(history, tmp_path, ("entropy", "lighter_rh"), "NVDA", 30, now=4)
    assert len(history["forward"]) == 3
    assert all(t <= 4 for t, _ in history["forward"])


def test_calibrated_entry_still_requires_the_latest_raw_edge_to_be_available():
    cfg = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_rh", tiers=(TierSpec(1, 7.77),))
    engine = TierEngine(cfg, PositionLedger())
    assert not engine.on_sample(stamp=1, edge=9, raw_edge=1, baseline=0, price_hint=100,
                                 window=[9, 9], exchange={"entropy": 0, "lighter_rh": 0})


def test_legacy_position_keeps_its_original_exit_anchor():
    cfg = TierConfig(asset="NVDA", buy_venue="entropy", sell_venue="lighter_rh", tiers=(TierSpec(1, 7.77),))
    ledger = PositionLedger(); engine = TierEngine(cfg, ledger)
    engine.restore_state({"open_tiers": [{"index": 1, "qty": .2, "notional_usd": 20,
                                         "frozen_baseline": 0, "entry_edge": 10}]})
    actions = engine.on_sample(stamp=1, edge=10, raw_edge=0, baseline=5, price_hint=100,
                                window=[10, 10], exchange={"entropy": .2, "lighter_rh": -.2}, allow_entry=False)
    assert actions[0]["action"] == "close"
    assert engine.open_tiers[1].frozen_baseline == 0


def fill_row(oid="7", qty=".2", tid="1", fee=".01"):
    return {"oid": oid, "coin": "NVDA", "sz": qty, "px": "100", "fee": fee,
            "tid": tid, "time": 1794936078800, "side": "B"}


def test_stream_deduplicates_snapshots_and_waits_for_all_split_fills():
    stream = EntropyFillStream("wss://unused", "unused")
    packet = {"channel": "userFills", "data": {"isSnapshot": True, "fills": [fill_row(qty=".1")]}}
    stream.handle(packet); stream.handle(packet)
    assert stream.rows("7", "NVDA", D(".2")) is None
    stream.handle({"channel": "userFills", "data": {"fills": [fill_row(qty=".1", tid="2")]}})
    assert len(stream.rows("7", "NVDA", D(".2"))) == 2
    assert stream.rows("8", "NVDA") is None


def test_ack_and_cached_ws_fill_avoid_rest_polling():
    info = _EntropyInfo("0"); executor = _entropy_executor(info)
    stream = EntropyFillStream("unused", "unused")
    stream.handle({"channel": "userFills", "data": {"fills": [fill_row()]}})
    executor._fill_stream = stream
    executor.exchange.order = lambda *a, **k: {"response": {"data": {"statuses": [{"filled": {
        "totalSz": ".2", "avgPx": "100", "oid": 7}}]}}}
    info.user_fills_by_time = lambda *args: (_ for _ in ()).throw(AssertionError("REST must not run"))
    receipt = asyncio.run(executor.market_order(asset=_EntropyAsset(), side="buy", qty=D(".2"),
                                                quote={"bid": "99.9", "ask": "100"},
                                                client_order_id="ack-ws", known_position_before=D("0")))
    assert receipt["status"] == "filled"
    assert receipt["confirmation_source"] == "exchange_ack"
    assert receipt["fill_details_source"] == "user_fills_ws"
    assert receipt["fee_usd"] == "0.01"


def test_ws_fill_can_win_while_a_rest_call_is_slow():
    async def run():
        info = _EntropyInfo("0"); executor = _entropy_executor(info)
        executor.timeout_seconds = .5
        stream = EntropyFillStream("unused", "unused"); executor._fill_stream = stream
        executor.exchange.order = lambda *a, **k: {"response": {"data": {"statuses": [{"filled": {
            "totalSz": ".2", "avgPx": "100", "oid": 7}}]}}}
        def slow(*args):
            time.sleep(.2)
            return []
        info.user_fills_by_time = slow
        async def push():
            await asyncio.sleep(.01)
            stream.handle({"channel": "userFills", "data": {"fills": [fill_row()]}})
        task = asyncio.create_task(push())
        start = time.monotonic()
        receipt = await executor.market_order(asset=_EntropyAsset(), side="buy", qty=D(".2"),
                                               quote={"bid": "99.9", "ask": "100"},
                                               client_order_id="ws-first", known_position_before=D("0"))
        assert time.monotonic() - start < .15
        await task
        assert receipt["fill_details_source"] == "user_fills_ws"
    asyncio.run(run())


def test_missing_ack_fee_is_explicit_not_an_assumed_zero():
    executor = _entropy_executor(_EntropyInfo("0"))
    executor.exchange.order = lambda *a, **k: {"response": {"data": {"statuses": [{"filled": {
        "totalSz": ".2", "avgPx": "100", "oid": 7}}]}}}
    receipt = asyncio.run(executor.market_order(asset=_EntropyAsset(), side="buy", qty=D(".2"),
                                                quote={"bid": "99.9", "ask": "100"},
                                                client_order_id="missing-fee", known_position_before=D("0")))
    assert receipt["status"] == "filled"
    assert receipt["fee_usd"] is None and receipt["fee_pending"]
    assert receipt["source_at"] is None


def test_deferred_close_fee_is_finalized_exactly_once_and_survives_saved_stats():
    class Executor:
        def enrich_receipt(self, receipt, asset):
            return {**receipt, "fee_usd": ".01"}
    receipts = {"entropy": {"status": "filled", "filled_qty": ".2", "avg_price": "100", "fee_usd": None},
                "lighter_rh": {"status": "filled", "filled_qty": ".2", "avg_price": "100", "fee_usd": None}}
    stats = {"unresolved_cost_events": 1, "pending_costs": [{"category": "closed", "execution_token": "one",
             "buy": "entropy", "sell": "lighter_rh", "receipts": receipts, "recovery": {},
             "rebate_credited": ".011", "action": {"qty": .2, "notional_usd": 20,
             "entry_buy_price": 100, "entry_sell_price": 101, "entry_fee_usd": .01}}]}
    stats = json.loads(json.dumps(stats))
    realized, rebate, events = finalize_costs(stats, {"entropy": Executor()}, None, {})
    assert realized == D(".18") and rebate == D(".011")
    assert len(events) == 1 and not stats["pending_costs"]
    assert finalize_costs(stats, {"entropy": Executor()}, None, {})[:2] == (0, 0)


def test_bbo_sizes_and_depth_shortfall_are_audit_only(tmp_path):
    from tools.market_capture.config import CaptureConfig
    from tools.market_capture.hub import MarketHub
    from tools.market_capture.storage import TapeWriter
    from tools.market_capture.model import book_from_levels
    from tools.market_capture.venues.entropy import EntropyBookFeed
    import logging

    writer = TapeWriter(tmp_path).start()
    try:
        hub = MarketHub(writer, CaptureConfig(output_dir=tmp_path, quote_alignment=True))
        feed = EntropyBookFeed(info_url="unused", ws_url="unused", dex="io",
                               asset_symbols={"NVDA": "io:NVDA"}, writer=writer, hub=hub,
                               logger=logging.getLogger("test"), fast_depth=True)
        feed.coin_to_asset = {"io:NVDA": "NVDA"}
        at = datetime.now(timezone.utc)
        hub.update_book(book_from_levels("entropy", "NVDA", [["100", ".001"]], [["101", ".001"]],
                                          recv_at=at, source_at=at))
        feed.handle({"channel": "bbo", "data": {"coin": "io:NVDA", "time": int(at.timestamp()*1000),
                      "bbo": [{"px": "100", "sz": ".001"}, {"px": "101", "sz": ".002"}]}})
        snap = hub.latest("entropy", "NVDA")
        assert snap["executable_buy"] == D("101")
        audit = hub.depth_audit("entropy", "NVDA", D(".129"))
        assert audit["bid_covers_qty"] is False
        assert audit["sell_visible_qty_shortfall"] == "0.128"
        assert audit["audit_only"]
    finally:
        writer.close()
    rows = [json.loads(line) for path in (tmp_path / "quotes").rglob("*.jsonl")
            for line in path.read_text().splitlines()]
    assert next(r for r in rows if r.get("quote_kind") == "bbo")["ask_size"] == "0.002"


def test_fast_depth_uses_the_existing_book_channel_without_adding_a_second_subscription(tmp_path, monkeypatch):
    import logging
    import pytest
    from tools.market_capture.venues.entropy import EntropyBookFeed
    sent = []
    class Socket:
        async def send(self, payload):
            sent.append(json.loads(payload))
        def __aiter__(self):
            return self
        async def __anext__(self):
            raise asyncio.CancelledError
    class Connection:
        async def __aenter__(self):
            return Socket()
        async def __aexit__(self, *args):
            pass
    monkeypatch.setattr("tools.market_capture.venues.entropy.websockets.connect", lambda *a, **kw: Connection())
    feed = EntropyBookFeed(info_url="unused", ws_url="unused", dex="io", asset_symbols={},
                           writer=None, hub=None, logger=logging.getLogger("test"), fast_depth=True)
    feed.coin_to_asset = {"io:NVDA": "NVDA"}
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(feed.run())
    books = [r for r in sent if r["subscription"]["type"] == "l2Book"]
    assert len(books) == 1 and books[0]["subscription"]["fast"] is True
    assert any(r["subscription"]["type"] == "bbo" for r in sent)


def test_partial_fee_enrichment_waits_for_the_full_order_fill():
    executor = _entropy_executor(_EntropyInfo("0"))
    stream = EntropyFillStream("unused", "unused"); executor._fill_stream = stream
    receipt = {"order_id": "7", "filled_qty": ".1", "fee_usd": None, "fee_allocation_ratio": ".5"}
    stream.handle({"channel": "userFills", "data": {"fills": [fill_row(qty=".1", fee=".01")]}})
    assert executor.enrich_receipt(receipt, _EntropyAsset())["fee_usd"] is None
    stream.handle({"channel": "userFills", "data": {"fills": [fill_row(qty=".1", tid="2", fee=".01")]}})
    assert D(executor.enrich_receipt(receipt, _EntropyAsset())["fee_usd"]) == D(".01")
