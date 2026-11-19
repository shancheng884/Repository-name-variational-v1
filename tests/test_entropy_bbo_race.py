import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from tools.market_capture.venues.entropy import EntropyBookFeed


def _feed(symbol="NVDA", *, race_bbo=True):
    coin = f"io:{symbol}"
    feed = EntropyBookFeed(info_url="unused", ws_url="wss://unused", dex="io",
                           asset_symbols={symbol: coin}, writer=Mock(), hub=Mock(),
                           logger=logging.getLogger(__name__), fast_depth=True, race_bbo=race_bbo)
    feed.coin_to_asset[coin] = symbol
    return feed


def _bbo(at, *, coin="io:NVDA", bid="100", size="2"):
    return {"channel": "bbo", "data": {"coin": coin, "time": int(at.timestamp() * 1000),
            "bbo": [{"px": bid, "sz": size}, {"px": "101", "sz": "3"}]}}


def test_first_valid_quote_wins_and_late_copies_do_not_refresh_it():
    feed = _feed("SNDK")
    now = datetime.now(timezone.utc)
    msg = _bbo(now, coin="io:SNDK")
    feed.handle(msg, lane="backup", recv_at=now)
    later = now + timedelta(milliseconds=30)
    feed.handle(msg, lane="primary", recv_at=later)
    feed.handle(_bbo(now, coin="io:SNDK", size="3"), lane="primary", recv_at=later)
    feed.handle(_bbo(now - timedelta(seconds=1), coin="io:SNDK"), lane="primary", recv_at=later)
    assert feed.hub.update_bbo.call_count == 1
    assert feed.hub.update_bbo.call_args.kwargs["recv_at"] == now
    assert feed.hub.update_bbo.call_args.kwargs["symbol"] == "SNDK"
    assert feed.health.last_quote_at == now
    assert feed.bbo_drops == {"duplicate": 1, "older": 1, "conflict": 1, "invalid": 0}
    feed.handle(_bbo(later, coin="io:SNDK", size="3"), lane="primary", recv_at=later)
    assert feed.hub.update_bbo.call_count == 2
    assert feed.bbo_wins == {"primary": 1, "backup": 1}


def test_unusable_frame_cannot_win_or_freeze_the_source_watermark():
    feed = _feed()
    now = datetime.now(timezone.utc)
    for msg in (_bbo(now + timedelta(seconds=1)), _bbo(now - timedelta(seconds=7)),
                _bbo(now, bid="102"), _bbo(now, size="0")):
        feed.handle(msg, lane="backup", recv_at=now)
    feed.handle(_bbo(now), recv_at=now)
    assert feed.bbo_drops["invalid"] == 4
    assert feed.bbo_wins == {"primary": 1, "backup": 0}
    assert feed.hub.update_bbo.call_count == 1


def test_default_single_lane_keeps_legacy_capture_behavior():
    feed = _feed(race_bbo=False)
    now = datetime.now(timezone.utc)
    feed.handle(_bbo(now - timedelta(seconds=7)), recv_at=now)
    assert feed.hub.update_bbo.call_count == 1
    assert feed.backup_health is None


def test_only_bbo_is_duplicated_and_primary_keeps_running_after_backup_drops(monkeypatch):
    async def scenario():
        feed = _feed()
        ws_lanes = []
        now = datetime.now(timezone.utc) - timedelta(milliseconds=100)
        backup_sent = asyncio.Event()
        primary_sent = asyncio.Event()

        class Socket:
            def __init__(self):
                self.subs = []
                self.closed = False
                ws_lanes.append(self)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                self.closed = True

            async def send(self, raw):
                self.subs.append(json.loads(raw)["subscription"])

            async def recv(self):
                if not backup_sent.is_set():
                    backup_sent.set()
                    return json.dumps(_bbo(now))
                raise ConnectionError("backup lost")

            async def __aiter__(self):
                await backup_sent.wait()
                yield json.dumps(_bbo(now))
                yield json.dumps(_bbo(datetime.now(timezone.utc), size="4"))
                primary_sent.set()
                await asyncio.Event().wait()

        monkeypatch.setattr("tools.market_capture.venues.entropy.websockets", SimpleNamespace(connect=lambda *a, **kw: Socket()))
        task = asyncio.create_task(feed.run())
        try:
            await asyncio.wait_for(primary_sent.wait(), timeout=1)
            assert sorted([s["type"] for s in lane.subs] for lane in ws_lanes) == [
                ["bbo"], ["l2Book", "bbo", "trades"]]
            assert feed.health.connected and not feed.backup_health.connected
            assert feed.backup_health.reconnects == 1
            assert feed.bbo_wins == {"primary": 1, "backup": 1}
            assert feed.hub.update_bbo.call_count == 2
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert all(lane.closed for lane in ws_lanes)

    asyncio.run(scenario())
