from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tools.oil_swap_collector import (
    OilSwapCollector,
    parse_args,
    quantity_for_notional,
    validate_market_metadata,
)
from tools.basis_collector import VariationalQuoteClient


def market(**overrides):
    return {
        "symbol": "WTI",
        "market_id": 145,
        "market_type": "perp",
        "status": "active",
        "is_frozen": False,
        "min_base_amount": "0.060",
        "min_quote_amount": "10.000000",
        "supported_size_decimals": 3,
        "supported_price_decimals": 3,
        "multiplier": "1.000000000000000000",
        "taker_fee": "0.0000",
        "is_taker_fee_enabled": True,
        **overrides,
    }


def test_oil_collector_requires_external_forwarder_mode(tmp_path) -> None:
    with pytest.raises(SystemExit):
        parse_args(["--output-dir", str(tmp_path)])
    args = parse_args(["--reuse-forwarder", "--output-dir", str(tmp_path)])
    assert args.forwarder_command_port == 8768
    assert args.interval_seconds == 10


def test_quantity_rounds_up_and_respects_lighter_minimums() -> None:
    assert quantity_for_notional(
        notional_usd=Decimal("20"), reference_price=Decimal("100"), market=market()
    ) == Decimal("0.200")
    assert quantity_for_notional(
        notional_usd=Decimal("10"), reference_price=Decimal("100"),
        market=market(min_base_amount="0.251"),
    ) == Decimal("0.251")
    assert quantity_for_notional(
        notional_usd=Decimal("10"), reference_price=Decimal("100"),
        market=market(min_quote_amount="25"),
    ) == Decimal("0.250")


def test_oil_collector_rejects_inactive_or_frozen_market() -> None:
    with pytest.raises(RuntimeError, match="not active"):
        validate_market_metadata(market(status="inactive"))
    with pytest.raises(RuntimeError, match="frozen"):
        validate_market_metadata(market(is_frozen=True))


def test_swap_quote_command_preserves_zero_interval_and_is_non_ordering() -> None:
    async def run() -> None:
        class FakeSocket:
            sent = None

            async def send(self, raw):
                self.sent = json.loads(raw)

            async def recv(self):
                return json.dumps({"requestId": self.sent["requestId"], "ok": True})

            async def close(self):
                return None

        client = VariationalQuoteClient("ws://unused", timeout=1, logger=logging.getLogger("test"))
        socket = FakeSocket()
        client.websocket = socket
        await client.quote(
            "WTI", Decimal("0.2"), market="USOILP", instrument_type="swap",
            kind="commodity", funding_interval_s=0
        )
        assert socket.sent["type"] == "VAR_API_QUOTE"
        assert socket.sent["confirm"] is False
        assert socket.sent["market"] == "USOILP"
        assert socket.sent["instrumentType"] == "swap"
        assert socket.sent["kind"] == "commodity"
        assert socket.sent["fundingIntervalS"] == 0

    asyncio.run(run())


def test_oil_sample_quotes_same_qty_as_lighter_depth_and_uses_swap(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(["--reuse-forwarder", "--output-dir", str(tmp_path)])
        collector = OilSwapCollector(args)
        collector.books.market_metadata["WTI"] = market()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        snapshots = [
            {
                "bid": Decimal("99.99"), "ask": Decimal("100.01"),
                "sell_price": Decimal("99.99"), "buy_price": Decimal("100.01"),
                "nonce": 10, "server_timestamp_ms": now_ms, "book_age_seconds": 0.1,
                "continuity_ok": True, "cold": False, "sequence_gaps": 0,
            },
            {
                "bid": Decimal("99.99"), "ask": Decimal("100.01"),
                "sell_price": Decimal("99.99"), "buy_price": Decimal("100.01"),
                "nonce": 11, "server_timestamp_ms": now_ms, "book_age_seconds": 0.1,
                "continuity_ok": True, "cold": False, "sequence_gaps": 0,
            },
            {
                "bid": Decimal("99.99"), "ask": Decimal("100.01"),
                "sell_price": Decimal("99.99"), "buy_price": Decimal("100.01"),
                "nonce": 12, "server_timestamp_ms": now_ms, "book_age_seconds": 0.1,
                "continuity_ok": True, "cold": False, "sequence_gaps": 0,
            },
        ]
        seen_qty = []

        async def fake_snapshot(_asset, _notional, *, base_qty=None):
            seen_qty.append(base_qty)
            return snapshots.pop(0)

        async def fake_quote(asset, qty, **kwargs):
            assert asset == "WTI"
            assert qty == Decimal("0.200")
            assert kwargs == {
                "market": "USOILP", "instrument_type": "swap",
                "kind": "commodity", "funding_interval_s": 0,
            }
            observed = datetime.now(timezone.utc)
            return {
                "ok": True,
                "result": {
                    "bid": "100.00", "ask": "100.02", "quoteId": "q1",
                    "quoteTimestamp": observed.isoformat(),
                },
            }, 30.0

        status_at = datetime.now(timezone.utc)
        collector.market_status_feed.connected = True
        collector.market_status_feed.status = "open"
        collector.market_status_feed.status_observed_at = status_at
        collector.market_status_feed.heartbeat_at = status_at
        collector.trading_sessions = [{
            "open": (status_at - timedelta(minutes=1)).isoformat(),
            "close": (status_at + timedelta(hours=6)).isoformat(),
        }]
        collector.session_schedule_updated_at = status_at.isoformat()
        collector.books.snapshot = fake_snapshot
        collector.quote_client.quote = fake_quote
        row = await collector._collect_once()

        assert row["sample_quality"] == "valid"
        assert row["asset"] == "WTI"
        assert row["variational_underlying"] == "USOILP"
        assert row["variational_instrument_type"] == "swap"
        assert row["variational_instrument_kind"] == "commodity"
        assert row["variational_funding_interval_s"] == 0
        assert Decimal(row["lighter_qty"]) == Decimal(row["variational_qty"]) == Decimal("0.200")
        assert row["lighter_market_id"] == 145
        assert row["lighter_supported_price_decimals"] == 3
        assert row["session_status"] == "open"
        assert row["market_status"] == "open"
        assert row["session_status_source"] == "variational_market_status_websocket"
        assert row["session_schedule_source"] == "variational_supported_assets_api"
        assert row["session_id"].endswith(
            (status_at + timedelta(hours=6)).isoformat()
        )
        assert row["session_closing_at"] is not None
        assert row["orders_attempted"] is False
        assert seen_qty == [None, Decimal("0.200"), Decimal("0.200")]

    asyncio.run(run())


def test_execution_refresh_preserves_requested_pair_quantity(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(["--reuse-forwarder", "--output-dir", str(tmp_path)])
        collector = OilSwapCollector(args)
        collector.books.market_metadata["WTI"] = market()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        snapshots = [
            {
                "bid": Decimal("101.99"), "ask": Decimal("102.01"),
                "sell_price": Decimal("101.99"), "buy_price": Decimal("102.01"),
                "nonce": n, "server_timestamp_ms": now_ms, "book_age_seconds": 0.1,
                "continuity_ok": True, "cold": False, "sequence_gaps": 0,
            }
            for n in (20, 21)
        ]
        requested_depth = []
        quoted_qty = []

        async def fake_snapshot(_asset, _notional, *, base_qty=None):
            requested_depth.append(base_qty)
            return snapshots.pop(0)

        async def fake_quote(_asset, qty, **_kwargs):
            quoted_qty.append(qty)
            observed = datetime.now(timezone.utc)
            return {
                "ok": True,
                "result": {
                    "bid": "102.00", "ask": "102.02",
                    "quoteTimestamp": observed.isoformat(),
                },
            }, 25.0

        observed = datetime.now(timezone.utc)
        collector.market_status_feed.connected = True
        collector.market_status_feed.status = "open"
        collector.market_status_feed.status_observed_at = observed
        collector.market_status_feed.heartbeat_at = observed
        collector.trading_sessions = [{
            "open": (observed - timedelta(minutes=1)).isoformat(),
            "close": (observed + timedelta(hours=6)).isoformat(),
        }]
        collector.session_schedule_updated_at = observed.isoformat()
        collector.books.snapshot = fake_snapshot
        collector.quote_client.quote = fake_quote

        row = await collector.collect_fresh_sample(
            var_qty=Decimal("0.198"), lighter_qty=Decimal("0.198")
        )

        assert row["sample_quality"] == "valid"
        assert Decimal(row["variational_qty"]) == Decimal("0.198")
        assert Decimal(row["lighter_qty"]) == Decimal("0.198")
        assert requested_depth == [Decimal("0.198"), Decimal("0.198")]
        assert quoted_qty == [Decimal("0.198")]

    asyncio.run(run())


def test_execution_refresh_rejects_inconsistent_requested_pair_quantity(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(["--reuse-forwarder", "--output-dir", str(tmp_path)])
        collector = OilSwapCollector(args)
        collector.books.market_metadata["WTI"] = market()
        with pytest.raises(RuntimeError, match="requested_pair_quantity_multiplier_mismatch"):
            await collector.collect_fresh_sample(
                var_qty=Decimal("0.198"), lighter_qty=Decimal("0.196")
            )

    asyncio.run(run())


def test_market_metadata_command_is_read_only_and_returns_actual_schedule_fields() -> None:
    async def run() -> None:
        class FakeSocket:
            sent = None

            async def send(self, raw):
                self.sent = json.loads(raw)

            async def recv(self):
                return json.dumps({
                    "type": "VAR_API_MARKET_METADATA_RESULT",
                    "requestId": self.sent["requestId"],
                    "ok": True,
                    "result": {
                        "market": {
                            "symbol": "USOILP",
                            "trading_sessions": [{
                                "open": "2026-10-22T04:00:00Z",
                                "close": "2026-10-22T21:00:00Z",
                            }],
                        }
                    },
                })

            async def close(self):
                return None

        client = VariationalQuoteClient("ws://unused", timeout=1, logger=logging.getLogger("test"))
        socket = FakeSocket()
        client.websocket = socket
        response, _ = await client.market_metadata("USOILP")
        assert socket.sent == {
            "type": "VAR_API_MARKET_METADATA",
            "requestId": socket.sent["requestId"],
            "market": "USOILP",
            "requestTimeoutMs": 800,
            "operationTimeoutMs": 800,
        }
        assert response["ok"] is True
        assert response["result"]["market"]["trading_sessions"][0]["close"].endswith("Z")

    asyncio.run(run())


def test_market_metadata_forwarder_error_is_reported_without_waiting_for_timeout() -> None:
    async def run() -> None:
        class FakeSocket:
            async def send(self, _raw):
                return None

            async def recv(self):
                return json.dumps({
                    "type": "ERROR",
                    "ok": False,
                    "error": "Unsupported message type: VAR_API_MARKET_METADATA",
                })

            async def close(self):
                return None

        client = VariationalQuoteClient("ws://unused", timeout=1, logger=logging.getLogger("test"))
        client.websocket = FakeSocket()
        with pytest.raises(RuntimeError, match="Unsupported message type"):
            await client.market_metadata("USOILP")

    asyncio.run(run())


def test_quote_wait_uses_one_deadline_when_unrelated_messages_arrive() -> None:
    async def run() -> None:
        class FakeSocket:
            async def send(self, _raw):
                return None

            async def recv(self):
                await asyncio.sleep(0.01)
                return json.dumps({"type": "PONG"})

            async def close(self):
                return None

        client = VariationalQuoteClient("ws://unused", timeout=0.04, logger=logging.getLogger("test"))
        client.websocket = FakeSocket()
        with pytest.raises(asyncio.TimeoutError):
            await client.market_metadata("USOILP")

    asyncio.run(run())


def test_variational_quote_command_omits_optional_kind_for_perpetuals() -> None:
    async def run() -> None:
        class FakeSocket:
            sent = None

            async def send(self, raw):
                self.sent = json.loads(raw)

            async def recv(self):
                return json.dumps({"requestId": self.sent["requestId"], "ok": True})

            async def close(self):
                return None

        client = VariationalQuoteClient("ws://unused", timeout=1, logger=logging.getLogger("test"))
        socket = FakeSocket()
        client.websocket = socket
        await client.quote("ETH", Decimal("0.01"), market="ETH")
        assert socket.sent["market"] == "ETH"
        assert "kind" not in socket.sent
        assert socket.sent["requestTimeoutMs"] == 800
        assert socket.sent["operationTimeoutMs"] == 800

    asyncio.run(run())
