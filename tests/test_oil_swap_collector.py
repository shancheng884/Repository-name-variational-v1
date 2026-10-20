from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
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
            return {
                "ok": True,
                "result": {
                    "bid": "100.00", "ask": "100.02", "quoteId": "q1",
                    "quoteTimestamp": datetime.now(timezone.utc).isoformat(),
                },
            }, 30.0

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
        assert row["orders_attempted"] is False
        assert seen_qty == [None, Decimal("0.200"), Decimal("0.200")]

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

    asyncio.run(run())
