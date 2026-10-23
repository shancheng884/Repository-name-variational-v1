from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from tools.lib.wti_live_gateway import WTIProductionGateway, lighter_client_order_index
from tools.lib.wti_readonly_adapter import WTIReadOnlyError


class FakeVariational:
    def __init__(self, response=None, orders=None):
        self.response = response or {}
        self.orders = orders or []
        self.requests = []

    async def request(self, payload):
        self.requests.append(dict(payload))
        if payload["type"] == "VAR_API_ORDER":
            return {
                "type": "VAR_API_ORDER_RESULT",
                "requestId": payload["requestId"],
                "ok": True,
                "result": {"ok": True, "httpStatus": 200, "rfqId": "rfq-123"},
            }
        return {
            "type": "VAR_API_ORDERS_RESULT",
            "requestId": payload["requestId"],
            "ok": True,
            "result": {"ok": True, "orders": self.orders},
        }


def _gateway(var, *, market=None, lighter=None):
    return WTIProductionGateway(
        loop=asyncio.new_event_loop(),
        variational=var,
        account_reader=object(),
        lighter_client=lighter,
        account_index=10,
        api_key_index=1,
        market=market or {
            "market_id": 145,
            "supported_size_decimals": 2,
            "supported_price_decimals": 2,
        },
    )


def test_variational_submission_uses_exact_usoil_swap_and_rfqs(monkeypatch):
    var = FakeVariational()
    gateway = _gateway(var)

    async def lookup(client_order_id, request):
        assert request["rfq_id"] == "rfq-123"
        return {
            "ok": True,
            "final": True,
            "client_order_id": client_order_id,
            "order_id": "var-order-1",
            "native_order_id": "var-order-1",
            "native_client_order_id": request["rfq_id"],
            "status": "filled",
            "filled_qty": "0.29",
            "average_price": "70.12",
        }

    gateway._lookup_order = lookup
    request = {
        "client_order_id": "action-variational",
        "venue": "variational",
        "side": "buy",
        "qty": "0.29",
        "reduce_only": True,
        "max_slippage_bps": "8",
    }
    result = asyncio.run(gateway._submit_order(request))
    order = var.requests[0]
    assert order["type"] == "VAR_API_ORDER"
    assert order["market"] == "USOILP"
    assert order["instrumentType"] == "swap"
    assert order["kind"] == "commodity"
    assert order["fundingIntervalS"] == 0
    assert order["side"] == "BUY"
    assert order["amount"] == "0.29"
    assert order["maxSlippage"] == 0.0008
    assert order["reduceOnly"] is True
    assert result["native_client_order_id"] == "rfq-123"
    gateway.loop.close()


def test_variational_cleared_record_requires_explicit_fill_fields():
    submitted = datetime.now(timezone.utc).isoformat()
    request = {
        "venue": "variational", "side": "buy", "qty": "0.29",
        "submitted_at": submitted, "rfq_id": "rfq-123",
    }
    var = FakeVariational(orders=[{
        "underlying": "USOILP", "side": "buy", "qty": "0.29",
        "created_at": submitted, "rfq_id": "rfq-123", "order_id": "ord-1",
        "status": "cleared", "price": "70.12",
    }])
    gateway = _gateway(var)
    result = asyncio.run(gateway._lookup_variational("c1", request))
    assert result["final"] is False
    assert result["ok"] is False
    gateway.loop.close()


def test_variational_order_lookup_requires_unique_exact_native_rfq_match():
    submitted = datetime.now(timezone.utc).isoformat()
    request = {
        "venue": "variational", "side": "buy", "qty": "0.29",
        "submitted_at": submitted, "rfq_id": "rfq-123",
    }
    order = {
        "underlying": "USOILP", "side": "BUY", "qty": "0.29",
        "filled_qty": "0.29", "average_fill_price": "70.12",
        "created_at": submitted, "rfq_id": "rfq-123", "order_id": "ord-1",
        "status": "cleared", "fee_usd": "0.01",
    }
    var = FakeVariational(orders=[order, {**order, "order_id": "ord-2"}])
    gateway = _gateway(var)
    result = asyncio.run(gateway._lookup_variational("c1", request))
    assert result["final"] is False
    gateway.loop.close()


def test_variational_lookup_accepts_exact_native_match_and_explicit_fills():
    submitted = datetime.now(timezone.utc).isoformat()
    request = {
        "venue": "variational", "side": "buy", "qty": "0.29",
        "submitted_at": submitted, "rfq_id": "rfq-123",
    }
    var = FakeVariational(orders=[{
        "underlying": "USOILP", "side": "buy", "qty": "0.29",
        "filled_qty": "0.29", "average_fill_price": "70.12",
        "created_at": submitted, "rfq_id": "rfq-123", "order_id": "ord-1",
        "status": "cleared", "fee_usd": "0.01",
    }])
    gateway = _gateway(var)
    result = asyncio.run(gateway._lookup_variational("c1", request))
    assert result["status"] == "filled"
    assert result["filled_qty"] == "0.29"
    assert result["average_price"] == "70.12"
    assert result["native_order_id"] == "ord-1"
    assert result["native_client_order_id"] == "rfq-123"
    assert result["fee_usd"] == "0.01"
    gateway.loop.close()


def test_variational_lookup_matches_nested_order_info_and_separate_native_ids():
    submitted = datetime.now(timezone.utc).isoformat()
    order = {
        "symbol": "WTI",
        "order_info": {
            "instrument": {"underlying": "USOILP", "instrument_type": "swap"},
            "side": "BUY",
            "amount": "0.198",
            "created_at": submitted,
            "rfq_id": "rfq-live-123",
            "order_id": "order-live-456",
            "status": "filled",
            "filled_amount": "0.198",
            "average_fill_price": "71.25",
        }
    }
    var = FakeVariational(orders=[{"unrelated": "row"}, order])
    gateway = _gateway(var)
    request = {
        "venue": "variational",
        "side": "buy",
        "qty": "0.198",
        "submitted_at": submitted,
        "native_client_order_id": "rfq-live-123",
        "native_order_id": "order-live-456",
    }

    result = asyncio.run(gateway._lookup_variational("action-variational", request))

    assert result["final"] is True
    assert result["status"] == "filled"
    assert result["filled_qty"] == "0.198"
    assert result["average_price"] == "71.25"
    assert result["native_order_id"] == "order-live-456"
    assert result["native_client_order_id"] == "rfq-live-123"
    assert "filled" in var.requests[0]["status"]
    gateway.loop.close()

    native_order_only = _gateway(FakeVariational(orders=[order]))
    result = asyncio.run(native_order_only._lookup_variational(
        "action-variational",
        {**request, "native_client_order_id": None, "native_order_id": "order-live-456"},
    ))
    assert result["final"] is True
    assert result["native_order_id"] == "order-live-456"
    native_order_only.loop.close()


def test_lighter_order_payload_is_ioc_market_with_exact_precision(monkeypatch):
    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(OrderApi=object))

    class FakeSigner:
        ORDER_TYPE_MARKET = 1
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 2
        DEFAULT_IOC_EXPIRY = 3
        api_client = object()

        def __init__(self):
            self.kwargs = None

        async def create_order(self, **kwargs):
            self.kwargs = kwargs
            return ({"client_order_index": kwargs["client_order_index"]}, {"code": 200}, None)

    signer = FakeSigner()
    gateway = _gateway(FakeVariational(), lighter=signer)
    request = {
        "client_order_id": "pair-action-lighter",
        "venue": "lighter",
        "side": "sell",
        "qty": "0.29",
        "reduce_only": True,
        "limit_price": "69.99",
    }
    asyncio.run(gateway._submit_lighter(request))
    assert signer.kwargs == {
        "market_index": 145,
        "client_order_index": lighter_client_order_index(request["client_order_id"]),
        "base_amount": 29,
        "price": 6999,
        "is_ask": True,
        "order_type": 1,
        "time_in_force": 2,
        "order_expiry": 3,
        "reduce_only": True,
        "api_key_index": 1,
    }
    gateway.loop.close()


def test_lighter_order_rejects_base_quantity_that_cannot_be_encoded_exactly(monkeypatch):
    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(OrderApi=object))

    class FakeSigner:
        api_client = object()

        async def create_order(self, **kwargs):
            raise AssertionError("precision-invalid order must not be submitted")

    gateway = _gateway(FakeVariational(), lighter=FakeSigner())
    with pytest.raises(WTIReadOnlyError, match="lighter_order_precision_invalid"):
        asyncio.run(gateway._submit_lighter({
            "client_order_id": "bad-precision",
            "venue": "lighter",
            "side": "buy",
            "qty": "0.291",
            "reduce_only": True,
            "limit_price": "70.00",
        }))
    gateway.loop.close()


def test_entry_risk_block_prevents_any_order_submission(monkeypatch):
    var = FakeVariational()
    gateway = _gateway(var)

    async def blocked_account():
        return {"risk": {"risk_action": "block_entry", "risk_data_complete": True}}

    gateway._read_account = blocked_account
    result = asyncio.run(gateway._submit_order({
        "client_order_id": "risk-blocked", "venue": "variational",
        "side": "buy", "qty": "0.29", "reduce_only": False,
        "max_slippage_bps": "8",
    }))
    assert result["status"] == "rejected"
    assert not var.requests
    gateway.loop.close()
