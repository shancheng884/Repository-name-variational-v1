from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from tools.lib.wti_live_gateway import (
    VARIATIONAL_ORDERS_STATUS_FILTER,
    WTIProductionGateway,
    lighter_client_order_index,
)
from tools.lib.wti_readonly_adapter import WTIReadOnlyError
from tools.lib.lighter_order_identity import (
    LEGACY_INDEX_REJECTION,
    legacy_lighter_client_order_index,
    lighter_order_identity,
)


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
        if payload.get("status") != VARIATIONAL_ORDERS_STATUS_FILTER:
            return {
                "type": "VAR_API_ORDERS_RESULT",
                "requestId": payload["requestId"],
                "ok": False,
                "result": {
                    "ok": False,
                    "httpStatus": 400,
                    "error": "Failed to deserialize query string: Matching variant not found",
                },
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


def _booked_wti_order():
    # Captured orders-v2 schema; IDs and timestamps are replaced for the test.
    now = datetime.now(timezone.utc)
    return {
        "instrument": {"underlying": "USOILP"},
        "status": "cleared",
        "clearing_status": "success_trades_booked_into_pool",
        "side": "buy",
        "qty": "0.198",
        "price": "100.426",
        "order_id": "wti-booked-order",
        "rfq_id": "rfq-123",
        "created_at": now.isoformat(),
        "execution_timestamp": (now - timedelta(milliseconds=100)).isoformat(),
    }


def _booked_wti_request():
    return {
        "venue": "variational", "side": "buy", "qty": "0.198",
        "submitted_at": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
        "rfq_id": "rfq-123",
    }


@pytest.mark.parametrize("known_native_ids", [False, True])
def test_variational_lookup_decodes_observed_booked_cleared_qty_and_price(known_native_ids):
    gateway = _gateway(FakeVariational(orders=[_booked_wti_order()]))
    request = _booked_wti_request()
    if known_native_ids:
        request["native_order_id"] = "wti-booked-order"
    else:
        request.pop("rfq_id")
    try:
        result = asyncio.run(gateway._lookup_variational("action-variational", request))
    finally:
        gateway.loop.close()
    assert result["final"] is True
    assert result["status"] == "filled"
    assert result["filled_qty"] == "0.198"
    assert result["average_price"] == "100.426"
    assert result["native_order_id"] == "wti-booked-order"
    assert result["native_client_order_id"] == "rfq-123"
    assert result["fee_usd"] is None


@pytest.mark.parametrize("override", [
    {"status": "pending"},
    {"status": "filled"},
    {"clearing_status": None},
    {"clearing_status": "rejected_failed_taker_funding"},
    {"execution_timestamp": None},
    {"execution_timestamp": "2020-01-01T00:00:00Z"},
    {"execution_timestamp": "2999-01-01T00:00:00Z"},
    {"order_id": None},
    {"price": None},
    {"price": "NaN"},
    {"price": "0"},
    {"qty": "0.197"},
    {"side": "sell"},
    {"created_at": "2020-01-01T00:00:00Z"},
    {"instrument": {"underlying": "ETH"}},
    {"rfq_id": "some-other-rfq"},
    {"filled_qty": "0"},
    {"average_fill_price": "0"},
])
def test_variational_canonical_fill_requires_complete_success_evidence(override):
    order = {**_booked_wti_order(), **override}
    gateway = _gateway(FakeVariational(orders=[order]))
    try:
        result = asyncio.run(gateway._lookup_variational("action-variational", _booked_wti_request()))
    finally:
        gateway.loop.close()
    assert result["final"] is False


def test_variational_booked_order_without_known_native_id_still_requires_unique_match():
    order = _booked_wti_order()
    gateway = _gateway(FakeVariational(orders=[order, {**order, "order_id": "other-order"}]))
    request = _booked_wti_request()
    request.pop("rfq_id")
    try:
        result = asyncio.run(gateway._lookup_variational("action-variational", request))
    finally:
        gateway.loop.close()
    assert result["final"] is False


def test_variational_booked_order_does_not_override_contradictory_clearing_status():
    order = {
        **_booked_wti_order(),
        "clearing_status": "rejected_failed_taker_funding",
        "filled_qty": "0.198",
        "average_fill_price": "100.426",
    }
    gateway = _gateway(FakeVariational(orders=[order]))
    try:
        result = asyncio.run(gateway._lookup_variational("action-variational", _booked_wti_request()))
    finally:
        gateway.loop.close()
    assert result["final"] is False


@pytest.mark.parametrize("reduce_only", [False, True])
def test_variational_submit_confirms_booked_record_without_resubmitting(reduce_only):
    var = FakeVariational(orders=[_booked_wti_order()])
    gateway = _gateway(var)

    async def healthy_account():
        return {"risk": {
            "risk_action": "normal", "risk_data_complete": True,
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }}

    gateway._read_account = healthy_account
    request = {
        **_booked_wti_request(), "client_order_id": "action-variational",
        "reduce_only": reduce_only, "max_slippage_bps": "8",
    }
    try:
        result = asyncio.run(gateway._submit_order(request))
    finally:
        gateway.loop.close()
    assert result["final"] is True
    assert result["filled_qty"] == "0.198"
    assert result["average_price"] == "100.426"
    assert [r["type"] for r in var.requests] == ["VAR_API_ORDER", "VAR_API_ORDERS"]


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
    assert var.requests[0]["status"] == "pending,canceled,cleared,rejected"
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


def test_real_sdk_create_order_object_does_not_erase_accepted_send_receipt():
    from lighter.transactions import CreateOrder

    class Signer:
        ORDER_TYPE_MARKET = 1
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
        DEFAULT_IOC_EXPIRY = 0

        async def create_order(self, **kwargs):
            assert 1 <= kwargs["client_order_index"] < 1 << 48
            return CreateOrder.from_json("{}"), {"code": 200, "tx_hash": "tx-accepted"}, None

    gateway = _gateway(FakeVariational(), lighter=Signer())
    result = asyncio.run(gateway._submit_lighter({
        "client_order_id": "sdk-object-lighter", "side": "sell", "qty": "0.29",
        "limit_price": "70.00", "reduce_only": False,
    }))
    assert result["ok"] is True
    assert result["submission_acknowledged"] is True
    assert result["transaction_hash"] == "tx-accepted"
    gateway.loop.close()


def test_lighter_legacy_invalid_index_is_proven_local_rejection_without_api_calls():
    gateway = _gateway(FakeVariational(), lighter=object())
    cid = "legacy-live-action-lighter"
    result = asyncio.run(gateway._lookup_lighter(cid, {
        "client_order_id": cid, "venue": "lighter", "side": "sell", "qty": "0.198",
    }))
    assert result["ok"] is result["final"] is True
    assert result["no_order_confirmed"] is True
    assert result["filled_qty"] == "0"
    assert result["error_code"] == LEGACY_INDEX_REJECTION
    assert result["native_client_order_id"] == str(legacy_lighter_client_order_index(cid))
    gateway.loop.close()


def test_sdk_submission_error_remains_unknown_with_safe_diagnostics():
    class Signer:
        ORDER_TYPE_MARKET = 1
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
        DEFAULT_IOC_EXPIRY = 0

        async def create_order(self, **kwargs):
            return None, None, "unexpected response with confidential detail"

    gateway = _gateway(FakeVariational(), lighter=Signer())
    result = asyncio.run(gateway._submit_lighter({
        "client_order_id": "sdk-error-lighter", "side": "sell", "qty": "0.29",
        "limit_price": "70.00", "reduce_only": False,
    }))
    assert result["final"] is False
    assert result["error_code"] == "lighter_sdk_submission_failed"
    assert result["submission_stage"] == "sdk_create_order"
    assert "confidential" not in str(result)
    gateway.loop.close()


@pytest.mark.parametrize("mode", ["absent", "active", "timeout", "filled_fee_timeout", "filled_missing_id"])
def test_lighter_lookup_preserves_saved_identity_and_reports_specific_evidence(monkeypatch, mode):
    cid = "legacy-live-action-lighter"
    identity = lighter_order_identity(cid)
    index = int(identity["native_client_order_id"])

    class API:
        def __init__(self, client):
            pass

        async def account_inactive_orders(self, **kwargs):
            if mode == "timeout":
                raise TimeoutError("do not persist arbitrary exception payload")
            rows = [] if mode not in {"filled_fee_timeout", "filled_missing_id"} else [{
                "market_index": 145, "client_order_index": index, "is_ask": True,
                "status": "filled", "filled_base_amount": "0.29",
                "filled_quote_amount": "20.30", "order_index": 123456,
            }]
            if mode == "filled_missing_id":
                rows[0].pop("order_index")
            return {"code": 200, "orders": rows}

        async def account_active_orders(self, **kwargs):
            return {"code": 200, "orders": [{"client_order_index": index}] if mode == "active" else []}

        async def trades(self, **kwargs):
            raise TimeoutError("fee query unavailable")

    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(OrderApi=API))
    signer = SimpleNamespace(
        api_client=object(), create_auth_token_with_expiry=lambda **kwargs: ("test-token", None),
    )
    gateway = _gateway(FakeVariational(), lighter=signer)
    result = asyncio.run(gateway._lookup_lighter(cid, {
        "client_order_id": cid, "venue": "lighter", "side": "sell", "qty": "0.29", **identity,
    }))
    assert result["native_client_order_id"] == str(index)
    if mode == "filled_fee_timeout":
        assert result["ok"] is result["final"] is True
        assert result["status"] == "filled"
        assert result["filled_qty"] == "0.29"
        assert result["trade_ids"] is result["fee_usd"] is None
        assert result["lookup_reason"] == "lighter_trade_evidence_unavailable"
    else:
        assert result["final"] is False
        assert result.get("no_order_confirmed") is not True
        assert result["lookup_reason"] == {
            "absent": "lighter_order_absent_from_active_and_inactive",
            "active": "lighter_order_still_active",
            "timeout": "lighter_lookup_request_failed",
            "filled_missing_id": "lighter_native_order_id_missing",
        }[mode]
        if mode == "timeout":
            assert result["lookup_stage"] == "inactive_orders"
            assert result["error_type"] == "TimeoutError"
    gateway.loop.close()


@pytest.mark.parametrize(
    ("trade", "expected_fee", "expected_reason"),
    [
        ({
            "trade_id": 501, "market_id": 145, "ask_id": 123456,
            "bid_id": 123457, "ask_account_id": 10, "bid_account_id": 99,
            "maker_fee": -12, "taker_fee": 18,
        }, None, "lighter_trade_fee_usd_unverified"),
        ({
            "trade_id": 501, "market_id": 145, "ask_id": 123456,
            "bid_id": 123457, "ask_account_id": 10, "bid_account_id": 99,
            "maker_fee": -12, "taker_fee": 18, "fee_usd": "-0.004",
        }, "-0.004", None),
        ({
            "trade_id": 501, "market_id": 145, "ask_id": 123456,
            "bid_id": 123457, "ask_account_id": 77, "bid_account_id": 99,
            "fee_usd": "0.004",
        }, None, "lighter_trade_fee_usd_unverified"),
        ({
            "trade_id": 502, "market_id": 145, "ask_id": 123456,
            "bid_id": 123457, "ask_account_id": 77, "bid_account_id": 10,
            "fee_usd": "0.004",
        }, None, "lighter_trade_fee_usd_unverified"),
    ],
)
def test_lighter_trade_fee_requires_exact_identity_and_explicit_usd(
    monkeypatch, trade, expected_fee, expected_reason,
):
    cid = "fee-evidence-lighter"
    identity = lighter_order_identity(cid)
    index = int(identity["native_client_order_id"])

    class API:
        def __init__(self, client):
            pass

        async def account_inactive_orders(self, **kwargs):
            return {"code": 200, "orders": [{
                "market_index": 145, "client_order_index": index, "is_ask": True,
                "status": "filled", "filled_base_amount": "0.29",
                "filled_quote_amount": "20.30", "order_index": 123456,
            }]}

        async def account_active_orders(self, **kwargs):
            return {"code": 200, "orders": []}

        async def trades(self, **kwargs):
            return {"code": 200, "trades": [trade]}

    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(OrderApi=API))
    signer = SimpleNamespace(
        api_client=object(),
        create_auth_token_with_expiry=lambda **kwargs: ("test-token", None),
    )
    gateway = _gateway(FakeVariational(), lighter=signer)
    result = asyncio.run(gateway._lookup_lighter(cid, {
        "client_order_id": cid, "venue": "lighter", "side": "sell",
        "qty": "0.29", **identity,
    }))
    assert result["status"] == "filled"
    assert result["filled_qty"] == "0.29"
    assert result["fee_usd"] == expected_fee
    assert result.get("lookup_reason") == expected_reason
    evidence = result["trade_fee_evidence"][0]
    assert evidence["identity_verified"] is (
        trade["ask_id"] == 123456 and trade["ask_account_id"] == 10
    )
    assert evidence["raw_fee_unit"] == "unverified"
    if expected_fee is None:
        assert evidence["maker_fee_raw"] in (None, trade.get("maker_fee"))
    gateway.loop.close()
