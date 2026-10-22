from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tools.wti_account_probe import LighterSDKReadOnlyReader
from tools.lib.wti_readonly_adapter import (
    WTIReadOnlyAccountAdapter,
    WTIReadOnlyError,
    fetch_variational_wti_pending_orders,
    normalize_lighter_active_orders,
    normalize_lighter_account,
    validate_lighter_market_metadata,
)


def _market(**overrides):
    row = {
        "symbol": "WTI",
        "market_id": 145,
        "market_type": "perp",
        "status": "active",
        "is_frozen": False,
        "multiplier": "1",
        "supported_size_decimals": 2,
        "supported_price_decimals": 2,
        "min_base_amount": "0.01",
        "min_quote_amount": "1",
    }
    row.update(overrides)
    return row


class FakeVariational:
    def __init__(self, *, pending_pages=None):
        self.pending_pages = pending_pages or [[]]
        self.calls = []

    async def request(self, payload):
        self.calls.append(dict(payload))
        if payload["type"] == "VAR_API_POSITIONS":
            return {
                "type": "VAR_API_POSITIONS_RESULT",
                "requestId": payload["requestId"],
                "ok": True,
                "result": {
                    "ok": True,
                    "httpStatus": 200,
                    "positions": {
                        "positions": [{
                            "position_info": {
                                "instrument": {
                                    "underlying": "USOILP",
                                    "instrument_type": "swap",
                                    "settlement_asset": "USDC",
                                    "kind": "commodity",
                                    "funding_interval_s": 0,
                                },
                                "qty": "0.25",
                            }
                        }]
                    },
                },
            }
        if payload["type"] == "VAR_API_PORTFOLIO":
            return {
                "type": "VAR_API_PORTFOLIO_RESULT",
                "requestId": payload["requestId"],
                "ok": True,
                "result": {"ok": True, "httpStatus": 200, "portfolio": {"balance": "100"}},
            }
        page = payload["offset"] // payload["limit"]
        return {
            "type": "VAR_API_ORDERS_RESULT",
            "requestId": payload["requestId"],
            "ok": True,
            "result": {
                "ok": True,
                "httpStatus": 200,
                "orders": {"result": self.pending_pages[page]},
            },
        }


class FakeLighter:
    def __init__(self, *, qty="-0.25", orders=None):
        self.market_ids = []
        self.qty = qty
        self.orders = orders or []

    async def read_account_and_active_orders(self, market_id):
        self.market_ids.append(market_id)
        return (
            {
                "accounts": [{"positions": [{
                    "market_id": market_id,
                    "symbol": "WTI",
                    "position": str(abs(Decimal(self.qty))),
                    "sign": -1 if Decimal(self.qty) < 0 else 1,
                }]}]
            },
            {"code": 200, "orders": self.orders},
        )


class FakeVariationalPositions(FakeVariational):
    def __init__(self, positions, *, response_ok=True, http_status=200):
        super().__init__()
        self.positions = positions
        self.response_ok = response_ok
        self.http_status = http_status

    async def request(self, payload):
        response = await super().request(payload)
        if payload["type"] == "VAR_API_POSITIONS":
            response["ok"] = self.response_ok
            response["result"].update({
                "ok": self.response_ok,
                "httpStatus": self.http_status,
                "positions": self.positions,
            })
        return response


@pytest.mark.parametrize("positions", [[], {"positions": []}])
def test_confirmed_empty_var_positions_are_zero_for_both_response_formats(positions):
    adapter = WTIReadOnlyAccountAdapter(
        variational=FakeVariationalPositions(positions),
        lighter=FakeLighter(qty="0"),
        market=_market(),
    )

    snapshot = asyncio_call(adapter.read_account())

    assert snapshot["normalized"] == {
        "variational_qty": "0", "lighter_qty": "0", "active_order_count": 0,
    }


def test_bare_var_position_list_preserves_exact_wti_identity_and_signed_quantity():
    position = {
        "instrument": {
            "underlying": "USOILP", "instrument_type": "swap",
            "settlement_asset": "USDC", "kind": "commodity", "funding_interval_s": 0,
        },
        "qty": "-0.25",
    }
    adapter = WTIReadOnlyAccountAdapter(
        variational=FakeVariationalPositions([position]),
        lighter=FakeLighter(qty="0.25"),
        market=_market(),
    )

    snapshot = asyncio_call(adapter.read_account())

    assert snapshot["normalized"]["variational_qty"] == "-0.25"
    assert snapshot["normalized"]["lighter_qty"] == "0.25"


@pytest.mark.parametrize("positions", [None, "", [None], [{"qty": "0"}]])
def test_malformed_var_positions_never_become_zero(positions):
    var = FakeVariationalPositions(positions)
    adapter = WTIReadOnlyAccountAdapter(
        variational=var, lighter=FakeLighter(qty="0"), market=_market(),
    )

    with pytest.raises(WTIReadOnlyError, match="position_unverifiable"):
        asyncio_call(adapter.read_account())
    assert [call["type"] for call in var.calls] == ["VAR_API_POSITIONS"]


@pytest.mark.parametrize("response_ok,http_status", [(False, 200), (True, 401)])
def test_failed_var_response_with_empty_list_is_not_a_flat_account(response_ok, http_status):
    adapter = WTIReadOnlyAccountAdapter(
        variational=FakeVariationalPositions(
            [], response_ok=response_ok, http_status=http_status,
        ),
        lighter=FakeLighter(qty="0"),
        market=_market(),
    )

    with pytest.raises(WTIReadOnlyError, match="variational_read"):
        asyncio_call(adapter.read_account())


def test_probe_keeps_verified_market_metadata_when_account_read_fails(monkeypatch):
    import json
    import sys
    from types import SimpleNamespace
    from tools import wti_account_probe as probe

    closed = []

    class FakeSigner:
        def __init__(self, **kwargs):
            self.api_client = self

        def check_client(self):
            return None

        async def close(self):
            closed.append(True)

    class FakeForwarder:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class FailedAdapter:
        def __init__(self, **kwargs):
            pass

        async def read_account(self):
            raise WTIReadOnlyError("variational_wti_position_unverifiable")

    monkeypatch.setattr(probe, "_load_env", lambda: (12, 3, "test-private-key"))
    monkeypatch.setattr(
        probe, "fetch_lighter_market_metadata",
        lambda *args, **kwargs: validate_lighter_market_metadata({"order_books": [_market()]}),
    )
    monkeypatch.setitem(sys.modules, "lighter.signer_client", SimpleNamespace(SignerClient=FakeSigner))
    monkeypatch.setattr(probe, "VariationalForwarderClient", FakeForwarder)
    monkeypatch.setattr(probe, "WTIReadOnlyAccountAdapter", FailedAdapter)

    report = asyncio_call(probe.run_probe(probe.parse_args([])))

    assert report["status"] == "unverified"
    assert report["error"] == "variational_wti_position_unverifiable"
    assert report["lighter_market"]["market_id"] == 145
    assert report["lighter_market"]["multiplier"] == "1"
    assert "snapshot" not in report
    assert report["orders_attempted"] is False
    assert report["execution_enabled"] is False
    assert "test-private-key" not in json.dumps(report)
    assert closed == [True]


def test_market_metadata_requires_unique_active_wti_and_can_pin_id():
    metadata = validate_lighter_market_metadata(
        {"order_books": [_market(), {"symbol": "ETH", "market_id": 0}]},
        expected_market_id=145,
    )
    assert metadata["market_id"] == 145
    assert metadata["multiplier"] == Decimal("1")
    with pytest.raises(WTIReadOnlyError, match="market_id_changed"):
        validate_lighter_market_metadata({"order_books": [_market()]}, expected_market_id=146)
    with pytest.raises(WTIReadOnlyError, match="missing_or_ambiguous"):
        validate_lighter_market_metadata({"order_books": [_market(), _market(market_id=146)]})
    with pytest.raises(WTIReadOnlyError, match="metadata_invalid"):
        validate_lighter_market_metadata({"order_books": [_market(multiplier=None)]})


def test_var_pending_order_scan_paginates_and_filters_exact_underlying():
    first = [
        {"instrument": "P-ETH-USDC-3600", "order_id": str(index)}
        for index in range(2)
    ]
    second = [{"instrument": {"underlying": "USOILP"}, "id": "private-id"}]
    client = FakeVariational(pending_pages=[first, second])

    matched = asyncio_call(fetch_variational_wti_pending_orders(client, page_size=2))

    assert len(matched) == 1
    assert matched[0]["id"] == "private-id"
    assert [call["offset"] for call in client.calls if call["type"] == "VAR_API_ORDERS"] == [0, 2]
    assert all(call["type"] != "VAR_API_ORDER" for call in client.calls)


def asyncio_call(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def test_var_pending_order_unknown_identity_fails_closed():
    client = FakeVariational(pending_pages=[[{"order_id": "redacted"}]])
    with pytest.raises(WTIReadOnlyError, match="instrument_unidentifiable"):
        asyncio_call(fetch_variational_wti_pending_orders(client))


def test_var_pending_order_command_error_does_not_become_zero_orders():
    class FailedClient:
        async def request(self, payload):
            return {
                "type": "VAR_API_ORDERS_RESULT",
                "requestId": payload["requestId"],
                "ok": False,
                "error": "unauthorized",
            }

    with pytest.raises(WTIReadOnlyError, match="variational_read_failed"):
        asyncio_call(fetch_variational_wti_pending_orders(FailedClient()))


def test_lighter_account_and_active_orders_validate_shapes_and_market_scope():
    account = normalize_lighter_account({"accounts": [{"positions": []}]})
    assert account["code"] == 200
    assert normalize_lighter_active_orders(
        {"code": 200, "orders": [{"order_id": "opaque"}]}, market_id=145
    )
    with pytest.raises(WTIReadOnlyError, match="market_id_mismatch"):
        normalize_lighter_active_orders(
            {"code": 200, "orders": [{"market_id": 146}]}, market_id=145
        )
    with pytest.raises(WTIReadOnlyError, match="rows_missing"):
        normalize_lighter_account({"accounts": []})
    with pytest.raises(WTIReadOnlyError, match="shape_unrecognized"):
        normalize_lighter_active_orders({"code": 200}, market_id=145)


def test_lighter_sdk_reader_uses_account_and_market_scoped_read_calls(monkeypatch):
    calls = []

    class FakeAccountApi:
        def __init__(self, client):
            assert client == "api-client"

        async def account(self, **kwargs):
            calls.append(("account", kwargs))
            return {"code": 200, "accounts": [{"positions": []}]}

    class FakeOrderApi:
        def __init__(self, client):
            assert client == "api-client"

        async def account_active_orders(self, **kwargs):
            calls.append(("active_orders", kwargs))
            return {"code": 200, "orders": []}

    import sys
    from types import SimpleNamespace

    monkeypatch.setitem(
        sys.modules,
        "lighter",
        SimpleNamespace(AccountApi=FakeAccountApi, OrderApi=FakeOrderApi),
    )

    class FakeSigner:
        api_client = "api-client"

        def create_auth_token_with_expiry(self, *, api_key_index):
            assert api_key_index == 3
            return "read-token", None

    reader = LighterSDKReadOnlyReader(
        client=FakeSigner(),
        account_index=12,
        api_key_index=3,
    )
    account, orders = asyncio_call(reader.read_account_and_active_orders(145))

    assert account["code"] == 200
    assert orders == {"code": 200, "orders": []}
    assert calls[0] == (
        "account",
        {"by": "index", "value": "12", "_request_timeout": 10.0},
    )
    assert calls[1][0] == "active_orders"
    assert calls[1][1]["market_id"] == 145
    assert calls[1][1]["account_index"] == 12
    assert calls[1][1]["auth"] == "read-token"


def test_account_adapter_returns_fresh_sanitized_snapshot_and_has_no_order_api():
    now = datetime.now(timezone.utc)
    var = FakeVariational(pending_pages=[[
        {"instrument": "P-ETH-USDC-3600", "id": "unrelated"}
    ]])
    lighter = FakeLighter(orders=[{"market_id": 145, "order_id": "also-private"}])
    adapter = WTIReadOnlyAccountAdapter(
        variational=var,
        lighter=lighter,
        market=_market(),
        clock=lambda: now,
    )

    snapshot = asyncio_call(adapter.read_account())

    assert snapshot["normalized"] == {
        "variational_qty": "0.25",
        "lighter_qty": "-0.25",
        "active_order_count": 1,
    }
    assert snapshot["active_orders"]["orders"][0]["venue"] == "lighter"
    assert snapshot["observed_at"] == now.isoformat()
    assert snapshot["variational_portfolio"]["result"]["balance"] == "100"
    assert lighter.market_ids == [145]
    assert not hasattr(adapter, "submit_order")
    assert not any(call["type"] == "VAR_API_ORDER" for call in var.calls)


def test_account_adapter_refuses_malformed_position_and_slow_snapshot():
    now = datetime.now(timezone.utc)

    class MalformedLighter:
        async def read_account_and_active_orders(self, _market_id):
            return {"accounts": []}, {"code": 200, "orders": []}

    adapter = WTIReadOnlyAccountAdapter(
        variational=FakeVariational(),
        lighter=MalformedLighter(),
        market=_market(),
        clock=lambda: now,
    )
    with pytest.raises(WTIReadOnlyError, match="account_rows_missing"):
        asyncio_call(adapter.read_account())

    moments = iter([now, now + timedelta(seconds=16)])
    slow_adapter = WTIReadOnlyAccountAdapter(
        variational=FakeVariational(),
        lighter=FakeLighter(),
        market=_market(),
        clock=lambda: next(moments),
        max_read_window_seconds=15,
    )
    with pytest.raises(WTIReadOnlyError, match="read_window_exceeded"):
        asyncio_call(slow_adapter.read_account())


def test_account_adapter_rejects_naive_clock_and_nonfinite_read_window():
    adapter = WTIReadOnlyAccountAdapter(
        variational=FakeVariational(),
        lighter=FakeLighter(),
        market=_market(),
        clock=lambda: datetime(2026, 1, 1),
    )
    with pytest.raises(WTIReadOnlyError, match="clock_timezone_missing"):
        asyncio_call(adapter.read_account())
    with pytest.raises(ValueError, match="must be positive"):
        WTIReadOnlyAccountAdapter(
            variational=FakeVariational(),
            lighter=FakeLighter(),
            market=_market(),
            max_read_window_seconds=float("nan"),
        )
