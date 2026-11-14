from datetime import datetime, timezone
from decimal import Decimal as D
import asyncio

from tools.lib.venue_probe import EntropyProbeExecutor
from tools.market_capture.config import DEFAULT_ASSETS


NVDA = next(a for a in DEFAULT_ASSETS if a.name == "NVDA")


class FakeInfo:
    def __init__(self, positions=None, orders=None, fills=None):
        self.positions = positions or []
        self.orders = orders or []
        self.fills = fills or []
    def meta(self, dex):
        return {"universe": [{"name": "io:NVDA", "szDecimals": 3, "maxLeverage": 25, "onlyIsolated": False}]}
    def name_to_asset(self, coin):
        assert coin == "io:NVDA"
        return 200014
    def user_state(self, address, dex):
        return {"assetPositions": self.positions, "marginSummary": {"accountValue": "200"}}
    def open_orders(self, address, dex):
        return self.orders
    def all_mids(self, dex):
        return {"io:NVDA": "100"}
    def spot_user_state(self, address):
        return {"balances": [{"coin": "USDC", "total": "1055", "hold": "863"}],
                "tokenToAvailableAfterMaintenance": [[0, "192.27"]]}
    def user_fills_by_time(self, address, start_time):
        return self.fills


class FakeExchange:
    def __init__(self, result=None):
        self.calls = []
        self.result = result or {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 1}}]}}}
    def order(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


def make_executor(info, exchange, *, key="0x" + "1" * 64):
    return EntropyProbeExecutor(
        info_url="https://api.hyperliquid.xyz/info",
        account_address="0x" + "2" * 40,
        private_key=key,
        info=info,
        exchange=exchange,
    )


def test_entropy_preflight_checks_only_selected_asset_contract():
    ex = make_executor(FakeInfo(), FakeExchange())
    asyncio.run(ex.start())
    out = asyncio.run(ex.preflight(NVDA))
    assert out["ok"] is True
    assert out["contract"]["asset_index"] == 200014
    assert out["contract"]["sz_decimals"] == 3


def test_entropy_preflight_blocks_open_position_and_order():
    pos = [{"position": {"coin": "io:NVDA", "szi": "0.1"}}]
    ex = make_executor(FakeInfo(positions=pos), FakeExchange())
    asyncio.run(ex.start())
    out = asyncio.run(ex.preflight(NVDA))
    assert out == {"ok": False, "error": "entropy_position_not_flat", "qty": "0.1"}

    ex = make_executor(FakeInfo(orders=[{"coin": "io:NVDA", "oid": 3}]), FakeExchange())
    asyncio.run(ex.start())
    out = asyncio.run(ex.preflight(NVDA))
    assert out == {"ok": False, "error": "entropy_active_order", "count": 1}


def test_entropy_market_order_uses_cloid_and_aggregates_fills():
    fill = {
        "coin": "io:NVDA", "px": "100", "sz": "0.1", "side": "B",
        "time": 1793890449817, "fee": "0.01", "oid": 55,
        "cloid": "0x" + __import__("hashlib").sha256(b"probe-entry").hexdigest()[:32],
    }
    info = FakeInfo(fills=[fill])
    exchange = FakeExchange()
    ex = make_executor(info, exchange)
    asyncio.run(ex.start())
    quote = {"bid": "99.9", "ask": "100.1", "mid": "100"}
    out = asyncio.run(ex.market_order(
        asset=NVDA, side="buy", qty=D("0.1"), quote=quote,
        client_order_id="probe-entry",
    ))
    assert out["status"] == "filled"
    assert D(out["filled_qty"]) == D("0.1")
    assert D(out["avg_price"]) == D("100")
    assert D(out["fee_usd"]) == D("0.01")
    assert exchange.calls[0][1]["reduce_only"] is False
    assert exchange.calls[0][0][0] == "io:NVDA"
    assert exchange.calls[0][0][1] is True
    assert exchange.calls[0][0][3] == 100.08


def test_entropy_without_private_key_is_not_available():
    ex = EntropyProbeExecutor(info_url="https://x", account_address="0x" + "2" * 40, private_key="", info=FakeInfo(), exchange=FakeExchange())
    assert ex.available is False
    assert ex.reason == "entropy_private_key_missing"


def test_entropy_address_pasted_as_private_key_is_rejected_before_start():
    ex = EntropyProbeExecutor(
        info_url="https://x",
        account_address="0x" + "2" * 40,
        private_key="0x" + "3" * 40,
        info=FakeInfo(),
        exchange=FakeExchange(),
    )
    assert ex.available is False
    assert ex.reason == "entropy_private_key_must_be_32_bytes"
    assert asyncio.run(ex.preflight(NVDA))["error"] == "entropy_private_key_must_be_32_bytes"


def test_entropy_info_url_accepts_full_info_endpoint_form():
    ex = EntropyProbeExecutor(
        info_url="https://api.hyperliquid.xyz/info",
        account_address="0x" + "2" * 40,
        private_key="0x" + "1" * 64,
        info=FakeInfo(),
        exchange=FakeExchange(),
    )
    assert ex.info_url == "https://api.hyperliquid.xyz"
