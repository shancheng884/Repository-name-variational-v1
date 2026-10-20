import asyncio
import logging
from types import SimpleNamespace

import pytest

from main import VariationalToLighterRuntime
from variational.listener import VariationalMonitor


def _runtime(allowed_assets: set[str]) -> VariationalToLighterRuntime:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.runtime = SimpleNamespace(monitor=VariationalMonitor())
    runtime.live_allowed_assets = allowed_assets
    runtime.variational_ticker = "ETH"
    runtime.ticker = "ETH"
    runtime.logger = logging.getLogger("test_live_asset_isolation")
    return runtime


def _add_indicative_quote(monitor: VariationalMonitor, asset: str) -> None:
    assert monitor._update_quote(
        {
            "instrument": {"underlying": asset},
            "bid": "100.0",
            "ask": "100.1",
        }
    )


def _add_reference_quote(monitor: VariationalMonitor, asset: str) -> None:
    assert monitor._update_quote(
        {
            "channel": f"instrument_price:P-{asset}-USDC-3600",
            "price": "100.05",
        }
    )


def test_single_asset_runtime_ignores_other_assets_current_quote() -> None:
    runtime = _runtime({"ETH"})
    _add_indicative_quote(runtime.runtime.monitor, "ETH")
    _add_indicative_quote(runtime.runtime.monitor, "USOILP")

    assert runtime.runtime.monitor.current_quote_asset == "USOILP"
    assert asyncio.run(runtime.detect_current_variational_asset()) == "ETH"


def test_single_asset_runtime_waits_for_its_own_quote() -> None:
    runtime = _runtime({"ETH"})
    _add_indicative_quote(runtime.runtime.monitor, "USOILP")
    _add_reference_quote(runtime.runtime.monitor, "USOILP")

    assert asyncio.run(runtime.detect_current_variational_asset()) is None


def test_requested_eth_quotes_never_fall_back_to_wti() -> None:
    runtime = _runtime({"ETH"})
    _add_indicative_quote(runtime.runtime.monitor, "USOILP")
    _add_reference_quote(runtime.runtime.monitor, "USOILP")

    assert asyncio.run(runtime.get_variational_quote("ETH")) is None
    assert asyncio.run(runtime.get_variational_reference_quote("ETH")) is None


def test_non_allowlisted_quote_cannot_switch_fixed_runtime_asset() -> None:
    runtime = _runtime({"ETH"})
    runtime.accepted_assets = {"ETH"}
    runtime._asset_switch_lock = asyncio.Lock()
    runtime.requires_lighter_market_data = lambda: True

    def unexpected_market_lookup(_ticker: str):
        raise AssertionError("an unallowlisted asset must not query Lighter")

    runtime.get_lighter_market_config = unexpected_market_lookup

    asyncio.run(runtime.activate_asset("USOILP", reason="test_quote"))

    assert runtime.variational_ticker == "ETH"
    assert runtime.ticker == "ETH"
    assert runtime.accepted_assets == {"ETH"}


def test_failed_market_lookup_does_not_partially_switch_runtime() -> None:
    runtime = _runtime({"ETH", "BTC"})
    runtime.accepted_assets = {"ETH"}
    runtime._asset_switch_lock = asyncio.Lock()
    runtime.requires_lighter_market_data = lambda: True

    def failed_market_lookup(_ticker: str):
        raise RuntimeError("market unavailable")

    runtime.get_lighter_market_config = failed_market_lookup

    with pytest.raises(RuntimeError, match="market unavailable"):
        asyncio.run(runtime.activate_asset("BTC", reason="test_switch"))

    assert runtime.variational_ticker == "ETH"
    assert runtime.ticker == "ETH"
    assert runtime.accepted_assets == {"ETH"}
