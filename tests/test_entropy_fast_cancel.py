import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tools.lib.venue_probe import EntropyProbeExecutor


def executor():
    ex = EntropyProbeExecutor(info_url="https://api.hyperliquid.xyz/info",
                              account_address="0x" + "1" * 40, private_key="0x" + "2" * 64, dex="io")
    ex.exchange = SimpleNamespace(info=SimpleNamespace(name_to_asset=Mock(return_value=110001)),
                                  wallet=object(), vault_address=None, expires_after=None,
                                  base_url="https://api.hyperliquid.xyz", _post_action=Mock(),
                                  cancel=Mock(), cancel_by_cloid=Mock())
    return ex


@pytest.mark.parametrize("identity", [{"oid": 123}, {"client_order_id": "one-order"}])
def test_normal_cancel_signs_and_sends_fast_flag(identity, monkeypatch):
    sdk = pytest.importorskip("hyperliquid.utils.signing")
    sign = Mock(return_value="signature")
    monkeypatch.setattr(sdk, "sign_l1_action", sign)
    monkeypatch.setattr(sdk, "get_timestamp_ms", lambda: 456)
    ex = executor()
    asset = SimpleNamespace(symbol=lambda venue: "io:NVDA")
    asyncio.run(ex.cancel_order(asset, trigger_order=False, **identity))
    action, signature, nonce = ex.exchange._post_action.call_args.args
    assert ex.exchange._post_action.call_count == 1
    if "oid" in identity:
        assert action == {"type": "cancel", "cancels": [{"a": 110001, "o": 123}], "f": True}
    else:
        assert action == {"type": "cancelByCloid", "cancels": [
            {"asset": 110001, "cloid": ex._cloid("one-order").to_raw()}], "f": True}
    assert signature == "signature" and nonce == 456
    sign.assert_called_once_with(ex.exchange.wallet, action, None, 456, None, True)


def test_trigger_cancel_uses_original_sdk_and_requires_one_id():
    ex = executor()
    asset = SimpleNamespace(symbol=lambda venue: "io:NVDA")
    asyncio.run(ex.cancel_order(asset, trigger_order=True, oid=123))
    ex.exchange.cancel.assert_called_once_with("io:NVDA", 123)
    ex.exchange._post_action.assert_not_called()
    with pytest.raises(ValueError):
        asyncio.run(ex.cancel_order(asset, trigger_order=False, oid=123, client_order_id="one-order"))
    ex.exchange._post_action.assert_not_called()
