import asyncio
from decimal import Decimal

from main import VariationalToLighterRuntime
from tools.lib.exit_targets import batch_exit_exceeds_risk_limit


class FakeRecord:
    def __init__(self, known=True, failure=None):
        self._known = known
        self.failure_reason = failure


def _runtime(*, hedge_known=(), unwind_ok=True, multiplier=Decimal("10000")):
    rt = object.__new__(VariationalToLighterRuntime)
    rt.events = []
    rt.removed = []
    rt._hedge_queue = list(hedge_known)
    rt._unwind_ok = unwind_ok

    def market_config(ticker):
        return (0, int(multiplier), 100, Decimal("0.002"), Decimal("10"))

    rt.get_lighter_market_config = market_config
    rt._lighter_size_multiplier_cache = {}

    async def place_lighter(**kwargs):
        return ("record", "payload")

    rt.place_lighter_order_from_plan = place_lighter

    async def timed(coro):
        return (await coro, "1.0")

    rt._timed_submit = timed

    def known(record):
        return rt._hedge_queue.pop(0) if rt._hedge_queue else False

    rt.lighter_exit_submission_known = known

    async def send_var(**kwargs):
        rt.events.append(("var_order", kwargs))
        return {"ok": True} if rt._unwind_ok else {"ok": False, "error": "rejected"}

    rt.send_variational_place_order = send_var

    async def append(event, payload):
        rt.events.append((event, payload))

    rt.append_live_inventory_log = append

    def remove(**kwargs):
        rt.removed.append(kwargs)

    rt.remove_pending_live_inventory_var_fill_match = remove
    return rt


def _recover(rt, **overrides):
    kwargs = dict(
        asset="ETH",
        lot={"lot_id": 72},
        direction="short_var_long_lighter",
        qty=Decimal("0.0088"),
        var_exit_price=Decimal("2291.41"),
        exit_side="BUY",
        var_amount="0.0088",
        failure_reason="basis_exit_lighter_submit_failed",
    )
    kwargs.update(overrides)
    return asyncio.run(rt.recover_live_inventory_exit_hedge(**kwargs))


def test_base_units_match_the_lighter_scale():
    rt = _runtime()
    assert rt.live_inventory_lighter_base_units("ETH", "0.1144") == 1144
    assert rt.live_inventory_lighter_base_units("ETH", "0.0088") == 88


def test_unknown_multiplier_is_not_treated_as_a_limit_breach():
    rt = _runtime()

    def broken(_ticker):
        raise RuntimeError("offline")

    rt.get_lighter_market_config = broken
    assert rt.live_inventory_lighter_base_units("ETH", "0.1144") is None


def test_batch_size_guard_flags_the_thirteen_lot_order():
    assert batch_exit_exceeds_risk_limit(1144, 1000) is True
    assert batch_exit_exceeds_risk_limit(88, 1000) is False
    assert batch_exit_exceeds_risk_limit(None, 1000) is False
    assert batch_exit_exceeds_risk_limit(1144, 0) is False


def test_a_failed_hedge_is_retried_until_it_succeeds():
    rt = _runtime(hedge_known=[False, True])

    result = _recover(rt)

    assert result["status"] == "hedged"
    assert [name for name, _ in rt.events if isinstance(name, str)] == [
        "live_inventory_exit_hedge_retry_ok"
    ]
    assert rt.removed == []


def test_a_hedge_that_never_succeeds_is_unwound_on_var():
    rt = _runtime(hedge_known=[False, False], unwind_ok=True)

    result = _recover(rt)

    assert result["status"] == "unwound"
    var_orders = [payload for name, payload in rt.events if name == "var_order"]
    assert var_orders and var_orders[0]["side"] == "SELL"
    assert var_orders[0]["reduce_only"] is False
    assert rt.removed and rt.removed[0]["lot_id"] == 72


def test_an_unwind_that_also_fails_goes_to_manual_review():
    rt = _runtime(hedge_known=[False, False], unwind_ok=False)

    result = _recover(rt)

    assert result["status"] == "failed"
    assert rt.removed == []


def test_a_buy_exit_unwinds_with_a_sell():
    rt = _runtime(hedge_known=[False, False])
    _recover(rt, exit_side="BUY")
    assert [p["side"] for n, p in rt.events if n == "var_order"] == ["SELL"]


def test_a_sell_exit_unwinds_with_a_buy():
    rt = _runtime(hedge_known=[False, False])
    _recover(rt, exit_side="SELL", direction="long_var_short_lighter")
    assert [p["side"] for n, p in rt.events if n == "var_order"] == ["BUY"]