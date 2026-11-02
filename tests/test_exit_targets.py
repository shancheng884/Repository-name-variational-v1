import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from main import VariationalToLighterRuntime
from tools.lib.exit_targets import (
    DEFAULT_PORTFOLIO_EXIT_TARGET_BPS,
    portfolio_exit_target_bps,
)


def _runtime(*, ttl=600.0, lot_ids=("72", "84"), locked_at=None, extra_context=None):
    runtime = object.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_v4_exit_lock_ttl_seconds = ttl
    runtime.live_inventory_v4_portfolio_exit_lot_ids = set(lot_ids)
    runtime.live_inventory_v4_portfolio_exit_confirmations = deque(maxlen=3)
    context = {"locked_at": (locked_at or datetime.now(timezone.utc)).isoformat()}
    context.update(extra_context or {})
    runtime.live_inventory_v4_portfolio_exit_context = context
    runtime.events = []

    async def persist(reason=None):
        runtime.events.append(("persist", reason))

    async def append(event, payload):
        runtime.events.append((event, payload))

    runtime.persist_live_inventory_memory = persist
    runtime.append_live_inventory_log = append
    return runtime


def _expire(runtime, lot_ids):
    return asyncio.run(
        runtime.live_inventory_expire_exit_lock(set(lot_ids), state_payload={"asset": "ETH"})
    )


def test_portfolio_target_defaults_to_three_bps():
    assert DEFAULT_PORTFOLIO_EXIT_TARGET_BPS == Decimal("3.00")
    assert portfolio_exit_target_bps(None) == Decimal("3.00")


def test_portfolio_target_honours_a_configured_value():
    assert portfolio_exit_target_bps("2.5") == Decimal("2.5")
    assert portfolio_exit_target_bps(Decimal("4")) == Decimal("4")


def test_portfolio_target_falls_back_on_junk_or_negative():
    assert portfolio_exit_target_bps("abc") == DEFAULT_PORTFOLIO_EXIT_TARGET_BPS
    assert portfolio_exit_target_bps(-1) == DEFAULT_PORTFOLIO_EXIT_TARGET_BPS
    assert portfolio_exit_target_bps(float("nan")) == DEFAULT_PORTFOLIO_EXIT_TARGET_BPS


def test_a_lock_older_than_the_ttl_is_released_and_logged():
    runtime = _runtime(ttl=600, locked_at=datetime.now(timezone.utc) - timedelta(seconds=3600))

    remaining = _expire(runtime, ["72", "84"])

    assert remaining == set()
    assert runtime.live_inventory_v4_portfolio_exit_lot_ids == set()
    assert runtime.live_inventory_v4_portfolio_exit_context == {}
    events = [event for event in runtime.events if isinstance(event[0], str)]
    assert any(name == "live_inventory_v4_exit_lock_expired" for name, _ in events)
    payload = next(payload for name, payload in runtime.events if name == "live_inventory_v4_exit_lock_expired")
    assert payload["reason"] == "exit_lock_ttl_expired"
    assert payload["lot_ids"] == ["72", "84"]
    assert payload["ttl_seconds"] == 600


def test_a_lock_inside_the_ttl_is_kept():
    runtime = _runtime(ttl=600, locked_at=datetime.now(timezone.utc) - timedelta(seconds=60))

    remaining = _expire(runtime, ["72", "84"])

    assert remaining == {"72", "84"}
    assert runtime.live_inventory_v4_portfolio_exit_lot_ids == {"72", "84"}
    assert runtime.events == []


def test_an_operator_requested_exit_never_expires():
    runtime = _runtime(
        ttl=600,
        locked_at=datetime.now(timezone.utc) - timedelta(days=1),
        extra_context={"operator_requested_exit": True},
    )

    remaining = _expire(runtime, ["72", "84"])

    assert remaining == {"72", "84"}
    assert runtime.events == []


def test_an_account_risk_exit_never_expires():
    runtime = _runtime(
        ttl=600,
        locked_at=datetime.now(timezone.utc) - timedelta(days=1),
        extra_context={"account_risk_exit": True},
    )

    assert _expire(runtime, ["72"]) == {"72"}


def test_a_lock_without_a_timestamp_is_left_alone():
    runtime = _runtime(ttl=600)
    runtime.live_inventory_v4_portfolio_exit_context = {}

    assert _expire(runtime, ["72"]) == {"72"}
    assert runtime.events == []


def test_no_lock_is_a_no_op():
    runtime = _runtime(ttl=600)

    assert _expire(runtime, []) == set()
    assert runtime.events == []

def test_lot_target_defaults_to_the_batch_target():
    from tools.lib.exit_targets import DEFAULT_LOT_EXIT_TARGET_BPS, lot_exit_target_bps

    assert DEFAULT_LOT_EXIT_TARGET_BPS == Decimal("3.00")
    assert lot_exit_target_bps(None) == DEFAULT_PORTFOLIO_EXIT_TARGET_BPS
    assert lot_exit_target_bps("2.4") == Decimal("2.4")
    assert lot_exit_target_bps(-1) == DEFAULT_LOT_EXIT_TARGET_BPS


def test_the_adaptive_gate_is_capped_at_the_target():
    from tools.lib.exit_targets import cap_exit_threshold

    assert cap_exit_threshold(Decimal("4.50"), Decimal("3.00")) == Decimal("3.00")


def test_time_decay_below_the_target_is_preserved():
    from tools.lib.exit_targets import cap_exit_threshold

    assert cap_exit_threshold(Decimal("1.80"), Decimal("3.00")) == Decimal("1.80")


def test_a_missing_target_leaves_the_gate_untouched():
    from tools.lib.exit_targets import cap_exit_threshold

    assert cap_exit_threshold(Decimal("4.50"), None) == Decimal("4.50")
    assert cap_exit_threshold(Decimal("4.50"), Decimal("0")) == Decimal("4.50")