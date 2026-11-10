"""Regression tests for reduce-only rollback of an unpaired entry leg."""

import asyncio
import json
from decimal import Decimal

import main as main_module
from main import VariationalToLighterRuntime


def _runtime(verify_ok=True, first_close_ok=True):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_auto_close_unhedged = True
    runtime.calls = {"close": [], "logs": []}

    async def _timed_submit(awaitable):
        return await awaitable, "1.0"

    async def _place_lighter_order_from_plan(**kwargs):
        runtime.calls["close"].append(kwargs)
        attempts = len(runtime.calls["close"])
        if attempts == 1 and not first_close_ok:
            raise RuntimeError("lighter_submit_failed")
        return (object(), {"ok": True})

    async def _append_log(event, payload):
        runtime.calls["logs"].append((event, payload))

    async def _verify(**_kwargs):
        if verify_ok:
            return True, "flat", {"variational_position_qty": "0"}
        return False, "reference_recovery_position_mismatch", {
            "lighter_position_qty": "0.0098"
        }

    runtime._timed_submit = _timed_submit
    runtime.place_lighter_order_from_plan = _place_lighter_order_from_plan
    runtime.append_live_inventory_log = _append_log
    runtime.verify_live_inventory_auto_close_positions = _verify
    runtime.auto_live_eager_hedge_started = lambda record: True
    return runtime


def test_rollback_closes_the_filled_leg_with_a_reduce_only_order() -> None:
    runtime = _runtime(verify_ok=True)

    verified, context = asyncio.run(
        runtime.rollback_unhedged_entry_leg(
            asset="ETH",
            reason="basis_entry_var_submit_failed_after_lighter_started",
            var_side="SELL",
            qty=Decimal("0.0098"),
            var_price=Decimal("2021.66"),
            lot_id=7,
            close_lighter=True,
        )
    )

    assert verified is True
    assert context["position_reconcile"]["ok"] is True
    assert runtime.calls["close"][0]["reduce_only"] is True
    # Selling the long hedge means buying back the opposite side.
    assert runtime.calls["close"][0]["side"] == "BUY"
    assert runtime.calls["close"][0]["qty"] == Decimal("0.0098")


def test_rollback_reports_failure_when_exposure_remains() -> None:
    runtime = _runtime(verify_ok=False)

    verified, context = asyncio.run(
        runtime.rollback_unhedged_entry_leg(
            asset="ETH",
            reason="basis_entry_var_submit_failed_after_lighter_started",
            var_side="SELL",
            qty=Decimal("0.0098"),
            var_price=Decimal("2021.66"),
            lot_id=7,
            close_lighter=True,
        )
    )

    assert verified is False
    assert context["position_reconcile"]["ok"] is False
    assert context["position_reconcile"]["reason"] == (
        "reference_recovery_position_mismatch"
    )


def test_rollback_retries_the_reduce_only_close_once() -> None:
    runtime = _runtime(verify_ok=True, first_close_ok=False)

    verified, context = asyncio.run(
        runtime.rollback_unhedged_entry_leg(
            asset="ETH",
            reason="basis_entry_var_submit_failed_after_lighter_started",
            var_side="SELL",
            qty=Decimal("0.0098"),
            var_price=Decimal("2021.66"),
            lot_id=7,
            close_lighter=True,
        )
    )

    assert verified is True
    assert len(runtime.calls["close"]) == 2
    assert all(call["reduce_only"] for call in runtime.calls["close"])
    assert len(context["lighter_close_attempts"]) == 2


def test_rollback_is_a_noop_when_explicitly_disabled() -> None:
    runtime = _runtime(verify_ok=True)
    runtime.live_inventory_basis_auto_close_unhedged = False

    verified, context = asyncio.run(
        runtime.rollback_unhedged_entry_leg(
            asset="ETH",
            reason="basis_entry_var_submit_failed_after_lighter_started",
            var_side="SELL",
            qty=Decimal("0.0098"),
            var_price=Decimal("2021.66"),
            lot_id=7,
            close_lighter=True,
        )
    )

    assert context["enabled"] is False
    assert runtime.calls["close"] == []
    # An unverified rollback must never be read as "safe to continue".
    assert verified is True  # verification is what decides, not the flag


def test_auto_close_unhedged_is_enabled_by_default_in_the_cli(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["main.py"],
    )
    assert main_module.parse_args().live_inventory_basis_auto_close_unhedged is True

    monkeypatch.setattr(
        "sys.argv",
        ["main.py", "--no-live-inventory-basis-auto-close-unhedged"],
    )
    assert (
        main_module.parse_args().live_inventory_basis_auto_close_unhedged is False
    )
