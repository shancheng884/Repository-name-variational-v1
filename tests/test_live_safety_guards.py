import asyncio
import time
from decimal import Decimal

from main import VariationalToLighterRuntime


def test_future_variational_quote_is_rejected_when_age_guard_is_enabled() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_max_var_quote_age_ms = 1500

    ok, age_seconds = runtime.live_inventory_var_quote_age_ok(
        {"quoteTimestamp": "2999-01-01T00:00:00Z"}
    )

    assert ok is False
    assert age_seconds is not None and age_seconds < 0


def test_rate_limit_detection_recurses_into_extension_result() -> None:
    assert VariationalToLighterRuntime.live_inventory_rate_limit_error(
        {
            "ok": False,
            "result": {"httpStatus": 429, "rateLimitResetMs": 2500},
        }
    )
    assert (
        VariationalToLighterRuntime.live_inventory_rate_limit_backoff_seconds(
            {"result": {"rateLimitResetMs": 2500}}
        )
        == 2.5
    )
    assert not VariationalToLighterRuntime.live_inventory_rate_limit_error(
        {"ok": True, "result": {"httpStatus": 200, "rateLimitResetMs": 2500}}
    )


def test_quote_reuse_rejects_cross_asset_quote() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_max_var_quote_age_ms = 0

    ok, reason, _ = runtime.live_inventory_basis_quote_reuse_validation(
        asset="ETH",
        quote_id="quote-btc",
        quote_qty=Decimal("0.01"),
        submitted_qty=Decimal("0.01"),
        quote_size_mode="exact_base_qty_v1",
        quote_timestamp="2026-01-01T00:00:00Z",
        quote_asset="BTC",
    )

    assert ok is False
    assert reason == "quote_asset_mismatch"


def test_quote_reuse_rejects_missing_asset_metadata() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_max_var_quote_age_ms = 0

    ok, reason, _ = runtime.live_inventory_basis_quote_reuse_validation(
        asset="ETH",
        quote_id="quote-without-asset",
        quote_qty=Decimal("0.01"),
        submitted_qty=Decimal("0.01"),
        quote_size_mode="exact_base_qty_v1",
        quote_timestamp="2026-01-01T00:00:00Z",
        quote_asset=None,
    )

    assert ok is False
    assert reason == "quote_asset_missing"


def test_background_quote_cache_is_consumed_once() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_basis_background_quote_cache = {
            "asset": "ETH",
            "requested_qty": "0.01",
            "quote": {
                "quoteId": "one-shot",
                "quoteTimestamp": "2026-01-01T00:00:00Z",
            },
            "quote_ms": "10",
            "completed_monotonic": time.monotonic(),
        }
        runtime.live_inventory_basis_max_var_quote_age_ms = 0
        runtime.live_inventory_basis_background_quote_task = None
        runtime.live_inventory_basis_background_quote_cooldown_until_monotonic = 0
        runtime.live_inventory_basis_background_quote_last_started_monotonic = (
            time.monotonic()
        )

        quote, _ = await runtime.get_live_inventory_basis_quote(
            asset="ETH",
            qty=Decimal("0.01"),
            priority="background",
        )

        assert quote is not None
        assert quote["quoteId"] == "one-shot"
        assert runtime.live_inventory_basis_background_quote_cache is None

    asyncio.run(run())


def test_trade_command_timeout_is_enforced_end_to_end(monkeypatch) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.variational_submit_transport = "api"
        runtime.variational_api_max_slippage = 0.005

        class Limiter:
            async def acquire(self, **_kwargs):
                return 0.0

            def snapshot(self):
                return {}

        runtime.live_inventory_variational_order_limiter = Limiter()

        async def slow_command(**_kwargs):
            await asyncio.sleep(1)
            return {"ok": True}

        runtime.send_variational_command = slow_command
        monkeypatch.setattr(
            "main.LIVE_INVENTORY_VARIATIONAL_TRADE_OPERATION_TIMEOUT_MS",
            10,
        )

        result = await runtime.send_variational_place_order(
            asset="ETH",
            side="BUY",
            amount="0.008",
            expected_min_btc_qty=None,
            confirm=True,
            reduce_only=False,
        )

        assert result["ok"] is False
        assert result["step"] == "command_timeout"
        assert result["error"] == "variational_trade_operation_timeout"
        assert result["timedOut"] is True
        assert result["trade_timeout"] is True
        assert result["execution_unknown"] is True
        assert runtime.live_inventory_trade_execution_unknown({"timedOut": True})

    asyncio.run(run())


def test_order_limiter_factory_uses_runtime_persistent_state_path(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = tmp_path

    limiter = runtime.live_inventory_order_limiter("variational")

    assert limiter._state_path == tmp_path / "variational_rate_limiter.json"


def test_rate_limiter_ignores_invalid_state_shapes(tmp_path) -> None:
    state_path = tmp_path / "rate_limiter.json"
    state_path.write_text("[]", encoding="utf-8")

    from tools.lib.rolling_rate_limiter import RollingWindowRateLimiter

    limiter = RollingWindowRateLimiter(
        normal_limit=2,
        hard_limit=3,
        state_path=state_path,
    )
    assert limiter.snapshot()["used"] == 0

    state_path.write_text('{"categories": []}', encoding="utf-8")
    limiter = RollingWindowRateLimiter(
        normal_limit=2,
        hard_limit=3,
        state_path=state_path,
    )
    assert limiter.snapshot()["used"] == 0

    state_path.write_text('{"timestamps": 1}', encoding="utf-8")
    limiter = RollingWindowRateLimiter(
        normal_limit=2,
        hard_limit=3,
        state_path=state_path,
    )
    assert limiter.snapshot()["used"] == 0
