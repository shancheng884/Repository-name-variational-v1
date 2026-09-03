import asyncio

from tools.lib.rolling_rate_limiter import RollingWindowRateLimiter


def test_rolling_window_reserves_capacity_for_urgent_orders() -> None:
    async def run() -> None:
        now = [100.0]
        limiter = RollingWindowRateLimiter(
            normal_limit=2,
            hard_limit=3,
            window_seconds=60,
            clock=lambda: now[0],
        )
        assert await limiter.acquire() == 0
        assert await limiter.acquire() == 0
        assert limiter.snapshot()["used"] == 2
        assert await limiter.acquire(urgent=True) == 0
        assert limiter.snapshot()["used"] == 3

    asyncio.run(run())


def test_rolling_window_try_acquire_never_waits_when_background_budget_is_full() -> None:
    async def run() -> None:
        now = [100.0]
        limiter = RollingWindowRateLimiter(
            normal_limit=3,
            hard_limit=4,
            window_seconds=60,
            clock=lambda: now[0],
        )
        acquired, _ = await limiter.try_acquire(limit=2)
        assert acquired is True
        acquired, _ = await limiter.try_acquire(limit=2)
        assert acquired is True
        acquired, waited = await limiter.try_acquire(limit=2)
        assert acquired is False
        assert waited == 0
        assert limiter.snapshot()["used"] == 2

    asyncio.run(run())


def test_rolling_window_category_budget_shares_total_capacity_with_trade_work() -> None:
    async def run() -> None:
        now = [100.0]
        limiter = RollingWindowRateLimiter(
            normal_limit=3,
            hard_limit=4,
            window_seconds=60,
            clock=lambda: now[0],
        )
        acquired, _ = await limiter.try_acquire(
            limit=2,
            category="background_quote",
        )
        assert acquired is True
        acquired, _ = await limiter.try_acquire(
            limit=2,
            category="background_quote",
        )
        assert acquired is True
        acquired, waited = await limiter.try_acquire(
            limit=2,
            category="background_quote",
        )
        assert acquired is False
        assert waited == 0

        # The category cap does not incorrectly turn into a shared cap.
        assert await limiter.acquire() == 0
        assert limiter.snapshot()["used"] == 3
        assert await limiter.acquire(urgent=True) == 0
        assert limiter.snapshot()["used"] == 4

    asyncio.run(run())


def test_rolling_window_rejects_invalid_limits() -> None:
    try:
        RollingWindowRateLimiter(normal_limit=4, hard_limit=3)
    except ValueError as exc:
        assert "invalid" in str(exc)
    else:
        raise AssertionError("invalid limits must fail")


def test_rolling_window_cost_is_atomic_and_waits_for_all_tokens() -> None:
    async def run() -> None:
        now = [100.0]
        limiter = RollingWindowRateLimiter(
            normal_limit=4,
            hard_limit=5,
            window_seconds=60,
            clock=lambda: now[0],
        )
        assert await limiter.acquire(cost=2) == 0
        assert limiter.snapshot()["used"] == 2
        acquired, _ = await limiter.try_acquire(cost=3, urgent=True)
        assert acquired is True
        assert limiter.snapshot()["used"] == 5
        acquired, _ = await limiter.try_acquire(cost=1, urgent=True)
        assert acquired is False

    asyncio.run(run())


def test_rolling_window_cost_respects_category_budget() -> None:
    async def run() -> None:
        now = [100.0]
        limiter = RollingWindowRateLimiter(
            normal_limit=6,
            hard_limit=6,
            window_seconds=60,
            clock=lambda: now[0],
        )
        try:
            await limiter.try_acquire(
                cost=2,
                limit=1,
                category="background_quote",
            )
        except ValueError as exc:
            assert "at least cost" in str(exc)
        else:
            raise AssertionError("category budget below cost must fail")
        acquired, _ = await limiter.try_acquire(
            cost=2,
            limit=4,
            category="background_quote",
        )
        assert acquired is True
        acquired, _ = await limiter.try_acquire(
            cost=2,
            limit=4,
            category="background_quote",
        )
        assert acquired is True
        acquired, _ = await limiter.try_acquire(
            cost=1,
            limit=4,
            category="background_quote",
        )
        assert acquired is False

    asyncio.run(run())


def test_rolling_window_state_survives_process_restart(tmp_path) -> None:
    async def run() -> None:
        now = [100.0]
        wall_now = [1_000.0]
        state_path = tmp_path / "variational_rate_limiter.json"
        first = RollingWindowRateLimiter(
            normal_limit=2,
            hard_limit=3,
            window_seconds=60,
            clock=lambda: now[0],
            wall_clock=lambda: wall_now[0],
            state_path=state_path,
        )
        assert await first.acquire() == 0

        restarted = RollingWindowRateLimiter(
            normal_limit=2,
            hard_limit=3,
            window_seconds=60,
            clock=lambda: now[0],
            wall_clock=lambda: wall_now[0],
            state_path=state_path,
        )
        assert restarted.snapshot()["used"] == 1
        acquired, _ = await restarted.try_acquire(limit=1)
        assert acquired is True
        acquired, _ = await restarted.try_acquire(limit=1)
        assert acquired is False
